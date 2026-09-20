"""The Notifications consumer: care and telemetry facts become reminders.

The Notifications context owns one thing — a message waiting for the household —
and this consumer is its only *general* producer: it turns the care and telemetry
events the roadmap names (Phase 8.3) into
:class:`~plantkeeper.domain.notifications.notification.Notification` rows in the
same transaction as its idempotency claim. The onboarding saga still creates its
``plant_onboarded`` reminder and ``AdaptiveWateringSaga`` its
``soil_moisture_high`` one — one producer per notification type, so no type has
two writers that could disagree.

Three rules keep the stream from becoming noise:

* **A derived identifier.** The notification's id is a ``uuid5`` of the event it
  came from, and the consumer skips an identifier it already stored. A rebuilt
  consumer group — a reset offset plus a lost ledger — therefore re-creates
  nothing, exactly like ``JournalEntryConsumer``.
* **One unread per plant for telemetry.** ``SoilMoistureLow`` and
  ``TemperatureAnomaly`` fire on every reading that crosses a threshold, and a
  sensor reports every ten seconds. While the household has an unread
  notification of that type for that plant, further readings add nothing: the
  reminder is already on screen. The rule is the one ``AdaptiveWateringSaga``
  already applies to overwatering.
* **No plant, no notification.** An event for a plant the Garden context does not
  know is logged and dropped; there is no household to address.

A ``soil_moisture_low`` notification is deliberately not suppressed by
``AdaptiveWateringSaga`` pulling the watering forward: the schedule moving is a
decision, and the household still learns that its soil got dry.
"""

from __future__ import annotations

import logging
from typing import ClassVar, Final
from uuid import NAMESPACE_URL, uuid5

from cqrs.dispatcher.saga import SagaDispatcher
from pydantic import JsonValue

from plantkeeper.application.ports.clock import Clock
from plantkeeper.application.ports.unit_of_work import UnitOfWork
from plantkeeper.application.sagas.consumer import Consumer
from plantkeeper.domain.base import DomainEvent
from plantkeeper.domain.care.events import CareMissed, WateringDue, WateringRescheduled
from plantkeeper.domain.identifiers import HouseholdId, NotificationId, PlantId
from plantkeeper.domain.notifications.notification import Notification
from plantkeeper.domain.notifications.values import NotificationType
from plantkeeper.domain.telemetry.events import SoilMoistureLow, TemperatureAnomaly

logger = logging.getLogger(__name__)

NotificationTrigger = (
    WateringDue | WateringRescheduled | CareMissed | SoilMoistureLow | TemperatureAnomaly
)
"""The events a notification is made of."""

UNREAD_GUARDED: Final[frozenset[NotificationType]] = frozenset(
    {NotificationType.SOIL_MOISTURE_LOW, NotificationType.TEMPERATURE_ANOMALY}
)
"""Types that may exist at most once *unread* per plant.

These are the types a fast stream can repeat: one notification per reading would
be a storm, and the household only needs to be told once that something is wrong.
"""


def notification_id_for(event: DomainEvent) -> NotificationId:
    """Derive the notification identifier of one trigger event.

    The consumer derives it rather than minting one, so the same event always
    names the same notification and a rebuilt consumer group can recognise what it
    has already stored.
    """
    return NotificationId(
        uuid5(NAMESPACE_URL, f"plantkeeper:notification:{type(event).__name__}:{event.event_id}")
    )


class NotificationConsumer(Consumer):
    """Turn care and telemetry events into the household's reminders."""

    name = "notifications"
    handled_types: ClassVar[tuple[type[DomainEvent], ...]] = (
        WateringDue,
        WateringRescheduled,
        CareMissed,
        SoilMoistureLow,
        TemperatureAnomaly,
    )

    def __init__(self, unit_of_work: UnitOfWork, clock: Clock) -> None:
        super().__init__(unit_of_work)
        self._clock = clock

    async def handle(self, event: DomainEvent, dispatcher: SagaDispatcher) -> None:
        """Route the delivery to the reminder it becomes.

        ``dispatcher`` is unused: this consumer reacts on its own and starts no
        process manager.
        """
        if isinstance(event, WateringDue):
            await self._store(
                event,
                NotificationType.WATERING_DUE,
                {"plant_id": str(event.plant_id), "due_at": event.due_at.isoformat()},
            )
        elif isinstance(event, WateringRescheduled):
            await self._store(
                event,
                NotificationType.WATERING_RESCHEDULED,
                {
                    "plant_id": str(event.plant_id),
                    "previous_next_watering_at": event.previous_next_watering_at.isoformat(),
                    "next_watering_at": event.next_watering_at.isoformat(),
                    "reason": event.reason,
                },
            )
        elif isinstance(event, CareMissed):
            await self._store(
                event,
                NotificationType.CARE_MISSED,
                {
                    "plant_id": str(event.plant_id),
                    "next_watering_at": event.next_watering_at.isoformat(),
                },
            )
        elif isinstance(event, SoilMoistureLow):
            await self._store(
                event,
                NotificationType.SOIL_MOISTURE_LOW,
                {
                    "plant_id": str(event.plant_id),
                    "moisture": event.moisture.value,
                    "threshold": event.threshold,
                },
            )
        elif isinstance(event, TemperatureAnomaly):
            await self._store(
                event,
                NotificationType.TEMPERATURE_ANOMALY,
                {
                    "plant_id": str(event.plant_id),
                    "temperature": event.temperature.value,
                    "low_threshold": event.low_threshold,
                    "high_threshold": event.high_threshold,
                },
            )

    async def _store(
        self,
        event: NotificationTrigger,
        notification_type: NotificationType,
        payload: dict[str, JsonValue],
    ) -> None:
        """Store the notification ``event`` calls for, or explain why it does not."""
        notification_id = notification_id_for(event)
        plant = await self.unit_of_work.plants.get(event.plant_id)
        if plant is None:
            logger.info(
                "%s for unknown plant %s; no notification", type(event).__name__, event.plant_id
            )
            return
        if await self.unit_of_work.notifications.get(notification_id) is not None:
            logger.debug("notification %s already exists; nothing created", notification_id)
            return
        if notification_type in UNREAD_GUARDED and await self._has_unread(
            plant.household_id, notification_type, event.plant_id
        ):
            return
        await self.unit_of_work.notifications.add(
            Notification.create(
                household_id=plant.household_id,
                notification_type=notification_type,
                now=self._clock.now(),
                payload=payload,
                notification_id=notification_id,
            )
        )

    async def _has_unread(
        self,
        household_id: HouseholdId,
        notification_type: NotificationType,
        plant_id: PlantId,
    ) -> bool:
        """Whether the household has an unread notification of this type and plant."""
        pending = await self.unit_of_work.notifications.list_pending(household_id)
        return any(
            notification.notification_type is notification_type
            and notification.payload.get("plant_id") == str(plant_id)
            for notification in pending
        )
