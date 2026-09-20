"""``AdaptiveWateringSaga``: react to telemetry without a central coordinator.

Choreography: there is no saga state to keep, because the reaction is entirely a
function of one event plus the current schedule. The consumer group listens to
``telemetry.events`` and does two things:

* ``TelemetryReceived`` — if the soil is drier than the domain's low threshold and
  the next watering is still more than :data:`WATERING_HORIZON` away, pull it
  forward to *now* (``WateringRescheduled``). Once pulled forward the schedule is
  due, so repeated readings stop matching the condition: a 10-second telemetry
  stream cannot turn into an event storm.
* ``SoilMoistureHigh`` — raise one ``soil_moisture_high`` notification, and only
  while the household has none unread for that plant.

The thresholds come from the domain: the low-moisture boundary is exactly the one
:class:`~plantkeeper.domain.telemetry.sensor.Sensor` uses, so telemetry and its
consumer cannot disagree about what "dry" means, and :data:`WATERING_HORIZON` is
the only rule this module adds.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import ClassVar

from cqrs.dispatcher.saga import SagaDispatcher
from pydantic import JsonValue

from plantkeeper.application.ports.clock import Clock
from plantkeeper.application.ports.unit_of_work import UnitOfWork
from plantkeeper.application.sagas.consumer import Consumer
from plantkeeper.domain.base import DomainEvent
from plantkeeper.domain.notifications.notification import Notification
from plantkeeper.domain.notifications.values import NotificationType
from plantkeeper.domain.telemetry.events import SoilMoistureHigh, TelemetryReceived
from plantkeeper.domain.telemetry.sensor import MOISTURE_LOW_THRESHOLD

logger = logging.getLogger(__name__)

WATERING_HORIZON: timedelta = timedelta(days=2)
"""Only a watering this far away is worth pulling forward."""

RESCHEDULE_REASON = "telemetry: soil moisture below threshold"
"""Stored on ``WateringRescheduled`` so an operator can tell why it moved."""


class AdaptiveWateringSaga(Consumer):
    """The telemetry reaction, at choreography: no state, only decisions."""

    name = "adaptive-watering"
    handled_types: ClassVar[tuple[type[DomainEvent], ...]] = (TelemetryReceived, SoilMoistureHigh)

    def __init__(self, unit_of_work: UnitOfWork, clock: Clock) -> None:
        super().__init__(unit_of_work)
        self._clock = clock

    async def handle(self, event: DomainEvent, dispatcher: SagaDispatcher) -> None:
        """Route the delivery to its reaction.

        ``dispatcher`` is unused: choreography has no process manager to start.
        """
        if isinstance(event, TelemetryReceived):
            await self._on_telemetry_received(event)
        elif isinstance(event, SoilMoistureHigh):
            await self._on_soil_moisture_high(event)

    async def _on_telemetry_received(self, event: TelemetryReceived) -> None:
        """Pull a far-away watering forward when the soil is dry."""
        now = self._clock.now()
        schedule = await self.unit_of_work.care_schedules.get_for_update(event.plant_id)
        if schedule is None:
            # The plant has no schedule yet: the onboarding saga has not finished,
            # or the plant was never onboarded. Nothing to adapt.
            logger.info("no care schedule for plant %s; ignoring telemetry", event.plant_id)
            return
        if event.moisture.value >= MOISTURE_LOW_THRESHOLD:
            return
        if schedule.next_watering_at - now <= WATERING_HORIZON:
            return
        schedule.reschedule(
            next_watering_at=now,
            now=now,
            expected_version=schedule.version,
            reason=RESCHEDULE_REASON,
        )
        await self.unit_of_work.care_schedules.save(schedule)
        logger.info(
            "pulled watering of plant %s forward: moisture %.1f%%",
            event.plant_id,
            event.moisture.value,
        )

    async def _on_soil_moisture_high(self, event: SoilMoistureHigh) -> None:
        """Notify the household about overwatering, at most once per plant."""
        plant = await self.unit_of_work.plants.get(event.plant_id)
        if plant is None:
            logger.info("overwatering for unknown plant %s; ignoring", event.plant_id)
            return
        pending = await self.unit_of_work.notifications.list_pending(plant.household_id)
        if any(
            notification.notification_type is NotificationType.SOIL_MOISTURE_HIGH
            and notification.payload.get("plant_id") == str(event.plant_id)
            for notification in pending
        ):
            return
        payload: dict[str, JsonValue] = {
            "plant_id": str(event.plant_id),
            "moisture": event.moisture.value,
            "threshold": event.threshold,
        }
        await self.unit_of_work.notifications.add(
            Notification.create(
                household_id=plant.household_id,
                notification_type=NotificationType.SOIL_MOISTURE_HIGH,
                now=self._clock.now(),
                payload=payload,
            )
        )
