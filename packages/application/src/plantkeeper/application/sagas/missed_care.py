"""``MissedCareSaga``: the 24-hour grace period between "due" and "missed".

Choreography, and the only one of the four that needs a timer. ``WateringDue`` is
a fact with no deadline of its own, so the consumer opens a *window* — a row in
``write_care.missed_care_windows`` — and the grace period is that row's
``grace_deadline``:

* ``WateringDue`` — open (or leave open) a pending window for the plant;
* ``WateringCompleted`` — close the window as satisfied, as long as the watering
  happened inside the grace period. A late watering does not erase a miss that
  already happened;
* :meth:`MissedCareSaga.escalate_overdue` — the scheduled half. In one
  transaction it marks newly due schedules (``WateringDue``) so a window exists
  for them, and escalates every expired pending window: the schedule moves to the
  next interval (``CareMissed``), the household gets a ``care_missed``
  notification, and the window becomes ``missed``.

Splitting the timer from the reactions is what makes the saga testable without a
clock library: the scheduler is called directly with a ``now`` the test controls.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import ClassVar

from cqrs.dispatcher.saga import SagaDispatcher
from pydantic import JsonValue

from plantkeeper.application.ports.clock import Clock
from plantkeeper.application.ports.sagas import MissedCareState, MissedCareWindow
from plantkeeper.application.ports.unit_of_work import UnitOfWork
from plantkeeper.application.sagas.consumer import Consumer
from plantkeeper.domain.base import DomainEvent
from plantkeeper.domain.care.events import WateringCompleted, WateringDue
from plantkeeper.domain.notifications.notification import Notification
from plantkeeper.domain.notifications.values import NotificationType

logger = logging.getLogger(__name__)

GRACE_PERIOD: timedelta = timedelta(hours=24)
"""How long after the due moment a watering still counts as on time."""

TICK_BATCH_SIZE = 200
"""Bounds one scheduler tick, so a backlog cannot stall the worker forever."""


class MissedCareSaga(Consumer):
    """The grace-window state machine and its escalation tick."""

    name = "missed-care"
    handled_types: ClassVar[tuple[type[DomainEvent], ...]] = (WateringDue, WateringCompleted)

    def __init__(self, unit_of_work: UnitOfWork, clock: Clock) -> None:
        super().__init__(unit_of_work)
        self._clock = clock

    async def handle(self, event: DomainEvent, dispatcher: SagaDispatcher) -> None:
        """Open or close the plant's window.

        ``dispatcher`` is unused: choreography has no process manager to start.
        """
        if isinstance(event, WateringDue):
            await self._on_watering_due(event)
        elif isinstance(event, WateringCompleted):
            await self._on_watering_completed(event)

    async def _on_watering_due(self, event: WateringDue) -> None:
        """Open the grace window the escalation will later act on."""
        plant = await self.unit_of_work.plants.get(event.plant_id)
        if plant is None:
            logger.info("watering due for unknown plant %s; ignoring", event.plant_id)
            return
        existing = await self.unit_of_work.missed_care_windows.get(event.plant_id)
        if (
            existing is not None
            and existing.state is MissedCareState.PENDING
            and existing.due_at == event.due_at
        ):
            return
        await self.unit_of_work.missed_care_windows.save(
            MissedCareWindow(
                plant_id=event.plant_id,
                household_id=plant.household_id,
                due_at=event.due_at,
                grace_deadline=event.due_at + GRACE_PERIOD,
                state=MissedCareState.PENDING,
                updated_at=self._clock.now(),
            )
        )

    async def _on_watering_completed(self, event: WateringCompleted) -> None:
        """Satisfy the window when the watering was inside the grace period."""
        window = await self.unit_of_work.missed_care_windows.get(event.plant_id)
        if window is None or window.state is not MissedCareState.PENDING:
            return
        if event.completed_at > window.grace_deadline:
            return
        await self.unit_of_work.missed_care_windows.save(
            window.model_copy(
                update={"state": MissedCareState.SATISFIED, "updated_at": self._clock.now()}
            )
        )

    async def escalate_overdue(self, *, now: datetime) -> int:
        """Run one scheduled tick, returning how many care deadlines were missed.

        The caller passes ``now`` explicitly (the scheduler reads the clock), which
        is what lets a test step the grace period by 24 hours without freezing the
        process's real time.
        """
        async with self.unit_of_work:
            await self._open_windows_for_due(now=now)
            missed = await self._escalate_expired(now=now)
            await self.unit_of_work.commit()
        return missed

    async def _open_windows_for_due(self, *, now: datetime) -> None:
        """Record ``WateringDue`` and open a window for each newly due schedule."""
        due = await self.unit_of_work.care_schedules.list_due_without_pending_window(
            now, limit=TICK_BATCH_SIZE
        )
        for schedule in due:
            plant = await self.unit_of_work.plants.get(schedule.plant_id)
            if plant is None:
                continue
            schedule.mark_due(now=now)
            await self.unit_of_work.care_schedules.save(schedule)
            await self.unit_of_work.missed_care_windows.save(
                MissedCareWindow(
                    plant_id=schedule.plant_id,
                    household_id=plant.household_id,
                    due_at=schedule.next_watering_at,
                    grace_deadline=schedule.next_watering_at + GRACE_PERIOD,
                    state=MissedCareState.PENDING,
                    updated_at=now,
                )
            )

    async def _escalate_expired(self, *, now: datetime) -> int:
        """Shift the schedule, notify the household and close each expired window."""
        expired = await self.unit_of_work.missed_care_windows.list_overdue(
            now, limit=TICK_BATCH_SIZE
        )
        for window in expired:
            schedule = await self.unit_of_work.care_schedules.get_for_update(window.plant_id)
            if schedule is None:
                continue
            schedule.mark_missed(now=now, expected_version=schedule.version)
            await self.unit_of_work.care_schedules.save(schedule)
            payload: dict[str, JsonValue] = {
                "plant_id": str(window.plant_id),
                "next_watering_at": schedule.next_watering_at.isoformat(),
            }
            await self.unit_of_work.notifications.add(
                Notification.create(
                    household_id=window.household_id,
                    notification_type=NotificationType.CARE_MISSED,
                    now=now,
                    payload=payload,
                )
            )
            await self.unit_of_work.missed_care_windows.save(
                window.model_copy(update={"state": MissedCareState.MISSED, "updated_at": now})
            )
            logger.info("care missed for plant %s", window.plant_id)
        return len(expired)
