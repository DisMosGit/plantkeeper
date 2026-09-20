"""The Care read model.

Owns ``read_analytics.care_schedules`` and the denormalised
``plants.next_watering_at`` that makes the admin's plant list useful in one
query. The care row is authoritative and is written first: if the plant row has
not been projected yet, the next watering is still recorded where it belongs, and
the plant row is created or refreshed as a side effect.
"""

from __future__ import annotations

from datetime import datetime
from typing import ClassVar
from uuid import UUID

from plantkeeper.admin.projections.base import Projection, ProjectionHandler
from plantkeeper.admin.read_models.models import CareReadModel, PlantReadModel
from plantkeeper.domain.base import DomainEvent
from plantkeeper.domain.care.events import (
    CareMissed,
    CareScheduleCreated,
    CareSkipped,
    WateringCompleted,
    WateringRescheduled,
)
from plantkeeper.infrastructure.messaging.topics import CARE_EVENTS


class CareProjection(Projection):
    """Consumes ``care.events`` into the schedule read model."""

    name = "care"
    topics = (CARE_EVENTS,)

    def on_care_schedule_created(self, event: CareScheduleCreated) -> None:
        """Create (or refresh) the schedule for a plant."""
        self._set_next_watering(event.plant_id.value, event.next_watering_at)
        _, created = CareReadModel.objects.get_or_create(
            plant_id=event.plant_id.value,
            defaults={
                "watering_interval": event.watering_interval.value,
                "next_watering_at": event.next_watering_at,
                "version": 1,
            },
        )
        if not created:
            # The version is not reset: it belongs to the write side's optimistic
            # locking, and only a first schedule is version 1.
            CareReadModel.objects.filter(plant_id=event.plant_id.value).update(
                watering_interval=event.watering_interval.value,
                next_watering_at=event.next_watering_at,
            )

    def on_watering_completed(self, event: WateringCompleted) -> None:
        """Move the schedule on and remember when the plant was watered."""
        CareReadModel.objects.filter(plant_id=event.plant_id.value).update(
            last_watered_at=event.completed_at
        )
        self._set_next_watering(event.plant_id.value, event.next_watering_at)

    def on_watering_rescheduled(self, event: WateringRescheduled) -> None:
        """Follow a schedule that telemetry or a saga moved."""
        self._set_next_watering(event.plant_id.value, event.next_watering_at)

    def on_care_skipped(self, event: CareSkipped) -> None:
        """Follow a skipping household: the schedule moved without a watering."""
        self._set_next_watering(event.plant_id.value, event.next_watering_at)

    def on_care_missed(self, event: CareMissed) -> None:
        """Follow a missed watering's shift of the schedule."""
        self._set_next_watering(event.plant_id.value, event.next_watering_at)

    def _set_next_watering(self, plant_id: UUID, next_watering_at: datetime) -> None:
        """Write the next watering to the care row and the plant's copy of it.

        ``update_or_create`` on the plant is deliberate rather than
        ``filter().update()``: the garden event that names the plant is consumed
        from another topic, and a missing row must not swallow the fact. The
        other columns stay untouched, which is what keeps one writer per column.
        """
        CareReadModel.objects.filter(plant_id=plant_id).update(next_watering_at=next_watering_at)
        PlantReadModel.objects.update_or_create(
            plant_id=plant_id,
            defaults={"next_watering_at": next_watering_at},
        )

    handlers: ClassVar[dict[type[DomainEvent], ProjectionHandler]] = {
        CareScheduleCreated: on_care_schedule_created,
        WateringCompleted: on_watering_completed,
        WateringRescheduled: on_watering_rescheduled,
        CareSkipped: on_care_skipped,
        CareMissed: on_care_missed,
    }
