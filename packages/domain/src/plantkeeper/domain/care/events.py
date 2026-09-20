"""Domain events published by the Care context."""

from __future__ import annotations

from pydantic import AwareDatetime

from plantkeeper.domain.base import DomainEvent
from plantkeeper.domain.identifiers import PlantId
from plantkeeper.domain.values import WateringInterval


class CareScheduleCreated(DomainEvent):
    """A watering schedule was created for a plant."""

    plant_id: PlantId
    watering_interval: WateringInterval
    next_watering_at: AwareDatetime


class WateringDue(DomainEvent):
    """The schedule reached its next watering moment."""

    plant_id: PlantId
    due_at: AwareDatetime


class WateringCompleted(DomainEvent):
    """A plant was watered and the next watering moment was recomputed."""

    plant_id: PlantId
    completed_at: AwareDatetime
    next_watering_at: AwareDatetime


class WateringRescheduled(DomainEvent):
    """The next watering moment was moved (for example by telemetry)."""

    plant_id: PlantId
    previous_next_watering_at: AwareDatetime
    next_watering_at: AwareDatetime
    reason: str | None = None


class CareMissed(DomainEvent):
    """The grace period expired without a watering."""

    plant_id: PlantId
    next_watering_at: AwareDatetime


class CareSkipped(DomainEvent):
    """The household explicitly skipped this watering."""

    plant_id: PlantId
    skipped_at: AwareDatetime
    next_watering_at: AwareDatetime
