"""Care aggregate <-> row mapping."""

from __future__ import annotations

from plantkeeper.domain.care.schedule import CareSchedule
from plantkeeper.domain.identifiers import PlantId
from plantkeeper.domain.values import WateringInterval
from plantkeeper.infrastructure.persistence.models.care import CareScheduleModel


def care_schedule_to_domain(model: CareScheduleModel) -> CareSchedule:
    """Rebuild the ``CareSchedule`` aggregate from its row."""
    return CareSchedule(
        PlantId(model.plant_id),
        watering_interval=WateringInterval(value=model.watering_interval),
        next_watering_at=model.next_watering_at,
        version=model.version,
    )


def care_schedule_to_model(schedule: CareSchedule) -> CareScheduleModel:
    """Build the row that represents ``schedule``."""
    return CareScheduleModel(
        plant_id=schedule.plant_id.value,
        watering_interval=schedule.watering_interval.value,
        next_watering_at=schedule.next_watering_at,
        version=schedule.version,
    )
