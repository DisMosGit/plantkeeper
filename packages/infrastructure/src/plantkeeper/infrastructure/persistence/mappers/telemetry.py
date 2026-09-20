"""Telemetry aggregate <-> row mapping."""

from __future__ import annotations

from plantkeeper.domain.identifiers import PlantId, SensorId
from plantkeeper.domain.telemetry.sensor import Sensor
from plantkeeper.infrastructure.persistence.models.telemetry import SensorModel


def sensor_to_domain(model: SensorModel) -> Sensor:
    """Rebuild the ``Sensor`` aggregate from its row."""
    return Sensor(
        SensorId(model.id),
        plant_id=PlantId(model.plant_id),
        added_at=model.added_at,
        last_seen_at=model.last_seen_at,
    )


def sensor_to_model(sensor: Sensor) -> SensorModel:
    """Build the row that represents ``sensor``."""
    return SensorModel(
        id=sensor.id.value,
        plant_id=sensor.plant_id.value,
        added_at=sensor.added_at,
        last_seen_at=sensor.last_seen_at,
    )
