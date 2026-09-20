"""Telemetry aggregate <-> row mapping."""

from __future__ import annotations

from plantkeeper.domain.identifiers import PlantId, SensorId
from plantkeeper.domain.telemetry.reading import TelemetryReading
from plantkeeper.domain.telemetry.sensor import Sensor
from plantkeeper.domain.values import LightLevel, Moisture, Temperature
from plantkeeper.infrastructure.persistence.models.telemetry import (
    SensorModel,
    SensorReadingModel,
)


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


def reading_to_model(reading: TelemetryReading) -> SensorReadingModel:
    """Build the row that represents ``reading``.

    The value objects are unwrapped to the scalars the columns hold; the same
    scalars the wire format carries, so a stored row and a published event agree
    without a second mapping.
    """
    return SensorReadingModel(
        sensor_id=reading.sensor_id.value,
        recorded_at=reading.recorded_at,
        plant_id=reading.plant_id.value,
        moisture=reading.moisture.value,
        temperature=reading.temperature.value,
        light=reading.light.value,
    )


def reading_to_domain(model: SensorReadingModel) -> TelemetryReading:
    """Rebuild the reading from its row."""
    return TelemetryReading(
        sensor_id=SensorId(model.sensor_id),
        plant_id=PlantId(model.plant_id),
        recorded_at=model.recorded_at,
        moisture=Moisture(value=model.moisture),
        temperature=Temperature(value=model.temperature),
        light=LightLevel(value=model.light),
    )
