"""Domain events published by the Telemetry context."""

from __future__ import annotations

from datetime import timedelta

from pydantic import AwareDatetime

from plantkeeper.domain.base import DomainEvent
from plantkeeper.domain.identifiers import PlantId, SensorId
from plantkeeper.domain.values import LightLevel, Moisture, Temperature


class TelemetryReceived(DomainEvent):
    """A sensor reported one measurement."""

    sensor_id: SensorId
    plant_id: PlantId
    recorded_at: AwareDatetime
    moisture: Moisture
    temperature: Temperature
    light: LightLevel


class SoilMoistureLow(DomainEvent):
    """Soil moisture dropped strictly below the low threshold."""

    sensor_id: SensorId
    plant_id: PlantId
    moisture: Moisture
    threshold: float


class SoilMoistureHigh(DomainEvent):
    """Soil moisture rose strictly above the high threshold (overwatering)."""

    sensor_id: SensorId
    plant_id: PlantId
    moisture: Moisture
    threshold: float


class TemperatureAnomaly(DomainEvent):
    """The temperature left the comfortable band for the plant."""

    sensor_id: SensorId
    plant_id: PlantId
    temperature: Temperature
    low_threshold: float
    high_threshold: float


class SensorOffline(DomainEvent):
    """A sensor went silent for at least the offline threshold."""

    sensor_id: SensorId
    plant_id: PlantId
    last_seen_at: AwareDatetime
    offline_for: timedelta
