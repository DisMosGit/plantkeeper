"""Telemetry bounded context: sensor readings and their alerts."""

from __future__ import annotations

from plantkeeper.domain.telemetry.errors import (
    SensorNeverSeenError,
    SensorReadingMismatchError,
    SensorStillOnlineError,
    TelemetryError,
)
from plantkeeper.domain.telemetry.events import (
    SensorOffline,
    SoilMoistureHigh,
    SoilMoistureLow,
    TelemetryReceived,
    TemperatureAnomaly,
)
from plantkeeper.domain.telemetry.sensor import (
    MOISTURE_HIGH_THRESHOLD,
    MOISTURE_LOW_THRESHOLD,
    OFFLINE_AFTER,
    TEMPERATURE_HIGH_THRESHOLD,
    TEMPERATURE_LOW_THRESHOLD,
    Sensor,
)

__all__ = [
    "MOISTURE_HIGH_THRESHOLD",
    "MOISTURE_LOW_THRESHOLD",
    "OFFLINE_AFTER",
    "TEMPERATURE_HIGH_THRESHOLD",
    "TEMPERATURE_LOW_THRESHOLD",
    "Sensor",
    "SensorNeverSeenError",
    "SensorOffline",
    "SensorReadingMismatchError",
    "SensorStillOnlineError",
    "SoilMoistureHigh",
    "SoilMoistureLow",
    "TelemetryError",
    "TelemetryReceived",
    "TemperatureAnomaly",
]
