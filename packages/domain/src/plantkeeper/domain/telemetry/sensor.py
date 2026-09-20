"""The Sensor aggregate: one physical sensor bound to one plant."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Final

from plantkeeper.domain.base import AggregateRoot
from plantkeeper.domain.identifiers import PlantId, SensorId
from plantkeeper.domain.telemetry.errors import (
    SensorNeverSeenError,
    SensorReadingMismatchError,
    SensorStillOnlineError,
)
from plantkeeper.domain.telemetry.events import (
    SensorOffline,
    SoilMoistureHigh,
    SoilMoistureLow,
    TelemetryReceived,
    TemperatureAnomaly,
)
from plantkeeper.domain.values import SensorReading

MOISTURE_LOW_THRESHOLD: Final = 30.0
"""Moisture below this percentage means the plant is thirsty."""

MOISTURE_HIGH_THRESHOLD: Final = 80.0
"""Moisture above this percentage means the plant is being overwatered."""

TEMPERATURE_LOW_THRESHOLD: Final = 10.0
"""Temperature below this many degrees Celsius is an anomaly."""

TEMPERATURE_HIGH_THRESHOLD: Final = 35.0
"""Temperature above this many degrees Celsius is an anomaly."""

OFFLINE_AFTER: Final = timedelta(minutes=10)
"""Silence longer than this means the sensor is offline."""


class Sensor(AggregateRoot[SensorId]):
    """A sensor and the soil/temperature facts it reports.

    Registering a sensor records no event: the mapping from sensor to plant is
    the write side's own bookkeeping. Recording a reading always records
    :class:`TelemetryReceived` and adds a threshold event when the reading
    crosses one of the module-level thresholds (equality is not a crossing).
    """

    def __init__(
        self,
        sensor_id: SensorId,
        *,
        plant_id: PlantId,
        added_at: datetime,
        last_seen_at: datetime | None = None,
    ) -> None:
        """Rebuild a sensor from its stored state (no events are recorded)."""
        super().__init__(sensor_id)
        self._plant_id = plant_id
        self._added_at = added_at
        self._last_seen_at = last_seen_at

    @classmethod
    def register(
        cls,
        *,
        plant_id: PlantId,
        added_at: datetime,
        sensor_id: SensorId | None = None,
    ) -> Sensor:
        """Bind a new sensor to a plant."""
        return cls(sensor_id or SensorId.new(), plant_id=plant_id, added_at=added_at)

    @property
    def sensor_id(self) -> SensorId:
        """The identifier of this sensor (an alias of :attr:`id`)."""
        return self.id

    @property
    def plant_id(self) -> PlantId:
        """The plant this sensor watches."""
        return self._plant_id

    @property
    def added_at(self) -> datetime:
        """When the sensor was registered."""
        return self._added_at

    @property
    def last_seen_at(self) -> datetime | None:
        """When the sensor last reported, if it ever did."""
        return self._last_seen_at

    def record(self, reading: SensorReading) -> None:
        """Record one measurement and any threshold alert it triggers.

        Raises :class:`SensorReadingMismatchError` when the reading belongs to a
        different sensor.
        """
        if reading.sensor_id != self.id:
            raise SensorReadingMismatchError(
                f"reading for sensor {reading.sensor_id} cannot be recorded by sensor {self.id}"
            )
        recorded_at = reading.recorded_at
        self._last_seen_at = recorded_at
        self._record(
            TelemetryReceived(
                sensor_id=self.id,
                plant_id=self._plant_id,
                recorded_at=recorded_at,
                moisture=reading.moisture,
                temperature=reading.temperature,
                light=reading.light,
                occurred_at=recorded_at,
            )
        )
        if reading.moisture.value < MOISTURE_LOW_THRESHOLD:
            self._record(
                SoilMoistureLow(
                    sensor_id=self.id,
                    plant_id=self._plant_id,
                    moisture=reading.moisture,
                    threshold=MOISTURE_LOW_THRESHOLD,
                    occurred_at=recorded_at,
                )
            )
        if reading.moisture.value > MOISTURE_HIGH_THRESHOLD:
            self._record(
                SoilMoistureHigh(
                    sensor_id=self.id,
                    plant_id=self._plant_id,
                    moisture=reading.moisture,
                    threshold=MOISTURE_HIGH_THRESHOLD,
                    occurred_at=recorded_at,
                )
            )
        if (
            reading.temperature.value < TEMPERATURE_LOW_THRESHOLD
            or reading.temperature.value > TEMPERATURE_HIGH_THRESHOLD
        ):
            self._record(
                TemperatureAnomaly(
                    sensor_id=self.id,
                    plant_id=self._plant_id,
                    temperature=reading.temperature,
                    low_threshold=TEMPERATURE_LOW_THRESHOLD,
                    high_threshold=TEMPERATURE_HIGH_THRESHOLD,
                    occurred_at=recorded_at,
                )
            )

    def mark_offline(self, *, now: datetime) -> None:
        """Record :class:`SensorOffline` once the sensor has been silent long enough.

        Raises :class:`SensorNeverSeenError` when the sensor never reported and
        :class:`SensorStillOnlineError` when the silence is shorter than
        :data:`OFFLINE_AFTER`.
        """
        if self._last_seen_at is None:
            raise SensorNeverSeenError(f"sensor {self.id} never reported, so it cannot be offline")
        offline_for = now - self._last_seen_at
        if offline_for < OFFLINE_AFTER:
            raise SensorStillOnlineError(
                f"sensor {self.id} reported {offline_for} ago, which is not yet offline"
            )
        self._record(
            SensorOffline(
                sensor_id=self.id,
                plant_id=self._plant_id,
                last_seen_at=self._last_seen_at,
                offline_for=offline_for,
                occurred_at=now,
            )
        )
