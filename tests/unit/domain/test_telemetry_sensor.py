"""Invariant tests for the Sensor aggregate."""

from __future__ import annotations

from datetime import datetime, timedelta
from uuid import uuid4

import pytest

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
from plantkeeper.domain.telemetry.sensor import (
    MOISTURE_HIGH_THRESHOLD,
    MOISTURE_LOW_THRESHOLD,
    OFFLINE_AFTER,
    TEMPERATURE_HIGH_THRESHOLD,
    TEMPERATURE_LOW_THRESHOLD,
    Sensor,
)
from plantkeeper.domain.values import LightLevel, Moisture, SensorReading, Temperature


def _register(now: datetime, *, sensor_id: SensorId | None = None) -> Sensor:
    return Sensor.register(plant_id=PlantId(uuid4()), added_at=now, sensor_id=sensor_id)


def _reading(
    sensor_id: SensorId,
    recorded_at: datetime,
    *,
    moisture: float = 50.0,
    temperature: float = 20.0,
    light: float = 800.0,
) -> SensorReading:
    return SensorReading(
        sensor_id=sensor_id,
        recorded_at=recorded_at,
        moisture=Moisture(value=moisture),
        temperature=Temperature(value=temperature),
        light=LightLevel(value=light),
    )


def test_register_binds_the_sensor_to_a_plant(now: datetime) -> None:
    plant_id = PlantId(uuid4())

    sensor = Sensor.register(plant_id=plant_id, added_at=now)

    assert sensor.plant_id == plant_id
    assert sensor.sensor_id == sensor.id
    assert sensor.added_at == now
    assert sensor.last_seen_at is None
    assert sensor.collect_events() == []


def test_register_accepts_an_explicit_identifier(now: datetime) -> None:
    sensor_id = SensorId(uuid4())

    assert _register(now, sensor_id=sensor_id).id == sensor_id


def test_rebuilding_a_sensor_records_no_events(now: datetime) -> None:
    sensor = Sensor(
        SensorId(uuid4()),
        plant_id=PlantId(uuid4()),
        added_at=now,
        last_seen_at=now,
    )

    assert sensor.collect_events() == []
    assert sensor.last_seen_at == now


def test_record_records_telemetry_received(now: datetime) -> None:
    sensor = _register(now)
    recorded_at = now + timedelta(minutes=1)

    sensor.record(_reading(sensor.id, recorded_at))

    assert sensor.last_seen_at == recorded_at
    events = sensor.collect_events()
    assert len(events) == 1
    received = events[0]
    assert isinstance(received, TelemetryReceived)
    assert received.sensor_id == sensor.id
    assert received.plant_id == sensor.plant_id
    assert received.recorded_at == recorded_at
    assert received.moisture == Moisture(value=50.0)
    assert received.temperature == Temperature(value=20.0)
    assert received.light == LightLevel(value=800.0)
    assert received.occurred_at == recorded_at


def test_record_rejects_a_reading_from_another_sensor(now: datetime) -> None:
    sensor = _register(now)

    with pytest.raises(SensorReadingMismatchError):
        sensor.record(_reading(SensorId(uuid4()), now))

    assert sensor.last_seen_at is None
    assert sensor.collect_events() == []


def test_moisture_below_the_threshold_is_an_alert(now: datetime) -> None:
    sensor = _register(now)

    sensor.record(_reading(sensor.id, now, moisture=MOISTURE_LOW_THRESHOLD - 0.1))

    events = sensor.collect_events()
    assert len(events) == 2
    low = events[1]
    assert isinstance(low, SoilMoistureLow)
    assert low.moisture == Moisture(value=MOISTURE_LOW_THRESHOLD - 0.1)
    assert low.threshold == MOISTURE_LOW_THRESHOLD


def test_moisture_above_the_threshold_is_an_overwatering_alert(now: datetime) -> None:
    sensor = _register(now)

    sensor.record(_reading(sensor.id, now, moisture=MOISTURE_HIGH_THRESHOLD + 0.1))

    events = sensor.collect_events()
    assert len(events) == 2
    high = events[1]
    assert isinstance(high, SoilMoistureHigh)
    assert high.moisture == Moisture(value=MOISTURE_HIGH_THRESHOLD + 0.1)
    assert high.threshold == MOISTURE_HIGH_THRESHOLD


@pytest.mark.parametrize("moisture", [MOISTURE_LOW_THRESHOLD, MOISTURE_HIGH_THRESHOLD])
def test_moisture_at_a_threshold_is_not_an_alert(now: datetime, moisture: float) -> None:
    sensor = _register(now)

    sensor.record(_reading(sensor.id, now, moisture=moisture))

    assert len(sensor.collect_events()) == 1


def test_temperature_below_the_band_is_an_anomaly(now: datetime) -> None:
    sensor = _register(now)

    sensor.record(_reading(sensor.id, now, temperature=TEMPERATURE_LOW_THRESHOLD - 0.1))

    anomaly = sensor.collect_events()[1]
    assert isinstance(anomaly, TemperatureAnomaly)
    assert anomaly.temperature == Temperature(value=TEMPERATURE_LOW_THRESHOLD - 0.1)
    assert anomaly.low_threshold == TEMPERATURE_LOW_THRESHOLD
    assert anomaly.high_threshold == TEMPERATURE_HIGH_THRESHOLD


def test_temperature_above_the_band_is_an_anomaly(now: datetime) -> None:
    sensor = _register(now)

    sensor.record(_reading(sensor.id, now, temperature=TEMPERATURE_HIGH_THRESHOLD + 0.1))

    anomaly = sensor.collect_events()[1]
    assert isinstance(anomaly, TemperatureAnomaly)
    assert anomaly.temperature == Temperature(value=TEMPERATURE_HIGH_THRESHOLD + 0.1)


@pytest.mark.parametrize("temperature", [TEMPERATURE_LOW_THRESHOLD, TEMPERATURE_HIGH_THRESHOLD])
def test_temperature_at_a_band_edge_is_not_an_anomaly(now: datetime, temperature: float) -> None:
    sensor = _register(now)

    sensor.record(_reading(sensor.id, now, temperature=temperature))

    assert len(sensor.collect_events()) == 1


def test_marking_offline_before_the_threshold_is_rejected(now: datetime) -> None:
    sensor = _register(now)
    sensor.record(_reading(sensor.id, now))
    sensor.collect_events()

    with pytest.raises(SensorStillOnlineError):
        sensor.mark_offline(now=now + OFFLINE_AFTER - timedelta(seconds=1))

    assert sensor.collect_events() == []


def test_marking_offline_at_the_threshold_records_sensor_offline(now: datetime) -> None:
    sensor = _register(now)
    sensor.record(_reading(sensor.id, now))
    sensor.collect_events()
    offline_at = now + OFFLINE_AFTER

    sensor.mark_offline(now=offline_at)

    events = sensor.collect_events()
    assert len(events) == 1
    offline = events[0]
    assert isinstance(offline, SensorOffline)
    assert offline.sensor_id == sensor.id
    assert offline.plant_id == sensor.plant_id
    assert offline.last_seen_at == now
    assert offline.offline_for == OFFLINE_AFTER
    assert offline.occurred_at == offline_at


def test_a_sensor_that_never_reported_cannot_be_offline(now: datetime) -> None:
    sensor = _register(now)

    with pytest.raises(SensorNeverSeenError):
        sensor.mark_offline(now=now + OFFLINE_AFTER)

    assert sensor.collect_events() == []
