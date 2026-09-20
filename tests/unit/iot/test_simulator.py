"""The simulator's core: determinism, ids, and the stream it produces.

The CLI's timetable is in :mod:`tests.unit.iot.test_cli`; this module is about the
object under it — one run of one set of sensors.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from plantkeeper.infrastructure.messaging.topics import TELEMETRY_RAW
from plantkeeper.iot_simulator.scenarios import (
    DROUGHT,
    NORMAL,
    OVERWATERING,
    Scenario,
    scenario_named,
)
from plantkeeper.iot_simulator.simulator import (
    JSON_TOPIC,
    SensorSimulator,
    SensorSimulatorConfig,
    default_sensor_base_id,
    sensor_id_for,
)

START = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
STEP_SECONDS = 10.0


def a_simulator(
    *,
    sensor_count: int = 1,
    scenario: Scenario | None = None,
    seed: int = 42,
    step_seconds: float = STEP_SECONDS,
) -> SensorSimulator:
    """A simulator on a fixed base id, with anything overridden explicitly."""
    return SensorSimulator(
        SensorSimulatorConfig(
            sensor_count=sensor_count,
            base_id=UUID("00000000-0000-0000-0000-0000000000cc"),
            scenario=scenario if scenario is not None else scenario_named(NORMAL),
            seed=seed,
            step_seconds=step_seconds,
        )
    )


def test_every_sensor_id_is_derived_from_the_base_id_and_its_index() -> None:
    base = UUID("00000000-0000-0000-0000-0000000000cc")
    simulator = a_simulator(sensor_count=3)

    ids = [state.sensor_id for state in simulator.states]

    assert ids == [sensor_id_for(base, index) for index in range(3)]
    assert len(set(ids)) == 3
    assert sensor_id_for(base, 0) == sensor_id_for(base, 0)


def test_a_run_without_a_base_id_still_gets_distinct_sensor_ids() -> None:
    first = default_sensor_base_id()
    second = default_sensor_base_id()

    assert first != second
    assert len({sensor_id_for(first, index) for index in range(20)}) == 20


def test_the_same_seed_reproduces_the_same_stream() -> None:
    def stream() -> list[tuple[float, float, float]]:
        simulator = a_simulator(sensor_count=4)
        return [
            (r.moisture, r.temperature, r.light)
            for tick in range(3)
            for r in simulator.readings(START + timedelta(seconds=STEP_SECONDS * tick))
        ]

    assert stream() == stream()


def test_one_sensor_does_not_shift_another_sensors_draws() -> None:
    """Per-sensor generators: ten sensors is two runs of five, plus five."""
    ten = a_simulator(sensor_count=10)
    first_half = [reading.moisture for reading in ten.readings(START)]
    five = a_simulator(sensor_count=5)
    second_half = [reading.moisture for reading in five.readings(START)]

    assert first_half[:5] == second_half


def test_a_different_seed_gives_a_different_stream() -> None:
    one = a_simulator(seed=1)
    other = a_simulator(seed=2)

    assert one.readings(START)[0].moisture != other.readings(START)[0].moisture


def test_a_tick_reports_every_sensor_once() -> None:
    simulator = a_simulator(sensor_count=5)

    readings = simulator.readings(START)

    assert len(readings) == 5
    assert {reading.sensor_id for reading in readings} == {
        state.sensor_id for state in simulator.states
    }
    assert all(reading.recorded_at == START for reading in readings)


def test_moisture_falls_as_the_run_goes_on() -> None:
    simulator = a_simulator(scenario=scenario_named(DROUGHT))

    first = simulator.readings(START)[0].moisture
    last = simulator.readings(START + timedelta(seconds=STEP_SECONDS * 10))[0].moisture

    assert first > last


def test_overwatering_never_reports_more_than_a_sensor_can() -> None:
    simulator = a_simulator(scenario=scenario_named(OVERWATERING))

    readings = [
        simulator.readings(START + timedelta(seconds=STEP_SECONDS * tick))[0] for tick in range(50)
    ]

    assert all(0.0 <= reading.moisture <= 100.0 for reading in readings)
    assert all(reading.temperature >= -50.0 for reading in readings)


def test_a_simulator_needs_at_least_one_sensor_and_a_positive_step() -> None:
    with pytest.raises(ValueError, match="at least one sensor"):
        a_simulator(sensor_count=0)
    with pytest.raises(ValueError, match="step between two readings"):
        a_simulator(step_seconds=0.0)


def test_elapsed_time_is_measured_from_the_first_tick() -> None:
    simulator = a_simulator()
    simulator.readings(START)

    simulator.readings(START + timedelta(seconds=30))

    assert simulator.elapsed_seconds == 30.0
    assert simulator.config.step_seconds == STEP_SECONDS


def test_a_reading_serialises_to_the_raw_envelope() -> None:
    reading = a_simulator().readings(START)[0]

    document = json.loads(reading.to_json())

    assert set(document) == {"sensor_id", "recorded_at", "moisture", "temperature", "light"}
    assert document["sensor_id"] == str(reading.sensor_id)
    assert document["recorded_at"].endswith("+00:00")


def test_the_simulator_publishes_to_the_projects_raw_topic() -> None:
    """The tool declares the topic itself; this pins it to the infrastructure's."""
    assert JSON_TOPIC == TELEMETRY_RAW
