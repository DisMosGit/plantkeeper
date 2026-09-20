"""The five scenarios, each distinguishable by what it does to the soil.

A scenario is a handful of parameters, so these tests are about the *effects* the
roadmap promises: drought dries faster, overwatering stays above the high
threshold, a cold snap is cold, a failing sensor goes quiet and comes back.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from plantkeeper.iot_simulator.physics import MOISTURE_FLOOR
from plantkeeper.iot_simulator.scenarios import (
    COLD_SNAP_TEMPERATURE,
    DROUGHT,
    DROUGHT_HALF_LIFE_HOURS,
    NORMAL,
    OVERWATERING,
    OVERWATERING_FLOOR,
    SCENARIOS,
    SENSOR_FAILURE,
    SILENCE_AFTER,
    SILENCE_FOR,
    Scenario,
    scenario_named,
)
from plantkeeper.iot_simulator.simulator import (
    Reading,
    SensorSimulator,
    SensorSimulatorConfig,
)

START = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
BASE_ID = UUID("00000000-0000-0000-0000-0000000000aa")
STEP_SECONDS = 10.0


def a_simulator(name: str, *, sensors: int = 2, seed: int = 42) -> SensorSimulator:
    """A simulator of one scenario, on a fixed base id and step."""
    return SensorSimulator(
        SensorSimulatorConfig(
            sensor_count=sensors,
            base_id=BASE_ID,
            scenario=scenario_named(name),
            seed=seed,
            step_seconds=STEP_SECONDS,
        )
    )


def readings_at(simulator: SensorSimulator, tick: int) -> list[Reading]:
    """Advance the simulator to ``tick`` ten-second steps in and read it."""
    return simulator.readings(START + timedelta(seconds=STEP_SECONDS * tick))


def readings_over(name: str, *, ticks: int, sensors: int = 2) -> list[list[Reading]]:
    """Every reading of a scenario over ``ticks`` steps."""
    simulator = a_simulator(name, sensors=sensors)
    return [readings_at(simulator, tick) for tick in range(ticks)]


def test_every_roadmap_scenario_exists() -> None:
    assert set(SCENARIOS) == {NORMAL, DROUGHT, OVERWATERING, "cold_snap", SENSOR_FAILURE}


def test_an_unknown_scenario_is_rejected_by_name() -> None:
    with pytest.raises(ValueError, match="unknown scenario"):
        scenario_named("monsoon")

    assert scenario_named(NORMAL).name == NORMAL


def test_each_scenario_is_a_scenario() -> None:
    for scenario in SCENARIOS.values():
        assert isinstance(scenario, Scenario)
        assert scenario.moisture_floor >= MOISTURE_FLOOR
        assert scenario.half_life_hours > 0


def test_normal_soil_dries_slowly_over_the_first_minute() -> None:
    first, last = readings_over(NORMAL, ticks=7)[0][0], readings_over(NORMAL, ticks=7)[-1][0]

    assert first.moisture > last.moisture
    # A minute of a twelve-hour half-life is a fraction of a percent.
    assert first.moisture - last.moisture < 1.0


def test_drought_dries_much_faster_than_normal() -> None:
    drought = readings_over(DROUGHT, ticks=7)
    normal = readings_over(NORMAL, ticks=7)

    drought_loss = drought[0][0].moisture - drought[-1][0].moisture
    normal_loss = normal[0][0].moisture - normal[-1][0].moisture

    assert drought_loss > 10 * normal_loss
    assert scenario_named(DROUGHT).half_life_hours == DROUGHT_HALF_LIFE_HOURS


def test_overwatering_holds_the_soil_above_the_high_threshold() -> None:
    for batch in readings_over(OVERWATERING, ticks=10):
        for reading in batch:
            assert reading.moisture > 90.0
            assert reading.moisture >= OVERWATERING_FLOOR


def test_a_cold_snap_is_cold() -> None:
    for batch in readings_over("cold_snap", ticks=3):
        for reading in batch:
            assert reading.temperature < 10.0
            assert reading.temperature > COLD_SNAP_TEMPERATURE - 10.0


def test_a_failing_sensor_goes_quiet_and_then_comes_back() -> None:
    simulator = a_simulator(SENSOR_FAILURE, sensors=1)

    assert len(readings_at(simulator, 0)) == 1, "the sensor talks before its window"
    assert len(readings_at(simulator, int(SILENCE_AFTER / STEP_SECONDS) + 1)) == 0
    assert len(readings_at(simulator, int((SILENCE_AFTER + SILENCE_FOR) / STEP_SECONDS) + 3)) == 1


def test_sensors_do_not_all_fail_in_the_same_second() -> None:
    """Twenty sensors going quiet at once would be an artifact, not a scenario."""
    simulator = a_simulator(SENSOR_FAILURE, sensors=10, seed=1)

    speaking = [
        {reading.sensor_id for reading in readings_at(simulator, tick)}
        for tick in range(int(SILENCE_FOR / STEP_SECONDS) + 2)
    ]

    assert all(speaking), "the stream never goes completely silent"
    assert len({frozenset(group) for group in speaking}) > 1, "sensors fail at different times"
