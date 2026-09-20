"""The five telemetry scenarios.

A scenario is a small, declarative set of facts about the world the virtual
sensors live in: how warm the air is, how fast the soil dries, how much light
reaches the leaves, where the moisture starts and whether the sensor is talking
at all. The physical model is shared (:mod:`plantkeeper.iot_simulator.physics`);
a scenario only tilts its parameters, which is what keeps five scenarios from
becoming five simulations.

The scenarios are the ones ``ROADMAP.md`` names: ``normal``, ``drought``,
``overwatering``, ``cold_snap`` and ``sensor_failure``.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Final

from plantkeeper.iot_simulator.physics import (
    DEFAULT_HALF_LIFE_HOURS,
    DEFAULT_LIGHT_LUX,
    DEFAULT_TEMPERATURE,
    MOISTURE_FLOOR,
    TEMPERATURE_AMPLITUDE,
)

NORMAL: Final = "normal"
DROUGHT: Final = "drought"
OVERWATERING: Final = "overwatering"
COLD_SNAP: Final = "cold_snap"
SENSOR_FAILURE: Final = "sensor_failure"

SILENCE_AFTER: Final = 30.0
"""Seconds after a sensor starts before ``sensor_failure`` silences it."""

SILENCE_FOR: Final = 300.0
"""How long ``sensor_failure`` keeps a sensor silent (five minutes)."""

SILENCE_WINDOW: Final = 1800.0
"""The period the silence repeats over, so a long run sees sensor after sensor."""

COLD_SNAP_TEMPERATURE: Final = -2.0
"""Below the domain's low-temperature anomaly threshold, so cold telemetry alarms."""

OVERWATERING_FLOOR: Final = 92.0
"""Above the domain's high-moisture threshold, so overwatering is unmistakable."""

DROUGHT_MOISTURE: Final = 31.0
"""Just above the domain's low threshold: one step of drought crosses it."""

DROUGHT_DRY_FACTOR: Final = 4.0
"""How much faster the soil dries in a drought."""

DROUGHT_HALF_LIFE_HOURS: Final = 3.0
"""Drought soil halves its moisture every three hours, not every twelve."""

OVERWATERING_HALF_LIFE_HOURS: Final = 48.0
"""Waterlogged soil barely evaporates, which is why the floor holds."""


@dataclass(frozen=True, slots=True)
class Conditions:
    """What the world looks like for one sensor at one instant."""

    temperature: float
    light_multiplier: float = 1.0
    moisture_floor: float = MOISTURE_FLOOR
    """A scenario that waters the soil itself raises this above the dry floor."""


@dataclass(frozen=True, slots=True)
class Scenario:
    """One named set of environmental parameters."""

    name: str
    ambient_temperature: float = DEFAULT_TEMPERATURE
    dry_factor: float = 1.0
    initial_moisture: float = 60.0
    peak_lux: float = DEFAULT_LIGHT_LUX
    light_multiplier: float = 1.0
    moisture_floor: float = MOISTURE_FLOOR
    half_life_hours: float = DEFAULT_HALF_LIFE_HOURS
    silent_after: float | None = None
    silent_for: float = SILENCE_FOR

    def conditions(
        self, *, instant: datetime, elapsed_seconds: float, sensor_index: int
    ) -> Conditions:
        """Return the ambient conditions for one sensor at one instant.

        ``instant`` gives the scenario a time of day to work with, ``sensor_index``
        a way to keep the sensors from being identical, and ``elapsed_seconds`` a
        way to let a scenario change over the run.
        """
        minute_of_day = instant.hour * 60 + instant.minute
        daily_swing = TEMPERATURE_AMPLITUDE * math.sin(
            2 * math.pi * (minute_of_day - 13 * 60) / (24 * 60)
        )
        return Conditions(
            temperature=self.ambient_temperature
            + daily_swing
            + sensor_temperature_offset(sensor_index),
            light_multiplier=self.light_multiplier,
            moisture_floor=self.moisture_floor,
        )

    def emits(self, *, elapsed_seconds: float, sensor_index: int) -> bool:
        """Whether a sensor reports at ``elapsed_seconds`` into the run.

        Every scenario but ``sensor_failure`` always reports; the failure
        scenario is silent for its window and then speaks again, which is how a
        restarting sensor looks from the outside. The windows are staggered by
        sensor, because twenty sensors failing in the same second would be a
        simulator artifact rather than a scenario.
        """
        if self.silent_after is None:
            return True
        offset = self.silent_after + (sensor_index % 5) * SILENCE_WINDOW / 5.0
        if elapsed_seconds < offset:
            return True
        phase = (elapsed_seconds - offset) % SILENCE_WINDOW
        return phase >= self.silent_for


NORMAL_SCENARIO: Final = Scenario(name=NORMAL)

DROUGHT_SCENARIO: Final = Scenario(
    name=DROUGHT,
    ambient_temperature=31.0,
    dry_factor=DROUGHT_DRY_FACTOR,
    half_life_hours=DROUGHT_HALF_LIFE_HOURS,
    initial_moisture=DROUGHT_MOISTURE,
    light_multiplier=1.25,
)

OVERWATERING_SCENARIO: Final = Scenario(
    name=OVERWATERING,
    ambient_temperature=19.0,
    initial_moisture=OVERWATERING_FLOOR + 4.0,
    light_multiplier=0.5,
    moisture_floor=OVERWATERING_FLOOR,
    half_life_hours=OVERWATERING_HALF_LIFE_HOURS,
)

COLD_SNAP_SCENARIO: Final = Scenario(
    name=COLD_SNAP,
    ambient_temperature=COLD_SNAP_TEMPERATURE,
    dry_factor=0.4,
)

SENSOR_FAILURE_SCENARIO: Final = Scenario(
    name=SENSOR_FAILURE,
    silent_after=SILENCE_AFTER,
    silent_for=SILENCE_FOR,
)

SCENARIOS: Final[dict[str, Scenario]] = {
    scenario.name: scenario
    for scenario in (
        NORMAL_SCENARIO,
        DROUGHT_SCENARIO,
        OVERWATERING_SCENARIO,
        COLD_SNAP_SCENARIO,
        SENSOR_FAILURE_SCENARIO,
    )
}

DEFAULT_SCENARIO: Final = NORMAL_SCENARIO


def scenario_named(name: str) -> Scenario:
    """Return the scenario called ``name``.

    Raises :class:`ValueError` for an unknown name: the CLI validates its own
    argument eagerly, and a typo in a library call should fail rather than fall
    back to ``normal`` and quietly produce the wrong stream.
    """
    try:
        return SCENARIOS[name]
    except KeyError as exc:
        known = ", ".join(sorted(SCENARIOS))
        raise ValueError(f"unknown scenario {name!r}; expected one of: {known}") from exc


def sensor_temperature_offset(sensor_index: int) -> float:
    """Return a small, stable per-sensor air-temperature offset.

    Sensors are not identical: two of them on the same shelf disagree by a
    fraction of a degree. The offset is a deterministic function of the index,
    not a random draw, so it survives a reseed and cannot make replay differ.
    """
    return math.sin(sensor_index * 1.7) * 0.8
