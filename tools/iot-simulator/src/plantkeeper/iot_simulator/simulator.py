"""The virtual sensors and the stream they produce.

One :class:`SensorSimulator` owns a fixed set of :class:`SensorState` objects and
advances them one step at a time. It is deterministic: the seed, the number of
sensors, the scenario and the step length are all a run needs to reproduce a
stream byte for byte, which is what makes ``--replay`` and the tests possible.

The simulator is deliberately independent of every other package in the
workspace (``pyproject.toml`` builds it on its own, and an ``import-linter``
contract forbids the application layers): it speaks the raw JSON that
:data:`JSON_TOPIC` carries and knows nothing about the domain.
"""

from __future__ import annotations

import json
import random
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Final
from uuid import UUID, uuid4, uuid5

from plantkeeper.iot_simulator.physics import (
    DEFAULT_LIGHT_LUX,
    DEFAULT_NOISE,
    MOISTURE_MAX,
    NoiseProfile,
    clamp,
    dry_moisture,
    light_at,
    with_noise,
)
from plantkeeper.iot_simulator.scenarios import DEFAULT_SCENARIO, Scenario

JSON_TOPIC: Final = "telemetry.raw"
"""The topic the raw stream is published to.

Declared here rather than imported from the infrastructure package so the
simulator stays a standalone tool; ``tests/unit/iot/test_simulator.py`` pins the
two names together.
"""

SENSOR_ID_NAMESPACE: Final = UUID("6f9619ff-8b86-d011-b42d-00cf4fc964ff")
"""Namespace for the deterministic sensor identifiers, so a run's ids are stable."""

DEFAULT_SENSOR_COUNT: Final = 20

DEFAULT_STEP_SECONDS: Final = 10.0
"""The default interval between two readings of one sensor."""


def default_sensor_base_id() -> UUID:
    """Return a fresh base id for a run that did not pick one.

    The base id is part of every derived sensor id, so an operator can register a
    known set of sensors once and then reproduce exactly that set with
    ``--sensor-base-id``.
    """
    return uuid4()


def sensor_id_for(base_id: UUID, index: int) -> UUID:
    """Return the stable identifier of sensor number ``index`` of a run."""
    return uuid5(SENSOR_ID_NAMESPACE, f"{base_id}:{index}")


@dataclass(frozen=True, slots=True)
class Reading:
    """One measurement, in the shape the ``telemetry.raw`` JSON has."""

    sensor_id: UUID
    recorded_at: datetime
    moisture: float
    temperature: float
    light: float

    def to_json(self) -> str:
        """Render the reading as the compact JSON the topic carries."""
        return json.dumps(
            {
                "sensor_id": str(self.sensor_id),
                "recorded_at": self.recorded_at.astimezone(UTC).isoformat(),
                "moisture": round(self.moisture, 3),
                "temperature": round(self.temperature, 3),
                "light": round(self.light, 3),
            },
            separators=(",", ":"),
            sort_keys=True,
        )


@dataclass(slots=True)
class SensorState:
    """What one virtual sensor currently holds.

    ``moisture`` is the modelled soil water content, ``temperature`` and
    ``light_lux`` the last reported air conditions, and ``last_watered_at`` when
    a scenario last added water — ``None`` for the scenarios that only let the
    soil dry.
    """

    sensor_id: UUID
    moisture: float
    temperature: float
    light_lux: float
    last_watered_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class SensorSimulatorConfig:
    """Everything a run needs to be reproducible."""

    sensor_count: int = DEFAULT_SENSOR_COUNT
    base_id: UUID = field(default_factory=default_sensor_base_id)
    scenario: Scenario = DEFAULT_SCENARIO
    seed: int = 0
    step_seconds: float = DEFAULT_STEP_SECONDS
    noise: NoiseProfile = DEFAULT_NOISE
    peak_lux: float = DEFAULT_LIGHT_LUX


class SensorSimulator:
    """A deterministic set of virtual sensors and the soil they are planted in."""

    def __init__(self, config: SensorSimulatorConfig | None = None) -> None:
        self._config = config if config is not None else SensorSimulatorConfig()
        if self._config.sensor_count < 1:
            raise ValueError("a simulator needs at least one sensor")
        if self._config.step_seconds <= 0:
            raise ValueError("the step between two readings must be positive")
        self._elapsed_seconds = 0.0
        self._started_at: datetime | None = None
        self._states = self._build_states()

    @property
    def config(self) -> SensorSimulatorConfig:
        """The configuration this simulator was built with."""
        return self._config

    @property
    def scenario(self) -> Scenario:
        """The scenario every sensor follows."""
        return self._config.scenario

    @property
    def states(self) -> list[SensorState]:
        """The sensors, in the order they were derived."""
        return self._states

    @property
    def elapsed_seconds(self) -> float:
        """How much simulated time has passed since the run started."""
        return self._elapsed_seconds

    def _build_states(self) -> list[SensorState]:
        """Give every sensor its initial soil, air and per-sensor randomness."""
        return [
            SensorState(
                sensor_id=sensor_id_for(self._config.base_id, index),
                moisture=clamp(
                    moisture=self._config.scenario.initial_moisture + index % 5 - 2.0,
                ),
                temperature=self._config.scenario.ambient_temperature,
                light_lux=0.0,
            )
            for index in range(self._config.sensor_count)
        ]

    def _rng_for(self, index: int) -> random.Random:
        """Return the sensor's own generator.

        One generator per sensor, seeded apart, so one sensor's draws can never
        shift another's: a run of twenty sensors is two runs of ten, plus ten
        more.
        """
        return random.Random(self._config.seed + index)

    def readings(self, instant: datetime) -> list[Reading]:
        """Advance every sensor one step and return what each reported.

        Sensors the scenario has silenced report nothing, which is how
        ``sensor_failure`` looks from Kafka: the partition simply stops receiving
        that key.
        """
        if self._started_at is None:
            self._started_at = instant
        self._elapsed_seconds = (instant - self._started_at).total_seconds()
        readings: list[Reading] = []
        for index, state in enumerate(self._states):
            reading = self._step(index, state, instant)
            if reading is not None:
                readings.append(reading)
        return readings

    def _step(self, index: int, state: SensorState, instant: datetime) -> Reading | None:
        """Advance one sensor and return its reading, if the scenario allows one."""
        conditions = self._config.scenario.conditions(
            instant=instant,
            elapsed_seconds=self._elapsed_seconds,
            sensor_index=index,
        )
        light_lux = light_at(instant, peak_lux=self._config.peak_lux) * conditions.light_multiplier
        state.moisture = dry_moisture(
            state.moisture,
            elapsed_seconds=self._config.step_seconds,
            temperature=conditions.temperature,
            light_lux=light_lux,
            half_life_hours=self._config.scenario.half_life_hours,
            dry_factor=self._config.scenario.dry_factor,
            moisture_floor=conditions.moisture_floor,
        )
        state.temperature = conditions.temperature
        state.light_lux = light_lux

        if not self._config.scenario.emits(
            elapsed_seconds=self._elapsed_seconds, sensor_index=index
        ):
            return None

        rng = self._rng_for(index)
        return Reading(
            sensor_id=state.sensor_id,
            recorded_at=instant,
            moisture=with_noise(
                state.moisture,
                rng=rng,
                profile=self._config.noise,
                minimum=max(conditions.moisture_floor, 0.0),
                maximum=MOISTURE_MAX,
            ),
            temperature=with_noise(
                state.temperature,
                rng=rng,
                profile=self._config.noise,
                minimum=-50.0,
                maximum=60.0,
            ),
            light=with_noise(
                state.light_lux,
                rng=rng,
                profile=self._config.noise,
                minimum=0.0,
                maximum=self._config.peak_lux * 2.0,
            ),
        )
