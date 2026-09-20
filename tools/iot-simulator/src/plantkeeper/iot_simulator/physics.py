"""The physical model behind the virtual sensors.

Three behaviours make the stream look like a real windowsill rather than a random
number generator:

* **light** follows a diurnal cycle — a half-sine between sunrise and sunset,
  zero at night, peaking at :data:`LIGHT_PEAK_INSTANT`;
* **soil moisture** evaporates exponentially, and the rate depends on the
  ambient temperature and on how much light the plant is getting, so a warm
  bright afternoon dries the soil faster than a cold dark night;
* **measurements** carry Gaussian noise and the occasional outlier, which is what
  a cheap sensor actually reports.

Everything here is a pure function of its arguments, so the suite can assert the
model without a clock, a broker or a seed: the randomness lives in the caller
(:mod:`plantkeeper.iot_simulator.simulator`), which passes in its own
:class:`random.Random`.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from datetime import datetime
from typing import Final

MINUTES_PER_DAY: Final = 1440
"""How many minutes one diurnal cycle has."""

SECONDS_PER_MINUTE: Final = 60
SECONDS_PER_HOUR: Final = 3600

DAYS_PER_YEAR: Final = 365.0

SUNRISE_MINUTE: Final = 6 * 60
SUNSET_MINUTE: Final = 20 * 60
LIGHT_PEAK_MINUTE: Final = 13 * 60
"""When the sun is highest, and therefore when the soil dries fastest."""

MOISTURE_MIN: Final = 0.0
MOISTURE_MAX: Final = 100.0
MOISTURE_FLOOR: Final = 2.0
"""Soil never reads exactly zero: the sensor would be in a puddle of dust."""

LIGHT_FACTOR_RANGE: Final = 0.4
"""Night still evaporates a little, through the air rather than the sun."""

TEMPERATURE_APPROACH: Final = 0.05
"""How fast a sensor's temperature converges on the ambient one per step.

Small on purpose: a sensor reads the air it sits in, and the air does not jump
to the scenario's target the instant the scenario changes.
"""

TEMPERATURE_AMPLITUDE: Final = 1.5

DEFAULT_TEMPERATURE: Final = 22.0
DEFAULT_LIGHT_LUX: Final = 3000.0
DEFAULT_HALF_LIFE_HOURS: Final = 12.0
"""Hours for half of the moisture above the floor to evaporate at 22 °C and a
light factor of 1.0. A half-life rather than a rate: it is the same number
however large a step the caller takes, so a ten-second tick and an hour-long tick
dry the soil to the same place.
"""

DEFAULT_NOISE_SIGMA: Final = 0.4
DEFAULT_OUTLIER_PROBABILITY: Final = 0.002
DEFAULT_OUTLIER_SIGMA: Final = 12.0


@dataclass(frozen=True, slots=True)
class NoiseProfile:
    """How much a sensor's readings wander around the modelled value."""

    sigma: float = DEFAULT_NOISE_SIGMA
    outlier_probability: float = DEFAULT_OUTLIER_PROBABILITY
    outlier_sigma: float = DEFAULT_OUTLIER_SIGMA


DEFAULT_NOISE: Final = NoiseProfile()
"""Gaussian noise plus a rare spike, as a cheap sensor produces."""


def light_at(instant: datetime, *, peak_lux: float = DEFAULT_LIGHT_LUX) -> float:
    """Return the ambient illuminance in lux at ``instant``.

    A half-sine from sunrise to sunset, and darkness in between: the model is
    continuous at both ends, so a reading never jumps when the sun comes up.
    """
    minutes = instant.hour * SECONDS_PER_MINUTE + instant.minute
    if not SUNRISE_MINUTE <= minutes <= SUNSET_MINUTE:
        return 0.0
    progress = (minutes - SUNRISE_MINUTE) / (SUNSET_MINUTE - SUNRISE_MINUTE)
    return peak_lux * math.sin(math.pi * progress)


def light_factor(light_lux: float, *, peak_lux: float = DEFAULT_LIGHT_LUX) -> float:
    """Return how much ``light_lux`` accelerates evaporation, in ``0..1``."""
    if peak_lux <= 0:
        return LIGHT_FACTOR_RANGE
    normalized = min(max(light_lux / peak_lux, 0.0), 1.0)
    return LIGHT_FACTOR_RANGE + (1.0 - LIGHT_FACTOR_RANGE) * normalized


def dry_moisture(
    moisture: float,
    *,
    elapsed_seconds: float,
    temperature: float,
    light_lux: float,
    half_life_hours: float = DEFAULT_HALF_LIFE_HOURS,
    dry_factor: float = 1.0,
    moisture_floor: float = MOISTURE_FLOOR,
) -> float:
    """Return the moisture after ``elapsed_seconds`` of evaporation.

    ``m - floor = (m0 - floor) * 0.5 ** (dt / half_life)``, where the half-life
    shortens as the temperature rises above freezing (never below it, or a cold
    snap would freeze the model) and as the light gets brighter. Exponential
    rather than linear: the drier the soil, the harder the remaining water is to
    evaporate. Expressing the model as a half-life rather than a per-hour rate
    keeps the result independent of how large a step the caller takes.
    """
    if elapsed_seconds <= 0 or moisture <= moisture_floor:
        return moisture
    hours = elapsed_seconds / SECONDS_PER_HOUR
    warmth = max(temperature, 0.0) / DEFAULT_TEMPERATURE
    effective_half_life = half_life_hours / max(warmth * light_factor(light_lux) * dry_factor, 1e-6)
    remaining = (moisture - moisture_floor) * 0.5 ** (hours / effective_half_life)
    return clamp(moisture=moisture_floor + remaining)


def clamp(*, moisture: float) -> float:
    """Keep a moisture value inside the range a sensor can report."""
    return min(max(moisture, MOISTURE_FLOOR), MOISTURE_MAX)


def ambient_temperature(instant: datetime, base: float) -> float:
    """Return the air temperature at ``instant``: a yearly wave with a daily one.

    Cold in the small hours, warm in the afternoon, and cooler in winter — enough
    seasonality that a scenario is not the only thing moving the number.
    """
    minute_of_day = instant.hour * SECONDS_PER_MINUTE + instant.minute
    daily = math.sin(2 * math.pi * (minute_of_day - LIGHT_PEAK_MINUTE) / MINUTES_PER_DAY)
    seasonal = math.sin(2 * math.pi * instant.timetuple().tm_yday / DAYS_PER_YEAR)
    return base + TEMPERATURE_AMPLITUDE * daily + 2.0 * seasonal


def with_noise(
    value: float,
    *,
    rng: random.Random,
    profile: NoiseProfile = DEFAULT_NOISE,
    minimum: float = 0.0,
    maximum: float = MOISTURE_MAX,
) -> float:
    """Return ``value`` plus sensor noise, clamped to ``[minimum, maximum]``.

    The outlier draw happens on every call, not only when one is produced, so a
    given seed produces the same stream however the values compare.
    """
    noisy = value + rng.gauss(0.0, profile.sigma)
    if rng.random() < profile.outlier_probability:
        noisy += rng.gauss(0.0, profile.outlier_sigma)
    return min(max(noisy, minimum), maximum)
