"""The physical model, without a simulator or a seed.

The model is what makes the stream worth watching, and every claim it makes is a
property of one pure function: light rises and falls with the sun, soil dries
exponentially and faster when it is warm and bright, and a reading stays inside
the range a sensor can report.
"""

from __future__ import annotations

import random
from datetime import UTC, datetime

import pytest

from plantkeeper.iot_simulator.physics import (
    DEFAULT_HALF_LIFE_HOURS,
    LIGHT_FACTOR_RANGE,
    MOISTURE_FLOOR,
    NoiseProfile,
    ambient_temperature,
    dry_moisture,
    light_at,
    light_factor,
    with_noise,
)


def an_instant(hour: int, minute: int = 0) -> datetime:
    """A UTC instant on a fixed day, so tests never depend on the run's date."""
    return datetime(2026, 9, 20, hour, minute, tzinfo=UTC)


# --- the diurnal cycle --------------------------------------------------------


def test_light_is_dark_before_sunrise_and_after_sunset() -> None:
    assert light_at(an_instant(3)) == 0.0
    assert light_at(an_instant(5, 59)) == 0.0
    assert light_at(an_instant(20, 1)) == 0.0
    assert light_at(an_instant(23, 59)) == 0.0


def test_light_peaks_in_the_middle_of_the_day() -> None:
    noon = light_at(an_instant(13))

    assert noon == pytest.approx(3000.0)
    assert noon > light_at(an_instant(9))
    assert light_at(an_instant(9)) > light_at(an_instant(7))


def test_the_diurnal_cycle_is_continuous_at_both_ends() -> None:
    """A reading must not jump when the sun crosses the horizon."""
    just_before_sunset = light_at(an_instant(19, 59))
    just_after_sunrise = light_at(an_instant(6, 1))

    assert just_after_sunrise == pytest.approx(just_before_sunset, rel=1e-3)
    assert 0.0 < just_after_sunrise < 20.0

    # The last minute of daylight is dim, and the first minute of night is dark:
    # the model is continuous where it stops, it does not fall off a cliff.
    assert light_at(an_instant(19, 59)) < light_at(an_instant(19, 0))


def test_a_brighter_day_dries_faster() -> None:
    night = light_factor(light_at(an_instant(2)))
    noon = light_factor(light_at(an_instant(13)))

    assert night == pytest.approx(LIGHT_FACTOR_RANGE)
    assert noon == pytest.approx(1.0)
    assert noon > night


def test_ambient_temperature_is_cooler_at_night_than_in_the_afternoon() -> None:
    assert ambient_temperature(an_instant(3), 22.0) < ambient_temperature(an_instant(15), 22.0)


# --- evaporation --------------------------------------------------------------


def test_half_the_moisture_above_the_floor_evaporates_in_one_half_life() -> None:
    dried = dry_moisture(
        62.0,
        elapsed_seconds=DEFAULT_HALF_LIFE_HOURS * 3600,
        temperature=22.0,
        light_lux=3000.0,
    )

    assert dried == pytest.approx(MOISTURE_FLOOR + 30.0, rel=0.05)


def test_the_dryness_is_independent_of_how_large_a_step_the_caller_takes() -> None:
    """A ten-second tick and an hour-long one must reach the same place."""
    in_steps = 60.0
    for _ in range(360):
        in_steps = dry_moisture(in_steps, elapsed_seconds=10, temperature=22.0, light_lux=3000.0)
    in_one_go = dry_moisture(60.0, elapsed_seconds=3600, temperature=22.0, light_lux=3000.0)

    assert in_steps == pytest.approx(in_one_go, abs=0.05)


def test_warm_and_bright_soil_dries_faster_than_cold_and_dark_soil() -> None:
    hot_and_bright = dry_moisture(70.0, elapsed_seconds=3600, temperature=30.0, light_lux=3000.0)
    cold_and_dark = dry_moisture(70.0, elapsed_seconds=3600, temperature=6.0, light_lux=0.0)

    assert hot_and_bright < cold_and_dark
    assert cold_and_dark < 70.0


def test_freezing_temperature_does_not_flip_the_evaporation_rate() -> None:
    """``max(temperature, 0)``: a cold snap slows drying to a floor, never reverses it."""
    freezing = dry_moisture(70.0, elapsed_seconds=3600, temperature=-20.0, light_lux=0.0)
    just_above = dry_moisture(70.0, elapsed_seconds=3600, temperature=0.0, light_lux=0.0)

    assert freezing == pytest.approx(just_above)
    assert freezing <= 70.0


def test_soil_never_dries_below_its_floor_or_rises_above_saturation() -> None:
    almost_dry = dry_moisture(3.0, elapsed_seconds=10**7, temperature=40.0, light_lux=3000.0)

    assert almost_dry >= MOISTURE_FLOOR
    assert dry_moisture(200.0, elapsed_seconds=0, temperature=40.0, light_lux=3000.0) == 200.0


def test_no_elapsed_time_means_no_evaporation() -> None:
    assert dry_moisture(55.0, elapsed_seconds=0, temperature=30.0, light_lux=3000.0) == 55.0


# --- noise --------------------------------------------------------------------


def test_noise_stays_inside_the_bounds_it_is_given() -> None:
    rng = random.Random(7)
    loud = NoiseProfile(sigma=50.0, outlier_probability=0.0)

    values = [
        with_noise(50.0, rng=rng, profile=loud, minimum=0.0, maximum=100.0) for _ in range(500)
    ]

    assert all(0.0 <= value <= 100.0 for value in values)
    assert len(set(values)) > 1, "noise should actually vary the value"


def test_outliers_appear_when_the_profile_asks_for_them() -> None:
    rng = random.Random(3)
    with_outliers = NoiseProfile(sigma=0.1, outlier_probability=1.0, outlier_sigma=20.0)

    values = [with_noise(50.0, rng=rng, profile=with_outliers) for _ in range(50)]

    assert max(values) - min(values) > 10.0


def test_the_same_seed_reproduces_the_same_noise() -> None:
    profile = NoiseProfile()

    first = [with_noise(50.0, rng=random.Random(11), profile=profile) for _ in range(20)]
    second = [with_noise(50.0, rng=random.Random(11), profile=profile) for _ in range(20)]

    assert first == second
