"""Contract tests for the Trefle -> domain mapping.

The mapping is where Trefle's vocabulary stops, so it is pinned value by value:
the documented bands, the fallbacks, and the determinism of the identifier that
joins the local catalogue to the upstream one.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from plantkeeper.domain.catalog.values import LightRequirement
from plantkeeper.domain.values import WateringInterval
from plantkeeper.infrastructure.external.trefle.mapping import (
    DEFAULT_WATERING_INTERVAL,
    light_requirement_for,
    record_from_detail,
    species_id_for,
    watering_interval_for,
)
from plantkeeper.infrastructure.external.trefle.models import TrefleGrowth, TrefleSpeciesDetail


@pytest.mark.parametrize(
    ("light", "expected"),
    [
        (None, LightRequirement.MEDIUM),
        (1, LightRequirement.LOW),
        (3, LightRequirement.LOW),
        (4, LightRequirement.MEDIUM),
        (6, LightRequirement.MEDIUM),
        (7, LightRequirement.HIGH),
        (9, LightRequirement.HIGH),
        # Out-of-range values are clamped, not rejected: the feed is external.
        (0, LightRequirement.LOW),
        (99, LightRequirement.HIGH),
    ],
)
def test_light_requirement_follows_the_ellenberg_bands(
    light: int | None, expected: LightRequirement
) -> None:
    assert light_requirement_for(light) is expected


@pytest.mark.parametrize(
    ("soil_humidity", "days"),
    [
        (1, 3),
        (2, 3),
        (3, 5),
        (4, 5),
        (5, 7),
        (6, 7),
        (7, 10),
        (8, 10),
        (9, 14),
        (10, 14),
        (11, 21),
        (12, 21),
        (0, 3),
        (99, 21),
    ],
)
def test_watering_interval_follows_the_soil_humidity_bands(soil_humidity: int, days: int) -> None:
    assert watering_interval_for(soil_humidity) == WateringInterval(value=timedelta(days=days))


def test_a_missing_indicator_falls_back_to_the_weekly_default() -> None:
    expected = WateringInterval(value=timedelta(days=7))

    assert watering_interval_for(None) == expected
    assert expected == DEFAULT_WATERING_INTERVAL


def test_the_identifier_is_a_stable_hash_of_the_slug() -> None:
    first = species_id_for("monstera-deliciosa")

    assert species_id_for("monstera-deliciosa") == first
    assert species_id_for("nephrolepis-exaltata") != first


def a_detail(**overrides: object) -> TrefleSpeciesDetail:
    payload: dict[str, object] = {
        "id": 1,
        "slug": "monstera-deliciosa",
        "scientific_name": "Monstera deliciosa",
        "common_name": "Swiss cheese plant",
        "growth": TrefleGrowth(light=6, soil_humidity=6),
    }
    payload.update(overrides)
    return TrefleSpeciesDetail.model_validate(payload)


def test_a_detail_record_maps_to_a_species_record() -> None:
    record = record_from_detail(a_detail())

    assert record is not None
    assert record.species_id == species_id_for("monstera-deliciosa")
    assert record.scientific_name == "Monstera deliciosa"
    assert record.common_name == "Swiss cheese plant"
    assert record.watering_interval == WateringInterval(value=timedelta(days=7))
    assert record.light_requirement is LightRequirement.MEDIUM


def test_a_missing_common_name_falls_back_to_the_scientific_one() -> None:
    record = record_from_detail(a_detail(common_name=None))

    assert record is not None
    assert record.common_name == "Monstera deliciosa"


def test_a_missing_growth_gets_the_documented_defaults() -> None:
    record = record_from_detail(a_detail(growth=None))

    assert record is not None
    assert record.watering_interval == DEFAULT_WATERING_INTERVAL
    assert record.light_requirement is LightRequirement.MEDIUM


@pytest.mark.parametrize("name", ["", "   "])
def test_a_record_without_a_scientific_name_is_not_a_catalogue_entry(name: str) -> None:
    assert record_from_detail(a_detail(scientific_name=name)) is None


def test_names_are_trimmed() -> None:
    record = record_from_detail(
        a_detail(scientific_name="  Monstera deliciosa  ", common_name="  Cheese  ")
    )

    assert record is not None
    assert record.scientific_name == "Monstera deliciosa"
    assert record.common_name == "Cheese"
