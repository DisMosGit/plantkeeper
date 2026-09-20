"""Invariant tests for the Species aggregate."""

from __future__ import annotations

from datetime import datetime, timedelta
from uuid import uuid4

import pytest

from plantkeeper.domain.catalog.errors import SpeciesNameEmptyError, SpeciesVersionConflictError
from plantkeeper.domain.catalog.events import (
    SpeciesCacheInvalidated,
    SpeciesSyncRequested,
    SpeciesUpdated,
)
from plantkeeper.domain.catalog.species import Species
from plantkeeper.domain.catalog.values import LightRequirement
from plantkeeper.domain.identifiers import SpeciesId
from plantkeeper.domain.values import WateringInterval

INTERVAL = WateringInterval(value=timedelta(days=7))
LONGER_INTERVAL = WateringInterval(value=timedelta(days=14))


def _create() -> Species:
    return Species.create(
        scientific_name="Monstera deliciosa",
        common_name="Swiss cheese plant",
        watering_interval=INTERVAL,
        light_requirement=LightRequirement.MEDIUM,
    )


def test_create_strips_the_names_and_starts_at_version_one() -> None:
    species = Species.create(
        scientific_name="  Monstera deliciosa  ",
        common_name="  Swiss cheese plant  ",
        watering_interval=INTERVAL,
        light_requirement=LightRequirement.MEDIUM,
    )

    assert species.scientific_name == "Monstera deliciosa"
    assert species.common_name == "Swiss cheese plant"
    assert species.watering_interval == INTERVAL
    assert species.light_requirement == LightRequirement.MEDIUM
    assert species.version == 1


def test_create_accepts_an_explicit_identifier() -> None:
    species_id = SpeciesId(uuid4())

    species = Species.create(
        scientific_name="Monstera deliciosa",
        common_name="Swiss cheese plant",
        watering_interval=INTERVAL,
        light_requirement=LightRequirement.MEDIUM,
        species_id=species_id,
    )

    assert species.id == species_id


@pytest.mark.parametrize("name", ["", "   "])
def test_create_rejects_a_blank_scientific_name(name: str) -> None:
    with pytest.raises(SpeciesNameEmptyError):
        Species.create(
            scientific_name=name,
            common_name="Swiss cheese plant",
            watering_interval=INTERVAL,
            light_requirement=LightRequirement.MEDIUM,
        )


@pytest.mark.parametrize("name", ["", "   "])
def test_create_rejects_a_blank_common_name(name: str) -> None:
    with pytest.raises(SpeciesNameEmptyError):
        Species.create(
            scientific_name="Monstera deliciosa",
            common_name=name,
            watering_interval=INTERVAL,
            light_requirement=LightRequirement.MEDIUM,
        )


def test_create_records_no_events() -> None:
    assert _create().collect_events() == []


def test_rebuilding_a_species_records_no_events() -> None:
    species = Species(
        SpeciesId(uuid4()),
        scientific_name="Monstera deliciosa",
        common_name="Swiss cheese plant",
        watering_interval=INTERVAL,
        light_requirement=LightRequirement.MEDIUM,
        version=4,
    )

    assert species.collect_events() == []
    assert species.version == 4


def test_update_without_changes_returns_false(now: datetime) -> None:
    species = _create()

    changed = species.update(
        scientific_name="Monstera deliciosa",
        common_name="Swiss cheese plant",
        watering_interval=INTERVAL,
        light_requirement=LightRequirement.MEDIUM,
        now=now,
        expected_version=1,
    )

    assert changed is False
    assert species.version == 1
    assert species.collect_events() == []


def test_update_of_the_scientific_name_records_species_updated(now: datetime) -> None:
    species = _create()

    changed = species.update(
        scientific_name="Monstera adansonii",
        common_name="Swiss cheese plant",
        watering_interval=INTERVAL,
        light_requirement=LightRequirement.MEDIUM,
        now=now,
        expected_version=1,
    )

    assert changed is True
    assert species.scientific_name == "Monstera adansonii"
    assert species.version == 2
    events = species.collect_events()
    assert len(events) == 1
    updated = events[0]
    assert isinstance(updated, SpeciesUpdated)
    assert updated.species_id == species.id
    assert updated.scientific_name == "Monstera adansonii"
    assert updated.common_name == "Swiss cheese plant"
    assert updated.watering_interval == INTERVAL
    assert updated.light_requirement == LightRequirement.MEDIUM
    assert updated.version == 2
    assert updated.occurred_at == now


def test_update_of_the_common_name_is_recorded(now: datetime) -> None:
    species = _create()

    changed = species.update(
        scientific_name="Monstera deliciosa",
        common_name="Cheese plant",
        watering_interval=INTERVAL,
        light_requirement=LightRequirement.MEDIUM,
        now=now,
        expected_version=1,
    )

    assert changed is True
    assert species.common_name == "Cheese plant"
    assert isinstance(species.collect_events()[0], SpeciesUpdated)


def test_update_of_the_watering_interval_is_recorded(now: datetime) -> None:
    species = _create()

    changed = species.update(
        scientific_name="Monstera deliciosa",
        common_name="Swiss cheese plant",
        watering_interval=LONGER_INTERVAL,
        light_requirement=LightRequirement.MEDIUM,
        now=now,
        expected_version=1,
    )

    assert changed is True
    assert species.watering_interval == LONGER_INTERVAL
    assert isinstance(species.collect_events()[0], SpeciesUpdated)


def test_update_of_the_light_requirement_is_recorded(now: datetime) -> None:
    species = _create()

    changed = species.update(
        scientific_name="Monstera deliciosa",
        common_name="Swiss cheese plant",
        watering_interval=INTERVAL,
        light_requirement=LightRequirement.HIGH,
        now=now,
        expected_version=1,
    )

    assert changed is True
    assert species.light_requirement == LightRequirement.HIGH
    assert isinstance(species.collect_events()[0], SpeciesUpdated)


def test_update_rejects_a_stale_version(now: datetime) -> None:
    species = _create()

    with pytest.raises(SpeciesVersionConflictError):
        species.update(
            scientific_name="Monstera adansonii",
            common_name="Swiss cheese plant",
            watering_interval=INTERVAL,
            light_requirement=LightRequirement.MEDIUM,
            now=now,
            expected_version=2,
        )

    assert species.scientific_name == "Monstera deliciosa"
    assert species.version == 1
    assert species.collect_events() == []


def test_update_rejects_a_blank_name_without_changing_the_version(now: datetime) -> None:
    species = _create()

    with pytest.raises(SpeciesNameEmptyError):
        species.update(
            scientific_name="   ",
            common_name="Swiss cheese plant",
            watering_interval=INTERVAL,
            light_requirement=LightRequirement.MEDIUM,
            now=now,
            expected_version=1,
        )

    assert species.version == 1
    assert species.scientific_name == "Monstera deliciosa"
    assert species.collect_events() == []


def test_update_rejects_a_blank_common_name(now: datetime) -> None:
    species = _create()

    with pytest.raises(SpeciesNameEmptyError):
        species.update(
            scientific_name="Monstera deliciosa",
            common_name="   ",
            watering_interval=INTERVAL,
            light_requirement=LightRequirement.MEDIUM,
            now=now,
            expected_version=1,
        )


def test_species_sync_requested_is_a_payload_less_trigger() -> None:
    requested = SpeciesSyncRequested()

    assert requested.event_id.version == 7
    assert requested.occurred_at.tzinfo is not None


def test_species_cache_invalidated_carries_the_species_id(now: datetime) -> None:
    species = _create()

    invalidated = SpeciesCacheInvalidated(species_id=species.id, occurred_at=now)

    assert invalidated.species_id == species.id


def test_light_requirement_exposes_three_levels() -> None:
    assert {level.value for level in LightRequirement} == {"low", "medium", "high"}
