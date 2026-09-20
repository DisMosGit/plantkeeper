"""Invariant tests for the Household aggregate."""

from __future__ import annotations

from uuid import uuid4

import pytest

from plantkeeper.domain.garden.errors import (
    HouseholdCapacityExceededError,
    HouseholdNameEmptyError,
    HouseholdPlantNotFoundError,
)
from plantkeeper.domain.garden.household import MAX_PLANTS, Household
from plantkeeper.domain.identifiers import HouseholdId, PlantId


def test_create_strips_the_name() -> None:
    assert Household.create(name="  Home  ").name == "Home"


def test_create_accepts_an_explicit_identifier() -> None:
    household_id = HouseholdId(uuid4())

    assert Household.create(name="Home", household_id=household_id).id == household_id


@pytest.mark.parametrize("name", ["", "   "])
def test_create_rejects_a_blank_name(name: str) -> None:
    with pytest.raises(HouseholdNameEmptyError):
        Household.create(name=name)


def test_household_owns_exactly_the_cap() -> None:
    household = Household.create(name="Home")
    plant_ids = [PlantId(uuid4()) for _ in range(MAX_PLANTS)]

    for plant_id in plant_ids:
        household.add_plant(plant_id)

    assert household.plant_count == MAX_PLANTS
    assert household.plant_ids == frozenset(plant_ids)


def test_the_plant_after_the_cap_is_rejected() -> None:
    household = Household.create(name="Home")
    for _ in range(MAX_PLANTS):
        household.add_plant(PlantId(uuid4()))

    with pytest.raises(HouseholdCapacityExceededError):
        household.add_plant(PlantId(uuid4()))

    assert household.plant_count == MAX_PLANTS


def test_adding_the_same_plant_twice_is_idempotent() -> None:
    household = Household.create(name="Home")
    plant_id = PlantId(uuid4())

    household.add_plant(plant_id)
    household.add_plant(plant_id)

    assert household.plant_count == 1


def test_removing_a_plant_frees_a_slot() -> None:
    household = Household.create(name="Home")
    for _ in range(MAX_PLANTS):
        household.add_plant(PlantId(uuid4()))
    removed = next(iter(household.plant_ids))

    household.remove_plant(removed)
    replacement = PlantId(uuid4())
    household.add_plant(replacement)

    assert household.plant_count == MAX_PLANTS
    assert removed not in household.plant_ids
    assert replacement in household.plant_ids


def test_removing_an_unknown_plant_is_rejected() -> None:
    household = Household.create(name="Home")

    with pytest.raises(HouseholdPlantNotFoundError):
        household.remove_plant(PlantId(uuid4()))


def test_rebuilding_a_household_over_the_cap_is_rejected() -> None:
    too_many = [PlantId(uuid4()) for _ in range(MAX_PLANTS + 1)]

    with pytest.raises(HouseholdCapacityExceededError):
        Household(HouseholdId(uuid4()), name="Home", plant_ids=too_many)


def test_household_records_no_events() -> None:
    household = Household.create(name="Home")

    household.add_plant(PlantId(uuid4()))
    household.remove_plant(next(iter(household.plant_ids)))

    assert household.collect_events() == []


def test_plant_ids_is_an_immutable_snapshot() -> None:
    household = Household.create(name="Home")
    snapshot = household.plant_ids

    household.add_plant(PlantId(uuid4()))

    assert snapshot == frozenset()
    assert household.plant_count == 1
