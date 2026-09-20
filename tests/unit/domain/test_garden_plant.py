"""Invariant tests for the Plant aggregate."""

from __future__ import annotations

from datetime import datetime, timedelta
from uuid import uuid4

import pytest

from plantkeeper.domain.garden.errors import (
    PlantAlreadyRemovedError,
    PlantNameEmptyError,
    PlantRepottingTooSoonError,
    PlantWateringTooSoonError,
)
from plantkeeper.domain.garden.events import PlantAdded, PlantMoved, PlantRemoved
from plantkeeper.domain.garden.plant import (
    MIN_REPOTTING_INTERVAL,
    MIN_WATERING_INTERVAL,
    Plant,
)
from plantkeeper.domain.identifiers import HouseholdId, PlantId, SpeciesId
from plantkeeper.domain.values import Location

LIVING_ROOM = Location(value="Living room")
BALCONY = Location(value="Balcony")


def _add(now: datetime, *, name: str = "Monstera") -> Plant:
    return Plant.add(
        household_id=HouseholdId(uuid4()),
        species_id=SpeciesId(uuid4()),
        name=name,
        location=LIVING_ROOM,
        now=now,
    )


def test_add_records_plant_added(now: datetime) -> None:
    plant = _add(now)

    events = plant.collect_events()

    assert len(events) == 1
    added = events[0]
    assert isinstance(added, PlantAdded)
    assert added.plant_id == plant.id
    assert added.household_id == plant.household_id
    assert added.species_id == plant.species_id
    assert added.name == "Monstera"
    assert added.location == LIVING_ROOM
    assert added.added_at == now
    assert added.occurred_at == now
    assert plant.added_at == now
    assert plant.last_watered_at is None
    assert plant.last_repotted_at is None
    assert plant.is_removed is False


def test_add_strips_the_name(now: datetime) -> None:
    assert _add(now, name="  Monstera  ").name == "Monstera"


def test_add_accepts_an_explicit_identifier(now: datetime) -> None:
    plant_id = PlantId(uuid4())

    plant = Plant.add(
        household_id=HouseholdId(uuid4()),
        species_id=SpeciesId(uuid4()),
        name="Monstera",
        location=LIVING_ROOM,
        now=now,
        plant_id=plant_id,
    )

    assert plant.id == plant_id


@pytest.mark.parametrize("name", ["", "   "])
def test_add_rejects_a_blank_name(now: datetime, name: str) -> None:
    with pytest.raises(PlantNameEmptyError):
        _add(now, name=name)


def test_rebuilding_a_plant_records_no_events(now: datetime) -> None:
    plant = Plant(
        PlantId(uuid4()),
        household_id=HouseholdId(uuid4()),
        species_id=SpeciesId(uuid4()),
        name="Monstera",
        location=LIVING_ROOM,
        added_at=now,
    )

    assert plant.collect_events() == []


def test_water_is_allowed_exactly_one_interval_later(now: datetime) -> None:
    plant = _add(now)
    plant.water(now=now)

    later = now + MIN_WATERING_INTERVAL
    plant.water(now=later)

    assert plant.last_watered_at == later


def test_water_too_soon_is_rejected_and_changes_nothing(now: datetime) -> None:
    plant = _add(now)
    plant.water(now=now)

    with pytest.raises(PlantWateringTooSoonError):
        plant.water(now=now + MIN_WATERING_INTERVAL - timedelta(seconds=1))

    assert plant.last_watered_at == now


def test_water_records_no_garden_event(now: datetime) -> None:
    plant = _add(now)
    plant.collect_events()

    plant.water(now=now)

    assert plant.collect_events() == []


def test_repot_is_allowed_exactly_one_interval_later(now: datetime) -> None:
    plant = _add(now)
    plant.repot(now=now)

    later = now + MIN_REPOTTING_INTERVAL
    plant.repot(now=later)

    assert plant.last_repotted_at == later


def test_repot_too_soon_is_rejected_and_changes_nothing(now: datetime) -> None:
    plant = _add(now)
    plant.repot(now=now)

    with pytest.raises(PlantRepottingTooSoonError):
        plant.repot(now=now + MIN_REPOTTING_INTERVAL - timedelta(days=1))

    assert plant.last_repotted_at == now


def test_repot_records_no_garden_event(now: datetime) -> None:
    plant = _add(now)
    plant.collect_events()

    plant.repot(now=now)

    assert plant.collect_events() == []


def test_move_records_the_previous_location(now: datetime) -> None:
    plant = _add(now)
    plant.collect_events()

    plant.move(BALCONY, now=now)

    events = plant.collect_events()
    assert len(events) == 1
    moved = events[0]
    assert isinstance(moved, PlantMoved)
    assert moved.plant_id == plant.id
    assert moved.previous_location == LIVING_ROOM
    assert moved.location == BALCONY
    assert moved.occurred_at == now
    assert plant.location == BALCONY


def test_move_to_the_same_location_is_a_no_op(now: datetime) -> None:
    plant = _add(now)
    plant.collect_events()

    plant.move(Location(value="Living room"), now=now)

    assert plant.collect_events() == []
    assert plant.location == LIVING_ROOM


def test_remove_records_plant_removed(now: datetime) -> None:
    plant = _add(now)
    plant.collect_events()

    plant.remove(now=now)

    events = plant.collect_events()
    assert len(events) == 1
    removed = events[0]
    assert isinstance(removed, PlantRemoved)
    assert removed.plant_id == plant.id
    assert removed.removed_at == now
    assert removed.occurred_at == now
    assert plant.is_removed is True


def test_removing_twice_is_rejected(now: datetime) -> None:
    plant = _add(now)
    plant.remove(now=now)

    with pytest.raises(PlantAlreadyRemovedError):
        plant.remove(now=now)


def test_a_removed_plant_cannot_be_mutated(now: datetime) -> None:
    plant = _add(now)
    plant.remove(now=now)

    with pytest.raises(PlantAlreadyRemovedError):
        plant.water(now=now)
    with pytest.raises(PlantAlreadyRemovedError):
        plant.repot(now=now)
    with pytest.raises(PlantAlreadyRemovedError):
        plant.move(BALCONY, now=now)
