"""Garden aggregate <-> row mapping."""

from __future__ import annotations

from collections.abc import Iterable

from plantkeeper.domain.garden.household import Household
from plantkeeper.domain.garden.plant import Plant
from plantkeeper.domain.identifiers import HouseholdId, PlantId, SpeciesId
from plantkeeper.domain.values import Location
from plantkeeper.infrastructure.persistence.models.garden import HouseholdModel, PlantModel


def plant_to_domain(model: PlantModel) -> Plant:
    """Rebuild the ``Plant`` aggregate from its row."""
    return Plant(
        PlantId(model.id),
        household_id=HouseholdId(model.household_id),
        species_id=SpeciesId(model.species_id),
        name=model.name,
        location=Location(value=model.location),
        added_at=model.added_at,
        last_watered_at=model.last_watered_at,
        last_repotted_at=model.last_repotted_at,
        removed=model.removed,
    )


def plant_to_model(plant: Plant) -> PlantModel:
    """Build the row that represents ``plant``.

    Rebuilding the row rather than mutating a loaded one keeps the mapping in a
    single place: the repository can ``add`` it or ``merge`` it by primary key
    and either way the columns come from here.
    """
    return PlantModel(
        id=plant.id.value,
        household_id=plant.household_id.value,
        species_id=plant.species_id.value,
        name=plant.name,
        location=plant.location.value,
        added_at=plant.added_at,
        last_watered_at=plant.last_watered_at,
        last_repotted_at=plant.last_repotted_at,
        removed=plant.is_removed,
    )


def household_to_domain(model: HouseholdModel, plant_ids: Iterable[PlantId]) -> Household:
    """Rebuild the ``Household`` aggregate from its row and its plants.

    Membership is passed in rather than read from the model: it is derived from
    the plants table so that a household and its plant list cannot disagree.
    """
    return Household(HouseholdId(model.id), name=model.name, plant_ids=plant_ids)


def household_to_model(household: Household) -> HouseholdModel:
    """Build the row that represents ``household``."""
    return HouseholdModel(id=household.id.value, name=household.name)
