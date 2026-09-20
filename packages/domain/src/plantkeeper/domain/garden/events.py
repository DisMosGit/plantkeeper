"""Domain events published by the Garden context."""

from __future__ import annotations

from pydantic import AwareDatetime

from plantkeeper.domain.base import DomainEvent
from plantkeeper.domain.identifiers import HouseholdId, PlantId, SpeciesId
from plantkeeper.domain.values import Location


class PlantAdded(DomainEvent):
    """A plant was added to the household's garden."""

    plant_id: PlantId
    household_id: HouseholdId
    species_id: SpeciesId
    name: str
    location: Location
    added_at: AwareDatetime


class PlantRemoved(DomainEvent):
    """A plant was removed from the household's garden."""

    plant_id: PlantId
    removed_at: AwareDatetime


class PlantMoved(DomainEvent):
    """A plant was moved to a different location."""

    plant_id: PlantId
    previous_location: Location
    location: Location


class PlantOnboarded(DomainEvent):
    """The onboarding saga finished for a plant.

    Published by ``OnboardPlantSaga`` (Phase 4) once the care schedule and the
    first reminder exist; the ``Plant`` aggregate does not raise it itself.
    """

    plant_id: PlantId
    household_id: HouseholdId
    species_id: SpeciesId
    next_watering_at: AwareDatetime
