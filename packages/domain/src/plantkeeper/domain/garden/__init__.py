"""Garden bounded context: the plants a household owns."""

from __future__ import annotations

from plantkeeper.domain.garden.errors import (
    GardenError,
    HouseholdCapacityExceededError,
    HouseholdNameEmptyError,
    HouseholdPlantNotFoundError,
    PlantAlreadyRemovedError,
    PlantNameEmptyError,
    PlantRepottingTooSoonError,
    PlantWateringTooSoonError,
)
from plantkeeper.domain.garden.events import (
    PlantAdded,
    PlantMoved,
    PlantOnboarded,
    PlantRemoved,
)
from plantkeeper.domain.garden.household import MAX_PLANTS, Household
from plantkeeper.domain.garden.plant import (
    MIN_REPOTTING_INTERVAL,
    MIN_WATERING_INTERVAL,
    Plant,
)

__all__ = [
    "MAX_PLANTS",
    "MIN_REPOTTING_INTERVAL",
    "MIN_WATERING_INTERVAL",
    "GardenError",
    "Household",
    "HouseholdCapacityExceededError",
    "HouseholdNameEmptyError",
    "HouseholdPlantNotFoundError",
    "Plant",
    "PlantAdded",
    "PlantAlreadyRemovedError",
    "PlantMoved",
    "PlantNameEmptyError",
    "PlantOnboarded",
    "PlantRemoved",
    "PlantRepottingTooSoonError",
    "PlantWateringTooSoonError",
]
