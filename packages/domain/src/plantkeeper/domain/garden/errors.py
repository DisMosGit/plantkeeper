"""Garden invariant violations."""

from __future__ import annotations

from plantkeeper.domain.base import DomainError


class GardenError(DomainError):
    """Base class for every Garden rule violation."""


class PlantNameEmptyError(GardenError):
    """A plant must have a name that is not blank."""


class PlantWateringTooSoonError(GardenError):
    """The plant was watered less than ``MIN_WATERING_INTERVAL`` ago."""


class PlantRepottingTooSoonError(GardenError):
    """The plant was repotted less than ``MIN_REPOTTING_INTERVAL`` ago."""


class PlantAlreadyRemovedError(GardenError):
    """The plant no longer belongs to the garden."""


class HouseholdNameEmptyError(GardenError):
    """A household must have a name that is not blank."""


class HouseholdCapacityExceededError(GardenError):
    """The household already owns ``MAX_PLANTS`` plants."""


class HouseholdPlantNotFoundError(GardenError):
    """The household does not own the plant being removed."""
