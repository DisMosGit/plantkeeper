"""The Household aggregate: the family unit that owns the plants."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Final

from plantkeeper.domain.base import AggregateRoot
from plantkeeper.domain.garden.errors import (
    HouseholdCapacityExceededError,
    HouseholdNameEmptyError,
    HouseholdPlantNotFoundError,
)
from plantkeeper.domain.identifiers import HouseholdId, PlantId

MAX_PLANTS: Final = 50
"""The hard cap on how many plants one household may own."""


class Household(AggregateRoot[HouseholdId]):
    """A household and the set of plants it owns.

    The aggregate exists to enforce the cap of :data:`MAX_PLANTS` plants per
    household. It records no events of its own: ``PlantAdded`` carries the plant
    data that downstream contexts need, and the household is saved in the same
    transaction (Phase 2).
    """

    def __init__(
        self,
        household_id: HouseholdId,
        *,
        name: str,
        plant_ids: Iterable[PlantId] = (),
    ) -> None:
        """Rebuild a household from its stored state (no events are recorded)."""
        super().__init__(household_id)
        normalized_name = name.strip()
        if not normalized_name:
            raise HouseholdNameEmptyError("a household name must not be blank")
        self._name = normalized_name
        self._plant_ids = set(plant_ids)
        if len(self._plant_ids) > MAX_PLANTS:
            raise HouseholdCapacityExceededError(
                f"household {self.id} cannot own {len(self._plant_ids)} plants (max {MAX_PLANTS})"
            )

    @classmethod
    def create(cls, *, name: str, household_id: HouseholdId | None = None) -> Household:
        """Create an empty household."""
        return cls(household_id or HouseholdId.new(), name=name)

    @property
    def name(self) -> str:
        """The household's display name."""
        return self._name

    @property
    def plant_ids(self) -> frozenset[PlantId]:
        """The plants owned by this household."""
        return frozenset(self._plant_ids)

    @property
    def plant_count(self) -> int:
        """How many plants the household owns."""
        return len(self._plant_ids)

    def add_plant(self, plant_id: PlantId) -> None:
        """Register a plant, raising :class:`HouseholdCapacityExceededError` at the cap.

        Adding a plant that is already registered is a no-op, so a retried
        command cannot make the household overflow.
        """
        if plant_id in self._plant_ids:
            return
        if len(self._plant_ids) >= MAX_PLANTS:
            raise HouseholdCapacityExceededError(
                f"household {self.id} already owns {MAX_PLANTS} plants"
            )
        self._plant_ids.add(plant_id)

    def remove_plant(self, plant_id: PlantId) -> None:
        """Unregister a plant, raising :class:`HouseholdPlantNotFoundError` if absent."""
        if plant_id not in self._plant_ids:
            raise HouseholdPlantNotFoundError(f"household {self.id} does not own plant {plant_id}")
        self._plant_ids.discard(plant_id)
