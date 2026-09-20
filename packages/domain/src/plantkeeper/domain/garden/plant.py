"""The Plant aggregate: one plant in a household's garden."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Final

from plantkeeper.domain.base import AggregateRoot
from plantkeeper.domain.garden.errors import (
    PlantAlreadyRemovedError,
    PlantNameEmptyError,
    PlantRepottingTooSoonError,
    PlantWateringTooSoonError,
)
from plantkeeper.domain.garden.events import PlantAdded, PlantMoved, PlantRemoved
from plantkeeper.domain.identifiers import HouseholdId, PlantId, SpeciesId
from plantkeeper.domain.values import Location

MIN_WATERING_INTERVAL: Final = timedelta(hours=1)
"""Watering twice within this window is a rule violation."""

MIN_REPOTTING_INTERVAL: Final = timedelta(days=182)
"""Roughly six months; repotting more often than this is a rule violation."""


class Plant(AggregateRoot[PlantId]):
    """A physical plant, identified by its :class:`PlantId`.

    The aggregate owns the care rules that do not depend on a species schedule:
    the name must not be blank, watering twice within an hour is refused, and
    repotting more often than every six months is refused.

    ``water`` and ``repot`` record no Garden event on purpose: the watering fact
    belongs to Care (``WateringCompleted``) and the repotting fact to Journal.
    They only advance the timestamps the Garden invariants compare against.
    """

    def __init__(
        self,
        plant_id: PlantId,
        *,
        household_id: HouseholdId,
        species_id: SpeciesId,
        name: str,
        location: Location,
        added_at: datetime,
        last_watered_at: datetime | None = None,
        last_repotted_at: datetime | None = None,
        removed: bool = False,
    ) -> None:
        """Rebuild a plant from its stored state (no events are recorded).

        Use :meth:`add` when the plant is being created: only then does the
        aggregate raise :class:`~plantkeeper.domain.garden.events.PlantAdded`.
        """
        super().__init__(plant_id)
        normalized_name = name.strip()
        if not normalized_name:
            raise PlantNameEmptyError("a plant name must not be blank")
        self._household_id = household_id
        self._species_id = species_id
        self._name = normalized_name
        self._location = location
        self._added_at = added_at
        self._last_watered_at = last_watered_at
        self._last_repotted_at = last_repotted_at
        self._removed = removed

    @classmethod
    def add(
        cls,
        *,
        household_id: HouseholdId,
        species_id: SpeciesId,
        name: str,
        location: Location,
        now: datetime,
        plant_id: PlantId | None = None,
    ) -> Plant:
        """Create a plant and record :class:`PlantAdded`."""
        plant = cls(
            plant_id or PlantId.new(),
            household_id=household_id,
            species_id=species_id,
            name=name,
            location=location,
            added_at=now,
        )
        plant._record(
            PlantAdded(
                plant_id=plant.id,
                household_id=household_id,
                species_id=species_id,
                name=plant.name,
                location=location,
                added_at=now,
                occurred_at=now,
            )
        )
        return plant

    @property
    def household_id(self) -> HouseholdId:
        """The household that owns this plant."""
        return self._household_id

    @property
    def species_id(self) -> SpeciesId:
        """The catalogue species this plant belongs to."""
        return self._species_id

    @property
    def name(self) -> str:
        """The household's name for the plant."""
        return self._name

    @property
    def location(self) -> Location:
        """Where the plant currently stands."""
        return self._location

    @property
    def added_at(self) -> datetime:
        """When the plant joined the garden."""
        return self._added_at

    @property
    def last_watered_at(self) -> datetime | None:
        """When the plant was last watered, if it ever was."""
        return self._last_watered_at

    @property
    def last_repotted_at(self) -> datetime | None:
        """When the plant was last repotted, if it ever was."""
        return self._last_repotted_at

    @property
    def is_removed(self) -> bool:
        """Whether the plant has been removed from the garden."""
        return self._removed

    def water(self, *, now: datetime) -> None:
        """Record that the plant was watered at ``now``.

        Raises :class:`PlantWateringTooSoonError` when the previous watering is
        less than :data:`MIN_WATERING_INTERVAL` ago.
        """
        self._ensure_active()
        if (
            self._last_watered_at is not None
            and now - self._last_watered_at < MIN_WATERING_INTERVAL
        ):
            raise PlantWateringTooSoonError(
                f"plant {self.id} was already watered at {self._last_watered_at.isoformat()}"
            )
        self._last_watered_at = now

    def repot(self, *, now: datetime) -> None:
        """Record that the plant was repotted at ``now``.

        Raises :class:`PlantRepottingTooSoonError` when the previous repotting is
        less than :data:`MIN_REPOTTING_INTERVAL` ago.
        """
        self._ensure_active()
        if (
            self._last_repotted_at is not None
            and now - self._last_repotted_at < MIN_REPOTTING_INTERVAL
        ):
            raise PlantRepottingTooSoonError(
                f"plant {self.id} was already repotted at {self._last_repotted_at.isoformat()}"
            )
        self._last_repotted_at = now

    def move(self, location: Location, *, now: datetime) -> None:
        """Move the plant and record :class:`PlantMoved`.

        Moving to the current location is a no-op: it changes nothing and
        records nothing.
        """
        self._ensure_active()
        if location == self._location:
            return
        previous_location = self._location
        self._location = location
        self._record(
            PlantMoved(
                plant_id=self.id,
                previous_location=previous_location,
                location=location,
                occurred_at=now,
            )
        )

    def remove(self, *, now: datetime) -> None:
        """Remove the plant from the garden and record :class:`PlantRemoved`."""
        self._ensure_active()
        self._removed = True
        self._record(PlantRemoved(plant_id=self.id, removed_at=now, occurred_at=now))

    def _ensure_active(self) -> None:
        if self._removed:
            raise PlantAlreadyRemovedError(f"plant {self.id} was already removed")
