"""Repository ports.

One protocol per aggregate that the write side persists. They are deliberately
narrow: a handler asks for the aggregate it needs, never for a query builder,
and the aggregate it gets back is a real domain object on which the invariants
still hold.

``Repository`` is generic over the aggregate and its identifier. The two type
parameters are independent because a type-parameter bound may not reference a
sibling type parameter (PEP 695), so the pairing is a convention made explicit
by every concrete protocol below — never by a bare ``Repository`` in a handler
signature.
"""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable

from plantkeeper.domain.base import Entity
from plantkeeper.domain.care.schedule import CareSchedule
from plantkeeper.domain.catalog.species import Species
from plantkeeper.domain.garden.household import Household
from plantkeeper.domain.garden.plant import Plant
from plantkeeper.domain.identifiers import (
    HouseholdId,
    JournalEntryId,
    NotificationId,
    PlantId,
    SensorId,
    SpeciesId,
    UuidIdentifier,
)
from plantkeeper.domain.journal.entry import JournalEntry
from plantkeeper.domain.notifications.notification import Notification
from plantkeeper.domain.telemetry.sensor import Sensor


@runtime_checkable
class Repository[
    TAggregate: Entity[UuidIdentifier],
    IdT: UuidIdentifier,
](Protocol):
    """The four operations every aggregate repository offers."""

    async def add(self, aggregate: TAggregate) -> None:
        """Insert a new aggregate."""
        ...

    async def get(self, entity_id: IdT) -> TAggregate | None:
        """Return the aggregate, or ``None`` when it does not exist."""
        ...

    async def save(self, aggregate: TAggregate) -> None:
        """Persist the current state of an existing aggregate."""
        ...

    async def delete(self, entity_id: IdT) -> None:
        """Remove the aggregate. Removing something absent is not an error."""
        ...


@runtime_checkable
class PlantRepository(Repository[Plant, PlantId], Protocol):
    """Plants of the Garden context."""

    async def list_by_household(
        self, household_id: HouseholdId, *, include_removed: bool = False
    ) -> list[Plant]:
        """List the household's plants, oldest first."""
        ...


@runtime_checkable
class HouseholdRepository(Repository[Household, HouseholdId], Protocol):
    """Households of the Garden context.

    ``Household`` membership is derived from the plants table rather than stored
    as its own relation, so ``save`` only ever writes the household's own row.
    """


@runtime_checkable
class CareScheduleRepository(Repository[CareSchedule, PlantId], Protocol):
    """Watering schedules of the Care context, keyed by plant."""

    async def list_due(self, household_id: HouseholdId, until: datetime) -> list[CareSchedule]:
        """Schedules of a household that come due at or before ``until``.

        Removed plants are excluded, so a stale schedule cannot show up in
        "what needs watering today".
        """
        ...


@runtime_checkable
class SpeciesRepository(Repository[Species, SpeciesId], Protocol):
    """Catalogue entries of the Catalog context."""

    async def list_all(self) -> list[Species]:
        """List every species in the local catalogue."""
        ...


@runtime_checkable
class SensorRepository(Repository[Sensor, SensorId], Protocol):
    """Sensors of the Telemetry context."""

    async def list_by_plant(self, plant_id: PlantId) -> list[Sensor]:
        """List the sensors bound to one plant."""
        ...


@runtime_checkable
class JournalEntryRepository(Repository[JournalEntry, JournalEntryId], Protocol):
    """Journal entries of the Journal context.

    The write side is append-only: ``delete`` exists only because the generic
    repository has it, and the SQL implementation refuses to remove a journal
    row (see ``docs/domain.md``).
    """

    async def list_by_plant(self, plant_id: PlantId) -> list[JournalEntry]:
        """List a plant's journal entries in chronological order."""
        ...


@runtime_checkable
class NotificationRepository(Repository[Notification, NotificationId], Protocol):
    """Notifications of the Notifications context."""

    async def list_pending(self, household_id: HouseholdId) -> list[Notification]:
        """List the household's unacknowledged notifications, oldest first."""
        ...
