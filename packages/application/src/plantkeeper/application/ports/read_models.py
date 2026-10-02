"""The read-model query port: the questions the query side answers from the read side.

CQRS splits the questions in two. A command's answer — and every read a command
needs to enforce its own rules — stays on the write store, so a successful command
is never invisible to the caller. A question about *what the system looks like* —
a list, a browsable timeline, a report — is answered here, from the read models the
projections maintain on the read side's own database.

The port answers with flat rows, not aggregates: a read model is already a
projection of the facts, and rebuilding an aggregate from it would only add a
second way for the two answers to disagree. A row carries exactly the columns the
question needs, and the handler turns it into the view the API answers with.

Nothing here decides *when* a read model is up to date. The projections are
asynchronous, so a list served moments after a command may not show that command's
effect yet; that window is the price of the split, it is documented in
``docs/cqrs.md``, and a client tolerates it.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Protocol, runtime_checkable

from pydantic import AwareDatetime, BaseModel, ConfigDict

from plantkeeper.domain.catalog.values import LightRequirement
from plantkeeper.domain.identifiers import (
    HouseholdId,
    JournalEntryId,
    PlantId,
    SpeciesId,
)
from plantkeeper.domain.journal.values import JournalEntryType

_ROW_CONFIG = ConfigDict(frozen=True, extra="forbid")


class ReadRow(BaseModel):
    """Base class for the rows a read model answers with."""

    model_config = _ROW_CONFIG


class PlantListRow(ReadRow):
    """One plant of a household, as the read side knows it.

    A row is listed only once the Garden projection has filled the columns a plant
    is rendered from (name, location, household, species, ``added_at``). A care or
    journal event can reach the read side before the ``PlantAdded`` that names the
    plant, leaving a placeholder row behind; it becomes listable when that event
    arrives.
    """

    plant_id: PlantId
    household_id: HouseholdId
    species_id: SpeciesId
    name: str
    location: str
    added_at: AwareDatetime
    last_watered_at: AwareDatetime | None
    removed: bool
    next_watering_at: AwareDatetime | None


class CareScheduleRow(ReadRow):
    """One plant's watering schedule, as the read side knows it."""

    plant_id: PlantId
    watering_interval: timedelta
    next_watering_at: AwareDatetime
    last_watered_at: AwareDatetime | None
    version: int


class SpeciesListRow(ReadRow):
    """One catalogue entry, as the read side knows it."""

    species_id: SpeciesId
    scientific_name: str
    common_name: str
    watering_interval: timedelta
    light_requirement: LightRequirement
    version: int


class JournalTimelineRow(ReadRow):
    """One journal entry of a plant, as the read side knows it."""

    entry_id: JournalEntryId
    plant_id: PlantId
    entry_type: JournalEntryType
    occurred_at: AwareDatetime
    note: str | None


@runtime_checkable
class ReadModelReader(Protocol):
    """The read models, as the list and report queries ask for them.

    Every method is a plain read of the read side's database. Implementations must
    keep the connection read-only and must not write: the read side has exactly one
    writer per table, and it is a projection.
    """

    async def list_plants(
        self, household_id: HouseholdId, *, include_removed: bool
    ) -> list[PlantListRow]:
        """List a household's plants, oldest first."""
        ...

    async def list_due_care(
        self, household_id: HouseholdId, until: datetime
    ) -> list[CareScheduleRow]:
        """List a household's schedules due at or before ``until``, soonest first.

        Removed plants are excluded, exactly as the write-side query excludes them:
        a stale schedule must not show up in "what needs watering today".
        """
        ...

    async def list_species(self) -> list[SpeciesListRow]:
        """List every catalogue entry, by scientific name."""
        ...

    async def plant_exists(self, plant_id: PlantId) -> bool:
        """Whether the read side knows the plant.

        Used for the "unknown plant is a 404" rule on a read-model answer. Like
        every read here it shares the projections' staleness window: a plant that
        was created a moment ago and has not been projected yet reads as absent.
        """
        ...

    async def list_journal_entries(self, plant_id: PlantId) -> list[JournalTimelineRow]:
        """List one plant's journal entries, in the order the care happened."""
        ...
