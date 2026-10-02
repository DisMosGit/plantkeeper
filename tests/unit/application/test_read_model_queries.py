"""Unit tests for the queries that moved onto the read-model port.

Each handler is exercised with a stub reader and nothing else: the point of the
split is that these questions are answered without a write-side repository, so a
handler that quietly still needed one could not be constructed here at all.

The stub records what it was asked, which is also how the ``include_removed`` flag
and the "end of today" boundary are pinned — they are the query's own arguments,
and passing them through correctly is the whole of the handler's logic.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

import pytest

from plantkeeper.application.errors import NotFoundError
from plantkeeper.application.ports.clock import Clock
from plantkeeper.application.ports.read_models import (
    CareScheduleRow,
    JournalTimelineRow,
    PlantListRow,
    ReadModelReader,
    SpeciesListRow,
)
from plantkeeper.application.queries.care import GetTodayCareQuery, GetTodayCareQueryHandler
from plantkeeper.application.queries.catalog import ListSpeciesQuery, ListSpeciesQueryHandler
from plantkeeper.application.queries.garden import ListPlantsQuery, ListPlantsQueryHandler
from plantkeeper.application.queries.journal import (
    GetJournalTimelineHandler,
    GetJournalTimelineQuery,
)
from plantkeeper.domain.catalog.values import LightRequirement
from plantkeeper.domain.identifiers import (
    HouseholdId,
    JournalEntryId,
    PlantId,
    SpeciesId,
)
from plantkeeper.domain.journal.values import JournalEntryType

NOW = datetime(2026, 3, 1, 9, 30, tzinfo=UTC)
WEEK = timedelta(days=7)
HOUSEHOLD = HouseholdId.new()
PLANT = PlantId.new()
SPECIES = SpeciesId.new()


class FrozenClock:
    """A clock frozen at :data:`NOW`."""

    def now(self) -> datetime:
        """Return the fixed instant."""
        return NOW


class StubReader:
    """A ``ReadModelReader`` that answers with what the test configured.

    Every call is recorded, so an assertion can be about *what was asked* as well
    as about what was answered — which is where the date boundary and the
    ``include_removed`` flag live.
    """

    def __init__(
        self,
        *,
        plants: Sequence[PlantListRow] = (),
        care: Sequence[CareScheduleRow] = (),
        species: Sequence[SpeciesListRow] = (),
        journal: Sequence[JournalTimelineRow] = (),
        plant_exists: bool = True,
    ) -> None:
        self.plants = list(plants)
        self.care = list(care)
        self.species = list(species)
        self.journal = list(journal)
        self.known_plant = plant_exists
        self.plant_queries: list[tuple[HouseholdId, bool]] = []
        self.care_queries: list[tuple[HouseholdId, datetime]] = []
        self.journal_queries: list[PlantId] = []
        self.existence_queries: list[PlantId] = []

    async def list_plants(
        self, household_id: HouseholdId, *, include_removed: bool
    ) -> list[PlantListRow]:
        """Return the configured plants, recording the request."""
        self.plant_queries.append((household_id, include_removed))
        return self.plants

    async def list_due_care(
        self, household_id: HouseholdId, until: datetime
    ) -> list[CareScheduleRow]:
        """Return the configured schedules, recording the request."""
        self.care_queries.append((household_id, until))
        return self.care

    async def list_species(self) -> list[SpeciesListRow]:
        """Return the configured catalogue."""
        return self.species

    async def plant_exists(self, plant_id: PlantId) -> bool:
        """Answer whether the configured plant is known."""
        self.existence_queries.append(plant_id)
        return self.known_plant

    async def list_journal_entries(self, plant_id: PlantId) -> list[JournalTimelineRow]:
        """Return the configured entries, recording the request."""
        self.journal_queries.append(plant_id)
        return self.journal


def a_plant_row(
    *, name: str = "Fern", removed: bool = False, last_watered_at: datetime | None = None
) -> PlantListRow:
    """One projected plant."""
    return PlantListRow(
        plant_id=PLANT,
        household_id=HOUSEHOLD,
        species_id=SPECIES,
        name=name,
        location="Shelf",
        added_at=NOW,
        last_watered_at=last_watered_at,
        removed=removed,
        next_watering_at=NOW + WEEK,
    )


def a_care_row(*, next_watering_at: datetime = NOW) -> CareScheduleRow:
    """One projected schedule."""
    return CareScheduleRow(
        plant_id=PLANT,
        watering_interval=WEEK,
        next_watering_at=next_watering_at,
        last_watered_at=None,
        version=2,
    )


def a_species_row(*, common_name: str = "Boston fern") -> SpeciesListRow:
    """One projected catalogue entry."""
    return SpeciesListRow(
        species_id=SPECIES,
        scientific_name="Nephrolepis exaltata",
        common_name=common_name,
        watering_interval=WEEK,
        light_requirement=LightRequirement.MEDIUM,
        version=1,
    )


def a_journal_row(*, note: str | None = "Watered") -> JournalTimelineRow:
    """One projected journal entry."""
    return JournalTimelineRow(
        entry_id=JournalEntryId.new(),
        plant_id=PLANT,
        entry_type=JournalEntryType.WATERING,
        occurred_at=NOW,
        note=note,
    )


def test_the_stub_satisfies_the_port() -> None:
    """The double is structurally the port, so the handlers cannot drift from it."""
    assert isinstance(StubReader(), ReadModelReader)


async def test_listing_plants_answers_from_the_read_model() -> None:
    reader = StubReader(plants=[a_plant_row()])
    handler = ListPlantsQueryHandler(reader)

    view = await handler.handle(ListPlantsQuery(household_id=HOUSEHOLD))

    assert reader.plant_queries == [(HOUSEHOLD, False)]
    assert len(view.items) == 1
    plant = view.items[0]
    assert plant.plant_id == PLANT
    assert plant.household_id == HOUSEHOLD
    assert plant.species_id == SPECIES
    assert plant.name == "Fern"
    assert plant.location == "Shelf"
    assert plant.added_at == NOW
    assert plant.removed is False


async def test_listing_plants_passes_include_removed_through() -> None:
    reader = StubReader(plants=[a_plant_row(removed=True)])
    handler = ListPlantsQueryHandler(reader)

    view = await handler.handle(ListPlantsQuery(household_id=HOUSEHOLD, include_removed=True))

    assert reader.plant_queries == [(HOUSEHOLD, True)]
    assert view.items[0].removed is True
    assert view.items[0].last_watered_at is None


async def test_today_care_asks_for_the_end_of_the_utc_day() -> None:
    """The boundary is the last moment of the day the clock is in, as documented."""
    reader = StubReader(care=[a_care_row()])
    handler = GetTodayCareQueryHandler(reader, FrozenClock())

    view = await handler.handle(GetTodayCareQuery(household_id=HOUSEHOLD))

    ((household, until),) = reader.care_queries
    assert household == HOUSEHOLD
    assert until == datetime(2026, 3, 1, 23, 59, 59, 999999, tzinfo=UTC)
    assert view.items[0].plant_id == PLANT
    assert view.items[0].watering_interval == WEEK
    assert view.items[0].version == 2


async def test_listing_species_answers_from_the_read_model() -> None:
    reader = StubReader(species=[a_species_row()])
    handler = ListSpeciesQueryHandler(reader)

    view = await handler.handle(ListSpeciesQuery())

    species = view.items[0]
    assert species.species_id == SPECIES
    assert species.common_name == "Boston fern"
    assert species.light_requirement is LightRequirement.MEDIUM
    assert species.watering_interval == WEEK


async def test_the_journal_timeline_answers_from_the_read_model() -> None:
    reader = StubReader(journal=[a_journal_row()])
    handler = GetJournalTimelineHandler(reader)

    view = await handler.handle(GetJournalTimelineQuery(plant_id=PLANT))

    assert reader.existence_queries == [PLANT]
    assert reader.journal_queries == [PLANT]
    entry = view.items[0]
    assert entry.plant_id == PLANT
    assert entry.entry_type is JournalEntryType.WATERING
    assert entry.occurred_at == NOW
    assert entry.note == "Watered"


async def test_the_journal_of_an_unknown_plant_is_not_found() -> None:
    """The 404 is answered by the read model, which is what the query reads."""
    reader = StubReader(plant_exists=False)
    handler = GetJournalTimelineHandler(reader)

    with pytest.raises(NotFoundError):
        await handler.handle(GetJournalTimelineQuery(plant_id=PLANT))

    assert reader.journal_queries == []


def test_the_moved_handlers_need_no_write_side_repository() -> None:
    """Constructing them takes a clock and the read-model port, and nothing else.

    A regression that reintroduced a write-side dependency would fail here rather
    than in production, where the point of the split is that the write side can be
    unreachable while a list is still served.
    """
    clock: Clock = FrozenClock()
    reader = StubReader()

    assert ListPlantsQueryHandler(reader)
    assert GetTodayCareQueryHandler(reader, clock)
    assert ListSpeciesQueryHandler(reader)
    assert GetJournalTimelineHandler(reader)
