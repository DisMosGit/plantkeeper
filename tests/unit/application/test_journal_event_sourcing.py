"""Tests for the journal's application plumbing.

Two things are pinned here, both without a database:

* the append path — what ``record_journal_entry`` and ``AddJournalEntryHandler``
  write, in what order, and with which expected version;
* the replay path — that ``load_journal_aggregate`` really starts from a snapshot,
  and that a stream which cannot be replayed fails loudly instead of quietly
  dropping a fact.

The fakes are deliberately a small working store rather than a recorder of calls:
a test that appends and then loads exercises the same version arithmetic the
Postgres implementation does.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from types import TracebackType
from typing import Self, cast
from uuid import uuid4

import pytest

from plantkeeper.application.commands.journal import AddJournalEntryCommand, AddJournalEntryHandler
from plantkeeper.application.errors import EventStoreCorruptionError, NotFoundError
from plantkeeper.application.journal import store as journal_store
from plantkeeper.application.journal.store import (
    load_journal_aggregate,
    record_journal_entry,
    watering_entry_id,
)
from plantkeeper.application.ports.event_store import StoredEvent
from plantkeeper.application.ports.unit_of_work import UnitOfWork
from plantkeeper.domain.base import DomainEvent
from plantkeeper.domain.garden.events import PlantAdded
from plantkeeper.domain.garden.plant import Plant
from plantkeeper.domain.identifiers import (
    HouseholdId,
    JournalEntryId,
    PlantId,
    SpeciesId,
)
from plantkeeper.domain.journal.entry import JournalEntry
from plantkeeper.domain.journal.events import JournalEntryAdded
from plantkeeper.domain.journal.state import JournalState
from plantkeeper.domain.journal.values import JournalEntryType
from plantkeeper.domain.values import Location

NOW = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)


class FakeClock:
    """A clock frozen at the instant the test works from."""

    def now(self) -> datetime:
        """Return the fixed instant."""
        return NOW


class FakeEventStore:
    """An in-memory stream that assigns versions the way the real store does."""

    def __init__(self) -> None:
        self.rows: list[StoredEvent] = []
        self.appends: list[tuple[PlantId, int, list[DomainEvent]]] = []
        self.load_calls: list[tuple[PlantId, int]] = []

    async def append(
        self,
        stream_id: PlantId,
        *,
        expected_version: int,
        events: Sequence[DomainEvent],
    ) -> None:
        """Record the append and extend the stream with consecutive versions."""
        self.appends.append((stream_id, expected_version, list(events)))
        position = len(self.rows)
        for offset, event in enumerate(events, start=1):
            position += 1
            self.rows.append(
                StoredEvent(
                    stream_id=stream_id,
                    version=expected_version + offset,
                    global_position=position,
                    event=event,
                )
            )

    async def load_stream(self, stream_id: PlantId, *, after_version: int = 0) -> list[StoredEvent]:
        """Return the stream's tail, remembering what was asked for."""
        self.load_calls.append((stream_id, after_version))
        return [
            row for row in self.rows if row.stream_id == stream_id and row.version > after_version
        ]

    async def load_all(self, *, after_position: int = 0, limit: int = 1000) -> list[StoredEvent]:
        """Return every stream's events in append order."""
        return [row for row in self.rows if row.global_position > after_position][:limit]


class FakeSnapshots:
    """A checkpoint store that keeps the newest state per stream."""

    def __init__(self) -> None:
        self.saved: list[JournalState] = []

    async def latest(self, plant_id: PlantId) -> JournalState | None:
        """Answer with the highest-versioned checkpoint of the plant."""
        candidates = [state for state in self.saved if state.plant_id == plant_id]
        return max(candidates, key=lambda state: state.version) if candidates else None

    async def save(self, state: JournalState) -> None:
        """Keep one more checkpoint."""
        self.saved.append(state)


class FakePlants:
    """A plant repository holding whatever the test seeded."""

    def __init__(self, plant: Plant | None) -> None:
        self.plant = plant

    async def get(self, plant_id: PlantId) -> Plant | None:
        """Return the seeded plant when the id matches."""
        return self.plant if self.plant is not None and self.plant.id == plant_id else None


class FakeJournalEntries:
    """A mirror repository that records what was appended."""

    def __init__(self) -> None:
        self.added: list[JournalEntry] = []

    async def add(self, entry: JournalEntry) -> None:
        """Record one mirror row."""
        self.added.append(entry)


class FakeOutbox:
    """A sink for the events the append path publishes."""

    def __init__(self) -> None:
        self.appended: list[DomainEvent] = []

    async def append(self, event: DomainEvent) -> None:
        """Record one published event."""
        self.appended.append(event)


class FakeUnitOfWork:
    """One transaction over the journal fakes."""

    def __init__(self, plant: Plant | None = None) -> None:
        self.plants = FakePlants(plant)
        self.event_store = FakeEventStore()
        self.journal_snapshots = FakeSnapshots()
        self.journal_entries = FakeJournalEntries()
        self.outbox = FakeOutbox()
        self.commits = 0

    async def __aenter__(self) -> Self:
        """Enter the transaction."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Leave the transaction; the fakes have nothing to roll back."""
        return None

    async def commit(self) -> None:
        """Count the commit."""
        self.commits += 1


def a_plant() -> Plant:
    """One plant for the journal to belong to."""
    return Plant.add(
        household_id=HouseholdId.new(),
        species_id=SpeciesId.new(),
        name="Fern",
        location=Location(value="Shelf"),
        now=NOW,
    )


def a_uow(plant: Plant | None = None) -> FakeUnitOfWork:
    """A fake unit of work with a fixed clock's world."""
    return FakeUnitOfWork(plant)


def as_unit_of_work(uow: FakeUnitOfWork) -> UnitOfWork:
    """Erase the fake's type, the way the other unit tests cast their unit of work."""
    return cast(UnitOfWork, uow)


def an_added(
    plant_id: PlantId,
    *,
    occurred_at: datetime,
    recorded_at: datetime,
    entry_id: JournalEntryId | None = None,
) -> JournalEntryAdded:
    """One ``JournalEntryAdded`` to seed a fake stream with."""
    return JournalEntryAdded(
        entry_id=entry_id or JournalEntryId.new(),
        plant_id=plant_id,
        entry_type=JournalEntryType.WATERING,
        entry_occurred_at=occurred_at,
        occurred_at=recorded_at,
    )


async def test_recording_an_entry_appends_at_the_next_version() -> None:
    plant = a_plant()
    uow = a_uow(plant)

    entry = await record_journal_entry(
        as_unit_of_work(uow),
        plant_id=plant.id,
        entry_type=JournalEntryType.WATERING,
        occurred_at=NOW,
        now=NOW,
    )

    assert len(uow.event_store.appends) == 1
    stream_id, expected_version, events = uow.event_store.appends[0]
    assert stream_id == plant.id
    assert expected_version == 0
    assert len(events) == 1
    assert isinstance(events[0], JournalEntryAdded)
    assert events[0].entry_id == entry.id


async def test_recording_two_entries_continues_the_stream() -> None:
    plant = a_plant()
    uow = a_uow(plant)

    await record_journal_entry(
        as_unit_of_work(uow),
        plant_id=plant.id,
        entry_type=JournalEntryType.WATERING,
        occurred_at=NOW,
        now=NOW,
    )
    await record_journal_entry(
        as_unit_of_work(uow),
        plant_id=plant.id,
        entry_type=JournalEntryType.NOTE,
        occurred_at=NOW,
        now=NOW,
    )

    assert [expected for _, expected, _ in uow.event_store.appends] == [0, 1]
    assert [row.version for row in uow.event_store.rows] == [1, 2]


async def test_recording_an_entry_publishes_the_same_event_to_the_outbox() -> None:
    plant = a_plant()
    uow = a_uow(plant)

    await record_journal_entry(
        as_unit_of_work(uow),
        plant_id=plant.id,
        entry_type=JournalEntryType.WATERING,
        occurred_at=NOW,
        now=NOW,
    )

    assert len(uow.outbox.appended) == 1
    published = uow.outbox.appended[0]
    assert published is uow.event_store.rows[0].event


async def test_recording_an_entry_writes_the_tabular_mirror() -> None:
    plant = a_plant()
    uow = a_uow(plant)

    entry = await record_journal_entry(
        as_unit_of_work(uow),
        plant_id=plant.id,
        entry_type=JournalEntryType.WATERING,
        occurred_at=NOW,
        now=NOW,
    )

    assert uow.journal_entries.added == [entry]


async def test_recording_an_entry_that_is_already_stored_changes_nothing() -> None:
    """An append-only stream has no upsert: the same fact is recorded once."""
    plant = a_plant()
    uow = a_uow(plant)
    entry = await record_journal_entry(
        as_unit_of_work(uow),
        plant_id=plant.id,
        entry_type=JournalEntryType.WATERING,
        occurred_at=NOW,
        now=NOW,
    )

    again = await record_journal_entry(
        as_unit_of_work(uow),
        plant_id=plant.id,
        entry_type=JournalEntryType.WATERING,
        occurred_at=NOW,
        now=NOW,
        entry_id=entry.id,
    )

    assert again.id == entry.id
    assert len(uow.event_store.rows) == 1
    assert len(uow.outbox.appended) == 1
    assert uow.journal_entries.added == [entry]


def test_a_derived_watering_id_is_stable_and_specific() -> None:
    plant_id = PlantId(uuid4())
    other = PlantId(uuid4())

    assert watering_entry_id(plant_id, NOW) == watering_entry_id(plant_id, NOW)
    assert watering_entry_id(plant_id, NOW) != watering_entry_id(other, NOW)
    assert watering_entry_id(plant_id, NOW) != watering_entry_id(
        plant_id, NOW + timedelta(seconds=1)
    )


async def test_a_replayed_watering_is_not_journalled_twice() -> None:
    """A rebuilt consumer group replays an event whose entry already exists."""
    plant = a_plant()
    uow = a_uow(plant)
    entry_id = watering_entry_id(plant.id, NOW)

    first = await record_journal_entry(
        as_unit_of_work(uow),
        plant_id=plant.id,
        entry_type=JournalEntryType.WATERING,
        occurred_at=NOW,
        now=NOW,
        entry_id=entry_id,
    )
    second = await record_journal_entry(
        as_unit_of_work(uow),
        plant_id=plant.id,
        entry_type=JournalEntryType.WATERING,
        occurred_at=NOW,
        now=NOW,
        entry_id=entry_id,
    )

    assert first.id == second.id == entry_id
    assert [row.version for row in uow.event_store.rows] == [1]


async def test_recording_does_not_commit_on_its_own() -> None:
    """The caller owns the transaction; the consumer and the handler open it differently."""
    plant = a_plant()
    uow = a_uow(plant)

    await record_journal_entry(
        as_unit_of_work(uow),
        plant_id=plant.id,
        entry_type=JournalEntryType.WATERING,
        occurred_at=NOW,
        now=NOW,
    )

    assert uow.commits == 0


async def test_the_loader_uses_the_snapshot_and_reads_only_the_tail() -> None:
    plant = a_plant()
    uow = a_uow(plant)
    first = an_added(plant.id, occurred_at=NOW, recorded_at=NOW)
    second = an_added(plant.id, occurred_at=NOW + timedelta(days=1), recorded_at=NOW)
    await uow.event_store.append(plant.id, expected_version=0, events=[first, second])
    await uow.journal_snapshots.save(
        JournalState(
            plant_id=plant.id,
            version=1,
            entries=(),
        )
    )

    aggregate = await load_journal_aggregate(
        event_store=uow.event_store,
        snapshots=uow.journal_snapshots,
        plant_id=plant.id,
    )

    assert uow.event_store.load_calls == [(plant.id, 1)]
    assert [entry.id for entry in aggregate.entries] == [second.entry_id]


async def test_a_snapshot_replays_to_the_same_state_as_the_whole_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plant = a_plant()
    monkeypatch.setattr(journal_store, "SNAPSHOT_EVERY", 1)
    uow = a_uow(plant)
    await record_journal_entry(
        as_unit_of_work(uow),
        plant_id=plant.id,
        entry_type=JournalEntryType.WATERING,
        occurred_at=NOW,
        now=NOW,
    )
    assert uow.journal_snapshots.saved, "an interval of one checkpoints every entry"

    await record_journal_entry(
        as_unit_of_work(uow),
        plant_id=plant.id,
        entry_type=JournalEntryType.NOTE,
        occurred_at=NOW + timedelta(days=1),
        now=NOW,
    )
    from_snapshot = await load_journal_aggregate(
        event_store=uow.event_store,
        snapshots=uow.journal_snapshots,
        plant_id=plant.id,
    )

    plain_store = FakeEventStore()
    plain_store.rows = list(uow.event_store.rows)
    from_scratch = await load_journal_aggregate(
        event_store=plain_store,
        snapshots=FakeSnapshots(),
        plant_id=plant.id,
    )

    assert [entry.id for entry in from_snapshot.entries] == [
        entry.id for entry in from_scratch.entries
    ]
    assert from_snapshot.version == from_scratch.version


async def test_a_version_gap_is_corruption() -> None:
    plant = a_plant()
    uow = a_uow(plant)
    await uow.event_store.append(
        plant.id, expected_version=0, events=[an_added(plant.id, occurred_at=NOW, recorded_at=NOW)]
    )
    uow.event_store.rows.append(
        StoredEvent(
            stream_id=plant.id,
            version=7,
            global_position=99,
            event=an_added(plant.id, occurred_at=NOW, recorded_at=NOW),
        )
    )

    with pytest.raises(EventStoreCorruptionError):
        await load_journal_aggregate(
            event_store=uow.event_store,
            snapshots=uow.journal_snapshots,
            plant_id=plant.id,
        )


async def test_a_stream_holding_another_contexts_event_is_corruption() -> None:
    plant = a_plant()
    uow = a_uow(plant)
    foreign = PlantAdded(
        plant_id=plant.id,
        household_id=plant.household_id,
        species_id=plant.species_id,
        name="Fern",
        location=Location(value="Shelf"),
        added_at=NOW,
        occurred_at=NOW,
    )
    uow.event_store.rows.append(
        StoredEvent(stream_id=plant.id, version=1, global_position=1, event=foreign)
    )

    with pytest.raises(EventStoreCorruptionError):
        await load_journal_aggregate(
            event_store=uow.event_store,
            snapshots=uow.journal_snapshots,
            plant_id=plant.id,
        )


async def test_the_handler_appends_publishes_mirrors_and_commits_once() -> None:
    plant = a_plant()
    uow = a_uow(plant)
    handler = AddJournalEntryHandler(as_unit_of_work(uow), FakeClock())

    view = await handler.handle(
        AddJournalEntryCommand(
            plant_id=plant.id,
            entry_type=JournalEntryType.NOTE,
            note="  watch the leaves  ",
        )
    )

    assert uow.commits == 1
    assert uow.event_store.appends[0][1] == 0
    assert view.plant_id == plant.id
    assert view.entry_type is JournalEntryType.NOTE
    assert view.note == "watch the leaves"
    assert view.occurred_at == NOW
    assert view.entry_id == uow.journal_entries.added[0].id


async def test_the_handler_keeps_the_care_moment_the_caller_gave() -> None:
    plant = a_plant()
    uow = a_uow(plant)
    handler = AddJournalEntryHandler(as_unit_of_work(uow), FakeClock())
    cared_for = NOW - timedelta(days=5)

    view = await handler.handle(
        AddJournalEntryCommand(
            plant_id=plant.id,
            entry_type=JournalEntryType.REPOTTING,
            occurred_at=cared_for,
        )
    )

    assert view.occurred_at == cared_for


async def test_the_handler_honours_the_callers_entry_id() -> None:
    plant = a_plant()
    uow = a_uow(plant)
    handler = AddJournalEntryHandler(as_unit_of_work(uow), FakeClock())
    entry_id = JournalEntryId(uuid4())

    view = await handler.handle(
        AddJournalEntryCommand(
            plant_id=plant.id,
            entry_type=JournalEntryType.NOTE,
            entry_id=entry_id,
        )
    )

    assert view.entry_id == entry_id


async def test_the_handler_refuses_a_plant_that_does_not_exist() -> None:
    uow = a_uow(None)
    handler = AddJournalEntryHandler(as_unit_of_work(uow), FakeClock())

    with pytest.raises(NotFoundError):
        await handler.handle(
            AddJournalEntryCommand(plant_id=PlantId(uuid4()), entry_type=JournalEntryType.NOTE)
        )

    assert uow.event_store.appends == []
    assert uow.commits == 0


async def test_an_empty_stream_replays_to_an_empty_aggregate() -> None:
    plant = a_plant()
    uow = a_uow(plant)

    aggregate = await load_journal_aggregate(
        event_store=uow.event_store,
        snapshots=uow.journal_snapshots,
        plant_id=plant.id,
    )

    assert aggregate.version == 0
    assert aggregate.entries == ()
