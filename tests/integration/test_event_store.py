"""Integration tests for the Journal's event store.

These run against Postgres because the two things worth proving are database
properties: ``(stream_id, version)`` really does reject the second writer of a
version, and ``global_position`` really does give ``load_all`` an append order. The
replay itself is unit-tested with fakes; what is pinned here is that the store the
project ships behaves the way those fakes assume.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from plantkeeper.application.errors import EventStoreConcurrencyError
from plantkeeper.application.journal import store as journal_store
from plantkeeper.application.journal.store import load_journal_aggregate, record_journal_entry
from plantkeeper.domain.identifiers import JournalEntryId, PlantId
from plantkeeper.domain.journal.events import JournalEntryAdded
from plantkeeper.domain.journal.state import JournalState
from plantkeeper.domain.journal.values import JournalEntryType
from plantkeeper.infrastructure.persistence.uow import SqlAlchemyUnitOfWork

pytestmark = pytest.mark.integration

NOW = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)


def an_added(
    plant_id: PlantId,
    *,
    occurred_at: datetime = NOW,
    entry_id: JournalEntryId | None = None,
) -> JournalEntryAdded:
    """One ``JournalEntryAdded`` for ``plant_id``."""
    return JournalEntryAdded(
        entry_id=entry_id or JournalEntryId.new(),
        plant_id=plant_id,
        entry_type=JournalEntryType.WATERING,
        entry_occurred_at=occurred_at,
        occurred_at=NOW,
    )


async def store_events(
    session_factory: async_sessionmaker[AsyncSession],
    plant_id: PlantId,
    events: list[JournalEntryAdded],
    *,
    expected_version: int = 0,
) -> None:
    """Append ``events`` in one committed transaction."""
    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        async with uow:
            await uow.event_store.append(plant_id, expected_version=expected_version, events=events)
            await uow.commit()


async def test_an_appended_stream_reads_back_in_version_order(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    plant_id = PlantId.new()
    first = an_added(plant_id)
    second = an_added(plant_id, occurred_at=NOW + timedelta(days=1))

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        async with uow:
            await uow.event_store.append(plant_id, expected_version=0, events=[first, second])
            await uow.commit()

    async with session_factory() as session:
        stored = await SqlAlchemyUnitOfWork(session).event_store.load_stream(plant_id)

    assert [row.version for row in stored] == [1, 2]
    assert [row.event for row in stored] == [first, second]
    assert stored[0].global_position < stored[1].global_position
    assert [row.stream_id for row in stored] == [plant_id, plant_id]


async def test_loading_after_a_version_returns_only_the_tail(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    plant_id = PlantId.new()
    await store_events(session_factory, plant_id, [an_added(plant_id), an_added(plant_id)])

    async with session_factory() as session:
        stored = await SqlAlchemyUnitOfWork(session).event_store.load_stream(
            plant_id, after_version=1
        )

    assert [row.version for row in stored] == [2]


async def test_load_all_orders_every_stream_by_append_position(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    first_plant = PlantId.new()
    second_plant = PlantId.new()
    first = an_added(first_plant)
    second = an_added(second_plant)
    third = an_added(first_plant)

    await store_events(session_factory, first_plant, [first])
    await store_events(session_factory, second_plant, [second])
    await store_events(session_factory, first_plant, [third], expected_version=1)

    async with session_factory() as session:
        everything = await SqlAlchemyUnitOfWork(session).event_store.load_all()

    positions = [row.global_position for row in everything]
    assert [row.event for row in everything] == [first, second, third]
    assert positions == sorted(positions)

    # Positional paging starts from the store's own positions: the shared database
    # fixture truncates without restarting the identity sequence, so the first append
    # of a test is not necessarily position 1.
    async with session_factory() as session:
        later = await SqlAlchemyUnitOfWork(session).event_store.load_all(
            after_position=positions[1]
        )

    assert [row.event for row in later] == [third]


async def test_load_all_honours_its_limit(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    plant_id = PlantId.new()
    await store_events(session_factory, plant_id, [an_added(plant_id), an_added(plant_id)])

    async with session_factory() as session:
        page = await SqlAlchemyUnitOfWork(session).event_store.load_all(limit=1)

    assert len(page) == 1
    assert page[0].version == 1


async def test_a_lost_race_for_a_version_is_a_concurrency_error(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    plant_id = PlantId.new()
    await store_events(session_factory, plant_id, [an_added(plant_id)])

    with pytest.raises(EventStoreConcurrencyError):
        await store_events(session_factory, plant_id, [an_added(plant_id)], expected_version=0)

    async with session_factory() as session:
        stored = await SqlAlchemyUnitOfWork(session).event_store.load_stream(plant_id)

    assert [row.version for row in stored] == [1], "the failed append left nothing behind"


async def test_appending_the_same_event_twice_is_refused(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    plant_id = PlantId.new()
    event = an_added(plant_id)
    await store_events(session_factory, plant_id, [event])

    with pytest.raises(EventStoreConcurrencyError):
        await store_events(session_factory, plant_id, [event], expected_version=1)


async def test_snapshots_round_trip_and_latest_wins(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    plant_id = PlantId.new()
    earlier = JournalState(plant_id=plant_id, version=1)
    later = JournalState(plant_id=plant_id, version=4)
    other = JournalState(plant_id=PlantId.new(), version=9)

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        async with uow:
            await uow.journal_snapshots.save(earlier)
            await uow.journal_snapshots.save(later)
            await uow.journal_snapshots.save(other)
            await uow.commit()

    async with session_factory() as session:
        latest = await SqlAlchemyUnitOfWork(session).journal_snapshots.latest(plant_id)

    assert latest == later


async def test_an_empty_stream_has_no_snapshot(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        latest = await SqlAlchemyUnitOfWork(session).journal_snapshots.latest(PlantId.new())

    assert latest is None


async def test_recording_an_entry_stores_the_event_the_outbox_row_and_the_mirror(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    plant_id = PlantId.new()

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        async with uow:
            entry = await record_journal_entry(
                uow,
                plant_id=plant_id,
                entry_type=JournalEntryType.WATERING,
                occurred_at=NOW,
                now=NOW,
                note="Watered",
            )
            await uow.commit()
        entry_id = entry.id

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        stored = await uow.event_store.load_stream(plant_id)
        mirror = await uow.journal_entries.get(entry_id)
        events = await uow.outbox.fetch_unpublished(limit=10)

    assert [row.version for row in stored] == [1]
    assert isinstance(stored[0].event, JournalEntryAdded)
    assert stored[0].event.note == "Watered"
    assert mirror is not None
    assert mirror.note == "Watered"
    assert [message.event_name for message in events] == ["JournalEntryAdded"]


async def test_an_automatic_snapshot_lets_the_loader_read_only_the_tail(
    session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plant_id = PlantId.new()
    monkeypatch.setattr(journal_store, "SNAPSHOT_EVERY", 2)

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        async with uow:
            for _ in range(2):
                await record_journal_entry(
                    uow,
                    plant_id=plant_id,
                    entry_type=JournalEntryType.WATERING,
                    occurred_at=NOW,
                    now=NOW,
                )
            await uow.commit()

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        snapshot = await uow.journal_snapshots.latest(plant_id)
        aggregate = await load_journal_aggregate(
            event_store=uow.event_store,
            snapshots=uow.journal_snapshots,
            plant_id=plant_id,
        )

    assert snapshot is not None
    assert snapshot.version == 2
    assert len(snapshot.entries) == 2
    assert aggregate.version == 2
    assert len(aggregate.entries) == 2
