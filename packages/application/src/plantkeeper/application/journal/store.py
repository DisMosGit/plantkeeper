"""The Journal's event-store plumbing.

Two operations live here, shared by the command handler and the Kafka consumer so
that the append path exists exactly once:

* :func:`load_journal_aggregate` replays a plant's stream into a
  :class:`~plantkeeper.domain.journal.aggregate.JournalAggregate`, starting from the
  newest snapshot. This is where the roadmap's "``load_stream`` uses a snapshot"
  lives: the port reads the events *after* the snapshot's version and the aggregate
  starts from the snapshot's state, so a checkpoint bounds the replay without the
  repository having to know what an aggregate is.
* :func:`record_journal_entry` appends one entry: the event-store row, the outbox
  row that announces it, the write side's own tabular mirror, and — every
  :data:`SNAPSHOT_EVERY` entries — a checkpoint. It never commits: the command
  handler and the consumer open their transactions differently, so the caller owns
  that decision.
"""

from __future__ import annotations

from datetime import datetime
from typing import Final
from uuid import NAMESPACE_URL, uuid5

from plantkeeper.application.errors import EventStoreCorruptionError
from plantkeeper.application.ports.event_store import (
    EventStoreRepository,
    JournalSnapshotRepository,
)
from plantkeeper.application.ports.unit_of_work import UnitOfWork
from plantkeeper.domain.identifiers import JournalEntryId, PlantId
from plantkeeper.domain.journal.aggregate import JournalAggregate
from plantkeeper.domain.journal.entry import JournalEntry
from plantkeeper.domain.journal.events import JournalEntryAdded
from plantkeeper.domain.journal.values import JournalEntryType

SNAPSHOT_EVERY: Final = 50
"""Checkpoint every this many events.

A module constant rather than a setting, like the sagas' grace period and tick
sizes: how often the journal checkpoints itself is a policy of the journal, not a
property of the environment.
"""


def watering_entry_id(plant_id: PlantId, completed_at: datetime) -> JournalEntryId:
    """Derive the entry identifier of one watering.

    The consumer derives it from the event it reacts to rather than minting one, so
    the same ``WateringCompleted`` always names the same entry. An append-only stream
    has no upsert: without this, replaying ``care.events`` into a fresh consumer group
    would append the whole watering history a second time.
    """
    return JournalEntryId(
        uuid5(NAMESPACE_URL, f"plantkeeper:journal:watering:{plant_id}:{completed_at.isoformat()}")
    )


async def load_journal_aggregate(
    *,
    event_store: EventStoreRepository,
    snapshots: JournalSnapshotRepository,
    plant_id: PlantId,
) -> JournalAggregate:
    """Replay the plant's stream, using its newest snapshot when there is one.

    The versions of the replayed events are checked for continuity: ``append``
    writes them consecutively in one statement, so a gap means the stream is not the
    stream this aggregate was rebuilt from, and a journal that hides that would be
    worse than one that refuses to load.
    """
    state = await snapshots.latest(plant_id)
    after_version = 0 if state is None else state.version
    stored = await event_store.load_stream(plant_id, after_version=after_version)
    aggregate = JournalAggregate(plant_id) if state is None else JournalAggregate.from_state(state)

    expected_version = after_version + 1
    for item in stored:
        if item.version != expected_version:
            raise EventStoreCorruptionError(
                f"journal stream {plant_id} jumps to version {item.version}, "
                f"expected {expected_version}"
            )
        if not isinstance(item.event, JournalEntryAdded):
            raise EventStoreCorruptionError(
                f"journal stream {plant_id} holds {type(item.event).__name__}, "
                "which is not a journal event"
            )
        aggregate.apply(item.event)
        expected_version += 1
    return aggregate


async def record_journal_entry(
    unit_of_work: UnitOfWork,
    *,
    plant_id: PlantId,
    entry_type: JournalEntryType,
    occurred_at: datetime,
    now: datetime,
    note: str | None = None,
    entry_id: JournalEntryId | None = None,
) -> JournalEntry:
    """Append one entry to the store, announce it and checkpoint when due.

    When the caller owns ``entry_id`` and the stream already holds that entry, this is
    a no-op that answers with the stored entry: the fact is already recorded, and
    appending it again would rewrite an append-only history. Every other write — the
    store row, the outbox row, the tabular mirror, a checkpoint — happens only for a
    genuinely new entry.

    The outbox row is appended explicitly rather than drained from a tracked
    aggregate: the event store is not an aggregate repository, so nothing registers
    the journal with the unit of work's tracker — the same shape the telemetry
    ingress uses for ``TelemetryReceived``.
    """
    aggregate = await load_journal_aggregate(
        event_store=unit_of_work.event_store,
        snapshots=unit_of_work.journal_snapshots,
        plant_id=plant_id,
    )
    if entry_id is not None:
        existing = aggregate.entry(entry_id)
        if existing is not None:
            return existing
    expected_version = aggregate.version
    entry = aggregate.add_entry(
        entry_type=entry_type,
        occurred_at=occurred_at,
        now=now,
        note=note,
        entry_id=entry_id,
    )
    events = aggregate.collect_events()
    await unit_of_work.event_store.append(
        plant_id, expected_version=expected_version, events=events
    )
    for event in events:
        await unit_of_work.outbox.append(event)
    # The Phase 2 tabular mirror. ``add_entry`` already drained the entry's own event
    # onto the aggregate, so the repository's tracker cannot publish it a second time.
    await unit_of_work.journal_entries.add(entry)
    if aggregate.should_snapshot(SNAPSHOT_EVERY):
        await unit_of_work.journal_snapshots.save(aggregate.state())
    return entry
