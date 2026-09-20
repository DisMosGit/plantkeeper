"""Tests for the event-sourced ``JournalAggregate``.

The aggregate is the only place that knows how a stream becomes a state, so these
tests pin the two directions separately: what ``add_entry`` records, and what
replaying those records rebuilds. The temporal query is tested here too, because
"which entries are in the state" is domain policy and not the API's.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from uuid import uuid4

import pytest

from plantkeeper.domain.identifiers import JournalEntryId, PlantId
from plantkeeper.domain.journal.aggregate import JournalAggregate
from plantkeeper.domain.journal.errors import (
    JournalEntryAlreadyAppendedError,
    JournalEntryPlantMismatchError,
)
from plantkeeper.domain.journal.events import JournalEntryAdded
from plantkeeper.domain.journal.state import JournalEntryState, JournalState
from plantkeeper.domain.journal.values import JournalEntryType


def an_added(
    plant_id: PlantId,
    *,
    occurred_at: datetime,
    recorded_at: datetime,
    entry_type: JournalEntryType = JournalEntryType.WATERING,
    note: str | None = None,
    entry_id: JournalEntryId | None = None,
) -> JournalEntryAdded:
    """One ``JournalEntryAdded``, as the store would hold it."""
    return JournalEntryAdded(
        entry_id=entry_id or JournalEntryId.new(),
        plant_id=plant_id,
        entry_type=entry_type,
        note=note,
        entry_occurred_at=occurred_at,
        occurred_at=recorded_at,
    )


def a_replay(plant_id: PlantId, events: list[JournalEntryAdded]) -> JournalAggregate:
    """An aggregate rebuilt from ``events``, oldest first."""
    aggregate = JournalAggregate(plant_id)
    for event in events:
        aggregate.apply(event)
    return aggregate


def test_a_fresh_journal_is_empty(now: datetime) -> None:
    aggregate = JournalAggregate(PlantId(uuid4()))

    assert aggregate.version == 0
    assert aggregate.entries == ()
    assert aggregate.collect_events() == []


def test_add_entry_records_one_event_and_increments_the_version(now: datetime) -> None:
    plant_id = PlantId(uuid4())
    aggregate = JournalAggregate(plant_id)

    entry = aggregate.add_entry(
        entry_type=JournalEntryType.WATERING,
        occurred_at=now,
        now=now,
        note="  watered  ",
    )

    assert aggregate.version == 1
    assert aggregate.entries == (entry,)
    events = aggregate.collect_events()
    assert len(events) == 1
    added = events[0]
    assert isinstance(added, JournalEntryAdded)
    assert added.entry_id == entry.id
    assert added.plant_id == plant_id
    assert added.note == "watered"
    assert added.entry_occurred_at == now
    assert added.occurred_at == now
    # The entry's own event was drained onto the aggregate, not left on both.
    assert entry.collect_events() == []
    assert aggregate.collect_events() == []


def test_add_entry_keeps_the_care_moment_and_the_record_moment_apart(now: datetime) -> None:
    cared_for = now - timedelta(days=3)
    aggregate = JournalAggregate(PlantId(uuid4()))

    aggregate.add_entry(entry_type=JournalEntryType.NOTE, occurred_at=cared_for, now=now)

    added = aggregate.collect_events()[0]
    assert isinstance(added, JournalEntryAdded)
    assert added.entry_occurred_at == cared_for
    assert added.occurred_at == now


def test_add_entry_accepts_the_callers_identifier(now: datetime) -> None:
    entry_id = JournalEntryId(uuid4())
    aggregate = JournalAggregate(PlantId(uuid4()))

    entry = aggregate.add_entry(
        entry_type=JournalEntryType.REPOTTING,
        occurred_at=now,
        now=now,
        entry_id=entry_id,
    )

    assert entry.id == entry_id


def test_replaying_events_rebuilds_the_entries(now: datetime) -> None:
    plant_id = PlantId(uuid4())
    earlier = an_added(plant_id, occurred_at=now - timedelta(days=1), recorded_at=now)
    later = an_added(plant_id, occurred_at=now, recorded_at=now)

    aggregate = a_replay(plant_id, [later, earlier])

    assert aggregate.version == 2
    assert [entry.id for entry in aggregate.entries] == [earlier.entry_id, later.entry_id]
    # Replay is not a new fact.
    assert aggregate.collect_events() == []


def test_replaying_a_foreign_event_is_refused(now: datetime) -> None:
    other = an_added(PlantId(uuid4()), occurred_at=now, recorded_at=now)

    with pytest.raises(JournalEntryPlantMismatchError):
        a_replay(PlantId(uuid4()), [other])


def test_replaying_the_same_entry_twice_is_refused(now: datetime) -> None:
    plant_id = PlantId(uuid4())
    event = an_added(plant_id, occurred_at=now, recorded_at=now)

    with pytest.raises(JournalEntryAlreadyAppendedError):
        a_replay(plant_id, [event, event])


def test_an_entry_can_be_looked_up_by_its_identifier(now: datetime) -> None:
    plant_id = PlantId(uuid4())
    entry_id = JournalEntryId(uuid4())
    aggregate = JournalAggregate(plant_id)
    entry = aggregate.add_entry(
        entry_type=JournalEntryType.WATERING,
        occurred_at=now,
        now=now,
        entry_id=entry_id,
    )

    assert aggregate.entry(entry_id) is entry
    assert aggregate.entry(JournalEntryId(uuid4())) is None


def test_state_round_trips_through_from_state(now: datetime) -> None:
    plant_id = PlantId(uuid4())
    aggregate = JournalAggregate(plant_id)
    aggregate.add_entry(
        entry_type=JournalEntryType.WATERING, occurred_at=now, now=now, note="Watered"
    )
    aggregate.add_entry(entry_type=JournalEntryType.NOTE, occurred_at=now, now=now)

    restored = JournalAggregate.from_state(aggregate.state())

    assert restored.version == aggregate.version
    assert [entry.id for entry in restored.entries] == [entry.id for entry in aggregate.entries]
    assert [entry.note for entry in restored.entries] == ["Watered", None]
    assert restored.collect_events() == []


def test_a_state_carries_the_plant_and_the_version(now: datetime) -> None:
    plant_id = PlantId(uuid4())
    aggregate = JournalAggregate(plant_id)
    aggregate.add_entry(entry_type=JournalEntryType.WATERING, occurred_at=now, now=now)

    state = aggregate.state()

    assert state.plant_id == plant_id
    assert state.version == 1
    assert state.entry_count == 1


def test_entry_state_to_domain_needs_the_streams_plant(now: datetime) -> None:
    plant_id = PlantId(uuid4())
    entry_state = JournalEntryState(
        entry_id=JournalEntryId(uuid4()),
        entry_type=JournalEntryType.FERTILIZING,
        occurred_at=now,
        note="Fed",
    )

    entry = entry_state.to_domain(plant_id)

    assert entry.plant_id == plant_id
    assert entry.note == "Fed"


def test_state_at_includes_care_at_the_cut_off_and_excludes_later_care(now: datetime) -> None:
    plant_id = PlantId(uuid4())
    aggregate = JournalAggregate(plant_id)
    aggregate.add_entry(
        entry_type=JournalEntryType.WATERING,
        occurred_at=now - timedelta(days=2),
        now=now,
    )
    aggregate.add_entry(
        entry_type=JournalEntryType.NOTE,
        occurred_at=now,
        now=now,
        note="Watch the leaves",
    )

    state = aggregate.state_at(now - timedelta(days=1))

    assert state.entry_count == 1
    assert state.entries[0].entry_type is JournalEntryType.WATERING
    # The later entry is excluded by its care moment, not by when it was recorded.
    assert aggregate.state_at(now).entry_count == 2


def test_state_at_keeps_the_stream_version(now: datetime) -> None:
    plant_id = PlantId(uuid4())
    aggregate = JournalAggregate(plant_id)
    aggregate.add_entry(entry_type=JournalEntryType.WATERING, occurred_at=now, now=now)

    earlier = aggregate.state_at(now - timedelta(days=1))

    assert earlier.entry_count == 0
    assert earlier.version == 1


def test_counts_by_type_summarises_the_state(now: datetime) -> None:
    plant_id = PlantId(uuid4())
    aggregate = JournalAggregate(plant_id)
    for entry_type in (
        JournalEntryType.WATERING,
        JournalEntryType.WATERING,
        JournalEntryType.NOTE,
    ):
        aggregate.add_entry(entry_type=entry_type, occurred_at=now, now=now)

    counts = aggregate.state().counts_by_type()

    assert counts == {JournalEntryType.WATERING: 2, JournalEntryType.NOTE: 1}


@pytest.mark.parametrize(
    ("version", "expected"), [(0, False), (49, False), (50, True), (100, True)]
)
def test_should_snapshot_is_true_on_the_intervals_multiples(version: int, expected: bool) -> None:
    aggregate = JournalAggregate(PlantId(uuid4()), version=version)

    assert aggregate.should_snapshot(50) is expected


def test_should_snapshot_is_never_true_for_a_non_positive_interval() -> None:
    aggregate = JournalAggregate(PlantId(uuid4()), version=50)

    assert aggregate.should_snapshot(0) is False
    assert aggregate.should_snapshot(-1) is False


def test_a_state_validates_as_a_checkpoint_payload(now: datetime) -> None:
    """A snapshot is JSON in the database, so the state has to survive that trip."""
    aggregate = JournalAggregate(PlantId(uuid4()))
    aggregate.add_entry(entry_type=JournalEntryType.WATERING, occurred_at=now, now=now)

    payload = aggregate.state().model_dump(mode="json")

    assert JournalState.model_validate(payload) == aggregate.state()
