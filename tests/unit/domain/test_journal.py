"""Tests for the JournalEntry aggregate and the JournalStream."""

from __future__ import annotations

from datetime import datetime, timedelta
from uuid import UUID, uuid4

import pytest

from plantkeeper.domain.identifiers import JournalEntryId, PlantId
from plantkeeper.domain.journal.entry import JournalEntry
from plantkeeper.domain.journal.errors import (
    JournalEntryAlreadyAppendedError,
    JournalEntryPlantMismatchError,
)
from plantkeeper.domain.journal.events import JournalEntryAdded
from plantkeeper.domain.journal.stream import JournalStream
from plantkeeper.domain.journal.values import JournalEntryType


def _add(
    plant_id: PlantId,
    now: datetime,
    *,
    occurred_at: datetime | None = None,
    note: str | None = "Watered",
    entry_id: JournalEntryId | None = None,
) -> JournalEntry:
    return JournalEntry.add(
        plant_id=plant_id,
        entry_type=JournalEntryType.WATERING,
        occurred_at=now if occurred_at is None else occurred_at,
        now=now,
        note=note,
        entry_id=entry_id,
    )


def test_add_records_journal_entry_added(now: datetime) -> None:
    plant_id = PlantId(uuid4())

    entry = _add(plant_id, now)

    assert entry.plant_id == plant_id
    assert entry.entry_type is JournalEntryType.WATERING
    assert entry.occurred_at == now
    assert entry.note == "Watered"

    events = entry.collect_events()
    assert len(events) == 1
    added = events[0]
    assert isinstance(added, JournalEntryAdded)
    assert added.entry_id == entry.id
    assert added.plant_id == plant_id
    assert added.entry_type is JournalEntryType.WATERING
    assert added.note == "Watered"
    assert added.entry_occurred_at == now
    assert added.occurred_at == now
    assert entry.collect_events() == []


def test_add_keeps_the_entry_time_separate_from_the_record_time(now: datetime) -> None:
    occurred_at = now - timedelta(days=2)

    entry = _add(PlantId(uuid4()), now, occurred_at=occurred_at)

    added = entry.collect_events()[0]
    assert isinstance(added, JournalEntryAdded)
    assert added.entry_occurred_at == occurred_at
    assert added.occurred_at == now


def test_add_strips_the_note(now: datetime) -> None:
    assert _add(PlantId(uuid4()), now, note="  repotted  ").note == "repotted"


@pytest.mark.parametrize("note", [None, "", "   "])
def test_blank_notes_become_none(now: datetime, note: str | None) -> None:
    assert _add(PlantId(uuid4()), now, note=note).note is None


def test_add_accepts_an_explicit_identifier(now: datetime) -> None:
    entry_id = JournalEntryId(uuid4())

    assert _add(PlantId(uuid4()), now, entry_id=entry_id).id == entry_id


def test_rebuilding_an_entry_records_no_events(now: datetime) -> None:
    entry = JournalEntry(
        JournalEntryId(uuid4()),
        plant_id=PlantId(uuid4()),
        entry_type=JournalEntryType.NOTE,
        occurred_at=now,
        note="  watch the leaves  ",
    )

    assert entry.note == "watch the leaves"
    assert entry.entry_type is JournalEntryType.NOTE
    assert entry.collect_events() == []


def test_an_entry_has_no_mutators(now: datetime) -> None:
    entry = _add(PlantId(uuid4()), now)

    assert not {"update", "remove", "delete", "edit"} & set(dir(entry))


def test_journal_entry_type_exposes_the_care_kinds() -> None:
    assert {kind.value for kind in JournalEntryType} == {
        "watering",
        "fertilizing",
        "repotting",
        "note",
    }


def test_a_stream_starts_empty() -> None:
    plant_id = PlantId(uuid4())

    stream = JournalStream(plant_id)

    assert stream.plant_id == plant_id
    assert stream.entries == ()


def test_appending_keeps_the_entries(now: datetime) -> None:
    plant_id = PlantId(uuid4())
    stream = JournalStream(plant_id)
    entry = _add(plant_id, now)

    stream.append(entry)

    assert stream.entries == (entry,)


def test_entries_are_ordered_chronologically(now: datetime) -> None:
    plant_id = PlantId(uuid4())
    later = _add(plant_id, now, occurred_at=now + timedelta(days=1))
    earlier = _add(plant_id, now, occurred_at=now - timedelta(days=1))
    stream = JournalStream(plant_id, [later])

    stream.append(earlier)

    assert [entry.id for entry in stream.entries] == [earlier.id, later.id]


def test_entries_with_the_same_moment_are_ordered_by_identifier(now: datetime) -> None:
    plant_id = PlantId(uuid4())
    lower = _add(plant_id, now, entry_id=JournalEntryId(UUID(int=1)))
    higher = _add(plant_id, now, entry_id=JournalEntryId(UUID(int=2)))

    stream = JournalStream(plant_id, [higher, lower])

    assert [entry.id for entry in stream.entries] == [lower.id, higher.id]


def test_a_foreign_entry_is_rejected_on_construction(now: datetime) -> None:
    entry = _add(PlantId(uuid4()), now)

    with pytest.raises(JournalEntryPlantMismatchError):
        JournalStream(PlantId(uuid4()), [entry])


def test_a_foreign_entry_is_rejected_on_append(now: datetime) -> None:
    stream = JournalStream(PlantId(uuid4()))

    with pytest.raises(JournalEntryPlantMismatchError):
        stream.append(_add(PlantId(uuid4()), now))


def test_a_duplicate_entry_is_rejected(now: datetime) -> None:
    plant_id = PlantId(uuid4())
    entry = _add(plant_id, now)

    with pytest.raises(JournalEntryAlreadyAppendedError):
        JournalStream(plant_id, [entry, entry])

    stream = JournalStream(plant_id, [entry])
    with pytest.raises(JournalEntryAlreadyAppendedError):
        stream.append(entry)

    assert stream.entries == (entry,)
