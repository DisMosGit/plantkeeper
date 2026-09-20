"""The JournalStream: the ordered journal of one plant."""

from __future__ import annotations

from collections.abc import Sequence

from plantkeeper.domain.identifiers import JournalEntryId, PlantId
from plantkeeper.domain.journal.entry import JournalEntry
from plantkeeper.domain.journal.errors import (
    JournalEntryAlreadyAppendedError,
    JournalEntryPlantMismatchError,
)


class JournalStream:
    """Every journal entry of one plant, in chronological order.

    The stream is the read side of the Journal aggregate: it forbids removal and
    edits, rejects entries that belong to another plant, and returns entries
    sorted by ``(occurred_at, entry id)`` so replay is deterministic (Phase 6).
    """

    def __init__(self, plant_id: PlantId, entries: Sequence[JournalEntry] = ()) -> None:
        """Build a stream, optionally replaying already stored entries."""
        self._plant_id = plant_id
        self._entries: list[JournalEntry] = []
        self._entry_ids: set[JournalEntryId] = set()
        for entry in entries:
            self._register(entry)

    @property
    def plant_id(self) -> PlantId:
        """The plant whose journal this is."""
        return self._plant_id

    @property
    def entries(self) -> tuple[JournalEntry, ...]:
        """The entries in chronological order."""
        return tuple(sorted(self._entries, key=lambda entry: (entry.occurred_at, entry.id.value)))

    def append(self, entry: JournalEntry) -> None:
        """Append one entry, rejecting a foreign plant or a duplicate entry."""
        self._register(entry)

    def _register(self, entry: JournalEntry) -> None:
        if entry.plant_id != self._plant_id:
            raise JournalEntryPlantMismatchError(
                f"entry {entry.id} belongs to plant {entry.plant_id}, "
                f"but this stream is for plant {self._plant_id}"
            )
        if entry.id in self._entry_ids:
            raise JournalEntryAlreadyAppendedError(f"entry {entry.id} was already appended")
        self._entries.append(entry)
        self._entry_ids.add(entry.id)
