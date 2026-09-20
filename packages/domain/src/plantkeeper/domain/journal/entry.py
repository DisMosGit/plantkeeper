"""The JournalEntry aggregate: one immutable record of care."""

from __future__ import annotations

from datetime import datetime

from plantkeeper.domain.base import AggregateRoot
from plantkeeper.domain.identifiers import JournalEntryId, PlantId
from plantkeeper.domain.journal.events import JournalEntryAdded
from plantkeeper.domain.journal.values import JournalEntryType


class JournalEntry(AggregateRoot[JournalEntryId]):
    """An append-only record of care performed on a plant.

    Once added, an entry never changes: the aggregate exposes read-only
    properties and no mutators, so "editing the journal" is impossible by
    construction. Corrections are new entries, which is what makes the Journal
    replayable (Phase 6).
    """

    def __init__(
        self,
        entry_id: JournalEntryId,
        *,
        plant_id: PlantId,
        entry_type: JournalEntryType,
        occurred_at: datetime,
        note: str | None = None,
    ) -> None:
        """Rebuild an entry from its stored state (no events are recorded)."""
        super().__init__(entry_id)
        self._plant_id = plant_id
        self._entry_type = entry_type
        self._occurred_at = occurred_at
        self._note = _normalize_note(note)

    @classmethod
    def add(
        cls,
        *,
        plant_id: PlantId,
        entry_type: JournalEntryType,
        occurred_at: datetime,
        now: datetime,
        note: str | None = None,
        entry_id: JournalEntryId | None = None,
    ) -> JournalEntry:
        """Append an entry and record :class:`JournalEntryAdded`."""
        entry = cls(
            entry_id or JournalEntryId.new(),
            plant_id=plant_id,
            entry_type=entry_type,
            occurred_at=occurred_at,
            note=note,
        )
        entry._record(
            JournalEntryAdded(
                entry_id=entry.id,
                plant_id=plant_id,
                entry_type=entry_type,
                note=entry.note,
                entry_occurred_at=occurred_at,
                occurred_at=now,
            )
        )
        return entry

    @property
    def plant_id(self) -> PlantId:
        """The plant this entry belongs to."""
        return self._plant_id

    @property
    def entry_type(self) -> JournalEntryType:
        """What kind of care the entry records."""
        return self._entry_type

    @property
    def occurred_at(self) -> datetime:
        """When the care happened."""
        return self._occurred_at

    @property
    def note(self) -> str | None:
        """The optional free-text note; blank notes are normalised to ``None``."""
        return self._note


def _normalize_note(note: str | None) -> str | None:
    if note is None:
        return None
    stripped = note.strip()
    return stripped or None
