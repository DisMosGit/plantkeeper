"""Serialisable state of the Journal aggregate.

A state is what a snapshot stores and what the temporal query answers with: the
ordered entries of one plant's journal, plus the stream version they were replayed
to. It is a Pydantic model rather than the aggregate itself because it has to
cross the event store's ``jsonb`` columns (Phase 6), and the aggregate is a plain
class by the project's own rule.

``version`` is the stream version the state covers, not the number of entries:
``state_at`` answers with a *subset* of the entries as of a care moment, and the
version still says which point of the stream was replayed to produce it.
"""

from __future__ import annotations

from pydantic import AwareDatetime, BaseModel, ConfigDict

from plantkeeper.domain.identifiers import JournalEntryId, PlantId
from plantkeeper.domain.journal.entry import JournalEntry
from plantkeeper.domain.journal.values import JournalEntryType


class JournalEntryState(BaseModel):
    """One journal entry, flattened for storage."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    entry_id: JournalEntryId
    entry_type: JournalEntryType
    occurred_at: AwareDatetime
    note: str | None = None

    @classmethod
    def from_domain(cls, entry: JournalEntry) -> JournalEntryState:
        """Flatten an entry, keeping the care moment and not the record moment."""
        return cls(
            entry_id=entry.id,
            entry_type=entry.entry_type,
            occurred_at=entry.occurred_at,
            note=entry.note,
        )

    def to_domain(self, plant_id: PlantId) -> JournalEntry:
        """Rebuild the entry for ``plant_id``.

        The plant is a parameter rather than a field: it is the stream's identity,
        and repeating it on every entry would only create a second place for it to
        be wrong. Rebuilding records no events, exactly like the aggregate's own
        constructor (``JournalEntry.__init__``).
        """
        return JournalEntry(
            self.entry_id,
            plant_id=plant_id,
            entry_type=self.entry_type,
            occurred_at=self.occurred_at,
            note=self.note,
        )


class JournalState(BaseModel):
    """One plant's journal, replayed to a stream version."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    plant_id: PlantId
    version: int
    entries: tuple[JournalEntryState, ...] = ()

    @property
    def entry_count(self) -> int:
        """How many entries the state holds."""
        return len(self.entries)

    def counts_by_type(self) -> dict[JournalEntryType, int]:
        """Count the entries per care kind, for a summary answer."""
        counts: dict[JournalEntryType, int] = {}
        for entry in self.entries:
            counts[entry.entry_type] = counts.get(entry.entry_type, 0) + 1
        return counts
