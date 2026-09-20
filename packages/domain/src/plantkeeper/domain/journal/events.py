"""Domain events published by the Journal context."""

from __future__ import annotations

from pydantic import AwareDatetime

from plantkeeper.domain.base import DomainEvent
from plantkeeper.domain.identifiers import JournalEntryId, PlantId
from plantkeeper.domain.journal.values import JournalEntryType


class JournalEntryAdded(DomainEvent):
    """An immutable care record was appended to a plant's journal.

    ``entry_occurred_at`` is when the care happened; the inherited
    ``occurred_at`` is when the entry was recorded.
    """

    entry_id: JournalEntryId
    plant_id: PlantId
    entry_type: JournalEntryType
    note: str | None = None
    entry_occurred_at: AwareDatetime
