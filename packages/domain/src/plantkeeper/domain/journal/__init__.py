"""Journal bounded context: an append-only care log (event sourced)."""

from __future__ import annotations

from plantkeeper.domain.journal.entry import JournalEntry
from plantkeeper.domain.journal.errors import (
    JournalEntryAlreadyAppendedError,
    JournalEntryPlantMismatchError,
    JournalError,
)
from plantkeeper.domain.journal.events import JournalEntryAdded
from plantkeeper.domain.journal.stream import JournalStream
from plantkeeper.domain.journal.values import JournalEntryType

__all__ = [
    "JournalEntry",
    "JournalEntryAdded",
    "JournalEntryAlreadyAppendedError",
    "JournalEntryPlantMismatchError",
    "JournalEntryType",
    "JournalError",
    "JournalStream",
]
