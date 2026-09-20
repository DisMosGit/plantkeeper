"""Journal value objects."""

from __future__ import annotations

from enum import StrEnum


class JournalEntryType(StrEnum):
    """The kind of care a journal entry records."""

    WATERING = "watering"
    FERTILIZING = "fertilizing"
    REPOTTING = "repotting"
    NOTE = "note"
