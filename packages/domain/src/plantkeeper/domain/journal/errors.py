"""Journal invariant violations."""

from __future__ import annotations

from plantkeeper.domain.base import DomainError


class JournalError(DomainError):
    """Base class for every Journal rule violation."""


class JournalEntryPlantMismatchError(JournalError):
    """An entry was appended to the stream of a different plant."""


class JournalEntryAlreadyAppendedError(JournalError):
    """The same entry was appended twice to one stream."""
