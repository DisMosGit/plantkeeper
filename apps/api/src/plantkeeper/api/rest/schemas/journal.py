"""HTTP schemas for the journal.

The journal is the one read endpoint whose answer is *replayed* rather than looked
up, so the schemas carry the care moment of every entry and, for the dated answer,
the cut-off that produced it — a client should never have to guess which question
the server answered.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

from plantkeeper.application.views import CollectionView, JournalEntryView, JournalStateView
from plantkeeper.domain.journal.values import JournalEntryType


class JournalEntryResponse(BaseModel):
    """One care record."""

    entry_id: UUID
    plant_id: UUID
    entry_type: JournalEntryType
    occurred_at: datetime
    note: str | None

    @classmethod
    def from_view(cls, view: JournalEntryView) -> JournalEntryResponse:
        """Map the view onto the wire format."""
        return cls(
            entry_id=view.entry_id.value,
            plant_id=view.plant_id.value,
            entry_type=view.entry_type,
            occurred_at=view.occurred_at,
            note=view.note,
        )


class JournalCollectionResponse(BaseModel):
    """A plant's whole journal, oldest first."""

    items: list[JournalEntryResponse]

    @classmethod
    def from_view(cls, view: CollectionView[JournalEntryView]) -> JournalCollectionResponse:
        """Map each entry view onto the wire format."""
        return cls(items=[JournalEntryResponse.from_view(item) for item in view.items])


class JournalStateResponse(BaseModel):
    """The journal as of a date, with the summary of what it held."""

    plant_id: UUID
    as_of: datetime
    entry_count: int
    counts_by_type: dict[JournalEntryType, int]
    entries: list[JournalEntryResponse]

    @classmethod
    def from_view(cls, view: JournalStateView) -> JournalStateResponse:
        """Map the state view onto the wire format."""
        return cls(
            plant_id=view.plant_id.value,
            as_of=view.as_of,
            entry_count=view.entry_count,
            counts_by_type=view.counts_by_type,
            entries=[JournalEntryResponse.from_view(entry) for entry in view.entries],
        )
