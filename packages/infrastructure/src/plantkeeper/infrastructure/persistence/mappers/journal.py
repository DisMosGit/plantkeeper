"""Journal aggregate <-> row mapping."""

from __future__ import annotations

from plantkeeper.domain.identifiers import JournalEntryId, PlantId
from plantkeeper.domain.journal.entry import JournalEntry
from plantkeeper.domain.journal.values import JournalEntryType
from plantkeeper.infrastructure.persistence.models.journal import JournalEntryModel


def journal_entry_to_domain(model: JournalEntryModel) -> JournalEntry:
    """Rebuild the ``JournalEntry`` aggregate from its row."""
    return JournalEntry(
        JournalEntryId(model.id),
        plant_id=PlantId(model.plant_id),
        entry_type=JournalEntryType(model.entry_type),
        occurred_at=model.occurred_at,
        note=model.note,
    )


def journal_entry_to_model(entry: JournalEntry) -> JournalEntryModel:
    """Build the row that represents ``entry``."""
    return JournalEntryModel(
        id=entry.id.value,
        plant_id=entry.plant_id.value,
        entry_type=entry.entry_type.value,
        occurred_at=entry.occurred_at,
        note=entry.note,
    )
