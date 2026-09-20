"""The Journal read model.

Owns ``read_analytics.journal_entries``, the table the plant page shows inline.
``JournalEntryAdded`` carries the moment the care happened separately from the
moment it was recorded, and both are kept as separate columns so the admin can
show either.
"""

from __future__ import annotations

from typing import ClassVar

from plantkeeper.admin.projections.base import Projection, ProjectionHandler
from plantkeeper.admin.read_models.models import JournalReadModel, PlantReadModel
from plantkeeper.domain.base import DomainEvent
from plantkeeper.domain.journal.events import JournalEntryAdded
from plantkeeper.infrastructure.messaging.topics import JOURNAL_EVENTS


class JournalProjection(Projection):
    """Consumes ``journal.events`` into ``read_analytics.journal_entries``."""

    name = "journal"
    topics = (JOURNAL_EVENTS,)

    def on_journal_entry_added(self, event: JournalEntryAdded) -> None:
        """Append the entry, creating a placeholder plant row if needed.

        The entry's foreign key needs its plant to exist. A journal entry can be
        projected before the garden event that names the plant, so the row is
        created with only its identifier; the Garden projection fills the
        garden-owned columns when ``PlantAdded`` arrives, and its
        ``update_or_create`` does not touch this entry.
        """
        plant, _ = PlantReadModel.objects.get_or_create(plant_id=event.plant_id.value)
        JournalReadModel.objects.update_or_create(
            entry_id=event.entry_id.value,
            defaults={
                "plant": plant,
                "entry_type": event.entry_type.value,
                "note": event.note,
                "occurred_at": event.entry_occurred_at,
                "recorded_at": event.occurred_at,
            },
        )

    handlers: ClassVar[dict[type[DomainEvent], ProjectionHandler]] = {
        JournalEntryAdded: on_journal_entry_added,
    }
