"""The Journal's Kafka consumer.

``WateringCompleted`` is the one care fact that becomes a journal entry today: the
Care context records that a watering happened, and the journal is where a household
reads it back. The entry's care moment is the event's ``completed_at``, so the
journal and the care history cannot disagree about when the plant was watered.

Skips and misses are deliberately not journaled — nothing was done — and
fertilizing, repotting and free-text notes have no care producer yet, so they can
only be appended through ``AddJournalEntryCommand`` (``docs/event-sourcing.md``).

The consumer group also subscribes to the Garden context's events, for one reason:
the journal needs to know which plant a watering concerns, and it may not read
``write_garden`` for it. ``PlantAdded``/``PlantMoved``/``PlantRemoved`` maintain this
context's own ``plant_refs`` rows (``plantkeeper.application.references``), and a
watering for a plant this context has never seen is logged and dropped rather than
journalled against a plant it cannot name.

Idempotency is defended twice. ``consume_once`` claims ``(consumer_group, event_id)``
in the same transaction as the append, exactly like the choreography sagas; on top of
that, the entry's identifier is derived from the event, so even a rebuilt consumer
group — a reset offset plus a lost ledger — finds the fact already in the stream and
appends nothing. An append-only history cannot afford the second delivery the ledger
alone would let through.
"""

from __future__ import annotations

import logging
from typing import ClassVar

from cqrs.dispatcher.saga import SagaDispatcher

from plantkeeper.application.journal.store import record_journal_entry, watering_entry_id
from plantkeeper.application.ports.clock import Clock
from plantkeeper.application.ports.plant_references import JournalPlantRefs
from plantkeeper.application.ports.unit_of_work import UnitOfWork
from plantkeeper.application.references import maintain_plant_reference
from plantkeeper.application.sagas.consumer import Consumer
from plantkeeper.domain.base import DomainEvent
from plantkeeper.domain.care.events import WateringCompleted
from plantkeeper.domain.garden.events import PlantAdded, PlantMoved, PlantRemoved
from plantkeeper.domain.journal.values import JournalEntryType

logger = logging.getLogger(__name__)


class JournalEntryConsumer(Consumer):
    """Turn ``WateringCompleted`` into a watering entry in the plant's journal."""

    name = "journal-entries"
    handled_types: ClassVar[tuple[type[DomainEvent], ...]] = (
        PlantAdded,
        PlantMoved,
        PlantRemoved,
        WateringCompleted,
    )

    def __init__(
        self, unit_of_work: UnitOfWork, clock: Clock, plant_refs: JournalPlantRefs
    ) -> None:
        super().__init__(unit_of_work)
        self._clock = clock
        self._plant_refs = plant_refs

    async def handle(self, event: DomainEvent, dispatcher: SagaDispatcher) -> None:
        """Keep the plant reference, or append the watering it reports.

        ``dispatcher`` is unused: this consumer reacts on its own and starts no
        process manager.
        """
        if isinstance(event, (PlantAdded, PlantMoved, PlantRemoved)):
            await maintain_plant_reference(self._plant_refs, event, context="journal")
            return
        if not isinstance(event, WateringCompleted):
            return
        plant = await self._plant_refs.get(event.plant_id)
        if plant is None:
            logger.info("watering for unknown plant %s; not journalled", event.plant_id)
            return
        await record_journal_entry(
            self.unit_of_work,
            plant_id=event.plant_id,
            entry_type=JournalEntryType.WATERING,
            occurred_at=event.completed_at,
            now=self._clock.now(),
            entry_id=watering_entry_id(event.plant_id, event.completed_at),
        )
