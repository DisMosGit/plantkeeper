"""The catalogue cache's invalidator.

The synchronisation saga does not delete cache keys itself: inside its step it
would be a Valkey call inside a database transaction, and its failure would be
one more thing the saga's compensation has to reason about. Instead the step
announces staleness through the outbox (``SpeciesCacheInvalidated``), and this
consumer — a normal worker consumer on ``catalog.events`` — drops the keys once
the event has actually travelled.

Two events invalidate, not one:

* ``SpeciesUpdated`` is the fact that a catalogue entry changed, whoever changed
  it (today only the synchronisation, including its compensation's restore);
* ``SpeciesCacheInvalidated`` is the synchronisation's explicit declaration of
  staleness, kept as the Phase 4 contract.

The consumer is idempotent three times over: the ``(consumer_group, event_id)``
ledger claim is committed with the work, ``DEL`` is naturally idempotent, and the
entry's TTL bounds any invalidation that is lost entirely.
"""

from __future__ import annotations

from typing import ClassVar

from cqrs.dispatcher.saga import SagaDispatcher

from plantkeeper.application.ports.catalog import SpeciesCache
from plantkeeper.application.ports.unit_of_work import UnitOfWork
from plantkeeper.application.sagas.consumer import Consumer
from plantkeeper.domain.base import DomainEvent
from plantkeeper.domain.catalog.events import SpeciesCacheInvalidated, SpeciesUpdated


class SpeciesCacheConsumer(Consumer):
    """Drop the cached catalogue entries an event declares stale."""

    name = "species-cache"
    handled_types: ClassVar[tuple[type[DomainEvent], ...]] = (
        SpeciesUpdated,
        SpeciesCacheInvalidated,
    )

    def __init__(self, unit_of_work: UnitOfWork, species_cache: SpeciesCache) -> None:
        super().__init__(unit_of_work)
        self._species_cache = species_cache

    async def handle(self, event: DomainEvent, dispatcher: SagaDispatcher) -> None:
        """Invalidate the species the event names.

        ``dispatcher`` is unused: this consumer reacts on its own and starts no
        process manager.
        """
        if isinstance(event, SpeciesUpdated | SpeciesCacheInvalidated):
            await self._species_cache.invalidate([event.species_id])
