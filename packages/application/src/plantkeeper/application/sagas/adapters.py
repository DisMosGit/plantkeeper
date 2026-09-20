"""Small adapters that connect the saga ports to the write side's own ports.

They live in the application layer because they depend only on other ports — a
repository protocol and a unit of work — never on SQLAlchemy or Kafka.
``OutboxSpeciesCache`` implements the narrow ``SpeciesCacheInvalidator`` protocol:
the synchronisation saga announces staleness through the outbox, and the Valkey
adapter in the worker drops the keys when ``SpeciesCacheConsumer`` receives the
event. The saga therefore never opens a Valkey connection inside its transaction.
"""

from __future__ import annotations

from collections.abc import Sequence

from plantkeeper.application.ports.catalog import SpeciesRecord
from plantkeeper.application.ports.repositories import SpeciesRepository
from plantkeeper.application.ports.unit_of_work import UnitOfWork
from plantkeeper.domain.catalog.events import SpeciesCacheInvalidated
from plantkeeper.domain.catalog.species import Species
from plantkeeper.domain.identifiers import SpeciesId


class RepositorySpeciesCatalog:
    """The ``SpeciesCatalog`` port over the local catalogue repository.

    Deliberately local-only: the synchronisation fills the catalogue on a
    schedule, so onboarding never waits on an external HTTP call for a species it
    has not seen.
    """

    def __init__(self, species: SpeciesRepository) -> None:
        self._species = species

    async def get(self, species_id: SpeciesId) -> Species | None:
        """Return the locally known species, or ``None``."""
        return await self._species.get(species_id)


class OutboxSpeciesCache:
    """The ``SpeciesCacheInvalidator`` port over the request's outbox.

    The event is the contract — ``SpeciesCacheInvalidated`` travels through the
    same outbox as every other event, and ``SpeciesCacheConsumer`` in the worker
    turns it into a Valkey ``DEL``. Publishing here keeps the saga's step
    sequence complete, gives its compensation something concrete to fail on, and
    keeps every external call out of the saga's database transaction.
    """

    def __init__(self, unit_of_work: UnitOfWork) -> None:
        self._unit_of_work = unit_of_work

    async def invalidate(self, species_ids: Sequence[SpeciesId]) -> None:
        """Stage one ``SpeciesCacheInvalidated`` per species."""
        for species_id in species_ids:
            await self._unit_of_work.outbox.append(SpeciesCacheInvalidated(species_id=species_id))


class UnconfiguredSpeciesSource:
    """The ``SpeciesSource`` binding when no Trefle token is configured.

    Returning nothing rather than raising keeps a manual ``POST /catalog/sync``
    from failing on a local checkout: the saga completes with zero changes, which
    is exactly what a catalogue with no configured upstream knows.
    """

    async def fetch_all(self) -> list[SpeciesRecord]:
        """Return no upstream entries; the real source arrives in Phase 9."""
        return []
