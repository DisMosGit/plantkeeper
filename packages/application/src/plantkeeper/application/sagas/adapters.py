"""Small adapters that connect the saga ports to the write side's own ports.

They live in the application layer because they depend only on other ports — a
repository protocol and a unit of work — never on SQLAlchemy or Kafka. Two of
them exist purely as seams for later phases: ``SpeciesCatalog`` grows a Trefle
fallback in Phase 9, and ``OutboxSpeciesCache`` is replaced by a Valkey adapter
that drops the cached entry as well as announcing it.
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
    """The ``SpeciesCatalog`` port over the local catalogue repository."""

    def __init__(self, species: SpeciesRepository) -> None:
        self._species = species

    async def get(self, species_id: SpeciesId) -> Species | None:
        """Return the locally known species, or ``None``."""
        return await self._species.get(species_id)


class OutboxSpeciesCache:
    """The ``SpeciesCache`` port as Phase 4 can honour it: announce the staleness.

    The event is the contract — ``SpeciesCacheInvalidated`` travels through the
    same outbox as every other event, and the Valkey consumer that acts on it
    arrives with the cache in Phase 9. Publishing here keeps the saga's step
    sequence complete and gives its compensation something concrete to fail on.
    """

    def __init__(self, unit_of_work: UnitOfWork) -> None:
        self._unit_of_work = unit_of_work

    async def invalidate(self, species_ids: Sequence[SpeciesId]) -> None:
        """Stage one ``SpeciesCacheInvalidated`` per species."""
        for species_id in species_ids:
            await self._unit_of_work.outbox.append(SpeciesCacheInvalidated(species_id=species_id))


class UnconfiguredSpeciesSource:
    """The ``SpeciesSource`` port before the Trefle client exists.

    Returning nothing rather than raising keeps a manual ``POST /catalog/sync``
    from failing in Phase 4: the saga completes with zero updates, which is
    exactly what a catalogue with no configured upstream knows. Phase 9 replaces
    this binding with the Trefle anti-corruption layer.
    """

    async def fetch_all(self) -> list[SpeciesRecord]:
        """Return no upstream entries; the real source arrives in Phase 9."""
        return []
