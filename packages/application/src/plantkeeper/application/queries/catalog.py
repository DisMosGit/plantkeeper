"""Catalog queries.

The catalogue has two read paths and they answer different questions. The list is
a question about what the system looks like, so it is answered from the read model
the ``SpeciesProjection`` maintains. Reading one species is what a client asks
right after naming it — and it is cache-aside through Valkey — so it stays on the
write store, where a catalogue entry that was just synchronised is visible before
the projection catches up.
"""

from __future__ import annotations

from plantkeeper.application.errors import NotFoundError
from plantkeeper.application.ports.catalog import SpeciesCache
from plantkeeper.application.ports.read_models import ReadModelReader, SpeciesListRow
from plantkeeper.application.ports.repositories import SpeciesRepository
from plantkeeper.application.queries.base import Query, QueryHandler
from plantkeeper.application.views import CollectionView, SpeciesView
from plantkeeper.domain.identifiers import SpeciesId


class ListSpeciesQuery(Query):
    """Read the local catalogue."""


class ListSpeciesQueryHandler(QueryHandler[ListSpeciesQuery, CollectionView[SpeciesView]]):
    """Answer with every species, by scientific name, from the read model.

    A species the synchronisation added moments ago may not be listed until the
    ``SpeciesProjection`` has seen its event (``docs/cqrs.md``).
    """

    def __init__(self, read_models: ReadModelReader) -> None:
        self._read_models = read_models

    async def handle(self, query: ListSpeciesQuery) -> CollectionView[SpeciesView]:
        """Return the projected catalogue."""
        rows = await self._read_models.list_species()
        return CollectionView[SpeciesView](items=[_species_view(row) for row in rows])


def _species_view(row: SpeciesListRow) -> SpeciesView:
    """Turn a read-model row into the view the API answers with."""
    return SpeciesView(
        species_id=row.species_id,
        scientific_name=row.scientific_name,
        common_name=row.common_name,
        watering_interval=row.watering_interval,
        light_requirement=row.light_requirement,
        version=row.version,
    )


class GetSpeciesQuery(Query):
    """Read one catalogue entry."""

    species_id: SpeciesId


class GetSpeciesQueryHandler(QueryHandler[GetSpeciesQuery, SpeciesView]):
    """Answer with the species, or fail.

    Cache-aside (Phase 9): the cache is consulted first and filled on a miss. A
    cache that is down degrades to the repository, so the endpoint never depends
    on Valkey being up. Entries are dropped by ``SpeciesCacheConsumer`` when
    ``SpeciesUpdated`` or ``SpeciesCacheInvalidated`` arrives, and expire on
    their own TTL.
    """

    def __init__(self, species: SpeciesRepository, species_cache: SpeciesCache) -> None:
        self._species = species
        self._species_cache = species_cache

    async def handle(self, query: GetSpeciesQuery) -> SpeciesView:
        """Return the species, or raise :class:`NotFoundError`."""
        cached = await self._species_cache.get(query.species_id)
        if cached is not None:
            return cached
        species = await self._species.get(query.species_id)
        if species is None:
            raise NotFoundError(f"species {query.species_id} does not exist")
        view = SpeciesView.from_domain(species)
        await self._species_cache.set(view)
        return view
