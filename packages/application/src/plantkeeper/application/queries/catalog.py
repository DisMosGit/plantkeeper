"""Catalog queries."""

from __future__ import annotations

from plantkeeper.application.errors import NotFoundError
from plantkeeper.application.ports.repositories import SpeciesRepository
from plantkeeper.application.queries.base import Query, QueryHandler
from plantkeeper.application.views import CollectionView, SpeciesView
from plantkeeper.domain.identifiers import SpeciesId


class ListSpeciesQuery(Query):
    """Read the local catalogue."""


class ListSpeciesQueryHandler(QueryHandler[ListSpeciesQuery, CollectionView[SpeciesView]]):
    """Answer with every species, by scientific name."""

    def __init__(self, species: SpeciesRepository) -> None:
        self._species = species

    async def handle(self, query: ListSpeciesQuery) -> CollectionView[SpeciesView]:
        """Return the catalogue."""
        species = await self._species.list_all()
        return CollectionView[SpeciesView](items=[SpeciesView.from_domain(s) for s in species])


class GetSpeciesQuery(Query):
    """Read one catalogue entry."""

    species_id: SpeciesId


class GetSpeciesQueryHandler(QueryHandler[GetSpeciesQuery, SpeciesView]):
    """Answer with the species, or fail."""

    def __init__(self, species: SpeciesRepository) -> None:
        self._species = species

    async def handle(self, query: GetSpeciesQuery) -> SpeciesView:
        """Return the species, or raise :class:`NotFoundError`."""
        species = await self._species.get(query.species_id)
        if species is None:
            raise NotFoundError(f"species {query.species_id} does not exist")
        return SpeciesView.from_domain(species)
