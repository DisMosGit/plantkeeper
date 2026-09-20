"""SQLAlchemy repository of the Catalog context."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from plantkeeper.domain.catalog.species import Species
from plantkeeper.domain.identifiers import SpeciesId
from plantkeeper.infrastructure.persistence.mappers.catalog import (
    species_to_domain,
    species_to_model,
)
from plantkeeper.infrastructure.persistence.models.catalog import SpeciesModel
from plantkeeper.infrastructure.persistence.tracking import AggregateTracker


class SqlAlchemySpeciesRepository:
    """The ``SpeciesRepository`` port over SQLAlchemy."""

    def __init__(self, session: AsyncSession, tracker: AggregateTracker) -> None:
        self._session = session
        self._tracker = tracker

    async def add(self, species: Species) -> None:
        """Insert a new catalogue entry."""
        self._session.add(species_to_model(species))
        self._tracker.track(species)

    async def get(self, species_id: SpeciesId) -> Species | None:
        """Return the species, or ``None``. Takes no lock."""
        model = await self._session.get(SpeciesModel, species_id.value)
        return None if model is None else species_to_domain(model)

    async def get_for_update(self, species_id: SpeciesId) -> Species | None:
        """Return the species, locking it for this transaction."""
        statement = (
            select(SpeciesModel).where(SpeciesModel.id == species_id.value).with_for_update()
        )
        model = (await self._session.execute(statement)).scalar_one_or_none()
        return None if model is None else species_to_domain(model)

    async def save(self, species: Species) -> None:
        """Persist the species' current state and version."""
        await self._session.merge(species_to_model(species))
        self._tracker.track(species)

    async def delete(self, species_id: SpeciesId) -> None:
        """Remove the catalogue entry, if it exists."""
        model = await self._session.get(SpeciesModel, species_id.value)
        if model is not None:
            await self._session.delete(model)

    async def list_all(self) -> list[Species]:
        """List every species, by scientific name."""
        statement = select(SpeciesModel).order_by(SpeciesModel.scientific_name, SpeciesModel.id)
        models = (await self._session.execute(statement)).scalars()
        return [species_to_domain(model) for model in models]
