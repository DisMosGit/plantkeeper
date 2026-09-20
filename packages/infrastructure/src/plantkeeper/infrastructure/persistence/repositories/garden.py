"""SQLAlchemy repositories of the Garden context."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from plantkeeper.domain.garden.household import Household
from plantkeeper.domain.garden.plant import Plant
from plantkeeper.domain.identifiers import HouseholdId, PlantId
from plantkeeper.infrastructure.persistence.mappers.garden import (
    household_to_domain,
    household_to_model,
    plant_to_domain,
    plant_to_model,
)
from plantkeeper.infrastructure.persistence.models.garden import HouseholdModel, PlantModel
from plantkeeper.infrastructure.persistence.tracking import AggregateTracker


class SqlAlchemyPlantRepository:
    """The ``PlantRepository`` port over SQLAlchemy."""

    def __init__(self, session: AsyncSession, tracker: AggregateTracker) -> None:
        self._session = session
        self._tracker = tracker

    async def add(self, plant: Plant) -> None:
        """Insert a new plant."""
        self._session.add(plant_to_model(plant))
        self._tracker.track(plant)

    async def get(self, plant_id: PlantId) -> Plant | None:
        """Return the plant, or ``None``."""
        model = await self._session.get(PlantModel, plant_id.value)
        return None if model is None else plant_to_domain(model)

    async def save(self, plant: Plant) -> None:
        """Persist the plant's current state.

        ``merge`` matches on the primary key, so this is one statement for an
        existing plant and still correct for a plant that was never loaded.
        """
        await self._session.merge(plant_to_model(plant))
        self._tracker.track(plant)

    async def delete(self, plant_id: PlantId) -> None:
        """Remove the plant's row, if it exists."""
        model = await self._session.get(PlantModel, plant_id.value)
        if model is not None:
            await self._session.delete(model)

    async def list_by_household(
        self, household_id: HouseholdId, *, include_removed: bool = False
    ) -> list[Plant]:
        """List the household's plants, oldest first."""
        statement = select(PlantModel).where(PlantModel.household_id == household_id.value)
        if not include_removed:
            statement = statement.where(PlantModel.removed.is_(False))
        statement = statement.order_by(PlantModel.added_at, PlantModel.id)
        models = (await self._session.execute(statement)).scalars()
        return [plant_to_domain(model) for model in models]


class SqlAlchemyHouseholdRepository:
    """The ``HouseholdRepository`` port over SQLAlchemy.

    Membership is read from the plants table: the ``households`` row holds only
    the household's own data, which is what keeps the two from disagreeing.
    """

    def __init__(self, session: AsyncSession, tracker: AggregateTracker) -> None:
        self._session = session
        self._tracker = tracker

    async def add(self, household: Household) -> None:
        """Insert a new household."""
        self._session.add(household_to_model(household))
        self._tracker.track(household)

    async def get(self, household_id: HouseholdId) -> Household | None:
        """Return the household with its current plant list, or ``None``."""
        model = await self._session.get(HouseholdModel, household_id.value)
        if model is None:
            return None
        return household_to_domain(model, await self._plant_ids(household_id))

    async def get_for_update(self, household_id: HouseholdId) -> Household | None:
        """Return the household, locking its row for this transaction."""
        statement = (
            select(HouseholdModel).where(HouseholdModel.id == household_id.value).with_for_update()
        )
        model = (await self._session.execute(statement)).scalar_one_or_none()
        if model is None:
            return None
        return household_to_domain(model, await self._plant_ids(household_id))

    async def save(self, household: Household) -> None:
        """Persist the household's own row.

        The plant list is derived, so there is nothing else to write: a plant
        that was added or removed is persisted through the plants repository.
        """
        await self._session.merge(household_to_model(household))
        self._tracker.track(household)

    async def delete(self, household_id: HouseholdId) -> None:
        """Remove the household and, by cascade, its plants."""
        model = await self._session.get(HouseholdModel, household_id.value)
        if model is not None:
            await self._session.delete(model)

    async def _plant_ids(self, household_id: HouseholdId) -> list[PlantId]:
        statement = select(PlantModel.id).where(
            PlantModel.household_id == household_id.value,
            PlantModel.removed.is_(False),
        )
        return [PlantId(row) for row in (await self._session.execute(statement)).scalars()]
