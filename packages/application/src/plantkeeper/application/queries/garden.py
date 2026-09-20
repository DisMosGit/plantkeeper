"""Garden queries."""

from __future__ import annotations

from plantkeeper.application.errors import NotFoundError
from plantkeeper.application.ports.repositories import HouseholdRepository, PlantRepository
from plantkeeper.application.queries.base import Query, QueryHandler
from plantkeeper.application.views import CollectionView, HouseholdView, PlantView
from plantkeeper.domain.identifiers import HouseholdId, PlantId


class GetPlantQuery(Query):
    """Read one plant."""

    plant_id: PlantId


class GetPlantQueryHandler(QueryHandler[GetPlantQuery, PlantView]):
    """Answer with the plant, or fail."""

    def __init__(self, plants: PlantRepository) -> None:
        self._plants = plants

    async def handle(self, query: GetPlantQuery) -> PlantView:
        """Return the plant, or raise :class:`NotFoundError`."""
        plant = await self._plants.get(query.plant_id)
        if plant is None:
            raise NotFoundError(f"plant {query.plant_id} does not exist")
        return PlantView.from_domain(plant)


class ListPlantsQuery(Query):
    """Read a household's plants."""

    household_id: HouseholdId
    include_removed: bool = False


class ListPlantsQueryHandler(QueryHandler[ListPlantsQuery, CollectionView[PlantView]]):
    """Answer with the household's plants, oldest first."""

    def __init__(self, plants: PlantRepository) -> None:
        self._plants = plants

    async def handle(self, query: ListPlantsQuery) -> CollectionView[PlantView]:
        """Return every plant of the household, unless removed ones are asked for."""
        plants = await self._plants.list_by_household(
            query.household_id, include_removed=query.include_removed
        )
        return CollectionView[PlantView](items=[PlantView.from_domain(p) for p in plants])


class GetHouseholdQuery(Query):
    """Read one household."""

    household_id: HouseholdId


class GetHouseholdQueryHandler(QueryHandler[GetHouseholdQuery, HouseholdView]):
    """Answer with the household and how many plants it owns."""

    def __init__(self, households: HouseholdRepository) -> None:
        self._households = households

    async def handle(self, query: GetHouseholdQuery) -> HouseholdView:
        """Return the household, or raise :class:`NotFoundError`."""
        household = await self._households.get(query.household_id)
        if household is None:
            raise NotFoundError(f"household {query.household_id} does not exist")
        return HouseholdView.from_domain(household)
