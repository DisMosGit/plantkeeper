"""Garden queries.

Two kinds of question live here, and CQRS answers them from two places. Reading
one plant is the answer to a command the caller just ran — or a read a command
needs — so it stays on the write store. Listing a household's plants is a question
about what the system looks like, so it is answered from the read model
(``docs/cqrs.md``).
"""

from __future__ import annotations

from plantkeeper.application.errors import NotFoundError
from plantkeeper.application.ports.read_models import PlantListRow, ReadModelReader
from plantkeeper.application.ports.repositories import HouseholdRepository, PlantRepository
from plantkeeper.application.queries.base import Query, QueryHandler
from plantkeeper.application.views import CollectionView, HouseholdView, PlantView
from plantkeeper.domain.identifiers import HouseholdId, PlantId


class GetPlantQuery(Query):
    """Read one plant."""

    plant_id: PlantId


class GetPlantQueryHandler(QueryHandler[GetPlantQuery, PlantView]):
    """Answer with the plant, or fail.

    The write store, not the read model: this is what ``POST /plants`` answers its
    caller with, and a plant that was just created must not read as missing while
    the projection catches up.
    """

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
    """Answer with the household's plants, oldest first, from the read model.

    The answer reflects projected state: a plant created moments ago may not be
    listed until ``GardenProjection`` has seen its ``PlantAdded``. That window is
    the price of the split, and it is documented in ``docs/cqrs.md``.
    """

    def __init__(self, read_models: ReadModelReader) -> None:
        self._read_models = read_models

    async def handle(self, query: ListPlantsQuery) -> CollectionView[PlantView]:
        """Return every projected plant of the household."""
        rows = await self._read_models.list_plants(
            query.household_id, include_removed=query.include_removed
        )
        return CollectionView[PlantView](items=[_plant_view(row) for row in rows])


def _plant_view(row: PlantListRow) -> PlantView:
    """Turn a read-model row into the view the API answers with."""
    return PlantView(
        plant_id=row.plant_id,
        household_id=row.household_id,
        species_id=row.species_id,
        name=row.name,
        location=row.location,
        added_at=row.added_at,
        last_watered_at=row.last_watered_at,
        removed=row.removed,
    )


class GetHouseholdQuery(Query):
    """Read one household."""

    household_id: HouseholdId


class GetHouseholdQueryHandler(QueryHandler[GetHouseholdQuery, HouseholdView]):
    """Answer with the household and how many plants it owns.

    The write store: the household is what ``POST /households`` answers with, and
    what ``POST /plants`` reads to enforce its cap, so both the command's answer
    and the read a command needs come from the side that owns the write.
    """

    def __init__(self, households: HouseholdRepository) -> None:
        self._households = households

    async def handle(self, query: GetHouseholdQuery) -> HouseholdView:
        """Return the household, or raise :class:`NotFoundError`."""
        household = await self._households.get(query.household_id)
        if household is None:
            raise NotFoundError(f"household {query.household_id} does not exist")
        return HouseholdView.from_domain(household)
