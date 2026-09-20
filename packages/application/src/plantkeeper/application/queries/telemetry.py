"""Telemetry queries."""

from __future__ import annotations

from plantkeeper.application.ports.repositories import SensorRepository
from plantkeeper.application.queries.base import Query, QueryHandler
from plantkeeper.application.views import CollectionView, SensorView
from plantkeeper.domain.identifiers import PlantId


class ListSensorsQuery(Query):
    """Read the sensors bound to a plant."""

    plant_id: PlantId


class ListSensorsQueryHandler(QueryHandler[ListSensorsQuery, CollectionView[SensorView]]):
    """Answer with the plant's sensors, oldest first."""

    def __init__(self, sensors: SensorRepository) -> None:
        self._sensors = sensors

    async def handle(self, query: ListSensorsQuery) -> CollectionView[SensorView]:
        """Return the sensors of the plant."""
        sensors = await self._sensors.list_by_plant(query.plant_id)
        return CollectionView[SensorView](items=[SensorView.from_domain(s) for s in sensors])
