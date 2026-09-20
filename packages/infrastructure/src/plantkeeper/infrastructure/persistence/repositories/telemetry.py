"""SQLAlchemy repository of the Telemetry context."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from plantkeeper.domain.identifiers import PlantId, SensorId
from plantkeeper.domain.telemetry.sensor import Sensor
from plantkeeper.infrastructure.persistence.mappers.telemetry import (
    sensor_to_domain,
    sensor_to_model,
)
from plantkeeper.infrastructure.persistence.models.telemetry import SensorModel
from plantkeeper.infrastructure.persistence.tracking import AggregateTracker


class SqlAlchemySensorRepository:
    """The ``SensorRepository`` port over SQLAlchemy."""

    def __init__(self, session: AsyncSession, tracker: AggregateTracker) -> None:
        self._session = session
        self._tracker = tracker

    async def add(self, sensor: Sensor) -> None:
        """Register a sensor."""
        self._session.add(sensor_to_model(sensor))
        self._tracker.track(sensor)

    async def get(self, sensor_id: SensorId) -> Sensor | None:
        """Return the sensor, or ``None``."""
        model = await self._session.get(SensorModel, sensor_id.value)
        return None if model is None else sensor_to_domain(model)

    async def save(self, sensor: Sensor) -> None:
        """Persist the sensor's current state."""
        await self._session.merge(sensor_to_model(sensor))
        self._tracker.track(sensor)

    async def delete(self, sensor_id: SensorId) -> None:
        """Unregister the sensor, if it exists."""
        model = await self._session.get(SensorModel, sensor_id.value)
        if model is not None:
            await self._session.delete(model)

    async def list_by_plant(self, plant_id: PlantId) -> list[Sensor]:
        """List the sensors bound to one plant, oldest first."""
        statement = (
            select(SensorModel)
            .where(SensorModel.plant_id == plant_id.value)
            .order_by(SensorModel.added_at, SensorModel.id)
        )
        models = (await self._session.execute(statement)).scalars()
        return [sensor_to_domain(model) for model in models]
