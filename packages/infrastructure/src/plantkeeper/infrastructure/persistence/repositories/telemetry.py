"""SQLAlchemy repository of the Telemetry context."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any, Final, cast

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession

from plantkeeper.domain.identifiers import PlantId, SensorId
from plantkeeper.domain.telemetry.reading import TelemetryReading
from plantkeeper.domain.telemetry.sensor import Sensor
from plantkeeper.infrastructure.persistence.mappers.telemetry import (
    reading_to_domain,
    reading_to_model,
    sensor_to_domain,
    sensor_to_model,
)
from plantkeeper.infrastructure.persistence.models.telemetry import (
    SensorModel,
    SensorReadingModel,
)
from plantkeeper.infrastructure.persistence.tracking import AggregateTracker

READING_BATCH_SIZE: Final = 500
"""Rows per ``INSERT``.

A bound parameter ceiling, not a throughput knob: an insert with every column of
every row is refused by PostgreSQL past 65535 parameters, and 500 readings is
comfortably inside it while still being one round trip for a realistic batch.
"""

DEFAULT_READING_LIMIT: Final = 1000


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


class SqlAlchemyTelemetryRepository:
    """The ``TelemetryRepository`` port over SQLAlchemy."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add_many(self, readings: Sequence[TelemetryReading]) -> int:
        """Store readings, letting the table's key skip the ones already stored.

        ``ON CONFLICT DO NOTHING`` rather than a pre-flight existence check: the
        check would race two consumers, and the conflict target is the same pair
        of columns either way. ``rowcount`` is what makes a duplicate legible to
        the caller — it counts rows the database actually inserted.
        """
        inserted = 0
        rows = [reading_to_model(reading) for reading in readings]
        for start in range(0, len(rows), READING_BATCH_SIZE):
            batch = rows[start : start + READING_BATCH_SIZE]
            statement = (
                insert(SensorReadingModel)
                .values(
                    [
                        {
                            "sensor_id": row.sensor_id,
                            "recorded_at": row.recorded_at,
                            "plant_id": row.plant_id,
                            "moisture": row.moisture,
                            "temperature": row.temperature,
                            "light": row.light,
                        }
                        for row in batch
                    ]
                )
                .on_conflict_do_nothing(index_elements=["sensor_id", "recorded_at"])
            )
            # This is an ``INSERT``, so the result is a ``CursorResult`` and
            # ``rowcount`` is the rows the database actually inserted — the rows
            # the conflict clause skipped are not counted. SQLAlchemy types the
            # async result as the base ``Result``, hence the cast.
            inserted += cast("CursorResult[Any]", await self._session.execute(statement)).rowcount
        return inserted

    async def list_by_plant(
        self,
        plant_id: PlantId,
        *,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int = DEFAULT_READING_LIMIT,
    ) -> list[TelemetryReading]:
        """List a plant's readings, oldest first, optionally within a window.

        Filtered on the readings' own denormalised ``plant_id`` — the column the
        ingress fills from the registry and the one
        ``ix_sensor_readings_plant_id_recorded_at`` covers — so a reader never
        joins to ask what a plant's soil has been doing.
        """
        statement = select(SensorReadingModel).where(SensorReadingModel.plant_id == plant_id.value)
        if since is not None:
            statement = statement.where(SensorReadingModel.recorded_at >= since)
        if until is not None:
            statement = statement.where(SensorReadingModel.recorded_at <= until)
        statement = statement.order_by(SensorReadingModel.recorded_at).limit(limit)
        models = (await self._session.execute(statement)).scalars()
        return [reading_to_domain(model) for model in models]
