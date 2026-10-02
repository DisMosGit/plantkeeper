"""Retention: the far end of the partition window, and what outlives it.

Raw readings are kept for a documented window, and the partition job enforces it by
dropping the partition of every month that lies entirely beyond it — a month per
``DROP TABLE`` rather than a ``DELETE`` of everything in it. Two things have to stay
true when that happens, and both need a real database:

* a reading whose month has been dropped still lands somewhere: the catch-all
  partition is the safety net the partition window is built on;
* the rollups computed from the dropped readings survive, because they live on the
  read instance and were never a copy of the raw rows.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from uuid import uuid4

import pytest
from asgiref.sync import sync_to_async
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from plantkeeper.admin.projections import ALL_PROJECTIONS
from plantkeeper.admin.projections.base import apply_event
from plantkeeper.admin.read_telemetry.models import TelemetryRollup
from plantkeeper.domain.identifiers import PlantId, SensorId
from plantkeeper.domain.telemetry.events import TelemetryReceived
from plantkeeper.domain.telemetry.reading import TelemetryReading
from plantkeeper.domain.values import LightLevel, Moisture, Temperature
from plantkeeper.infrastructure.persistence.models.telemetry import SensorReadingModel
from plantkeeper.infrastructure.persistence.partitions import (
    drop_expired_partitions,
    partition_name,
    partition_statement,
)
from plantkeeper.infrastructure.persistence.repositories.telemetry import (
    SqlAlchemyTelemetryRepository,
)

pytestmark = pytest.mark.integration

TODAY = date(2026, 7, 15)
RETENTION_MONTHS = 12

AGED_MONTH = date(2020, 1, 1)
AGED_READING = datetime(2020, 1, 15, 12, 0, tzinfo=UTC)

CONSUMER_GROUP = "test-telemetry-retention"


async def create_partition(session_factory: async_sessionmaker[AsyncSession], month: date) -> None:
    """Create one month's partition explicitly, as if it had been created in time."""
    async with session_factory() as session:
        await session.execute(text(partition_statement(month)))
        await session.commit()


async def store_reading(
    session_factory: async_sessionmaker[AsyncSession], *, recorded_at: datetime
) -> TelemetryReading:
    """Store one raw reading and return it."""
    reading = TelemetryReading(
        sensor_id=SensorId(uuid4()),
        plant_id=PlantId(uuid4()),
        recorded_at=recorded_at,
        moisture=Moisture(value=31.5),
        temperature=Temperature(value=19.0),
        light=LightLevel(value=700.0),
    )
    async with session_factory() as session:
        await SqlAlchemyTelemetryRepository(session).add_many([reading])
        await session.commit()
    return reading


async def partition_names(session_factory: async_sessionmaker[AsyncSession]) -> set[str]:
    """The bare names of the readings table's partitions."""
    statement = text(
        """
        SELECT child.relname
        FROM pg_inherits
        JOIN pg_class AS parent ON parent.oid = pg_inherits.inhparent
        JOIN pg_class AS child ON child.oid = pg_inherits.inhrelid
        JOIN pg_namespace AS namespace ON namespace.oid = parent.relnamespace
        WHERE namespace.nspname = 'write_telemetry' AND parent.relname = 'sensor_readings'
        """
    )
    async with session_factory() as session:
        return {row[0] for row in (await session.execute(statement)).all()}


async def partition_of(
    session_factory: async_sessionmaker[AsyncSession], *, recorded_at: datetime
) -> str:
    """Which partition holds the reading recorded at ``recorded_at``."""
    async with session_factory() as session:
        statement = text(
            "SELECT tableoid::regclass::text FROM write_telemetry.sensor_readings "
            "WHERE recorded_at = :recorded_at"
        )
        return str((await session.execute(statement, {"recorded_at": recorded_at})).scalar_one())


async def reading_count(session_factory: async_sessionmaker[AsyncSession]) -> int:
    """How many readings the table holds, across every partition."""
    async with session_factory() as session:
        return int(
            (
                await session.execute(select(func.count()).select_from(SensorReadingModel))
            ).scalar_one()
        )


async def test_a_month_entirely_beyond_the_window_is_dropped_with_its_rows(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """One month, one ``DROP TABLE``: the rows inside it go with the partition."""
    await create_partition(session_factory, AGED_MONTH)
    await store_reading(session_factory, recorded_at=AGED_READING)
    aged = partition_name(AGED_MONTH).rpartition(".")[2]
    assert aged in await partition_names(session_factory)
    assert await partition_of(session_factory, recorded_at=AGED_READING) == (
        "write_telemetry.sensor_readings_202001"
    )

    async with session_factory() as session:
        dropped = await drop_expired_partitions(
            session, today=TODAY, retention_months=RETENTION_MONTHS
        )
        await session.commit()

    assert dropped == [partition_name(AGED_MONTH)]
    assert aged not in await partition_names(session_factory)
    assert await reading_count(session_factory) == 0


async def test_a_reading_for_a_dropped_month_lands_in_the_catch_all(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """The retention window is a limit, not a failure: the reading is still stored."""
    await create_partition(session_factory, AGED_MONTH)
    async with session_factory() as session:
        await drop_expired_partitions(session, today=TODAY, retention_months=RETENTION_MONTHS)
        await session.commit()

    await store_reading(session_factory, recorded_at=AGED_READING)

    assert await partition_of(session_factory, recorded_at=AGED_READING) == (
        "write_telemetry.sensor_readings_default"
    )


async def test_the_rollup_of_a_dropped_month_outlives_the_readings(
    session_factory: async_sessionmaker[AsyncSession], read_side_database: str
) -> None:
    """The rollups are the read side's own rows; dropping raw readings cannot touch them."""
    await create_partition(session_factory, AGED_MONTH)
    reading = await store_reading(session_factory, recorded_at=AGED_READING)
    event = TelemetryReceived(
        sensor_id=reading.sensor_id,
        plant_id=reading.plant_id,
        recorded_at=reading.recorded_at,
        moisture=reading.moisture,
        temperature=reading.temperature,
        light=reading.light,
    )
    projected = 0
    for projection in ALL_PROJECTIONS:
        if await apply_event(
            projection, event, consumer_group=f"{CONSUMER_GROUP}-{projection.name}"
        ):
            projected += 1
    assert projected == 1

    # Django's ORM is synchronous and this test is async; the reads go through the
    # same ``sync_to_async`` wrapper the projections use.
    assert await rollup_count(reading.sensor_id) == 1

    async with session_factory() as session:
        await drop_expired_partitions(session, today=TODAY, retention_months=RETENTION_MONTHS)
        await session.commit()

    assert await reading_count(session_factory) == 0
    assert await rollup_count(reading.sensor_id) == 1


async def rollup_count(sensor_id: SensorId) -> int:
    """How many rollup windows the read side holds for one sensor."""

    def query() -> int:
        return int(TelemetryRollup.objects.filter(sensor_id=sensor_id.value).count())

    return await sync_to_async(query, thread_sensitive=True)()
