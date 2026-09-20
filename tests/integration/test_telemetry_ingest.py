"""The telemetry ingress against a real Postgres: partitions, keys, transactions.

The unit suite pins the consumer's decisions; this one pins the two facts that
only the database can answer:

* the readings table really is partitioned by month, the current window exists,
  and an out-of-window reading lands in the catch-all rather than failing;
* ``(sensor_id, recorded_at)`` really is the idempotency key, so a redelivery
  inserts nothing while the reading and its ``TelemetryReceived`` still travel in
  one transaction.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from plantkeeper.application.telemetry.ingest import TelemetryIngestConsumer
from plantkeeper.domain.identifiers import PlantId, SensorId
from plantkeeper.domain.telemetry.events import TelemetryReceived
from plantkeeper.domain.telemetry.reading import TelemetryReading
from plantkeeper.domain.telemetry.sensor import Sensor
from plantkeeper.domain.values import LightLevel, Moisture, Temperature
from plantkeeper.infrastructure.persistence.models.shared import OutboxModel
from plantkeeper.infrastructure.persistence.models.telemetry import SensorReadingModel
from plantkeeper.infrastructure.persistence.partitions import (
    ensure_telemetry_partitions,
    month_of,
    partition_name,
)
from plantkeeper.infrastructure.persistence.repositories.telemetry import (
    SqlAlchemySensorRepository,
    SqlAlchemyTelemetryRepository,
)
from plantkeeper.infrastructure.persistence.tracking import AggregateTracker
from plantkeeper.infrastructure.persistence.uow import SqlAlchemyUnitOfWork

pytestmark = pytest.mark.integration

NOW = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
GROUP = "test-telemetry-ingest"
OUT_OF_WINDOW = datetime(2019, 3, 4, 5, 6, tzinfo=UTC)


class FixedClock:
    """A clock the test sets."""

    def __init__(self, now: datetime = NOW) -> None:
        self._now = now

    def now(self) -> datetime:
        """Return the instant the test has set."""
        return self._now


def a_reading(
    sensor_id: SensorId,
    plant_id: PlantId,
    *,
    recorded_at: datetime = NOW,
    moisture: float = 44.5,
) -> TelemetryReading:
    """One reading with a known payload."""
    return TelemetryReading(
        sensor_id=sensor_id,
        plant_id=plant_id,
        recorded_at=recorded_at,
        moisture=Moisture(value=moisture),
        temperature=Temperature(value=21.0),
        light=LightLevel(value=900.0),
    )


async def register_sensor(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    sensor_id: SensorId,
    plant_id: PlantId,
) -> None:
    """Write one sensor binding, as the API's command handler would."""
    async with session_factory() as session:
        repository = SqlAlchemySensorRepository(session, AggregateTracker())
        await repository.add(
            Sensor(SensorId(sensor_id.value), plant_id=plant_id, added_at=NOW),
        )
        await session.commit()


async def stored_readings(
    session_factory: async_sessionmaker[AsyncSession],
) -> list[SensorReadingModel]:
    """Every reading row, oldest first."""
    async with session_factory() as session:
        statement = select(SensorReadingModel).order_by(SensorReadingModel.recorded_at)
        return list((await session.execute(statement)).scalars().all())


async def partition_names(session_factory: async_sessionmaker[AsyncSession]) -> set[str]:
    """The names of the readings table's partitions, from the catalogue."""
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


async def a_raw_message(sensor_id: SensorId, *, recorded_at: datetime = NOW) -> bytes:
    """The JSON the simulator publishes, for a registered sensor."""
    return json.dumps(
        {
            "sensor_id": str(sensor_id.value),
            "recorded_at": recorded_at.isoformat(),
            "moisture": 12.5,
            "temperature": 21.0,
            "light": 900.0,
        }
    ).encode("utf-8")


# --- partitions ---------------------------------------------------------------


async def test_the_migration_creates_the_catch_all_partition(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """The safety net exists before any data does, so a write can never be refused."""
    assert "sensor_readings_default" in await partition_names(session_factory)


async def test_the_job_creates_the_window_and_then_finds_it_in_place(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    month = month_of(datetime.now(UTC))
    async with session_factory() as session:
        created = await ensure_telemetry_partitions(session, today=month, months_ahead=3)
        await session.commit()
        again = await ensure_telemetry_partitions(session, today=month, months_ahead=3)
        await session.commit()

    # The catch-all already exists — migration 0003 created it — so the job has
    # only the months to add, and running it twice adds nothing.
    assert partition_name(month) in created
    assert again == []

    names = await partition_names(session_factory)
    assert "sensor_readings_default" in names
    assert partition_name(month).rpartition(".")[2] in names


async def test_an_out_of_window_reading_lands_in_the_catch_all_partition(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    sensor_id, plant_id = SensorId(uuid4()), PlantId(uuid4())
    async with session_factory() as session:
        await SqlAlchemyTelemetryRepository(session).add_many(
            [a_reading(sensor_id, plant_id, recorded_at=OUT_OF_WINDOW)]
        )
        await session.commit()

    async with session_factory() as session:
        statement = text(
            "SELECT tableoid::regclass::text FROM write_telemetry.sensor_readings "
            "WHERE recorded_at = :recorded_at"
        )
        partition = (await session.execute(statement, {"recorded_at": OUT_OF_WINDOW})).scalar_one()

    assert partition == "write_telemetry.sensor_readings_default"


# --- the table's key ----------------------------------------------------------


async def test_a_reading_is_stored_once_however_often_it_is_written(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    sensor_id, plant_id = SensorId(uuid4()), PlantId(uuid4())
    reading = a_reading(sensor_id, plant_id)
    async with session_factory() as session:
        repository = SqlAlchemyTelemetryRepository(session)

        first = await repository.add_many([reading])
        second = await repository.add_many([reading])
        third = await repository.add_many([reading, reading])
        await session.commit()

    assert (first, second, third) == (1, 0, 0)
    assert len(await stored_readings(session_factory)) == 1


async def test_readings_of_one_plant_can_be_read_back_within_a_window(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    sensor_id, plant_id = SensorId(uuid4()), PlantId(uuid4())
    other_plant = PlantId(uuid4())
    async with session_factory() as session:
        await SqlAlchemyTelemetryRepository(session).add_many(
            [
                a_reading(sensor_id, plant_id, recorded_at=NOW, moisture=10.0),
                a_reading(sensor_id, plant_id, recorded_at=NOW + timedelta(hours=1), moisture=20.0),
                a_reading(sensor_id, other_plant, recorded_at=NOW, moisture=30.0),
            ]
        )
        await session.commit()

    async with session_factory() as session:
        repository = SqlAlchemyTelemetryRepository(session)
        every = await repository.list_by_plant(plant_id)
        windowed = await repository.list_by_plant(
            plant_id, since=NOW + timedelta(minutes=30), limit=10
        )
        limited = await repository.list_by_plant(plant_id, limit=1)

    assert [reading.moisture.value for reading in every] == [10.0, 20.0]
    assert [reading.moisture.value for reading in windowed] == [20.0]
    assert len(limited) == 1


# --- the consumer's transaction -----------------------------------------------


async def test_the_consumer_stores_the_reading_and_its_event_together(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    sensor_id, plant_id = SensorId(uuid4()), PlantId(uuid4())
    await register_sensor(session_factory, sensor_id=sensor_id, plant_id=plant_id)

    async with session_factory() as session:
        consumer = TelemetryIngestConsumer(SqlAlchemyUnitOfWork(session), FixedClock())
        assert await consumer.ingest(await a_raw_message(sensor_id)) is True

    readings = await stored_readings(session_factory)
    assert len(readings) == 1
    assert readings[0].sensor_id == sensor_id.value
    assert readings[0].plant_id == plant_id.value
    assert readings[0].moisture == 12.5

    async with session_factory() as session:
        statement = select(OutboxModel).where(OutboxModel.event_name == "TelemetryReceived")
        events = list((await session.execute(statement)).scalars().all())
    assert len(events) == 1
    assert events[0].payload["plant_id"] == str(plant_id.value)
    assert events[0].partition_key == str(plant_id.value)


async def test_a_redelivery_leaves_exactly_one_reading_and_one_event(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """A redelivery is absorbed by the table's key and announces nothing new."""
    sensor_id, plant_id = SensorId(uuid4()), PlantId(uuid4())
    await register_sensor(session_factory, sensor_id=sensor_id, plant_id=plant_id)
    payload = await a_raw_message(sensor_id)

    async with session_factory() as session:
        consumer = TelemetryIngestConsumer(SqlAlchemyUnitOfWork(session), FixedClock())
        first = await consumer.ingest(payload)
        second = await consumer.ingest(payload)

    assert (first, second) == (True, True)
    assert len(await stored_readings(session_factory)) == 1
    async with session_factory() as session:
        statement = select(OutboxModel).where(OutboxModel.event_name == "TelemetryReceived")
        events = list((await session.execute(statement)).scalars().all())
    assert len(events) == 1


async def test_telemetry_from_an_unregistered_sensor_writes_nothing(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    sensor_id = SensorId(uuid4())

    async with session_factory() as session:
        consumer = TelemetryIngestConsumer(SqlAlchemyUnitOfWork(session), FixedClock())
        stored = await consumer.ingest(await a_raw_message(sensor_id))

    assert stored is False
    assert await stored_readings(session_factory) == []
    async with session_factory() as session:
        count = (await session.execute(select(func.count()).select_from(OutboxModel))).scalar_one()
    assert count == 0


async def test_a_failed_transaction_leaves_neither_the_reading_nor_the_event(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    sensor_id, plant_id = SensorId(uuid4()), PlantId(uuid4())
    await register_sensor(session_factory, sensor_id=sensor_id, plant_id=plant_id)

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        async with uow:
            await uow.telemetry.add_many([a_reading(sensor_id, plant_id)])
            await uow.outbox.append(
                TelemetryReceived(
                    sensor_id=sensor_id,
                    plant_id=plant_id,
                    recorded_at=NOW,
                    moisture=Moisture(value=12.5),
                    temperature=Temperature(value=21.0),
                    light=LightLevel(value=900.0),
                    occurred_at=NOW,
                )
            )
            # No commit: the transaction ends the way a crash before the commit
            # would, and neither the reading nor its announcement may survive.
            await uow.rollback()

    assert await stored_readings(session_factory) == []
    async with session_factory() as session:
        statement = select(OutboxModel).where(OutboxModel.event_name == "TelemetryReceived")
        events = list((await session.execute(statement)).scalars().all())
    assert events == []


async def test_a_batch_larger_than_one_statement_is_inserted_once(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    sensor_id, plant_id = SensorId(uuid4()), PlantId(uuid4())
    readings = [
        a_reading(sensor_id, plant_id, recorded_at=NOW + timedelta(seconds=index))
        for index in range(600)
    ]

    async with session_factory() as session:
        inserted = await SqlAlchemyTelemetryRepository(session).add_many(readings)
        await session.commit()
        again = await SqlAlchemyTelemetryRepository(session).add_many(readings)
        await session.commit()

    assert inserted == 600
    assert again == 0
    assert len(await stored_readings(session_factory)) == 600


async def test_a_sensor_registered_with_an_owned_id_answers_to_it(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    owned = SensorId(UUID("00000000-0000-0000-0000-0000000000a1"))
    plant_id = PlantId(uuid4())

    await register_sensor(session_factory, sensor_id=owned, plant_id=plant_id)

    async with session_factory() as session:
        sensor = await SqlAlchemySensorRepository(session, AggregateTracker()).get(owned)
    assert sensor is not None
    assert sensor.id == owned
    assert sensor.plant_id == plant_id
