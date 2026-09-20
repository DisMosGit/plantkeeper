"""End-to-end test of the telemetry pipeline.

Phase 5's acceptance test: the simulator's stream goes to ``telemetry.raw``, the
worker's real ingress registration stores each reading and announces it through
its outbox, the relay publishes ``TelemetryReceived``, and ``AdaptiveWateringSaga``
moves a care schedule forward because the soil is dry.

The pieces the simulator, the worker and the saga share are the production ones —
``register_telemetry_ingest``, ``register_consumers`` and ``OutboxRelay`` — so this
exercises the wiring rather than a test-only path.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

import pytest
from aiokafka import AIOKafkaProducer
from dishka import make_async_container
from faststream.kafka import KafkaBroker
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from plantkeeper.domain.care.schedule import CareSchedule
from plantkeeper.domain.identifiers import PlantId, SensorId
from plantkeeper.domain.telemetry.reading import TelemetryReading
from plantkeeper.domain.telemetry.sensor import Sensor
from plantkeeper.domain.values import WateringInterval
from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.di.providers import worker_providers
from plantkeeper.infrastructure.messaging.relay import OutboxRelay
from plantkeeper.infrastructure.messaging.topics import TELEMETRY_RAW
from plantkeeper.infrastructure.persistence.models.shared import OutboxModel
from plantkeeper.infrastructure.persistence.repositories.care import (
    SqlAlchemyCareScheduleRepository,
)
from plantkeeper.infrastructure.persistence.repositories.telemetry import (
    SqlAlchemySensorRepository,
    SqlAlchemyTelemetryRepository,
)
from plantkeeper.infrastructure.persistence.tracking import AggregateTracker
from plantkeeper.iot_simulator.scenarios import DROUGHT_SCENARIO
from plantkeeper.iot_simulator.simulator import (
    JSON_TOPIC,
    Reading,
    SensorSimulator,
    SensorSimulatorConfig,
    sensor_id_for,
)
from plantkeeper.workers.consumers import register_consumers, register_telemetry_ingest

pytestmark = pytest.mark.slow

PIPELINE_TIMEOUT_SECONDS = 45.0
POLL_INTERVAL_SECONDS = 0.25
WEEK = timedelta(days=7)

SIMULATOR_BASE_ID = uuid.UUID("00000000-0000-0000-0000-0000000000e5")
"""The base id the simulator derives its sensor ids from.

A run's sensor ids are ``uuid5(namespace, "<base>:<index>")``, not the base id
itself, so the test registers what :func:`sensor_id_for` says the simulator will
publish as, which is exactly what an operator does with ``--sensor-base-id``.
"""


def server_factory_on(database: str) -> async_sessionmaker[AsyncSession]:
    """A session factory over the test database."""
    return async_sessionmaker(create_async_engine(database), expire_on_commit=False)


async def poll_until[PolledT](
    check: Callable[[], Awaitable[PolledT | None]],
    *,
    description: str,
    timeout: float = PIPELINE_TIMEOUT_SECONDS,
) -> PolledT:
    """Poll ``check`` until it answers something, or fail the test.

    Polling rather than sleeping a fixed span: the pipeline crosses two Kafka hops
    and a database transaction, and a fixed wait would either be a flake or waste
    the suite's time.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        found = await check()
        if found is not None:
            return found
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
    pytest.fail(f"{description} did not happen within {timeout}s")


@asynccontextmanager
async def running_worker(settings: Settings) -> AsyncIterator[None]:
    """Run the worker's ingress, saga consumers and relay in-process.

    The registration calls are the production ones — the same two ``apps/workers``
    makes before it starts the broker — so the test exercises the wiring, not a
    test-only path. The relay runs in the background here because the pipeline has
    two hops through it: the reading's event out, and the saga's reschedule back
    in.
    """
    container = make_async_container(*worker_providers())
    broker = await container.get(KafkaBroker)
    relay = await container.get(OutboxRelay)
    register_consumers(broker, container=container, settings=settings)
    register_telemetry_ingest(broker, container=container, settings=settings)
    await broker.start()
    relay_task = asyncio.create_task(relay.run())
    try:
        yield
    finally:
        relay.stop()
        relay_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await relay_task
        await broker.stop()
        await container.close()


async def publish_raw(bootstrap_servers: str, readings: list[Reading]) -> None:
    """Publish readings exactly as the simulator's producer does."""
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await producer.start()
    try:
        for reading in readings:
            await producer.send_and_wait(
                TELEMETRY_RAW,
                reading.to_json().encode("utf-8"),
                key=str(reading.sensor_id).encode("utf-8"),
            )
    finally:
        await producer.stop()


async def seed_plant_with_sensor(
    database: str,
    *,
    sensor_id: SensorId,
    plant_id: PlantId,
    next_watering_at: datetime | None = None,
) -> None:
    """Bind a sensor to a plant, and give the plant a watering schedule."""
    factory = server_factory_on(database)
    async with factory() as session:
        await SqlAlchemySensorRepository(session, AggregateTracker()).add(
            Sensor(sensor_id, plant_id=plant_id, added_at=datetime.now(UTC))
        )
        if next_watering_at is not None:
            await SqlAlchemyCareScheduleRepository(session, AggregateTracker()).add(
                CareSchedule.create(
                    plant_id=plant_id,
                    watering_interval=WateringInterval(value=WEEK),
                    starts_at=next_watering_at,
                    now=datetime.now(UTC),
                )
            )
        await session.commit()


async def stored_readings(database: str, plant_id: PlantId) -> list[TelemetryReading]:
    """The plant's stored readings, oldest first.

    Returns :class:`~plantkeeper.iot_simulator.simulator.Reading`-shaped objects by
    name only: the repository hands back ``TelemetryReading``, which is what the
    assertions read fields off.
    """
    factory = server_factory_on(database)
    async with factory() as session:
        return list(await SqlAlchemyTelemetryRepository(session).list_by_plant(plant_id, limit=10))


async def matched_outbox_names(database: str, event_name: str) -> list[str]:
    """Return the names of outbox rows carrying ``event_name``."""
    factory = server_factory_on(database)
    async with factory() as session:
        statement = select(OutboxModel).where(OutboxModel.event_name == event_name)
        return [row.event_name for row in (await session.execute(statement)).scalars().all()]


async def current_schedule(database: str, plant_id: PlantId) -> CareSchedule | None:
    """The plant's care schedule, or ``None``."""
    factory = server_factory_on(database)
    async with factory() as session:
        return await SqlAlchemyCareScheduleRepository(session, AggregateTracker()).get(plant_id)


async def create_plant(client: AsyncClient, household_id: str) -> str:
    """Create a plant through the API and return its identifier."""
    response = await client.post(
        "/api/v1/plants",
        json={
            "household_id": household_id,
            "species_id": str(uuid.uuid4()),
            "name": "Fern",
            "location": "Shelf",
        },
    )
    assert response.status_code == 201, response.text
    plant_id: str = response.json()["plant_id"]
    return plant_id


async def a_household_and_plant(client: AsyncClient) -> tuple[str, PlantId]:
    """Create a household with one plant through the API."""
    household = await client.post("/api/v1/households", json={"name": "Home"})
    assert household.status_code == 201, household.text
    plant_id = PlantId(uuid.UUID(await create_plant(client, household.json()["household_id"])))
    return household.json()["household_id"], plant_id


async def test_a_simulated_stream_reaches_the_telemetry_table(
    api_client: AsyncClient,
    database: str,
    kafka_bootstrap_servers: str,
    worker_settings: Settings,
) -> None:
    """Simulator → ``telemetry.raw`` → ingress → readings table + outbox event."""
    _, plant_id = await a_household_and_plant(api_client)
    sensor_id = SensorId(sensor_id_for(SIMULATOR_BASE_ID, 0))
    await seed_plant_with_sensor(database, sensor_id=sensor_id, plant_id=plant_id)

    simulator = SensorSimulator(
        SensorSimulatorConfig(
            sensor_count=1,
            base_id=SIMULATOR_BASE_ID,
            scenario=DROUGHT_SCENARIO,
            seed=42,
            step_seconds=10.0,
        )
    )
    readings = simulator.readings(datetime.now(UTC))
    assert readings, "the scenario must produce at least one reading"

    async with running_worker(worker_settings):
        await publish_raw(kafka_bootstrap_servers, readings)

        async def find_reading() -> TelemetryReading | None:
            stored = await stored_readings(database, plant_id)
            return stored[0] if stored else None

        stored = await poll_until(find_reading, description="the reading reaching the table")

    assert stored.sensor_id == sensor_id
    assert stored.recorded_at == readings[0].recorded_at
    assert stored.moisture.value == pytest.approx(readings[0].moisture, abs=0.001)
    assert await matched_outbox_names(database, "TelemetryReceived") == ["TelemetryReceived"]


async def test_dry_telemetry_moves_the_watering_forward(
    api_client: AsyncClient,
    database: str,
    kafka_bootstrap_servers: str,
    worker_settings: Settings,
) -> None:
    """The simulator's drought scenario is enough to trip ``AdaptiveWateringSaga``."""
    _, plant_id = await a_household_and_plant(api_client)
    sensor_id = SensorId(sensor_id_for(SIMULATOR_BASE_ID, 0))
    scheduled_for = datetime.now(UTC) + WEEK
    await seed_plant_with_sensor(
        database,
        sensor_id=sensor_id,
        plant_id=plant_id,
        next_watering_at=scheduled_for,
    )

    simulator = SensorSimulator(
        SensorSimulatorConfig(
            sensor_count=1,
            base_id=SIMULATOR_BASE_ID,
            scenario=DROUGHT_SCENARIO,
            seed=7,
            step_seconds=10.0,
        )
    )
    readings = simulator.readings(datetime.now(UTC))
    assert readings and readings[0].moisture < 30.0, "drought starts below the low threshold"

    async with running_worker(worker_settings):
        await publish_raw(kafka_bootstrap_servers, readings)

        async def moved_forward() -> CareSchedule | None:
            schedule = await current_schedule(database, plant_id)
            if schedule is None or schedule.next_watering_at >= scheduled_for:
                return None
            return schedule

        schedule = await poll_until(
            moved_forward, description="the saga moving the watering forward"
        )

    assert schedule.version == 2
    assert schedule.next_watering_at <= datetime.now(UTC) + timedelta(seconds=5)
    assert await matched_outbox_names(database, "WateringRescheduled") == ["WateringRescheduled"]


def test_the_simulator_and_the_worker_agree_on_the_topic() -> None:
    """A rename on either side would make the pipeline silently do nothing."""
    assert JSON_TOPIC == TELEMETRY_RAW
