"""End-to-end tests of the notification stream.

The other end-to-end client is an in-process ASGI transport, which buffers a
response until the application has finished it — an open stream could never be
observed through it. These tests serve the real application over a real socket,
on an ephemeral port, exactly as ``make api`` serves it, and read the stream the
way a client does.

What they pin down is what the stream promises: a notification created while a
stream is open arrives without re-requesting; a reconnect that names its last
notification receives what it missed and nothing it already saw; a stream whose
signal is unavailable degrades to a plain read instead of failing; and an idle
stream holds no database session.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from uuid import UUID, uuid4

import httpx
import pytest
import uvicorn
from aiokafka import AIOKafkaProducer
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from valkey.asyncio import Valkey

from plantkeeper.api.main import create_app
from plantkeeper.api.rest.schemas.notifications import NotificationResponse
from plantkeeper.domain.identifiers import HouseholdId, PlantId, SensorId
from plantkeeper.domain.notifications.notification import Notification
from plantkeeper.domain.notifications.values import NotificationType
from plantkeeper.domain.telemetry.sensor import Sensor
from plantkeeper.infrastructure.messaging.topics import TELEMETRY_RAW
from plantkeeper.infrastructure.notifications.channel import ValkeyNotificationChannel
from plantkeeper.infrastructure.persistence.repositories.notifications import (
    SqlAlchemyNotificationRepository,
)
from plantkeeper.infrastructure.persistence.repositories.telemetry import (
    SqlAlchemySensorRepository,
)
from plantkeeper.infrastructure.persistence.tracking import AggregateTracker

pytestmark = pytest.mark.slow

STREAM_PATH = "/api/v1/notifications/stream"

SETUP_TIMEOUT_SECONDS = 10.0
"""Long enough for a server to bind and a stream to attach; short enough to fail fast."""

READ_TIMEOUT_SECONDS = 30.0
"""How long a read on the stream may block before the test calls it a hang."""

EVENT_TIMEOUT_SECONDS = 25.0
"""Long enough to cross the ingress, the relay, the consumer, the relay again and
the pusher; short enough that a broken pipeline fails the test instead of hanging."""


# --- the server ---------------------------------------------------------------


@asynccontextmanager
async def running_api() -> AsyncIterator[AsyncClient]:
    """Serve the real application over a real socket, on an ephemeral port.

    ``create_app`` is the same factory ``make api`` serves, and its lifespan opens
    and closes the process's container, so the fan-out, the readers and the pool
    are the production ones.
    """
    config = uvicorn.Config(
        create_app(),
        host="127.0.0.1",
        port=0,
        log_level="warning",
        access_log=False,
        log_config=None,
    )
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    try:
        async with asyncio.timeout(SETUP_TIMEOUT_SECONDS):
            while not server.started:
                await asyncio.sleep(0.01)
        port = _bound_port(server)
        timeout = httpx.Timeout(SETUP_TIMEOUT_SECONDS, read=READ_TIMEOUT_SECONDS)
        async with AsyncClient(base_url=f"http://127.0.0.1:{port}", timeout=timeout) as client:
            yield client
    finally:
        server.should_exit = True
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(task, timeout=SETUP_TIMEOUT_SECONDS)


def _bound_port(server: uvicorn.Server) -> int:
    """The port uvicorn bound, since the configuration asked for an ephemeral one."""
    [instance] = server.servers
    sockets = instance.sockets
    assert sockets, "the server has no bound socket"
    return int(sockets[0].getsockname()[1])


# --- reading a stream ---------------------------------------------------------


async def expect_line(lines: AsyncIterator[str], expected: str, *, timeout: float) -> None:
    """Read the stream until the expected line arrives.

    Anything else is skipped — frames for other notifications, keep-alive
    comments — but the stream ending first is a failure: the line asserted on is
    what says the stream is attached to the signal.
    """
    async with asyncio.timeout(timeout):
        while True:
            try:
                line = await anext(lines)
            except StopAsyncIteration:
                raise AssertionError(f"the stream ended before {expected!r}") from None
            if line == expected:
                return


async def next_notification(lines: AsyncIterator[str], *, timeout: float) -> NotificationResponse:
    """Read the stream until one notification frame's data is complete.

    The frame is parsed with the API's own response model: a stream frame and a
    ``/pending`` item are the same payload, and a test that parsed them
    differently would let that drift.
    """
    async with asyncio.timeout(timeout):
        data: list[str] = []
        while True:
            try:
                line = await anext(lines)
            except StopAsyncIteration:
                raise AssertionError("the stream ended before a notification arrived") from None
            if line == "":
                if data:
                    return NotificationResponse.model_validate_json("\n".join(data))
                continue
            if line.startswith("data:"):
                data.append(line.removeprefix("data:").lstrip())


# --- the fixtures' world ------------------------------------------------------


def session_factory_on(database: str) -> async_sessionmaker[AsyncSession]:
    """A session factory over the test database."""
    return async_sessionmaker(create_async_engine(database), expire_on_commit=False)


async def create_household(client: AsyncClient) -> str:
    """Create a household through the API, as a client would."""
    response = await client.post("/api/v1/households", json={"name": "Home"})
    assert response.status_code == 201, response.text
    return str(response.json()["household_id"])


async def create_household_and_plant(client: AsyncClient) -> tuple[str, PlantId]:
    """Create a household with one plant through the API, as a client would."""
    household_id = await create_household(client)
    plant = await client.post(
        "/api/v1/plants",
        json={
            "household_id": household_id,
            "species_id": str(uuid4()),
            "name": "Fern",
            "location": "Shelf",
        },
    )
    assert plant.status_code == 201, plant.text
    return household_id, PlantId(UUID(plant.json()["plant_id"]))


async def register_sensor(database: str, *, sensor_id: SensorId, plant_id: PlantId) -> None:
    """Bind a sensor to a plant, so the ingress can resolve the reading."""
    async with session_factory_on(database)() as session:
        await SqlAlchemySensorRepository(session, AggregateTracker()).add(
            Sensor(sensor_id, plant_id=plant_id, added_at=datetime.now(UTC))
        )
        await session.commit()


async def seed_notification(database: str, household_id: str) -> Notification:
    """Write one pending notification directly and return it."""
    notification = Notification.create(
        household_id=HouseholdId(UUID(household_id)),
        notification_type=NotificationType.WATERING_DUE,
        payload={"plant_id": str(uuid4())},
        now=datetime.now(UTC),
    )
    async with session_factory_on(database)() as session:
        await SqlAlchemyNotificationRepository(session, AggregateTracker()).add(notification)
        await session.commit()
    return notification


async def publish_dry_reading(
    bootstrap_servers: str, *, sensor_id: SensorId, moisture: float = 12.0
) -> None:
    """Publish one raw dry reading, in the simulator's envelope."""
    body = json.dumps(
        {
            "sensor_id": str(sensor_id),
            "recorded_at": datetime.now(UTC).isoformat(),
            "moisture": moisture,
            "temperature": 21.0,
            "light": 800.0,
        }
    ).encode("utf-8")
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await producer.start()
    try:
        await producer.send_and_wait(TELEMETRY_RAW, body, key=str(sensor_id).encode("utf-8"))
    finally:
        await producer.stop()


async def publish_nudge(valkey_url: str, household_id: str) -> None:
    """Publish the household's nudge, exactly as ``NotificationPusher`` does."""
    client = Valkey.from_url(valkey_url, decode_responses=True)
    try:
        await ValkeyNotificationChannel(client).publish(HouseholdId(UUID(household_id)))
    finally:
        await client.aclose()


IDLE_TRANSACTIONS = text(
    """
    SELECT count(*)
    FROM pg_stat_activity
    WHERE datname = current_database()
      AND pid <> pg_backend_pid()
      AND state = 'idle in transaction'
    """
)


async def idle_in_transaction_connections(database: str) -> int:
    """Count the other sessions holding an open transaction and doing nothing.

    This is what a request-scoped session looks like from the outside while it
    waits: the read that opened it left a transaction open, and SQLAlchemy's
    session holds its connection until it is closed. A stream that kept a session
    between nudges would show up here for as long as it stayed open.
    """
    engine = create_async_engine(database)
    try:
        async with engine.connect() as connection:
            return int((await connection.execute(IDLE_TRANSACTIONS)).scalar_one())
    finally:
        await engine.dispose()


# --- the tests ----------------------------------------------------------------


async def test_a_notification_appears_on_an_open_stream(
    database: str,
    kafka_bootstrap_servers: str,
    streaming_environment: None,
    running_worker: None,
) -> None:
    """Dry telemetry → notification in the database → a frame on the open stream."""
    async with running_api() as client:
        household_id, plant_id = await create_household_and_plant(client)
        sensor_id = SensorId(uuid4())
        await register_sensor(database, sensor_id=sensor_id, plant_id=plant_id)

        async with client.stream(
            "GET", STREAM_PATH, params={"household_id": household_id}
        ) as response:
            assert response.status_code == 200, response.text
            assert response.headers["content-type"].startswith("text/event-stream")
            lines = response.aiter_lines()
            # Subscribed before the reading is published: the opening comment
            # is written once the household's signal is attached.
            await expect_line(lines, ": stream open", timeout=SETUP_TIMEOUT_SECONDS)

            await publish_dry_reading(kafka_bootstrap_servers, sensor_id=sensor_id)
            notification = await next_notification(lines, timeout=EVENT_TIMEOUT_SECONDS)

    assert notification.notification_type == NotificationType.SOIL_MOISTURE_LOW
    assert notification.payload["plant_id"] == str(plant_id)
    assert notification.read_at is None


async def test_a_reconnected_stream_resumes_from_the_cursor(
    database: str,
    streaming_environment: None,
) -> None:
    """What it missed, and nothing it already saw."""
    async with running_api() as client:
        household_id = await create_household(client)
        first = await seed_notification(database, household_id)

        async with client.stream(
            "GET", STREAM_PATH, params={"household_id": household_id}
        ) as response:
            lines = response.aiter_lines()
            await expect_line(lines, ": stream open", timeout=SETUP_TIMEOUT_SECONDS)
            delivered = await next_notification(lines, timeout=EVENT_TIMEOUT_SECONDS)
            assert delivered.notification_id == first.id.value

        second = await seed_notification(database, household_id)

        async with client.stream(
            "GET",
            STREAM_PATH,
            params={"household_id": household_id, "since": str(first.id.value)},
        ) as response:
            lines = response.aiter_lines()
            await expect_line(lines, ": stream open", timeout=SETUP_TIMEOUT_SECONDS)
            resumed = await next_notification(lines, timeout=EVENT_TIMEOUT_SECONDS)

            assert resumed.notification_id == second.id.value
            # Nothing it already saw: the first frame after reconnecting is the
            # new notification, and no second frame follows. Read while the
            # stream is still open: leaving the block closes the response, and a
            # closed response raises before any timeout could.
            with pytest.raises(TimeoutError):
                await next_notification(lines, timeout=1.0)


async def test_the_stream_degrades_to_a_plain_read_without_the_signal(
    database: str,
    signalless_environment: None,
) -> None:
    """No signal, but the notifications are in the database: answer, then end."""
    async with running_api() as client:
        household_id = await create_household(client)
        notification = await seed_notification(database, household_id)

        async with asyncio.timeout(SETUP_TIMEOUT_SECONDS):
            response = await client.get(STREAM_PATH, params={"household_id": household_id})

    assert response.status_code == 200, response.text
    assert response.headers["content-type"].startswith("text/event-stream")
    assert ": signal unavailable" in response.text

    data = [
        line.removeprefix("data:").lstrip()
        for line in response.text.splitlines()
        if line.startswith("data:")
    ]
    assert len(data) == 1
    delivered = NotificationResponse.model_validate_json(data[0])
    assert delivered.notification_id == notification.id.value


async def test_an_idle_stream_holds_no_database_session(
    database: str,
    valkey_url: str,
    streaming_environment: None,
) -> None:
    """The requirement, from the outside: a waiting stream is not a waiting query."""
    async with running_api() as client:
        household_id = await create_household(client)

        async with client.stream(
            "GET", STREAM_PATH, params={"household_id": household_id}
        ) as response:
            lines = response.aiter_lines()
            await expect_line(lines, ": stream open", timeout=SETUP_TIMEOUT_SECONDS)
            # The opening comment is written before the first read: give the
            # stream time to read and release its session before looking.
            await asyncio.sleep(0.5)

            assert await idle_in_transaction_connections(database) == 0, (
                "a stream must read on a session of its own and hold none while idle"
            )

            # Idle, but alive: a nudge still delivers without re-requesting.
            notification = await seed_notification(database, household_id)
            await publish_nudge(valkey_url, household_id)
            delivered = await next_notification(lines, timeout=EVENT_TIMEOUT_SECONDS)

    assert delivered.notification_id == notification.id.value


async def test_the_idle_session_probe_sees_a_held_session(database: str) -> None:
    """The negative control for the probe above: it can fail.

    A connection that has run a query and not committed is exactly what a held
    request-scoped session looks like, so if the probe cannot see this one it
    cannot vouch for the stream either.
    """
    engine = create_async_engine(database)
    try:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
            assert await idle_in_transaction_connections(database) >= 1
    finally:
        await engine.dispose()
