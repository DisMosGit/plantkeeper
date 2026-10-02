"""Fixtures for the end-to-end suite.

The suite runs the API, the relay and the read side in-process against
containerised Postgres and Kafka: the same code paths as ``make api``,
``make workers`` and ``make admin``, without needing any of them to be running.

Two Postgres containers, because the platform runs two: the write instance the
commands commit to and the read instance the list/report queries answer from. A
test that does not need the read models never opens the read engine — the API's
provider is lazy — but the environment always names both instances, exactly as
``.env`` does.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Sequence
from urllib.parse import urlparse
from uuid import uuid4

import django
import grpc
import pytest
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.admin.records_to_delete import RecordsToDelete
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from plantkeeper.api.grpc.server import run_grpc_server
from plantkeeper.api.main import create_app
from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.messaging.topics import DLQ_TOPIC, EVENT_TOPICS, event_type_for
from plantkeeper.infrastructure.persistence.models.shared import OutboxModel

# Django is configured at import time, before pytest imports the test modules: a
# read model cannot be defined until ``INSTALLED_APPS`` is loaded, and pytest
# imports a module before running any fixture. No connection is opened here — the
# database named is whatever the environment says — and the ``django_ready``
# fixture re-points ``DATABASES`` at the session's container before the first
# query. ``DJANGO_DEBUG`` is forced on so that ``ALLOWED_HOSTS`` admits the test
# client's host whatever the developer's ``.env`` says.
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "plantkeeper.admin.settings")
os.environ.setdefault("DJANGO_DEBUG", "true")
django.setup()


def point_environment_at(
    monkeypatch: pytest.MonkeyPatch,
    *,
    database: str,
    bootstrap_servers: str,
    valkey_url: str = "",
    read_database: str = "",
) -> Settings:
    """Point the environment at the containers and return what that means.

    Settings come from the environment by design — that is how ``make api`` and
    ``make workers`` are configured — so a test configures the system the same
    way an operator would, instead of exercising a test-only code path.

    ``read_database`` is optional so that a process which must not hold the read
    instance (the workers) can be pointed at the write one only; the API and the
    gRPC server always get both.
    """
    parsed = urlparse(database)
    assert parsed.username and parsed.password and parsed.hostname and parsed.port
    monkeypatch.setenv("POSTGRES_USER", parsed.username)
    monkeypatch.setenv("POSTGRES_PASSWORD", parsed.password)
    monkeypatch.setenv("POSTGRES_HOST", parsed.hostname)
    monkeypatch.setenv("POSTGRES_PORT", str(parsed.port))
    monkeypatch.setenv("POSTGRES_DB", parsed.path.lstrip("/"))
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", bootstrap_servers)
    if valkey_url:
        # Only the long poll and the notification pusher talk to Valkey, so a
        # test that runs neither leaves the variable alone and needs no container.
        monkeypatch.setenv("VALKEY_URL", valkey_url)
    if read_database:
        read = urlparse(read_database)
        assert read.username and read.password and read.hostname and read.port
        monkeypatch.setenv("READ_POSTGRES_USER", read.username)
        monkeypatch.setenv("READ_POSTGRES_PASSWORD", read.password)
        monkeypatch.setenv("READ_POSTGRES_HOST", read.hostname)
        monkeypatch.setenv("READ_POSTGRES_PORT", str(read.port))
        monkeypatch.setenv("READ_POSTGRES_DB", read.path.lstrip("/"))
    return Settings()


async def outbox_rows(database: str) -> list[OutboxModel]:
    """Return every outbox row of the write database, oldest first."""
    engine = create_async_engine(database)
    try:
        async with async_sessionmaker(engine)() as session:
            statement = select(OutboxModel).order_by(OutboxModel.id)
            return list((await session.execute(statement)).scalars().all())
    finally:
        await engine.dispose()


async def project_the_outbox(database: str, *, prefix: str) -> int:
    """Project the write side's unpublished work into the read side, synchronously.

    The read side is a Kafka consumer, and the tests that run it end to end start
    that consumer themselves. A test that is about something else — a command, a
    gRPC call — still has to fill the read models the moved list queries answer
    from, and waiting on a broker would make it slower without testing anything
    more. This drives the *production* projections over the outbox rows the write
    side just committed: the same ``apply_event`` the consumer awaits, with the
    broker left out. Returns how many event/projection pairs were applied.
    """
    # Imported here rather than at module level: a projection is Django models,
    # and importing one before ``django.setup()`` — which this module runs further
    # down — raises ``AppRegistryNotReady``.
    from plantkeeper.admin.projections import ALL_PROJECTIONS
    from plantkeeper.admin.projections.base import apply_event

    projected = 0
    for row in await outbox_rows(database):
        model = event_type_for(row.event_name)
        if model is None:
            continue
        event = model.model_validate(row.payload)
        for projection in ALL_PROJECTIONS:
            if await apply_event(projection, event, consumer_group=f"{prefix}-{projection.name}"):
                projected += 1
    return projected


class OutboxProjector:
    """``await project_outbox()``: fill the read models from the write outbox.

    A callable object rather than a bare function so the fixture can hand a test
    the two things the projection needs — the write database and a consumer-group
    prefix — without the test restating either. The prefix is constant, not random
    like the consumer-group prefixes elsewhere: the read tables and their ledger
    are truncated before every test, so there is nothing for a fresh group to
    replay past.
    """

    def __init__(self, database: str, prefix: str = "e2e-projections") -> None:
        self._database = database
        self._prefix = prefix

    async def __call__(self) -> int:
        """Apply every pending outbox event to every projection."""
        return await project_the_outbox(self._database, prefix=self._prefix)


@pytest.fixture
def project_outbox(database: str, read_side_database: str) -> OutboxProjector:
    """A projector over the write outbox and the test's read models.

    Depends on ``read_side_database`` so Django has migrated the read instance and
    truncated its tables before anything is projected into them.
    """
    return OutboxProjector(database)


@pytest.fixture
async def api_client(
    database: str,
    read_side_database: str,
    kafka_bootstrap_servers: str,
    valkey_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[AsyncClient]:
    """An HTTP client speaking to the real application over empty databases.

    Both databases are the session's containers: the write one the commands
    commit to, and the read one the list queries answer from. Django's read
    models are empty for the test — ``read_side_database`` truncates them — so a
    test that lists something projects it first with :func:`project_the_outbox`.
    """
    point_environment_at(
        monkeypatch,
        database=database,
        bootstrap_servers=kafka_bootstrap_servers,
        valkey_url=valkey_url,
        read_database=read_side_database,
    )
    app = create_app()
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client


@pytest.fixture
async def read_side_settings(
    database: str,
    read_side_database: str,
    kafka_bootstrap_servers: str,
    monkeypatch: pytest.MonkeyPatch,
) -> Settings:
    """Settings for the read-side process, on consumer groups of its own.

    A fresh group prefix per test matters: the Kafka container is shared by the
    suite, and a group that had already consumed a topic would resume after its
    committed offset instead of replaying what this test published.
    """
    settings = point_environment_at(
        monkeypatch,
        database=database,
        bootstrap_servers=kafka_bootstrap_servers,
        read_database=read_side_database,
    )
    return settings.model_copy(update={"read_side_consumer_group_prefix": f"test-read-{uuid4()}"})


@pytest.fixture
async def worker_settings(
    database: str,
    kafka_bootstrap_servers: str,
    valkey_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> Settings:
    """Settings for the write-side workers, on consumer groups of their own.

    Fresh groups for the same reason as the read side: the shared broker may
    already hold offsets for the default prefix, and a group that resumed from
    them would never see what this test publishes. The worker is pointed at the
    Valkey container because ``NotificationPusher`` publishes nudges there.
    """
    settings = point_environment_at(
        monkeypatch,
        database=database,
        bootstrap_servers=kafka_bootstrap_servers,
        valkey_url=valkey_url,
    )
    return settings.model_copy(update={"worker_consumer_group_prefix": f"test-worker-{uuid4()}"})


SIGNALLESS_VALKEY_URL = "valkey://127.0.0.1:1/0?socket_connect_timeout=1"
"""A presence channel that cannot be reached: nothing listens on port 1.

The stream's degradation is about an unreachable Valkey, so the test that covers
it configures a real URL to a dead port rather than faking the failure.
"""


@pytest.fixture
def streaming_environment(
    database: str,
    kafka_bootstrap_servers: str,
    valkey_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Point the environment at the containers, for a test that serves the API itself.

    ``tests/e2e/test_notification_stream.py`` runs its own uvicorn server — an
    in-process ASGI transport buffers a stream and could never show one arriving —
    so the environment is configured before that server builds its container.

    The read instance is deliberately not set: a notification stream answers from
    the write tables, and the fixtures that need the read instance start it.
    """
    point_environment_at(
        monkeypatch,
        database=database,
        bootstrap_servers=kafka_bootstrap_servers,
        valkey_url=valkey_url,
    )


@pytest.fixture
def signalless_environment(
    database: str,
    kafka_bootstrap_servers: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The same, with a household signal that is not there."""
    point_environment_at(
        monkeypatch,
        database=database,
        bootstrap_servers=kafka_bootstrap_servers,
        valkey_url=SIGNALLESS_VALKEY_URL,
    )


@pytest.fixture
async def grpc_port(
    database: str, read_side_database: str, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[int]:
    """A running gRPC server on a free port, over empty databases.

    The gRPC process builds no Kafka broker — it uses ``api_providers``, which
    has no messaging provider — so the bootstrap address here is never dialled.
    It is set anyway because ``Settings`` is constructed from the environment. It
    does answer ``ListPlants`` and ``GetTodayCare``, so it gets the read instance
    too.
    """
    point_environment_at(
        monkeypatch,
        database=database,
        bootstrap_servers="localhost:9092",
        read_database=read_side_database,
    )
    async with run_grpc_server(host="127.0.0.1", port=0) as (_, bound_port):
        yield bound_port


@pytest.fixture
async def grpc_channel(grpc_port: int) -> AsyncIterator[grpc.aio.Channel]:
    """An async channel to the running server, ready for a generated stub."""
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{grpc_port}")
    await channel.channel_ready()
    try:
        yield channel
    finally:
        await channel.close()


# -----------------------------------------------------------------------------
# The broker between tests
# -----------------------------------------------------------------------------


def topics_the_platform_publishes_events_on(settings: Settings) -> tuple[str, ...]:
    """Every topic an end-to-end test has to be isolated from.

    Derived rather than listed: the values of ``EVENT_TOPICS`` cover the whole
    event catalogue, so a topic added to it is isolated without an edit here, and
    the two topics that are not in the catalogue — the simulator's raw feed and
    the dead-letter topic — are named by their setting and their constant rather
    than copied as literals.
    """
    return tuple(dict.fromkeys((*EVENT_TOPICS.values(), settings.telemetry_raw_topic, DLQ_TOPIC)))


async def delete_records_on(bootstrap_servers: str, topics: Sequence[str]) -> None:
    """Delete the records already on ``topics``, up to each partition's high-water mark.

    Deleting *up to the high-water mark*, rather than to some offset chosen in
    advance, leaves the log's start where the next record will be written: the
    topics stay usable, and the ``earliest`` a fresh consumer group resets to is
    the deletion point — what the test itself publishes, and nothing a previous
    test left behind.

    A topic nothing has published to yet does not exist (a broker creates a topic
    on its first produce, not on its first mention) and is already empty, so it is
    skipped instead of being created here.
    """
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    consumer = AIOKafkaConsumer(bootstrap_servers=bootstrap_servers)
    await admin.start()
    await consumer.start()
    try:
        known = set(await admin.list_topics())
        existing = [topic for topic in topics if topic in known]
        if not existing:
            return
        described = await admin.describe_topics(existing)
        partitions = [
            TopicPartition(description["topic"], partition["partition"])
            for description in described
            for partition in description["partitions"]
        ]
        high_water_marks = await consumer.end_offsets(partitions)
        await admin.delete_records(
            {
                partition: RecordsToDelete(before_offset=offset)
                for partition, offset in high_water_marks.items()
            }
        )
    finally:
        await consumer.stop()
        await admin.close()


@pytest.fixture
def broker_settings(kafka_bootstrap_servers: str, monkeypatch: pytest.MonkeyPatch) -> Settings:
    """Settings that name the broker, for a fixture that needs only the topic layout.

    Pointed at the session's broker so the raw telemetry topic is the one the
    suite's producers and consumers use. No database is named because nothing here
    opens one, and no consumer-group prefix is set because nothing here consumes.
    """
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", kafka_bootstrap_servers)
    return Settings()


@pytest.fixture
async def isolated_broker(broker_settings: Settings, kafka_bootstrap_servers: str) -> None:
    """Empty the event topics before a test starts its consumers.

    A dependency of :func:`running_worker` and of the read side's own harness
    rather than something a test asks for: consumer groups are created there, and
    isolation is worth nothing unless it happened before every one of them.

    The suite runs its tests one at a time, which is what lets this be a per-test
    truncation instead of a topic namespace per test.
    """
    await delete_records_on(
        kafka_bootstrap_servers, topics_the_platform_publishes_events_on(broker_settings)
    )
