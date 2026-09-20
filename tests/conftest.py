"""Shared pytest fixtures.

Container fixtures are **lazy**: they are defined here so both the integration
and the end-to-end suites can use them, but Docker is only touched when a test
actually asks for one of them, so ``make test-unit`` stays instant and works
without a daemon.

The Kafka container is written by hand rather than taken from testcontainers,
whose ``KafkaContainer`` is tied to the Confluent image layout. This one runs the
same ``apache/kafka`` image as ``docker-compose.yml``, in the same KRaft mode, so
the tests exercise the broker the project actually ships with.
"""

from __future__ import annotations

import asyncio
import os
import socket
import time
from collections.abc import Iterator
from datetime import UTC, datetime

import pytest
from aiokafka.admin import AIOKafkaAdminClient
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.community.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

# Imported for the side effect of registering every table, which the truncate
# helper needs to know about.
import plantkeeper.infrastructure.persistence.models  # noqa: F401
from plantkeeper.infrastructure.persistence.base import Base

DEFAULT_POSTGRES_IMAGE = "postgres:18-alpine"
DEFAULT_KAFKA_IMAGE = "apache/kafka:4.1.1"

KAFKA_INTERNAL_PORT = 9092
"""The port the broker listens on inside the container (see docker-compose.yml)."""

KAFKA_INTERNAL_BROKER_PORT = 29092
"""The container-internal listener the broker advertises to itself."""

KAFKA_CONTROLLER_PORT = 29093
"""The KRaft controller listener."""


@pytest.fixture
def anyio_backend() -> str:
    """Run anyio-based tests on the asyncio backend only."""
    return "asyncio"


@pytest.fixture
def now() -> datetime:
    """A fixed, timezone-aware instant used as the domain clock in tests.

    Domain code never reads the wall clock itself: aggregates take ``now`` as an
    argument, so tests stay deterministic without freezing time.
    """
    return datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


# -----------------------------------------------------------------------------
# Postgres
# -----------------------------------------------------------------------------


@pytest.fixture(scope="session")
def postgres_image() -> str:
    """Docker image used for the integration Postgres container."""
    return os.getenv("POSTGRES_IMAGE", DEFAULT_POSTGRES_IMAGE)


@pytest.fixture(scope="session")
def postgres_container(postgres_image: str) -> Iterator[PostgresContainer]:
    """Session-scoped Postgres, started only when a test requests this fixture.

    ``driver="asyncpg"`` matters: testcontainers defaults to psycopg2, which the
    project does not depend on, so the default URL cannot be opened.
    """
    with PostgresContainer(postgres_image, driver="asyncpg") as container:
        yield container


@pytest.fixture(scope="session")
def postgres_dsn(postgres_container: PostgresContainer) -> str:
    """The SQLAlchemy URL of the integration database."""
    return str(postgres_container.get_connection_url())


def _run_migrations(dsn: str) -> None:
    """Apply every migration to ``dsn``.

    Alembic's async environment runs its own event loop, so this is called from a
    synchronous fixture rather than from inside a test.
    """
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", dsn)
    command.upgrade(config, "head")


@pytest.fixture(scope="session")
def migrated_database(postgres_dsn: str) -> str:
    """A database with the whole write schema applied by Alembic.

    Migrating rather than calling ``create_all`` means every test runs against
    the schema the project actually ships — a migration that forgot a table
    would fail here instead of passing quietly.
    """
    _run_migrations(postgres_dsn)
    return postgres_dsn


@pytest.fixture
async def database(migrated_database: str) -> str:
    """A migrated database with every write table empty."""
    engine = create_async_engine(migrated_database)
    try:
        tables = ", ".join(
            f'"{table.schema}"."{table.name}"' for table in Base.metadata.sorted_tables
        )
        async with engine.begin() as connection:
            await connection.execute(text(f"TRUNCATE {tables} CASCADE"))
    finally:
        await engine.dispose()
    return migrated_database


# -----------------------------------------------------------------------------
# Kafka
# -----------------------------------------------------------------------------


def _free_tcp_port() -> int:
    """Reserve a free local port.

    The broker has to advertise the host port it will be reached on, and it can
    only do that at start-up, so the port is chosen here and bound explicitly
    instead of letting Docker pick one afterwards.
    """
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class ApacheKafkaContainer(DockerContainer):
    """Single-node Kafka in KRaft mode, on the image docker-compose uses.

    Two listeners, exactly as in ``docker-compose.yml``: ``PLAINTEXT_HOST`` for
    clients on the host and ``PLAINTEXT`` for the broker's own use. The broker
    advertises ``localhost:29092`` to itself, which resolves inside its own
    network namespace, so a single container needs no Docker network.
    """

    def __init__(
        self,
        image: str = DEFAULT_KAFKA_IMAGE,
        host_port: int | None = None,
    ) -> None:
        super().__init__(image)
        self._host_port = host_port if host_port is not None else _free_tcp_port()
        self.with_bind_ports(KAFKA_INTERNAL_PORT, self._host_port)
        self.with_env("CLUSTER_ID", "MkU3OEVBNTcwNTJENDM2Qk")
        self.with_env("KAFKA_NODE_ID", "1")
        self.with_env("KAFKA_PROCESS_ROLES", "broker,controller")
        self.with_env(
            "KAFKA_LISTENERS",
            f"CONTROLLER://:{KAFKA_CONTROLLER_PORT},"
            f"PLAINTEXT_HOST://:{KAFKA_INTERNAL_PORT},"
            f"PLAINTEXT://:{KAFKA_INTERNAL_BROKER_PORT}",
        )
        self.with_env(
            "KAFKA_ADVERTISED_LISTENERS",
            f"PLAINTEXT_HOST://127.0.0.1:{self._host_port},"
            f"PLAINTEXT://localhost:{KAFKA_INTERNAL_BROKER_PORT}",
        )
        self.with_env(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT",
        )
        self.with_env("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
        self.with_env("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        self.with_env("KAFKA_CONTROLLER_QUORUM_VOTERS", f"1@localhost:{KAFKA_CONTROLLER_PORT}")
        self.with_env("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        self.with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        self.with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        self.with_env("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        # The host is a laptop-sized machine; keep the broker JVM small.
        self.with_env("KAFKA_HEAP_OPTS", "-Xmx512m -Xms256m")
        self.waiting_for(
            LogMessageWaitStrategy(r".*Kafka Server started.*").with_startup_timeout(120)
        )

    def get_bootstrap_server(self) -> str:
        """Return the address a client on the host should connect to."""
        return f"{self.get_container_host_ip()}:{self._host_port}"

    def wait_until_broker_answers(self, timeout: float = 60.0) -> None:
        """Wait until the broker serves metadata.

        The log line says the process started; it does not say the advertised
        listener is ready to answer, and a client that connects in between fails
        for a reason that has nothing to do with the code under test.
        """
        deadline = time.monotonic() + timeout
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                asyncio.run(_probe_broker(self.get_bootstrap_server()))
            except Exception as exc:
                last_error = exc
                time.sleep(0.5)
            else:
                return
        raise RuntimeError(f"Kafka did not become reachable: {last_error}")


async def _probe_broker(bootstrap_servers: str) -> None:
    """Ask the broker for its metadata once."""
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers, request_timeout_ms=5000)
    await admin.start()
    try:
        await admin.list_topics()
    finally:
        await admin.close()


@pytest.fixture(scope="session")
def kafka_container() -> Iterator[ApacheKafkaContainer]:
    """Session-scoped Kafka, started only when a test requests this fixture."""
    image = os.getenv("KAFKA_IMAGE", DEFAULT_KAFKA_IMAGE)
    with ApacheKafkaContainer(image) as container:
        container.wait_until_broker_answers()
        yield container


@pytest.fixture(scope="session")
def kafka_bootstrap_servers(kafka_container: ApacheKafkaContainer) -> str:
    """The bootstrap address of the integration broker."""
    return kafka_container.get_bootstrap_server()
