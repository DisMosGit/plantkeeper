"""Testcontainers fixtures for integration tests.

Containers are opt-in: a fixture only starts Docker when a test requests it, so
``make test`` stays fast and works without a daemon. Kafka and Valkey fixtures are
added by the phases that first need them (see ``ROADMAP.md``).
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest
from testcontainers.community.postgres import PostgresContainer

DEFAULT_POSTGRES_IMAGE = "postgres:18-alpine"


@pytest.fixture(scope="session")
def postgres_image() -> str:
    """Docker image used for the integration Postgres container."""
    return os.getenv("POSTGRES_IMAGE", DEFAULT_POSTGRES_IMAGE)


@pytest.fixture(scope="session")
def postgres_container(postgres_image: str) -> Iterator[PostgresContainer]:
    """Session-scoped Postgres, started only when a test requests this fixture."""
    with PostgresContainer(postgres_image) as container:
        yield container
