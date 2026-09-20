"""Fixtures for the end-to-end suite.

The suite runs the API, the relay and the read side in-process against
containerised Postgres and Kafka: the same code paths as ``make api``,
``make workers`` and ``make admin``, without needing any of them to be running.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from urllib.parse import urlparse
from uuid import uuid4

import django
import pytest
from httpx import ASGITransport, AsyncClient

from plantkeeper.api.main import create_app
from plantkeeper.infrastructure.config import Settings

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
    monkeypatch: pytest.MonkeyPatch, *, database: str, bootstrap_servers: str
) -> Settings:
    """Point the environment at the containers and return what that means.

    Settings come from the environment by design — that is how ``make api`` and
    ``make workers`` are configured — so a test configures the system the same
    way an operator would, instead of exercising a test-only code path.
    """
    parsed = urlparse(database)
    assert parsed.username and parsed.password and parsed.hostname and parsed.port
    monkeypatch.setenv("POSTGRES_USER", parsed.username)
    monkeypatch.setenv("POSTGRES_PASSWORD", parsed.password)
    monkeypatch.setenv("POSTGRES_HOST", parsed.hostname)
    monkeypatch.setenv("POSTGRES_PORT", str(parsed.port))
    monkeypatch.setenv("POSTGRES_DB", parsed.path.lstrip("/"))
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", bootstrap_servers)
    return Settings()


@pytest.fixture
async def api_client(
    database: str,
    kafka_bootstrap_servers: str,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[AsyncClient]:
    """An HTTP client speaking to the real application over an empty database."""
    point_environment_at(monkeypatch, database=database, bootstrap_servers=kafka_bootstrap_servers)
    app = create_app()
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client


@pytest.fixture
async def read_side_settings(
    database: str,
    kafka_bootstrap_servers: str,
    monkeypatch: pytest.MonkeyPatch,
) -> Settings:
    """Settings for the read-side process, on consumer groups of its own.

    A fresh group prefix per test matters: the Kafka container is shared by the
    suite, and a group that had already consumed a topic would resume after its
    committed offset instead of replaying what this test published.
    """
    settings = point_environment_at(
        monkeypatch, database=database, bootstrap_servers=kafka_bootstrap_servers
    )
    return settings.model_copy(update={"read_side_consumer_group_prefix": f"test-read-{uuid4()}"})
