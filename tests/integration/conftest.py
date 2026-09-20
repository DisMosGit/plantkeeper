"""Fixtures for the integration suite.

The Postgres and Kafka containers themselves live in the root ``conftest.py``,
because the end-to-end suite uses the same ones; what stays here is the
integration-specific wiring.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator

import django
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

# Django is configured at import time, before pytest imports the test modules:
# a read model cannot be defined until ``INSTALLED_APPS`` is loaded, and pytest
# imports a module before running any fixture. No connection is opened here — the
# database named is whatever the environment says — and the ``django_ready``
# fixture re-points ``DATABASES`` at the session's container before the first
# query, which is why ``make test-unit`` still touches neither Django nor Docker:
# this file is only imported for the integration suite.
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "plantkeeper.admin.settings")
django.setup()


@pytest.fixture
async def session_factory(database: str) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    """A session factory over the empty, migrated integration database."""
    engine = create_async_engine(database)
    try:
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()
