"""Fixtures for the integration suite.

The Postgres and Kafka containers themselves live in the root ``conftest.py``,
because the end-to-end suite uses the same ones; what stays here is the
integration-specific wiring.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine


@pytest.fixture
async def session_factory(database: str) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    """A session factory over the empty, migrated integration database."""
    engine = create_async_engine(database)
    try:
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()
