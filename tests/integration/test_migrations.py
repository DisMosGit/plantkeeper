"""Integration test for the migrations themselves.

``upgrade head`` in a session fixture proves the migration runs; this proves it
can also be undone, which is what makes it a migration rather than a script. The
round trip is safe to run against the shared container because it ends where it
started, and the ``database`` fixture truncates before every test that needs data.
"""

from __future__ import annotations

import asyncio

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

# Imported for the side effect of registering every table.
import plantkeeper.infrastructure.persistence.models  # noqa: F401
from plantkeeper.infrastructure.persistence.base import Base

pytestmark = pytest.mark.integration


async def existing_tables(dsn: str) -> set[str]:
    """Return the ``schema.table`` names the write side owns.

    Partitions are excluded: ``sensor_readings`` is a partitioned parent
    (``relkind = 'p'``), and its monthly children are created by a maintenance job
    rather than by a model, so they are not what "the modelled tables" means.
    """
    engine = create_async_engine(dsn)
    try:
        async with engine.connect() as connection:
            result = await connection.execute(
                text(
                    "SELECT n.nspname || '.' || c.relname FROM pg_class AS c "
                    "JOIN pg_namespace AS n ON n.oid = c.relnamespace "
                    "WHERE c.relkind IN ('r', 'p') AND NOT c.relispartition "
                    "AND n.nspname LIKE 'write\\_%'"
                )
            )
            return {row[0] for row in result}
    finally:
        await engine.dispose()


def alembic_config(dsn: str) -> Config:
    """Return an Alembic configuration pointed at ``dsn``."""
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", dsn)
    return config


async def test_the_migrations_create_exactly_the_modelled_tables(postgres_dsn: str) -> None:
    config = alembic_config(postgres_dsn)

    # Start from nothing, so the assertion cannot pass because of earlier tests.
    await asyncio.to_thread(command.downgrade, config, "base")
    assert await existing_tables(postgres_dsn) == set()

    await asyncio.to_thread(command.upgrade, config, "head")

    expected = {f"{table.schema}.{table.name}" for table in Base.metadata.sorted_tables}
    assert await existing_tables(postgres_dsn) == expected


async def test_upgrading_twice_is_a_no_op(postgres_dsn: str) -> None:
    config = alembic_config(postgres_dsn)

    await asyncio.to_thread(command.upgrade, config, "head")
    before = await existing_tables(postgres_dsn)
    await asyncio.to_thread(command.upgrade, config, "head")

    assert await existing_tables(postgres_dsn) == before
