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


# --- The two instances ---------------------------------------------------------
#
# ``make migrate`` runs the same two commands these tests drive: Alembic against
# the write instance, Django against the read instance. What is asserted here is
# that each side's schema lands on *its own* database and nowhere else — an
# Alembic migration that quietly created ``read_analytics``, or a Django migration
# that reached into ``write_*``, would otherwise only show up as a production
# surprise.


async def schemas_of(dsn: str) -> set[str]:
    """Return the non-system schemas a database holds."""
    engine = create_async_engine(dsn)
    try:
        async with engine.connect() as connection:
            result = await connection.execute(
                text(
                    "SELECT nspname FROM pg_namespace "
                    "WHERE nspname NOT LIKE 'pg\\_%' AND nspname <> 'information_schema'"
                )
            )
            return {row[0] for row in result}
    finally:
        await engine.dispose()


async def tables_in(dsn: str, schema: str) -> set[str]:
    """Return the tables of one schema, partitions excluded."""
    engine = create_async_engine(dsn)
    try:
        async with engine.connect() as connection:
            result = await connection.execute(
                text(
                    "SELECT c.relname FROM pg_class AS c "
                    "JOIN pg_namespace AS n ON n.oid = c.relnamespace "
                    "WHERE c.relkind IN ('r', 'p') AND NOT c.relispartition "
                    "AND n.nspname = :schema"
                ),
                {"schema": schema},
            )
            return {row[0] for row in result}
    finally:
        await engine.dispose()


async def test_each_side_schema_is_applied_to_its_own_instance(
    postgres_dsn: str, read_side_database: str
) -> None:
    """The write schema is on the write instance, the read schemas on the read one.

    ``read_side_database`` is what makes the read instance real here: it runs
    Django's ``migrate`` against its own container, exactly as the second half of
    ``make migrate`` does, so this asserts both halves of that command — including
    the telemetry rollups' schema, which is an app of its own.
    """
    expected = read_side_tables()
    write_schemas = await schemas_of(postgres_dsn)
    read_schemas = await schemas_of(read_side_database)

    for schema, tables in expected.items():
        assert schema in read_schemas, schema
        assert tables <= await tables_in(read_side_database, schema), schema
        assert schema not in write_schemas, schema
    assert not {schema for schema in read_schemas if schema.startswith("write_")}


def read_side_tables() -> dict[str, set[str]]:
    """Return the ``schema -> bare table names`` of every read-side model.

    Derived from Django's own registry rather than listed, so a read model added
    without a migration fails here instead of passing quietly.
    """
    from django.apps import apps

    from plantkeeper.infrastructure.persistence.schemas import ALL_READ_SCHEMAS

    tables: dict[str, set[str]] = {schema: set() for schema in ALL_READ_SCHEMAS}
    for model in apps.get_models():
        table = model._meta.db_table
        for schema in ALL_READ_SCHEMAS:
            prefix = f'"{schema}"."'
            if table.startswith(prefix):
                tables[schema].add(table.removeprefix(prefix).rstrip('"'))
    return tables
