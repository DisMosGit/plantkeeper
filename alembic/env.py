"""Alembic environment: async engine, schemas included, URL from Settings.

The template is Alembic's ``async`` one, with two changes that matter here:
the URL comes from :class:`~plantkeeper.infrastructure.config.Settings` rather
than the ini file, and ``include_schemas`` is on, because the write side is
partitioned across eight Postgres schemas.
"""

from __future__ import annotations

import asyncio

from alembic import context
from sqlalchemy import Connection, pool
from sqlalchemy.ext.asyncio import async_engine_from_config

# Imported for the side effect of registering every table on `Base.metadata`.
import plantkeeper.infrastructure.persistence.models  # noqa: F401
from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.persistence.base import Base

config = context.config
target_metadata = Base.metadata


def database_url() -> str:
    """Return the database URL to migrate.

    A URL set through ``Config.set_main_option`` (integration tests, CI) wins;
    otherwise the environment decides.
    """
    return config.get_main_option("sqlalchemy.url") or Settings().postgres_dsn


def run_migrations_offline() -> None:
    """Emit SQL to stdout instead of touching a database."""
    context.configure(
        url=database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_schemas=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    """Run the migration on a synchronous connection from the async engine."""
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        include_schemas=True,
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """Connect with the async driver and hand a sync connection to Alembic."""
    connectable = async_engine_from_config(
        {"sqlalchemy.url": database_url()},
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


def run_migrations_online() -> None:
    """Entry point for a real migration run."""
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
