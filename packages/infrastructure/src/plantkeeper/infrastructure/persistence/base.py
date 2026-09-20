"""The declarative base shared by every write-side ORM model.

Constraint and index names are generated from this convention rather than left
to the database, so a migration can drop or alter a constraint by name. Alembic's
autogenerate depends on that; so does any future `DROP CONSTRAINT` in a
hand-written migration.
"""

from __future__ import annotations

from typing import Final

from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase

NAMING_CONVENTION: Final = {
    "ix": "ix_%(table_name)s_%(column_0_name)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    """Declarative base for the write schemas.

    Every model must set ``schema`` in its ``__table_args__``: the write side is
    partitioned by bounded context on purpose (``docs/adr/0002-bounded-contexts.md``).
    """

    metadata = MetaData(naming_convention=NAMING_CONVENTION)
