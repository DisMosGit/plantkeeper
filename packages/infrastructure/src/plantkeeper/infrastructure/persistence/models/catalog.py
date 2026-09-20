"""ORM models of the Catalog context."""

from __future__ import annotations

from datetime import timedelta
from uuid import UUID

from sqlalchemy import Integer, Interval, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from plantkeeper.infrastructure.persistence.base import Base
from plantkeeper.infrastructure.persistence.schemas import WRITE_CATALOG


class SpeciesModel(Base):
    """The ``write_catalog.species`` table.

    ``light_requirement`` stores the :class:`~plantkeeper.domain.catalog.values.LightRequirement`
    *value* rather than a Postgres enum: adding a level later is then a data
    change, not a type migration.
    """

    __tablename__ = "species"
    __table_args__ = ({"schema": WRITE_CATALOG},)

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    scientific_name: Mapped[str] = mapped_column(String(200), nullable=False)
    common_name: Mapped[str] = mapped_column(String(200), nullable=False)
    watering_interval: Mapped[timedelta] = mapped_column(Interval, nullable=False)
    light_requirement: Mapped[str] = mapped_column(String(20), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
