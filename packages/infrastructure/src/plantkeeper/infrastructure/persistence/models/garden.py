"""ORM models of the Garden context."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    String,
    Uuid,
)
from sqlalchemy.orm import Mapped, mapped_column

from plantkeeper.infrastructure.persistence.base import Base
from plantkeeper.infrastructure.persistence.schemas import WRITE_GARDEN


class HouseholdModel(Base):
    """The ``write_garden.households`` table.

    Only the household's own data lives here: which plants it owns is a query
    over ``plants``, not a stored relation, so the two can never disagree.
    """

    __tablename__ = "households"
    __table_args__ = ({"schema": WRITE_GARDEN},)

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)


class PlantModel(Base):
    """The ``write_garden.plants`` table.

    ``species_id`` is a plain identifier, not a foreign key: the species lives in
    another bounded context's schema, and a database-level constraint across
    contexts would couple two deployables that must stay independent.
    """

    __tablename__ = "plants"
    __table_args__ = (
        Index("ix_plants_household_id_removed", "household_id", "removed"),
        {"schema": WRITE_GARDEN},
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    household_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey(f"{WRITE_GARDEN}.households.id", ondelete="CASCADE"),
        nullable=False,
    )
    species_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    location: Mapped[str] = mapped_column(String(100), nullable=False)
    added_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_watered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_repotted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    removed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
