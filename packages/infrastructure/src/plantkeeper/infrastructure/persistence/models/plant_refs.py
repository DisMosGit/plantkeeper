"""ORM models of the reference rows a context keeps about another context.

A bounded context may not read another context's tables
(``event-transport``), but a consumer of care or journal facts still has to know
*which household* a plant belongs to in order to address a notification. So each
consuming context keeps a small row of its own, filled from the Garden context's
events under its own consumer group: one writer per table, and the coupling is an
event this context already consumes rather than a query into someone else's
schema.

The rows are deliberately not aggregates. They carry no behaviour and enforce no
rule; they are a local, eventually-consistent copy of three fields, and the Garden
context remains the only authority on what a plant is.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import DateTime, Index, String, Uuid, func
from sqlalchemy.orm import Mapped, mapped_column

from plantkeeper.infrastructure.persistence.base import Base
from plantkeeper.infrastructure.persistence.schemas import (
    WRITE_JOURNAL,
    WRITE_NOTIFICATIONS,
)


class PlantReferenceMixin:
    """The columns of a plant reference row, shared by the two contexts that need one.

    A mixin rather than one model imported twice: the two tables live in different
    schemas and belong to different contexts, and a shared *table* would be the
    coupling this replaces. What is shared is the shape, which is the same because
    the fact is the same.
    """

    plant_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    household_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    location: Mapped[str] = mapped_column(String(255), nullable=False)
    seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class NotificationPlantReferenceModel(PlantReferenceMixin, Base):
    """``write_notifications.plant_refs``: the households this context addresses.

    Filled from ``PlantAdded``/``PlantMoved``/``PlantRemoved`` by
    ``NotificationConsumer`` under its own group, so the notifications context can
    resolve a plant without reading ``write_garden``.
    """

    __tablename__ = "plant_refs"
    __table_args__ = (
        Index("ix_plant_refs_household_id", "household_id"),
        {"schema": WRITE_NOTIFICATIONS},
    )


class JournalPlantReferenceModel(PlantReferenceMixin, Base):
    """``write_journal.plant_refs``: the plants this context keeps a stream for."""

    __tablename__ = "plant_refs"
    __table_args__ = (
        Index("ix_plant_refs_household_id", "household_id"),
        {"schema": WRITE_JOURNAL},
    )
