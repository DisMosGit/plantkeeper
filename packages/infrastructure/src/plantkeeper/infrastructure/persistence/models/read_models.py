"""Read-only SQLAlchemy mappings of the read side's tables.

The read models are written by Django, in the read side's own database, and read
by two different processes: Django Admin through the Django ORM, and the API's
query side through SQLAlchemy. Two ORMs over one schema can drift, so these
mappings live in their own declarative base — deliberately *not* in
``Base.metadata``, which is what Alembic migrates — and
``tests/integration/test_read_model_mapping.py`` fails when a mapped column and a
Django field stop agreeing.

Read-only is a property of the whole layer, not of a flag here: nothing in the
infrastructure ever adds, updates or deletes one of these rows. The one writer per
table is a projection, and the reader's engine is opened with
``default_transaction_read_only`` (see ``di/providers.py``), so a write through it
would fail in Postgres rather than corrupt a read model.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from uuid import UUID

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    Interval,
    String,
    Text,
    Uuid,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from plantkeeper.infrastructure.persistence.schemas import READ_ANALYTICS


class ReadModelBase(DeclarativeBase):
    """Declarative base for the read schemas.

    A metadata of its own: Alembic migrates ``Base.metadata`` (the write schemas),
    and a read table appearing there would make the write side's migration own a
    table Django actually owns.
    """


class ReadPlantModel(ReadModelBase):
    """The ``read_analytics.plants`` table.

    Assembled from three event streams (garden, care, catalog), so the columns each
    projection does not own are nullable: a care or journal event can be projected
    before the garden event that names the plant, and a partial row is a better
    answer than a lost fact.
    """

    __tablename__ = "plants"
    __table_args__ = ({"schema": READ_ANALYTICS},)

    plant_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    household_id: Mapped[UUID | None] = mapped_column(Uuid(as_uuid=True), nullable=True)
    species_id: Mapped[UUID | None] = mapped_column(Uuid(as_uuid=True), nullable=True)
    name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    species_name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    location: Mapped[str | None] = mapped_column(String(100), nullable=True)
    added_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    removed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    next_watering_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    onboarded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class ReadCareScheduleModel(ReadModelBase):
    """The ``read_analytics.care_schedules`` table."""

    __tablename__ = "care_schedules"
    __table_args__ = ({"schema": READ_ANALYTICS},)

    plant_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    watering_interval: Mapped[timedelta] = mapped_column(Interval, nullable=False)
    next_watering_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_watered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class ReadSpeciesModel(ReadModelBase):
    """The ``read_analytics.species`` table."""

    __tablename__ = "species"
    __table_args__ = ({"schema": READ_ANALYTICS},)

    species_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    scientific_name: Mapped[str] = mapped_column(String(200), nullable=False)
    common_name: Mapped[str] = mapped_column(String(200), nullable=False)
    watering_interval: Mapped[timedelta] = mapped_column(Interval, nullable=False)
    light_requirement: Mapped[str] = mapped_column(String(20), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class ReadJournalEntryModel(ReadModelBase):
    """The ``read_analytics.journal_entries`` table.

    ``entry_occurred_at`` is when the care happened; ``recorded_at`` is when the
    write side appended the entry, and it is what puts a timeline back into the
    order the event stream replays.
    """

    __tablename__ = "journal_entries"
    __table_args__ = ({"schema": READ_ANALYTICS},)

    entry_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    plant_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey(f"{READ_ANALYTICS}.plants.plant_id", ondelete="CASCADE"),
        nullable=False,
    )
    entry_type: Mapped[str] = mapped_column(String(20), nullable=False)
    note: Mapped[str | None] = mapped_column(Text, nullable=True)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
