"""Read-only SQLAlchemy mappings of the telemetry read side.

The rollups and the latest-per-sensor row are written by Django, in the read
instance, and read through this layer by anything that answers a question about
telemetry. They share the domain read models' declarative base — deliberately not
``Base.metadata``, which is what Alembic migrates — and
``tests/integration/test_read_model_mapping.py`` fails when a mapped column and its
Django field stop agreeing.

Read-only is a property of the whole layer, not of a flag here: the one writer per
table is ``TelemetryRollupProjection``, and the engine a reader opens is
``default_transaction_read_only`` (see ``di/providers.py``), so a write through it
would fail in Postgres rather than corrupt a read model.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    DateTime,
    Float,
    Integer,
    String,
    Uuid,
)
from sqlalchemy.orm import Mapped, mapped_column

from plantkeeper.infrastructure.persistence.models.read_models import ReadModelBase
from plantkeeper.infrastructure.persistence.schemas import READ_TELEMETRY


class TelemetryRollupModel(ReadModelBase):
    """The ``read_telemetry.telemetry_rollups`` table.

    ``sensor_id`` and ``bucket_start`` name the window; ``id`` is the surrogate key
    Django gave the table, and the unique constraint over the two window columns is
    what the projection's upsert conflicts on.
    """

    __tablename__ = "telemetry_rollups"
    __table_args__ = ({"schema": READ_TELEMETRY},)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    sensor_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    bucket_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    plant_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    moisture_min: Mapped[float] = mapped_column(Float, nullable=False)
    moisture_max: Mapped[float] = mapped_column(Float, nullable=False)
    moisture_avg: Mapped[float] = mapped_column(Float, nullable=False)
    temperature_min: Mapped[float] = mapped_column(Float, nullable=False)
    temperature_max: Mapped[float] = mapped_column(Float, nullable=False)
    temperature_avg: Mapped[float] = mapped_column(Float, nullable=False)
    sample_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class SensorLatestModel(ReadModelBase):
    """The ``read_telemetry.sensor_latest`` table: one row per sensor.

    Three facts beyond the reading live here: ``offline_at``, the silence timer's
    announcement, and ``last_alert_at``/``last_alert_type``, the newest threshold
    event. They belong to this row rather than to the rollups because the reading
    that crossed a threshold is already a sample in its window — recording the alert
    there as well would double the sample count and skew the mean.
    """

    __tablename__ = "sensor_latest"
    __table_args__ = ({"schema": READ_TELEMETRY},)

    sensor_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    plant_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    moisture: Mapped[float] = mapped_column(Float, nullable=False)
    temperature: Mapped[float] = mapped_column(Float, nullable=False)
    light: Mapped[float] = mapped_column(Float, nullable=False)
    offline_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_alert_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_alert_type: Mapped[str | None] = mapped_column(String(40), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
