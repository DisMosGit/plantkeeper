"""ORM models of the Telemetry context."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import DateTime, Float, Index, PrimaryKeyConstraint, Uuid, text
from sqlalchemy.orm import Mapped, mapped_column

from plantkeeper.infrastructure.persistence.base import Base
from plantkeeper.infrastructure.persistence.schemas import WRITE_TELEMETRY


class SensorModel(Base):
    """The ``write_telemetry.sensors`` table.

    Only the sensor registry lives on the write side in Phase 2. The readings
    themselves arrive with the telemetry ingress in Phase 5 and get their own
    time-partitioned table (:class:`SensorReadingModel`).

    ``last_seen_at`` is *not* advanced by every reading: that would be a write per
    measurement to answer a question no reader asks, and the readings table already
    holds the last one.
    """

    __tablename__ = "sensors"
    __table_args__ = (
        Index("ix_sensors_plant_id", "plant_id"),
        {"schema": WRITE_TELEMETRY},
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    plant_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    added_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class SensorReadingModel(Base):
    """The ``write_telemetry.sensor_readings`` table.

    **The table is RANGE-partitioned by month on ``recorded_at``**, and this class
    is therefore only the parent that queries and inserts go through; the monthly
    partitions are created by Alembic ``0003`` and kept ahead of the clock by
    ``plantkeeper.infrastructure.scheduling.telemetry_partitions``. Two
    consequences are load-bearing and must not be "tidied up":

    * the primary key is ``(sensor_id, recorded_at)``, because PostgreSQL requires
      every unique constraint of a partitioned table to contain the partition key
      — and that pair is exactly the idempotency key the telemetry ingress relies
      on;
    * the insert path uses ``INSERT ... ON CONFLICT (sensor_id, recorded_at) DO
      NOTHING``, so the conflict target names those two columns.

    ``plant_id`` is denormalised from the sensor registry so a reader never has to
    join to ask "what has this plant's soil been doing". There is no foreign key
    to ``sensors``: the ingress resolves the sensor before it writes, and the
    readings are a log that a sensor's deletion should not rewrite.
    """

    __tablename__ = "sensor_readings"
    __table_args__ = (
        PrimaryKeyConstraint("sensor_id", "recorded_at", name="pk_sensor_readings"),
        Index("ix_sensor_readings_plant_id_recorded_at", "plant_id", "recorded_at"),
        Index("ix_sensor_readings_recorded_at", "recorded_at"),
        {"schema": WRITE_TELEMETRY},
    )

    sensor_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    plant_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    moisture: Mapped[float] = mapped_column(Float, nullable=False)
    temperature: Mapped[float] = mapped_column(Float, nullable=False)
    light: Mapped[float] = mapped_column(Float, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
