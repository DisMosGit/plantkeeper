"""ORM models of the Telemetry context."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import DateTime, Index, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from plantkeeper.infrastructure.persistence.base import Base
from plantkeeper.infrastructure.persistence.schemas import WRITE_TELEMETRY


class SensorModel(Base):
    """The ``write_telemetry.sensors`` table.

    Only the sensor registry lives on the write side in Phase 2. The readings
    themselves arrive with the telemetry consumer in Phase 5 and get their own
    time-partitioned table.
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
