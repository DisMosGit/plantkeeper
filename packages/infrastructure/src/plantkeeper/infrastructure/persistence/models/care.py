"""ORM models of the Care context."""

from __future__ import annotations

from datetime import datetime, timedelta
from uuid import UUID

from sqlalchemy import DateTime, Index, Integer, Interval, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from plantkeeper.infrastructure.persistence.base import Base
from plantkeeper.infrastructure.persistence.schemas import WRITE_CARE


class CareScheduleModel(Base):
    """The ``write_care.care_schedules`` table.

    A plant has exactly one schedule, so ``plant_id`` is the primary key. The
    ``version`` column is the optimistic-locking token the aggregate checks; the
    repository additionally locks the row while it is loaded, so two concurrent
    waterings cannot both see the same version.
    """

    __tablename__ = "care_schedules"
    __table_args__ = (
        Index("ix_care_schedules_next_watering_at", "next_watering_at"),
        {"schema": WRITE_CARE},
    )

    plant_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    watering_interval: Mapped[timedelta] = mapped_column(Interval, nullable=False)
    next_watering_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
