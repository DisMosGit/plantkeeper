"""ORM models of the Notifications context."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import JsonValue
from sqlalchemy import DateTime, Index, String, Uuid
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from plantkeeper.infrastructure.persistence.base import Base
from plantkeeper.infrastructure.persistence.schemas import WRITE_NOTIFICATIONS


class NotificationModel(Base):
    """The ``write_notifications.notifications`` table.

    ``payload`` is JSONB because the aggregate types it as ``dict[str, JsonValue]``:
    each notification type carries a different small body (a plant identifier,
    a moisture value), and one nullable column per possible key would be worse.
    """

    __tablename__ = "notifications"
    __table_args__ = (
        Index("ix_notifications_household_id_read_at", "household_id", "read_at"),
        {"schema": WRITE_NOTIFICATIONS},
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    household_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    notification_type: Mapped[str] = mapped_column(String(40), nullable=False)
    payload: Mapped[dict[str, JsonValue]] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    read_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
