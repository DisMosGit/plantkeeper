"""HTTP schemas for notifications."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

from plantkeeper.application.views import CollectionView, NotificationView
from plantkeeper.domain.notifications.values import NotificationType


class NotificationResponse(BaseModel):
    """One notification."""

    notification_id: UUID
    household_id: UUID
    notification_type: NotificationType
    created_at: datetime
    read_at: datetime | None

    @classmethod
    def from_view(cls, view: NotificationView) -> NotificationResponse:
        """Map the view onto the wire format."""
        return cls(
            notification_id=view.notification_id.value,
            household_id=view.household_id.value,
            notification_type=view.notification_type,
            created_at=view.created_at,
            read_at=view.read_at,
        )


class NotificationCollectionResponse(BaseModel):
    """A list of notifications."""

    items: list[NotificationResponse]

    @classmethod
    def from_view(cls, view: CollectionView[NotificationView]) -> NotificationCollectionResponse:
        """Map each notification view onto the wire format."""
        return cls(items=[NotificationResponse.from_view(item) for item in view.items])
