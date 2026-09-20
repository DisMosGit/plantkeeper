"""Notifications aggregate <-> row mapping."""

from __future__ import annotations

from plantkeeper.domain.identifiers import HouseholdId, NotificationId
from plantkeeper.domain.notifications.notification import Notification
from plantkeeper.domain.notifications.values import NotificationType
from plantkeeper.infrastructure.persistence.models.notifications import NotificationModel


def notification_to_domain(model: NotificationModel) -> Notification:
    """Rebuild the ``Notification`` aggregate from its row."""
    return Notification(
        NotificationId(model.id),
        household_id=HouseholdId(model.household_id),
        notification_type=NotificationType(model.notification_type),
        created_at=model.created_at,
        payload=model.payload,
        read_at=model.read_at,
    )


def notification_to_model(notification: Notification) -> NotificationModel:
    """Build the row that represents ``notification``."""
    return NotificationModel(
        id=notification.id.value,
        household_id=notification.household_id.value,
        notification_type=notification.notification_type.value,
        payload=notification.payload,
        created_at=notification.created_at,
        read_at=notification.read_at,
    )
