"""The Notifications read model.

Owns ``read_analytics.notifications``. ``NotificationRead`` is handled alongside
``NotificationCreated`` so that acknowledging a notification through the API is
visible in the admin; both events are keyed by household, so one partition
delivers them in the order the write side produced them.
"""

from __future__ import annotations

from typing import ClassVar

from plantkeeper.admin.projections.base import Projection, ProjectionHandler
from plantkeeper.admin.read_models.models import NotificationReadModel
from plantkeeper.domain.base import DomainEvent
from plantkeeper.domain.notifications.events import NotificationCreated, NotificationRead
from plantkeeper.infrastructure.messaging.topics import NOTIFICATIONS_EVENTS


class NotificationProjection(Projection):
    """Consumes ``notifications.events`` into ``read_analytics.notifications``."""

    name = "notifications"
    topics = (NOTIFICATIONS_EVENTS,)

    def on_notification_created(self, event: NotificationCreated) -> None:
        """Upsert the notification and its payload."""
        NotificationReadModel.objects.update_or_create(
            notification_id=event.notification_id.value,
            defaults={
                "household_id": event.household_id.value,
                "notification_type": event.notification_type.value,
                "payload": event.payload,
                "created_at": event.created_at,
            },
        )

    def on_notification_read(self, event: NotificationRead) -> None:
        """Record the acknowledgement."""
        NotificationReadModel.objects.filter(notification_id=event.notification_id.value).update(
            read_at=event.read_at
        )

    handlers: ClassVar[dict[type[DomainEvent], ProjectionHandler]] = {
        NotificationCreated: on_notification_created,
        NotificationRead: on_notification_read,
    }
