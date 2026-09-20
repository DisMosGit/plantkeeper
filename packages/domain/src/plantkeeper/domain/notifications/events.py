"""Domain events published by the Notifications context."""

from __future__ import annotations

from pydantic import AwareDatetime, JsonValue

from plantkeeper.domain.base import DomainEvent
from plantkeeper.domain.identifiers import HouseholdId, NotificationId
from plantkeeper.domain.notifications.values import NotificationType


class NotificationCreated(DomainEvent):
    """A notification is waiting for the household."""

    notification_id: NotificationId
    household_id: HouseholdId
    notification_type: NotificationType
    payload: dict[str, JsonValue]
    created_at: AwareDatetime


class NotificationRead(DomainEvent):
    """The household acknowledged a notification."""

    notification_id: NotificationId
    household_id: HouseholdId
    read_at: AwareDatetime
