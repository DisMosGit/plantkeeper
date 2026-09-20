"""The Notification aggregate: a message waiting for the household."""

from __future__ import annotations

from datetime import datetime

from pydantic import JsonValue

from plantkeeper.domain.base import AggregateRoot
from plantkeeper.domain.identifiers import HouseholdId, NotificationId
from plantkeeper.domain.notifications.errors import NotificationAlreadyReadError
from plantkeeper.domain.notifications.events import NotificationCreated, NotificationRead
from plantkeeper.domain.notifications.values import NotificationType


class Notification(AggregateRoot[NotificationId]):
    """A notification delivered to the household by HTTP long polling (Phase 8).

    The aggregate owns one rule: a notification is acknowledged at most once, and
    ``read_at`` is the acknowledgement itself. Creating one records
    :class:`NotificationCreated`; acknowledging it records
    :class:`NotificationRead`.
    """

    def __init__(
        self,
        notification_id: NotificationId,
        *,
        household_id: HouseholdId,
        notification_type: NotificationType,
        created_at: datetime,
        payload: dict[str, JsonValue] | None = None,
        read_at: datetime | None = None,
    ) -> None:
        """Rebuild a notification from its stored state (no events are recorded)."""
        super().__init__(notification_id)
        self._household_id = household_id
        self._notification_type = notification_type
        self._created_at = created_at
        # Copied so a caller cannot mutate the aggregate through its own dict.
        self._payload: dict[str, JsonValue] = dict(payload) if payload else {}
        self._read_at = read_at

    @classmethod
    def create(
        cls,
        *,
        household_id: HouseholdId,
        notification_type: NotificationType,
        now: datetime,
        payload: dict[str, JsonValue] | None = None,
        notification_id: NotificationId | None = None,
    ) -> Notification:
        """Create a notification and record :class:`NotificationCreated`."""
        notification = cls(
            notification_id or NotificationId.new(),
            household_id=household_id,
            notification_type=notification_type,
            created_at=now,
            payload=payload,
        )
        notification._record(
            NotificationCreated(
                notification_id=notification.id,
                household_id=household_id,
                notification_type=notification_type,
                payload=notification.payload,
                created_at=now,
                occurred_at=now,
            )
        )
        return notification

    @property
    def household_id(self) -> HouseholdId:
        """The household the notification is addressed to."""
        return self._household_id

    @property
    def notification_type(self) -> NotificationType:
        """Why the notification was created."""
        return self._notification_type

    @property
    def created_at(self) -> datetime:
        """When the notification was created."""
        return self._created_at

    @property
    def payload(self) -> dict[str, JsonValue]:
        """The notification's data, for example the plant identifier."""
        return dict(self._payload)

    @property
    def read_at(self) -> datetime | None:
        """When the household acknowledged the notification, if it did."""
        return self._read_at

    @property
    def is_read(self) -> bool:
        """Whether the notification has been acknowledged."""
        return self._read_at is not None

    def mark_read(self, *, now: datetime) -> None:
        """Acknowledge the notification and record :class:`NotificationRead`.

        Raises :class:`NotificationAlreadyReadError` when it was already
        acknowledged.
        """
        if self._read_at is not None:
            raise NotificationAlreadyReadError(
                f"notification {self.id} was already read at {self._read_at.isoformat()}"
            )
        self._read_at = now
        self._record(
            NotificationRead(
                notification_id=self.id,
                household_id=self._household_id,
                read_at=now,
                occurred_at=now,
            )
        )
