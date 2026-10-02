"""Notifications ports: the household signal, and the read behind a stream.

Notifications are delivered to clients connected to the API, by a server-sent
event stream or by the request-and-wait endpoint kept as its fallback. Both forms
are woken by the same per-household *presence channel*: the write side publishes a
**nudge**, never the notification itself — a household identifier published on
``household:<id>`` — and the receiver answers it by reading its own database. A
lost or duplicated nudge therefore costs latency or a redundant read, never a
notification, and the row in ``write_notifications.notifications`` stays the one
source of truth.

:class:`NotificationChannel` is what both sides see: the API subscribes, and the
worker's ``NotificationPusher`` publishes once a ``NotificationCreated`` event
has been committed and published to Kafka. :class:`PendingNotificationReader` is
the read behind the stream: every call opens storage and releases it again, so a
stream that is idle holds a subscription and no database session.

The Valkey adapter, the SQLAlchemy reader and the exact wire formats live in the
infrastructure layer, and nothing here imports it.
"""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from typing import Protocol, runtime_checkable

from plantkeeper.application.views import NotificationView
from plantkeeper.domain.identifiers import HouseholdId, NotificationId


class NotificationChannelError(Exception):
    """The presence channel could not be reached or failed mid-wait.

    A channel error is never fatal to the business: the notification itself is
    already committed, so a caller that cannot wait answers from its own storage
    and a publisher that cannot nudge logs and moves on.
    """


@runtime_checkable
class NotificationSubscription(Protocol):
    """One subscriber's open channel for one household."""

    async def wait(self, timeout: float) -> bool:
        """Wait for a nudge until ``timeout`` seconds have passed.

        Returns ``True`` when the channel signalled, ``False`` when the deadline
        passed. Raises :class:`NotificationChannelError` when the channel fails
        while waiting.
        """
        ...


@runtime_checkable
class NotificationChannel(Protocol):
    """The per-household presence channel behind a long poll."""

    async def publish(self, household_id: HouseholdId) -> None:
        """Signal the household that its notifications changed.

        Raises :class:`NotificationChannelError` when the signal cannot be sent.
        """
        ...

    def subscribe(
        self, household_id: HouseholdId
    ) -> AbstractAsyncContextManager[NotificationSubscription]:
        """Open a subscription for ``household_id`` until the context exits.

        Raises :class:`NotificationChannelError` when the subscription cannot be
        opened.
        """
        ...


@runtime_checkable
class PendingNotificationReader(Protocol):
    """The unacknowledged notifications a stream has not delivered yet.

    One call is one complete read on storage of its own: the stream calls this
    again after every wake-up and holds nothing in between, which is what lets an
    idle stream hold a channel subscription and no database session.
    """

    async def pending_since(
        self, household_id: HouseholdId, *, since: NotificationId | None
    ) -> list[NotificationView]:
        """Return the household's unacknowledged notifications after ``since``.

        ``since`` is the last notification the caller delivered; identifiers are
        UUIDv7, so "greater than the cursor" and "created after the cursor" are
        the same question, and ``None`` means "from the beginning". Oldest first,
        and shaped exactly like the request-and-wait answer.
        """
        ...
