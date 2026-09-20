"""Notifications port: the channel a long poll waits on.

Notifications are delivered by HTTP long polling (Phase 8): the client asks for
pending notifications and, when there are none, waits on a per-household
*presence channel* until the write side says something happened. The channel
carries a **nudge**, never the notification itself — a household identifier
published on ``household:<id>``. The long poll answers a nudge by reading its own
database again, so a lost or duplicated nudge costs latency, never a
notification, and the row in ``write_notifications.notifications`` stays the one
source of truth.

:class:`NotificationChannel` is what both sides see: the API subscribes, and the
worker's ``NotificationPusher`` publishes once a ``NotificationCreated`` event
has been committed and published to Kafka. The Valkey adapter and the exact wire
format live in the infrastructure layer, and nothing here imports it.
"""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from typing import Protocol, runtime_checkable

from plantkeeper.domain.identifiers import HouseholdId


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
