"""The notification pusher: wake the households a long poll is waiting for.

A notification is delivered by HTTP long polling, so the write side has to be
able to say "look again" without knowing who is listening. This consumer is that
signal: it subscribes to ``notifications.events``, and for every committed
``NotificationCreated`` it publishes a nudge on the household's presence channel.

Two properties make it the right seam:

* **It runs after the commit.** The event reaches Kafka through the outbox
  relay, which publishes only rows that were committed with the aggregate, so a
  poller woken by this consumer always finds the notification when it re-queries.
* **It is a signal, not the data.** A nudge carries the household identifier and
  nothing else. A lost nudge costs one poll interval of latency, a duplicated one
  costs a redundant query, and neither can corrupt anything — which is why a
  channel failure is logged and swallowed rather than failing the delivery and
  pinning the consumer group on an unreachable Valkey.

The class is a :class:`~plantkeeper.application.sagas.consumer.Consumer` like the
choreography sagas: it owns a consumer group and a ``(consumer_group, event_id)``
claim even though its work has no rows of its own to protect. Re-publishing a
nudge after a rebuild is harmless.
"""

from __future__ import annotations

import logging
from typing import ClassVar

from cqrs.dispatcher.saga import SagaDispatcher

from plantkeeper.application.ports.notifications import (
    NotificationChannel,
    NotificationChannelError,
)
from plantkeeper.application.ports.unit_of_work import UnitOfWork
from plantkeeper.application.sagas.consumer import Consumer
from plantkeeper.domain.base import DomainEvent
from plantkeeper.domain.notifications.events import NotificationCreated

logger = logging.getLogger(__name__)


class NotificationPusher(Consumer):
    """Publish one nudge per created notification."""

    name = "notification-push"
    handled_types: ClassVar[tuple[type[DomainEvent], ...]] = (NotificationCreated,)

    def __init__(self, unit_of_work: UnitOfWork, channel: NotificationChannel) -> None:
        super().__init__(unit_of_work)
        self._channel = channel

    async def handle(self, event: DomainEvent, dispatcher: SagaDispatcher) -> None:
        """Signal the household, or say so and move on.

        ``dispatcher`` is unused: a nudge starts no process manager.
        """
        if not isinstance(event, NotificationCreated):
            return
        try:
            await self._channel.publish(event.household_id)
        except NotificationChannelError as exc:
            # The notification is committed and a long poll re-reads its own
            # database at its deadline, so a missed nudge delays delivery and
            # nothing more. Failing here would only replay the delivery until
            # Valkey came back, blocking every other notification behind it.
            logger.warning(
                "could not signal household %s about notification %s: %s",
                event.household_id.value,
                event.notification_id.value,
                exc,
            )
