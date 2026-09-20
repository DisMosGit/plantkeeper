"""The event publisher port.

Deliberately dumb: it puts one already-serialised outbox message on a topic and
knows nothing about aggregates, transactions or retries. The relay owns the
retry policy, the dead-letter decision and the transaction in which the row is
marked as published.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from plantkeeper.application.ports.outbox import OutboxMessage


@runtime_checkable
class EventPublisher(Protocol):
    """Publishes outbox messages to the message broker."""

    async def publish(self, message: OutboxMessage) -> None:
        """Publish the message to its own topic.

        Raises when the broker did not acknowledge the message, so the relay can
        count the attempt.
        """
        ...

    async def publish_dead_letter(self, message: OutboxMessage, *, error: str) -> None:
        """Copy a message that exhausted its attempts to the dead-letter topic.

        The original topic, the event name and the error travel in the message
        headers so an operator can replay it by hand.
        """
        ...
