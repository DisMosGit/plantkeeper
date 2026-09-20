"""The Kafka implementation of the event publisher port.

The message body is the event's own JSON document — the shape ``docs/events.md``
documents — and everything a consumer needs beyond it travels in the headers.
Putting an envelope around the payload would mean a consumer has to unwrap it
before it can validate the event, for no gain: the topic already identifies the
context, and ``event_name`` identifies the event.
"""

from __future__ import annotations

import json

from faststream.kafka import KafkaBroker

from plantkeeper.application.ports.outbox import OutboxMessage
from plantkeeper.infrastructure.messaging.topics import (
    DLQ_TOPIC,
    HEADER_ERROR,
    HEADER_EVENT_ID,
    HEADER_EVENT_NAME,
    HEADER_ORIGINAL_TOPIC,
)


class KafkaEventPublisher:
    """The ``EventPublisher`` port over a FastStream :class:`KafkaBroker`."""

    def __init__(self, broker: KafkaBroker) -> None:
        self._broker = broker

    async def publish(self, message: OutboxMessage) -> None:
        """Publish the message to its topic and wait for Kafka's acknowledgement."""
        await self._broker.publish(
            _serialize(message),
            topic=message.topic,
            key=message.partition_key.encode("utf-8"),
            headers={
                HEADER_EVENT_NAME: message.event_name,
                HEADER_EVENT_ID: str(message.event_id),
            },
        )

    async def publish_dead_letter(self, message: OutboxMessage, *, error: str) -> None:
        """Copy the message to the dead-letter topic, with its origin.

        The original topic and the error go in the headers, so an operator can
        replay the message to the right place without decoding the body first.
        """
        await self._broker.publish(
            _serialize(message),
            topic=DLQ_TOPIC,
            key=message.partition_key.encode("utf-8"),
            headers={
                HEADER_EVENT_NAME: message.event_name,
                HEADER_EVENT_ID: str(message.event_id),
                HEADER_ORIGINAL_TOPIC: message.topic,
                HEADER_ERROR: error,
            },
        )


def _serialize(message: OutboxMessage) -> str:
    """Render the payload as compact JSON.

    Compact on purpose: the payload is the event document and nothing else, and
    a consumer parses it by field name, so whitespace would only cost bandwidth.
    """
    return json.dumps(message.payload, separators=(",", ":"), ensure_ascii=False)
