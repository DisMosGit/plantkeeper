"""The Kafka implementation of the publisher ports.

Two ports, one wrapper: :class:`~plantkeeper.application.ports.event_publisher.EventPublisher`
publishes outbox messages the relay drains, and
:class:`~plantkeeper.application.ports.dead_letter.DeadLetterPublisher` copies a
delivery a *consumer* gave up on. Both end on a Kafka topic, and both are
registered separately in the container — Dishka keys a factory by its return type,
so one factory cannot answer for the two.

The message body is the event's own JSON document — the shape ``docs/events.md``
documents — and everything a consumer needs beyond it travels in the headers.
Putting an envelope around the payload would mean a consumer has to unwrap it
before it can validate the event, for no gain: the topic already identifies the
context, and ``event_name`` identifies the event.

The headers are therefore the whole of the envelope:

* ``event_name`` / ``event_id`` — what the body is, and how to deduplicate it;
* ``correlation_id`` / ``causation_id`` — the conversation, and the message that
  caused this one;
* ``raised_by`` — the tagged actor that raised it;
* ``schema_version`` — which revision of the event document the body is;
* ``traceparent`` / ``occurred_at`` — the trace context, carried opaquely, and
  the instant the raising edge observed.

A provenance header is emitted only when the row has a value for it, so a message
published from a row written before the columns existed stays honest about having
no origin rather than asserting ``system:unknown`` as a fact.
"""

from __future__ import annotations

import json
from uuid import UUID

from faststream.kafka import KafkaBroker

from plantkeeper.application.ports.dead_letter import DeliveryDeadLetter
from plantkeeper.application.ports.outbox import OutboxMessage
from plantkeeper.infrastructure.messaging.topics import (
    DLQ_TOPIC,
    HEADER_CAUSATION_ID,
    HEADER_CONSUMER_GROUP,
    HEADER_CORRELATION_ID,
    HEADER_ERROR,
    HEADER_EVENT_ID,
    HEADER_EVENT_NAME,
    HEADER_OCCURRED_AT,
    HEADER_ORIGINAL_TOPIC,
    HEADER_RAISED_BY,
    HEADER_SCHEMA_VERSION,
    HEADER_TRACEPARENT,
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
            headers=headers_for(message),
        )

    async def publish_dead_letter(self, message: OutboxMessage, *, error: str) -> None:
        """Copy the message to the dead-letter topic, with its origin.

        The original topic and the error go in the headers, so an operator can
        replay the message to the right place without decoding the body first.
        The provenance travels with it, so a moved-aside message can still be
        traced back to the conversation that produced it.
        """
        await self._broker.publish(
            _serialize(message),
            topic=DLQ_TOPIC,
            key=message.partition_key.encode("utf-8"),
            headers={
                **headers_for(message),
                HEADER_ORIGINAL_TOPIC: message.topic,
                HEADER_ERROR: error,
            },
        )

    async def publish_moved_aside(self, delivery: DeliveryDeadLetter) -> None:
        """Copy a delivery a consumer gave up on to the dead-letter topic.

        The consumer group joins the origin and the cause in the headers: it is
        what tells a replay whose ledger claim to clear, and what tells an
        operator which consumer stopped coping. The delivery's own headers travel
        untouched, so the copy is the message that was delivered rather than a
        re-rendering of it.
        """
        await self._broker.publish(
            delivery.body,
            topic=DLQ_TOPIC,
            key=delivery.partition_key.encode("utf-8"),
            headers={
                **delivery.headers,
                HEADER_CONSUMER_GROUP: delivery.consumer_group,
                HEADER_ORIGINAL_TOPIC: delivery.original_topic,
                HEADER_ERROR: delivery.error,
            },
        )


def headers_for(message: OutboxMessage) -> dict[str, str]:
    """Return the headers this message carries, leaving out the absent ones.

    Public because the header set is the messaging contract: a test asserts it
    here rather than reaching into the broker's publish arguments.
    """
    headers = {
        HEADER_EVENT_NAME: message.event_name,
        HEADER_EVENT_ID: str(message.event_id),
        HEADER_SCHEMA_VERSION: str(message.schema_version),
    }
    optional: dict[str, UUID | str | None] = {
        HEADER_CORRELATION_ID: message.correlation_id,
        HEADER_CAUSATION_ID: message.causation_id,
        HEADER_RAISED_BY: message.raised_by,
        HEADER_TRACEPARENT: message.traceparent,
    }
    for name, value in optional.items():
        if value is not None:
            headers[name] = str(value)
    if message.observed_at is not None:
        headers[HEADER_OCCURRED_AT] = message.observed_at.isoformat()
    return headers


def _serialize(message: OutboxMessage) -> str:
    """Render the payload as compact JSON.

    Compact on purpose: the payload is the event document and nothing else, and
    a consumer parses it by field name, so whitespace would only cost bandwidth.
    """
    return json.dumps(message.payload, separators=(",", ":"), ensure_ascii=False)
