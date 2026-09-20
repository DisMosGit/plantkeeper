"""The Kafka side of the read side.

One subscriber per (projection, topic). The message is envelope-less — the body
is the event document and ``event_name``/``event_id`` travel in the headers — so
this module is where the header is turned back into a type, the body into an
event, and the event into a projection call.

Failure policy, deliberately three-way:

* an **unknown ``event_name``** or a body that does not validate is logged and
  acknowledged. It is a contract violation, and blocking the partition behind it
  would stop every later event from being projected. The message stays in the
  topic and can be replayed once the producer is fixed.
* an event **this projection does not handle** is acknowledged silently: a topic
  carries every event of its context.
* a **projection error** propagates, so FastStream rejects the delivery and the
  broker redelivers it. The ledger claim rolled back with the write, so a retry
  starts from nothing.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any, cast

from faststream.kafka import KafkaBroker, KafkaMessage
from pydantic import ValidationError

from plantkeeper.admin.projections import ALL_PROJECTIONS
from plantkeeper.admin.projections.base import Projection, apply_event
from plantkeeper.domain.base import DomainEvent
from plantkeeper.infrastructure.messaging.topics import (
    HEADER_EVENT_ID,
    HEADER_EVENT_NAME,
    event_type_for,
)

logger = logging.getLogger(__name__)

ProjectionSubscriber = Callable[[KafkaMessage], Awaitable[None]]


def register_projections(broker: KafkaBroker, *, prefix: str) -> None:
    """Attach every projection to its topics.

    Called before the broker starts, because FastStream refuses to add routes to a
    running broker. Each projection gets its own consumer group, so each one owns
    its offsets and its own idempotency domain.
    """
    for projection in ALL_PROJECTIONS:
        consumer_group = f"{prefix}-{projection.name}"
        for topic in projection.topics:
            broker.subscriber(
                topic,
                group_id=consumer_group,
                # A new consumer group replays its topic from the beginning; with
                # the ledger that is what makes a rebuilt read table possible.
                auto_offset_reset="earliest",
            )(build_handler(projection, consumer_group=consumer_group))


def build_handler(projection: Projection, *, consumer_group: str) -> ProjectionSubscriber:
    """Return the FastStream handler that feeds one projection."""

    async def handle(message: KafkaMessage) -> None:
        """Decode the delivery and project it, if it is this projection's."""
        event = decode_event(message)
        if event is None or not projection.handles(event):
            return
        projected = await apply_event(projection, event, consumer_group=consumer_group)
        if not projected:
            logger.debug(
                "event %s was already projected into %s",
                event.event_id,
                projection.name,
            )

    return handle


def decode_event(message: KafkaMessage) -> DomainEvent | None:
    """Turn a Kafka message back into the event its headers name.

    ``None`` means "skip and acknowledge": either the type is unknown or the body
    does not satisfy it. Both are logged with the ``event_id`` so the offending
    message can be found in the topic.
    """
    event_name = _header(message, HEADER_EVENT_NAME)
    event_id = _header(message, HEADER_EVENT_ID)
    if event_name is None:
        logger.warning("message without an %s header; skipping", HEADER_EVENT_NAME)
        return None
    model = event_type_for(event_name)
    if model is None:
        logger.warning("unknown event %r (event_id=%s); skipping", event_name, event_id)
        return None
    try:
        return model.model_validate(_body(message))
    except ValidationError:
        logger.exception("event %s (%s) failed validation; skipping", event_name, event_id)
        return None


def _header(message: KafkaMessage, name: str) -> str | None:
    """Read one header as text, whatever the broker handed over."""
    value: Any = message.headers.get(name)
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode()
    return str(value)


def _body(message: KafkaMessage) -> dict[str, Any]:
    """Return the message body as the mapping a Pydantic model can validate.

    FastStream's default JSON parser usually hands over a mapping already; a test
    or a custom deserialiser may leave the raw bytes in place.
    """
    body: Any = message.body
    if isinstance(body, bytes | str):
        return cast("dict[str, Any]", json.loads(body))
    return cast("dict[str, Any]", body)
