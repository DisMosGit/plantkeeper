"""Turning a Kafka delivery back into a domain event.

The message contract is envelope-less: the body is the event's own JSON document
and ``event_name``/``event_id`` travel in the headers (``docs/events.md``). This
module is the one place that reads the header, resolves the type and validates the
body, so the read side's projections and the write side's sagas cannot disagree
about what a delivery is.

The policy is the consumer's to decide, but two outcomes are shared:

* an unknown ``event_name`` or a body that does not validate is a contract
  violation — it is logged and dropped, never raised, because blocking a partition
  behind one bad message would stop every later event from being handled;
* decoding never decides whether *this* consumer handles the event; the caller
  filters by type.
"""

from __future__ import annotations

import json
import logging
from typing import Any, cast

from faststream.kafka import KafkaMessage
from pydantic import ValidationError

from plantkeeper.domain.base import DomainEvent
from plantkeeper.infrastructure.messaging.topics import (
    HEADER_EVENT_ID,
    HEADER_EVENT_NAME,
    event_type_for,
)

logger = logging.getLogger(__name__)


def decode_event(message: KafkaMessage) -> DomainEvent | None:
    """Turn a Kafka message back into the event its headers name.

    ``None`` means "skip and acknowledge": either the type is unknown or the body
    does not satisfy it. Both are logged with the ``event_id`` so the offending
    message can be found in the topic.
    """
    event_name = read_header(message, HEADER_EVENT_NAME)
    event_id = read_header(message, HEADER_EVENT_ID)
    if event_name is None:
        logger.warning("message without an %s header; skipping", HEADER_EVENT_NAME)
        return None
    model = event_type_for(event_name)
    if model is None:
        logger.warning("unknown event %r (event_id=%s); skipping", event_name, event_id)
        return None
    try:
        return model.model_validate(read_body(message))
    except ValidationError:
        logger.exception("event %s (%s) failed validation; skipping", event_name, event_id)
        return None


def read_header(message: KafkaMessage, name: str) -> str | None:
    """Read one header as text, whatever the broker handed over.

    ``Any`` is unavoidable at exactly this seam: ``KafkaMessage.headers`` is an
    untyped mapping of whatever aiokafka decoded, and the two shapes that matter
    (``bytes`` and ``str``) are both handled below. Every caller narrows
    immediately.
    """
    value: Any = message.headers.get(name)
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode()
    return str(value)


def read_body(message: KafkaMessage) -> dict[str, Any]:
    """Return the message body as the mapping a Pydantic model can validate.

    FastStream's default JSON parser usually hands over a mapping already; a test
    or a custom deserialiser may leave the raw bytes in place.
    """
    body: Any = message.body
    if isinstance(body, bytes | str):
        return cast("dict[str, Any]", json.loads(body))
    return cast("dict[str, Any]", body)
