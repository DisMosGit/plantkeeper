"""Turning a Kafka delivery back into a domain event, with its provenance.

The message contract is envelope-less: the body is the event's own JSON document
and everything else travels in the headers (``docs/events.md``). This module is
the one place that reads a header, resolves the type, validates the body and
builds the provenance context, so the read side's projections and the write
side's sagas cannot disagree about what a delivery is.

The policy is the consumer's to decide, but two outcomes are shared:

* an unknown ``event_name`` or a body that does not validate — including one that
  is not JSON at all — is a contract violation: it is logged and dropped, never
  raised, because blocking a partition behind one bad message would stop every
  later event from being handled. Nothing this module raises can reach a
  subscriber, which is what keeps such a message out of the retry loop and off the
  dead-letter topic;
* decoding never decides whether *this* consumer handles the event; the caller
  filters by type.

Provenance is treated differently from the type: it is *decorative*, so a
malformed or absent header is repaired rather than punished (see
:func:`provenance_of`). Losing the ability to follow a chain is a smaller cost
than losing the message.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, cast
from uuid import UUID

from faststream.kafka import KafkaMessage
from pydantic import BaseModel, ConfigDict, ValidationError

from plantkeeper.application.provenance import (
    UNKNOWN_RAISER,
    MessageContext,
    reaction_context,
)
from plantkeeper.domain.base import DomainEvent
from plantkeeper.infrastructure.messaging.topics import (
    HEADER_CORRELATION_ID,
    HEADER_EVENT_ID,
    HEADER_EVENT_NAME,
    HEADER_OCCURRED_AT,
    HEADER_RAISED_BY,
    HEADER_TRACEPARENT,
    event_type_for,
)

logger = logging.getLogger(__name__)


class PublishedMessage(BaseModel):
    """One delivery: the event it carries and the provenance it arrived with.

    The two travel together because they are read from the same message. Keeping
    them in one value means a consumer can bind the provenance for the work it is
    about to do without being handed the raw Kafka message, which is an
    infrastructure type the application layer must not see.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    event: DomainEvent
    context: MessageContext


def decode_message(message: KafkaMessage) -> PublishedMessage | None:
    """Turn a Kafka delivery into the event it carries and its provenance.

    ``None`` means "skip and acknowledge": the type is unknown, or the body does
    not satisfy it. Both are logged with the ``event_id`` so the offending message
    can be found in the topic.

    The provenance is always filled in, because a handler needs *some* context to
    stamp what it raises. A delivery missing the provenance headers — one
    published before the platform carried them, or by a hand-written producer — is
    attributed to :data:`~plantkeeper.application.provenance.UNKNOWN_RAISER` and
    starts its own conversation, keyed by its own identifier. That is the
    tolerance policy ``docs/events.md`` documents: absence is a gap in the
    metadata, never a reason to drop a delivery.
    """
    decoded = decode_event(message)
    if decoded is None:
        return None
    return PublishedMessage(event=decoded, context=provenance_of(message, decoded.event_id))


def decode_event(message: KafkaMessage) -> DomainEvent | None:
    """Turn a Kafka message back into the event its headers name."""
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
        document = read_body(message)
    except ValueError:
        # A body that is not JSON is the same case as one that does not validate:
        # a contract violation the consumer acknowledges rather than raises. It has
        # to be caught *here* — raising would take the delivery out of the failure
        # policy, and a subscriber that redelivers a failed delivery would offer
        # this one forever.
        logger.exception("event %s (%s) is not JSON; skipping", event_name, event_id)
        return None
    try:
        return model.model_validate(document)
    except ValidationError:
        logger.exception("event %s (%s) failed validation; skipping", event_name, event_id)
        return None


def provenance_of(message: KafkaMessage, event_id: UUID) -> MessageContext:
    """Build the provenance context for one delivery.

    The *delivered* event becomes the cause of everything the handler raises, so
    the next message's ``causation_id`` names this one and a chain is walkable
    from any link. The conversation continues from the delivery's
    ``correlation_id`` — or, absent one, from the delivery's own identifier, which
    starts a conversation rather than losing the link.

    ``raised_by`` is taken from the header when it is there. A consumer is free to
    overwrite it, because only the consumer knows whether it is acting as itself
    or on behalf of a process manager.
    """
    return reaction_context(
        raised_by=read_header(message, HEADER_RAISED_BY) or UNKNOWN_RAISER,
        caused_by=event_id,
        correlation_id=_as_uuid(read_header(message, HEADER_CORRELATION_ID)),
        traceparent=read_header(message, HEADER_TRACEPARENT),
        observed_at=_as_datetime(read_header(message, HEADER_OCCURRED_AT)),
    )


def _as_uuid(raw: str | None) -> UUID | None:
    """Parse a header that should be a UUID, tolerating a malformed one.

    A corrupted header must not cost the delivery: it is metadata, and the
    fallback (start a new conversation) is harmless.
    """
    if raw is None:
        return None
    try:
        return UUID(raw)
    except ValueError:
        logger.warning("ignoring malformed identifier header %r", raw)
        return None


def _as_datetime(raw: str | None) -> datetime | None:
    """Parse the observed-at header, tolerating a malformed one."""
    if raw is None:
        return None
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        logger.warning("ignoring malformed %s header %r", HEADER_OCCURRED_AT, raw)
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
