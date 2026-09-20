"""Outbox and idempotency row mapping.

The outbox row is where the messaging contract is frozen: the topic and the
partition key are decided once, when the event is appended, and stored with the
payload. The relay therefore never has to re-derive them, and a topic rename
cannot silently republish an old row to a topic nobody listens to.
"""

from __future__ import annotations

from plantkeeper.application.ports.idempotency import IdempotencyRecord
from plantkeeper.application.ports.outbox import OutboxMessage
from plantkeeper.domain.base import DomainEvent
from plantkeeper.infrastructure.messaging.topics import partition_key_for, topic_for
from plantkeeper.infrastructure.persistence.models.shared import (
    IdempotencyKeyModel,
    OutboxModel,
)


def outbox_model_from_event(event: DomainEvent) -> OutboxModel:
    """Build the outbox row for an event that has just been recorded.

    ``event_name`` is the class name, which is exactly what
    ``tests/unit/domain/test_events_catalogue.py`` and ``docs/events.md`` call the
    event, so a consumer can look an event up by name without a mapping table.
    """
    return OutboxModel(
        event_id=event.event_id,
        event_name=type(event).__name__,
        topic=topic_for(event),
        partition_key=partition_key_for(event),
        payload=event.model_dump(mode="json"),
        occurred_at=event.occurred_at,
        attempts=0,
    )


def outbox_message_from_model(model: OutboxModel) -> OutboxMessage:
    """Turn a persisted row into the message the publisher sends."""
    return OutboxMessage(
        outbox_id=model.id,
        event_id=model.event_id,
        event_name=model.event_name,
        topic=model.topic,
        partition_key=model.partition_key,
        payload=model.payload,
        attempts=model.attempts,
    )


def idempotency_to_model(record: IdempotencyRecord) -> IdempotencyKeyModel:
    """Build the row that represents ``record``."""
    return IdempotencyKeyModel(
        key=record.key,
        request_hash=record.request_hash,
        status_code=record.status_code,
        response=record.response,
        created_at=record.created_at,
    )


def idempotency_to_domain(model: IdempotencyKeyModel) -> IdempotencyRecord:
    """Rebuild the stored response from its row."""
    return IdempotencyRecord(
        key=model.key,
        request_hash=model.request_hash,
        status_code=model.status_code,
        response=model.response,
        created_at=model.created_at,
    )
