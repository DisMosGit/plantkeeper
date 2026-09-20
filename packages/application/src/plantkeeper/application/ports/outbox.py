"""The outbox port.

The transactional outbox decouples "the write happened" from "Kafka accepted the
message": the write side appends a row in the same transaction as the aggregate,
and a relay publishes it afterwards. See ``docs/adr/0003-write-side-outbox.md``.

The port is expressed in terms the caller can reason about — a domain event in,
an :class:`OutboxMessage` out — so neither the handlers nor the relay need to
know how a row is shaped.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable
from uuid import UUID

from pydantic import BaseModel, ConfigDict, JsonValue

from plantkeeper.domain.base import DomainEvent


class OutboxMessage(BaseModel):
    """One unpublished outbox row, ready to be published."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    outbox_id: int
    event_id: UUID
    event_name: str
    topic: str
    partition_key: str
    payload: dict[str, JsonValue]
    attempts: int


@runtime_checkable
class OutboxRepository(Protocol):
    """Read and write access to the outbox table."""

    async def append(self, event: DomainEvent) -> None:
        """Stage a row for ``event`` in the current transaction.

        The topic and the partition key are derived from the event type; an
        event type with no mapped topic is a programming error and raises.
        """
        ...

    async def fetch_unpublished(self, limit: int) -> list[OutboxMessage]:
        """Return up to ``limit`` rows that have neither been published nor
        dead-lettered, oldest first."""
        ...

    async def mark_published(self, outbox_id: int) -> None:
        """Record that the row's message reached Kafka."""
        ...

    async def record_failure(self, outbox_id: int, error: str) -> None:
        """Count one failed publish attempt and remember why."""
        ...

    async def dead_letter(self, outbox_id: int, error: str) -> None:
        """Stop retrying the row: it was copied to the dead-letter topic."""
        ...
