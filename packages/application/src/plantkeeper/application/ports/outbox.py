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

from pydantic import AwareDatetime, BaseModel, ConfigDict, JsonValue

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
    schema_version: int = 1
    """The version of the event document; ``1`` for a message stored before the
    column existed, which is exactly what the version policy calls it."""
    raised_by: str | None = None
    correlation_id: UUID | None = None
    causation_id: UUID | None = None
    traceparent: str | None = None
    observed_at: AwareDatetime | None = None
    """When the edge that produced the event saw it.

    Travels as the message's ``occurred_at`` header, which is what makes a chain
    of reactions sortable by when the work happened rather than by when each row
    was flushed. ``None`` for a row written before the column existed, and the
    relay then falls back to the row's own ``occurred_at``.
    """
    claimed_at: AwareDatetime | None = None
    """When this delivery leased the row, for the relay to release on the way out."""


@runtime_checkable
class OutboxRepository(Protocol):
    """Read and write access to the outbox table."""

    async def append(self, event: DomainEvent) -> None:
        """Stage a row for ``event`` in the current transaction.

        The topic and the partition key are derived from the event type; an
        event type with no mapped topic is a programming error and raises. The
        provenance columns are filled from the ambient context
        (:mod:`plantkeeper.application.provenance`), so a caller that is not an
        edge component does not pass them.
        """
        ...

    async def fetch_unpublished(self, limit: int, *, lease_seconds: int) -> list[OutboxMessage]:
        """Claim up to ``limit`` publishable rows and return them, oldest first.

        A claim is a lease: the row is marked as taken by this relay for
        ``lease_seconds``, and another relay may take it over once the lease is
        stale. A row is *publishable* when nothing older shares its partition
        key and still needs publishing, which is what keeps one aggregate's
        events in order even when one of them fails.
        """
        ...

    async def release_claim(self, outbox_id: int) -> None:
        """Give up the lease on a row this relay finished with.

        Called after the outcome of the publish is recorded, so the row is
        either published or dead-lettered and the claim no longer matters — but
        a failed row that stays claimed is invisible to a second relay until the
        lease expires, which would make the ordering barrier slower than it
        needs to be.
        """
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
