"""Base building blocks of the domain layer.

Aggregates are plain classes; the data that crosses a boundary (value objects
and domain events) is Pydantic. This module holds the small shared kernel: the
error root, the event root, and the entity/aggregate roots that record the
events an aggregate raises.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid7

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from plantkeeper.domain.identifiers import UuidIdentifier


class DomainError(Exception):
    """Base class for every business rule an aggregate enforces."""


class DomainEvent(BaseModel):
    """Base class for every domain event.

    Events are immutable and reject unknown fields, so a typo in a payload fails
    at construction rather than travelling on to Kafka. ``occurred_at`` must be
    timezone-aware and ``event_id`` defaults to a UUIDv7.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    event_id: UUID = Field(default_factory=uuid7)
    occurred_at: AwareDatetime = Field(default_factory=lambda: datetime.now(UTC))


class Entity[IdT: UuidIdentifier]:
    """An object with an identity that survives changes to its state.

    Equality is by concrete type *and* identifier, so a plant never equals
    another kind of entity that happens to wrap the same UUID.
    """

    def __init__(self, entity_id: IdT) -> None:
        self._id = entity_id

    @property
    def id(self) -> IdT:
        """The identifier of this entity."""
        return self._id

    def __eq__(self, other: object) -> bool:
        if type(self) is not type(other):
            return NotImplemented
        assert isinstance(other, Entity)
        return bool(self.id == other.id)

    def __hash__(self) -> int:
        return hash((type(self), self.id))

    def __repr__(self) -> str:
        return f"{type(self).__name__}(id={self.id!r})"


class AggregateRoot[IdT: UuidIdentifier](Entity[IdT]):
    """An entity that is the consistency boundary for a set of invariants.

    Aggregates record the events they raise. The application layer drains them
    with :meth:`collect_events` and writes them to the transactional outbox
    (Phase 2), so a domain event never leaves the domain by side effect.
    """

    def __init__(self, entity_id: IdT) -> None:
        super().__init__(entity_id)
        self._events: list[DomainEvent] = []

    def _record(self, event: DomainEvent) -> None:
        """Append an event that has just happened."""
        self._events.append(event)

    def collect_events(self) -> list[DomainEvent]:
        """Return the pending events in order and forget them.

        Draining, rather than copying, is what lets the Unit of Work publish
        each event exactly once: after ``collect_events()`` the aggregate is
        clean.
        """
        pending = self._events
        self._events = []
        return pending
