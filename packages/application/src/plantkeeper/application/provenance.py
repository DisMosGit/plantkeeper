"""Where a message came from, and which conversation it belongs to.

The message body stays the event document; provenance travels beside it, in the
headers, and is stored on the outbox row so the relay can emit it without
re-deriving anything (``docs/events.md``). This module is the one place that
knows the shape of those fields and how they are carried across a unit of work.

Provenance is *ambient*, not a parameter: a handler that raises an event does not
thread a correlation identifier through its own signature to have the event
stamped. The component at the edge that knows the answer — the API middleware, a
Kafka consumer, a timer — binds it for the span of the request, and the outbox
adapter reads it when the row is appended. That keeps the propagation out of the
application's business signatures while still making every stamped row
explainable by the component that bound it.

The fields:

* ``correlation_id`` — the conversation. One API request or one saga has one, and
  every event it causes, directly or through a chain of reactions, shares it;
* ``causation_id`` — the ``event_id`` of the message that caused this one, so a
  chain is reconstructable from any link rather than only from its head;
* ``raised_by`` — a tagged actor: ``user:<id>``, ``system:<job>``, ``saga:<id>``
  or ``service:<name>``. There is no authentication in this project, so this
  names a component or a claimed user, never a verified one (ADR 0010);
* ``schema_version`` — the version of the event document (see
  :attr:`plantkeeper.domain.base.DomainEvent.schema_version`);
* ``traceparent`` — the W3C trace context header, passed through opaquely. The
  platform does not run a tracer; it carries the value so an operator who does
  can correlate across it.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar, Token
from datetime import UTC, datetime
from uuid import UUID, uuid7

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

UNKNOWN_RAISER = "system:unknown"
"""What a message with no provenance at all is attributed to.

Absent provenance is *tolerated*, never fatal: messages published before this
platform carried headers still arrive, and refusing them would turn a metadata
gap into data loss. ``docs/events.md`` documents the reading.
"""


class MessageProvenance(BaseModel):
    """The provenance fields of one message."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    raised_by: str
    correlation_id: UUID | None = None
    causation_id: UUID | None = None
    traceparent: str | None = None


class MessageContext(MessageProvenance):
    """The provenance bound to the work in flight, plus when the edge saw it.

    ``observed_at`` is the edge's clock reading, not the database's: every event
    raised by one delivery shares one instant, so the events of a chain sort the
    way the work happened rather than the order the rows happened to be flushed
    in. It is stored in the outbox's existing ``created_at``, which the relay
    already reads.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    observed_at: AwareDatetime = Field(default_factory=lambda: datetime.now(UTC))


_current: ContextVar[MessageContext | None] = ContextVar("plantkeeper_provenance", default=None)
"""The provenance bound for the work in flight.

A context variable rather than an argument for the reason the module docstring
gives. It is set and reset around a whole request or delivery, so a consumer
handler, the saga it triggers and the step handlers the saga runs all observe the
same binding without any of them passing it on.
"""


def current_provenance() -> MessageContext | None:
    """Return the provenance bound to the work in flight, if any.

    ``None`` means nothing claimed this work: a background job that has not bound
    a context, or a unit test driving a repository directly. Callers that must
    emit something use :data:`UNKNOWN_RAISER`; callers that only decorate a
    message treat ``None`` as "carry what you have".
    """
    return _current.get()


def bind_provenance(context: MessageContext) -> Token[MessageContext | None]:
    """Bind ``context`` for the current task, returning the reset token."""
    return _current.set(context)


def reset_provenance(token: Token[MessageContext | None]) -> None:
    """Undo one :func:`bind_provenance`."""
    _current.reset(token)


@contextmanager
def provenance_scope(context: MessageContext) -> Iterator[MessageContext]:
    """Bind ``context`` for the duration of a synchronous block."""
    token = bind_provenance(context)
    try:
        yield context
    finally:
        reset_provenance(token)


@asynccontextmanager
async def async_provenance_scope(context: MessageContext) -> AsyncIterator[MessageContext]:
    """Bind ``context`` for the duration of an ``async with`` block.

    The async twin of :func:`provenance_scope`: a consumer handler and the request
    scope it opens are both ``async with`` blocks, so they need an asynchronous
    context manager rather than the synchronous one.
    """
    token = bind_provenance(context)
    try:
        yield context
    finally:
        reset_provenance(token)


def request_context(
    *,
    raised_by: str,
    traceparent: str | None = None,
    correlation_id: UUID | None = None,
    observed_at: datetime | None = None,
) -> MessageContext:
    """Start a conversation for one inbound request.

    A fresh ``correlation_id`` unless the caller supplies one, which lets a client
    or a test join an existing conversation deliberately.
    """
    return MessageContext(
        raised_by=raised_by,
        correlation_id=correlation_id if correlation_id is not None else uuid7(),
        traceparent=traceparent,
        observed_at=observed_at if observed_at is not None else datetime.now(UTC),
    )


def reaction_context(
    *,
    raised_by: str,
    caused_by: UUID,
    correlation_id: UUID | None = None,
    traceparent: str | None = None,
    observed_at: datetime | None = None,
) -> MessageContext:
    """Continue a conversation with one message as the cause.

    This is what a consumer builds from a delivery: the event it is handling
    becomes the ``causation_id`` of everything the handler raises, and the
    conversation it arrived on is the conversation that continues — an event with
    no correlation of its own starts a new one rather than losing the chain.
    """
    return MessageContext(
        raised_by=raised_by,
        correlation_id=correlation_id if correlation_id is not None else caused_by,
        causation_id=caused_by,
        traceparent=traceparent,
        observed_at=observed_at if observed_at is not None else datetime.now(UTC),
    )


def as_raiser(raised_by: str, *, ambient: MessageContext | None = None) -> MessageContext:
    """Re-attribute the ambient context to ``raised_by``, keeping its conversation.

    A saga's lifecycle events are raised *by the saga*, but they belong to the
    conversation that triggered it: this keeps the correlation and trace context
    and changes only who is named. With no ambient context, the raiser stands
    alone.
    """
    base = ambient if ambient is not None else current_provenance()
    if base is None:
        return MessageContext(raised_by=raised_by)
    return base.model_copy(update={"raised_by": raised_by})


@asynccontextmanager
async def acting_as(raised_by: str) -> AsyncIterator[MessageContext]:
    """Attribute everything raised inside the block to ``raised_by``.

    Used where a component works on behalf of something narrower than itself: a
    saga dispatching its steps is the saga, not the consumer that triggered it, so
    the events the steps raise say ``saga:<id>`` while still carrying the
    conversation the delivery arrived on.
    """
    async with async_provenance_scope(as_raiser(raised_by)):
        yield current_provenance() or MessageContext(raised_by=raised_by)
