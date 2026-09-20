"""Errors the application layer raises on its own behalf.

Domain errors come from the aggregates and say "a business rule was broken".
The errors here say something about a *use case*: the aggregate a handler was
asked to work on does not exist, or a client replayed an idempotency key with a
different request body. The API layer maps both kinds onto HTTP statuses.
"""

from __future__ import annotations


class ApplicationError(Exception):
    """Base class for every use-case failure that is not a domain error."""


class NotFoundError(ApplicationError):
    """The aggregate a command or query refers to does not exist."""


class IdempotencyKeyConflictError(ApplicationError):
    """An idempotency key was reused with a different request body.

    Replaying a request with the *same* body must return the original response;
    replaying it with a different body is a client bug, and silently treating it
    as the original request would hide it.
    """


class ConcurrentWriteError(ApplicationError):
    """A unique constraint was violated while committing.

    Raised by a unit of work instead of leaking the storage layer's own error
    type, so the application layer can react (an idempotency race re-reads the
    winner's response) without importing SQLAlchemy.
    """


class EventStoreConcurrencyError(ConcurrentWriteError):
    """An append lost the race for a stream version.

    The event store's ``(stream_id, version)`` key *is* its optimistic lock, so
    this is the same kind of failure as :class:`ConcurrentWriteError` — two writers
    read version N and both tried to append N+1. It stays a subclass so a caller
    that only knows about concurrent writes still catches it.
    """


class EventStoreCorruptionError(ApplicationError):
    """A stored stream cannot be replayed.

    Raised for an unknown ``event_type``, a payload that no longer validates, a
    version gap, or an event that does not belong in the stream it was read from.
    Unlike a Kafka delivery, a stored stream is local and must be fully replayable,
    so this is never skipped: a journal that silently drops a fact is worse than
    one that refuses to load.
    """


class UnhandledSagaTriggerError(ApplicationError):
    """A saga was asked to build a context from an event it does not trigger on.

    ``Saga.handle_event`` already filters by :attr:`Saga.trigger_events`, so this
    is a programming error in a subclass rather than a runtime condition: it makes
    the narrowing in ``context_from_event`` explicit instead of an ``assert``.
    """
