"""Idempotent create use cases.

An HTTP client cannot tell a lost response from a lost request, so a create
endpoint has to be replayable. The rule this module implements is the standard
one: the *response*, not just the aggregate, is stored under the client's
``Idempotency-Key`` — in the same transaction as the write — and a replay returns
that stored response untouched.

Two consequences follow from storing the response rather than a lock:

* a replay is free (one primary-key lookup, no writes);
* the stored record was produced by the original request, so the client sees the
  same body and the same status it would have seen had the first call succeeded.

Concurrency is handled by the primary key on the key column: two simultaneous
requests both try to insert, one wins, and the loser re-reads the winner's
record. That is why the unit of work translates a unique-constraint violation
into :class:`~plantkeeper.application.errors.ConcurrentWriteError` — so the race
can be resolved here without the application layer knowing about SQLAlchemy.
"""

from __future__ import annotations

import hashlib
from collections.abc import Awaitable, Callable
from typing import cast

from cqrs.response import PydanticResponse
from pydantic import BaseModel

from plantkeeper.application.commands.base import IdempotentCommand
from plantkeeper.application.errors import ConcurrentWriteError, IdempotencyKeyConflictError
from plantkeeper.application.ports.clock import Clock
from plantkeeper.application.ports.idempotency import IdempotencyRecord
from plantkeeper.application.ports.unit_of_work import UnitOfWork


def request_fingerprint(command: BaseModel) -> str:
    """Return a stable digest of a command's content.

    The fingerprint is taken over the command's own JSON rather than over the raw
    HTTP body: the command is where the request has been validated and normalised
    (whitespace stripped, defaults applied), so two requests that mean the same
    thing hash the same. The idempotency key itself is excluded — it is the thing
    the fingerprint is stored under.
    """
    payload = command.model_dump_json(exclude={"idempotency_key"})
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


async def commit_create[ViewT: PydanticResponse](
    *,
    uow: UnitOfWork,
    clock: Clock,
    command: IdempotentCommand,
    view_type: type[ViewT],
    status_code: int,
    operation: Callable[[], Awaitable[ViewT]],
) -> ViewT:
    """Run a create operation once, replaying it on a repeated idempotency key.

    ``operation`` must not commit: the helper owns the transaction, so the write
    and the stored response become visible together.
    """
    if command.idempotency_key is None:
        async with uow:
            view = await operation()
            await uow.commit()
        return view

    key = command.idempotency_key
    fingerprint = request_fingerprint(command)

    async with uow:
        stored = await uow.idempotency.get(key)
        if stored is not None:
            return _replayed(stored, fingerprint, view_type)

        view = await operation()
        await uow.idempotency.remember(
            IdempotencyRecord(
                key=key,
                request_hash=fingerprint,
                status_code=status_code,
                response=view.model_dump(mode="json"),
                created_at=clock.now(),
            )
        )
        try:
            await uow.commit()
        except ConcurrentWriteError:
            # Another request with the same key committed first; its response is
            # the one the client should see.
            await uow.rollback()
            winner = await uow.idempotency.get(key)
            if winner is None:
                raise
            return _replayed(winner, fingerprint, view_type)
        return view


def _replayed[ViewT: PydanticResponse](
    stored: IdempotencyRecord, fingerprint: str, view_type: type[ViewT]
) -> ViewT:
    """Return the stored response, refusing a key that was reused for other content."""
    if stored.request_hash != fingerprint:
        raise IdempotencyKeyConflictError(
            f"idempotency key {stored.key!r} was already used with a different request body"
        )
    # `ViewT` is bound to cqrs's untyped `PydanticResponse`, so `model_validate`
    # is untyped even though it returns exactly the requested view type.
    return cast(ViewT, view_type.model_validate(stored.response))
