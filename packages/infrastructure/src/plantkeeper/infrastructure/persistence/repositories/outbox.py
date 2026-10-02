"""SQLAlchemy outbox repository.

The relay polls this table with its own session, never with the request-scoped
session of a unit of work: reading rows the write side has not committed yet
would break the atomicity the outbox exists to provide.

Two properties the polling has to preserve, and both live in
:meth:`SqlAlchemyOutboxRepository.fetch_unpublished`:

* **one publisher per row** — a relay claims the rows it takes, so several relays
  may run at once (they do, briefly, during a rolling deploy) without publishing
  the same row twice;
* **order within a partition key** — a row is only publishable when no older row
  with the same key is still waiting, so a message that fails holds its key back
  instead of letting its successors overtake it. The hold is released when the
  older row is published or abandoned, and abandoning is what ``attempts`` and
  the dead-letter path already decide.
"""

from __future__ import annotations

from sqlalchemy import Select, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.sql import ColumnElement

from plantkeeper.application.ports.outbox import OutboxMessage
from plantkeeper.application.provenance import MessageContext
from plantkeeper.domain.base import DomainEvent
from plantkeeper.infrastructure.persistence.mappers.outbox import (
    outbox_message_from_model,
    outbox_model_from_event,
)
from plantkeeper.infrastructure.persistence.models.shared import OutboxModel


def _still_pending(column_owner: type[OutboxModel]) -> ColumnElement[bool]:
    """Whether a row identified by ``column_owner`` still needs publishing."""
    return (column_owner.published_at.is_(None)) & (column_owner.dead_lettered_at.is_(None))


def _lease_expired(lease_seconds: int) -> ColumnElement[bool]:
    """Whether a claim is old enough to be taken over by another relay.

    ``now() - claimed_at > lease`` rather than ``claimed_at < now() - lease``: the
    two say the same thing, and only the first keeps both sides of the comparison an
    interval. Written the other way round, Postgres reads ``now() - interval`` as a
    timestamp and then has nothing to compare it to.
    """
    return or_(
        OutboxModel.claimed_at.is_(None),
        func.now() - OutboxModel.claimed_at > func.make_interval(0, 0, 0, 0, 0, 0, lease_seconds),
    )


class SqlAlchemyOutboxRepository:
    """The ``OutboxRepository`` port over SQLAlchemy."""

    def __init__(self, session: AsyncSession, *, context: MessageContext | None = None) -> None:
        self._session = session
        self._context = context
        """Provenance to stamp on appended rows.

        ``None`` means "read the ambient context", which is what a unit of work
        inside a request or a delivery wants. A caller that has the context in
        hand may pass it explicitly instead; either way the row is stamped when
        it is appended, in the caller's transaction.
        """

    async def append(self, event: DomainEvent) -> None:
        """Stage a row for ``event`` in the caller's transaction."""
        self._session.add(outbox_model_from_event(event, context=self._context))

    async def fetch_unpublished(self, limit: int, *, lease_seconds: int) -> list[OutboxMessage]:
        """Claim up to ``limit`` publishable rows and return them, oldest first.

        The select takes ``FOR UPDATE SKIP LOCKED``, so a row this relay is about
        to publish is skipped rather than blocked on by a concurrent relay. The
        lock is held until the caller commits, which is why the update below is
        safe even though it re-checks the claim.

        ``lease_seconds`` bounds how long a claim survives a relay that dies
        mid-publish: after that, the row is publishable again and no operator has
        to intervene. A row whose older key-mate is still waiting is filtered out
        of the *selection*, not deferred after it, so a blocked key does not spend
        the batch on rows that cannot be published yet.
        """
        predecessor = aliased(OutboxModel)
        blocked = (
            select(predecessor.id)
            .where(
                predecessor.partition_key == OutboxModel.partition_key,
                predecessor.id < OutboxModel.id,
                _still_pending(predecessor),
            )
            .exists()
        )
        claimable: Select[tuple[OutboxModel]] = (
            select(OutboxModel)
            .where(_still_pending(OutboxModel), ~blocked, _lease_expired(lease_seconds))
            .order_by(OutboxModel.id)
            .limit(limit)
            .with_for_update(skip_locked=True, of=OutboxModel)
        )
        models = list((await self._session.execute(claimable)).scalars())
        if not models:
            return []

        claimed = (
            update(OutboxModel)
            .where(
                OutboxModel.id.in_([model.id for model in models]),
                _still_pending(OutboxModel),
                _lease_expired(lease_seconds),
            )
            .values(claimed_at=func.now())
            .returning(OutboxModel)
        )
        claimed_models = list((await self._session.execute(claimed)).scalars())
        return [outbox_message_from_model(model) for model in claimed_models]

    async def release_claim(self, outbox_id: int) -> None:
        """Give the lease back after the row's outcome was recorded."""
        await self._session.execute(
            update(OutboxModel).where(OutboxModel.id == outbox_id).values(claimed_at=None)
        )

    async def mark_published(self, outbox_id: int) -> None:
        """Record a successful publish."""
        await self._session.execute(
            update(OutboxModel)
            .where(OutboxModel.id == outbox_id)
            .values(published_at=func.now(), last_error=None)
        )

    async def record_failure(self, outbox_id: int, error: str) -> None:
        """Count one failed attempt and remember the reason."""
        await self._session.execute(
            update(OutboxModel)
            .where(OutboxModel.id == outbox_id)
            .values(attempts=OutboxModel.attempts + 1, last_error=error)
        )

    async def dead_letter(self, outbox_id: int, error: str) -> None:
        """Stop retrying a row that was copied to the dead-letter topic."""
        await self._session.execute(
            update(OutboxModel)
            .where(OutboxModel.id == outbox_id)
            .values(dead_lettered_at=func.now(), last_error=error)
        )
