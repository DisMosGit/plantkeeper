"""Repositories for the saga-side state.

Four small tables, one class each:

* :class:`SqlAlchemyProcessedEventRepository` — the consumer ledger. ``claim`` is
  a PostgreSQL ``INSERT … ON CONFLICT DO NOTHING``: the database decides the
  winner of two concurrent deliveries, and only the winner's transaction goes on
  to do the work.
* :class:`SqlAlchemySagaStateRepository` — read access to saga executions. The
  engine's own storage owns the writes and answers recovery with identifiers only,
  so this is where the rows themselves are read back.
* :class:`SqlAlchemyMissedCareWindowRepository` — the grace windows the
  choreography MissedCareSaga and its scheduler share.
* :class:`SqlAlchemySagaIntentRepository` — the recorded cross-context commands a
  saga's steps delegate (``docs/adr/0012-saga-command-dispatch.md``).
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import ColumnElement, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from plantkeeper.application.ports.saga_intents import (
    IntentClaim,
    IntentStatus,
    RecordedIntent,
    SagaIntent,
)
from plantkeeper.application.ports.sagas import (
    MissedCareState,
    MissedCareWindow,
    SagaState,
)
from plantkeeper.domain.identifiers import PlantId
from plantkeeper.infrastructure.persistence.mappers.saga_intents import saga_intent_to_domain
from plantkeeper.infrastructure.persistence.mappers.sagas import (
    missed_care_window_to_domain,
    missed_care_window_to_model,
    saga_state_to_domain,
)
from plantkeeper.infrastructure.persistence.models.care import MissedCareWindowModel
from plantkeeper.infrastructure.persistence.models.shared import (
    ProcessedEventModel,
    SagaIntentModel,
    SagaStateModel,
)


class SqlAlchemyProcessedEventRepository:
    """The ``ProcessedEventRepository`` port over SQLAlchemy."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def claim(self, consumer_group: str, event_id: UUID) -> bool:
        """Insert the delivery, answering ``True`` only for the first one.

        The insert is not flushed or committed here: it belongs to the caller's
        transaction, so a handler that fails releases the claim with it — unless
        the caller commits on its failure path (a saga trigger recording
        ``SagaFailed`` does), which is what makes a recorded saga failure final.
        """
        statement = (
            insert(ProcessedEventModel)
            .values(consumer_group=consumer_group, event_id=event_id)
            .on_conflict_do_nothing(
                index_elements=[
                    ProcessedEventModel.consumer_group,
                    ProcessedEventModel.event_id,
                ]
            )
            .returning(ProcessedEventModel.id)
        )
        # ``DO NOTHING`` returns no row exactly when the delivery was a duplicate,
        # which is the answer the caller needs.
        return (await self._session.execute(statement)).scalar_one_or_none() is not None


class SqlAlchemySagaStateRepository:
    """The ``SagaStateRepository`` port over SQLAlchemy."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, saga_id: UUID) -> SagaState | None:
        """Return the saga's row, or ``None``."""
        model = await self._session.get(SagaStateModel, saga_id)
        return None if model is None else saga_state_to_domain(model)

    async def list_recoverable(
        self,
        *,
        limit: int,
        max_attempts: int,
        stale_before: datetime | None = None,
    ) -> list[SagaState]:
        """Return unfinished sagas, least recently updated first."""
        statement = (
            select(SagaStateModel)
            .where(
                SagaStateModel.status.in_(("running", "compensating")),
                SagaStateModel.recovery_attempts < max_attempts,
            )
            .order_by(SagaStateModel.updated_at, SagaStateModel.id)
            .limit(limit)
        )
        if stale_before is not None:
            statement = statement.where(SagaStateModel.updated_at < stale_before)
        models = (await self._session.execute(statement)).scalars()
        return [saga_state_to_domain(model) for model in models]


class SqlAlchemyMissedCareWindowRepository:
    """The ``MissedCareWindowRepository`` port over SQLAlchemy."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, plant_id: PlantId) -> MissedCareWindow | None:
        """Return the plant's window, or ``None``."""
        model = await self._session.get(MissedCareWindowModel, plant_id.value)
        return None if model is None else missed_care_window_to_domain(model)

    async def save(self, window: MissedCareWindow) -> None:
        """Stage the window in the caller's transaction."""
        await self._session.merge(missed_care_window_to_model(window))

    async def list_overdue(self, until: datetime, *, limit: int) -> list[MissedCareWindow]:
        """Return pending windows whose grace period expired, oldest first."""
        statement = (
            select(MissedCareWindowModel)
            .where(
                MissedCareWindowModel.state == MissedCareState.PENDING.value,
                MissedCareWindowModel.grace_deadline <= until,
            )
            .order_by(MissedCareWindowModel.grace_deadline, MissedCareWindowModel.plant_id)
            .limit(limit)
        )
        models = (await self._session.execute(statement)).scalars()
        return [missed_care_window_to_domain(model) for model in models]


class SqlAlchemySagaIntentRepository:
    """The ``SagaIntentRepository`` port over ``write_shared.saga_intents``.

    ``record`` stages a row in the caller's transaction — the step's own — which is
    what makes "the process decided to delegate" and "the record of that decision
    exists" the same event. Everything else is the dispatcher's side of the
    hand-off: claim with a lease, execute, record the outcome.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def record(self, intent: RecordedIntent) -> None:
        """Stage one recorded command in the caller's transaction."""
        self._session.add(
            SagaIntentModel(
                saga_id=intent.saga_id,
                step_no=intent.step_no,
                command_name=intent.command_name,
                payload=intent.payload,
                status=IntentStatus.PENDING.value,
                attempts=0,
                idempotency_key=intent.idempotency_key,
            )
        )

    async def claim_pending(self, limit: int, *, lease_seconds: int) -> list[IntentClaim]:
        """Claim up to ``limit`` retryable intents, oldest first.

        Both ``pending`` and ``failed`` are claimable: a failure is a retry, not a
        verdict, until the dispatcher's budget parks it. The select takes
        ``FOR UPDATE SKIP LOCKED`` so two dispatchers never take the same row, and
        the ``UPDATE`` behind it marks the lease in the same transaction.
        """
        claimable = (
            select(SagaIntentModel)
            .where(
                SagaIntentModel.status.in_((IntentStatus.PENDING.value, IntentStatus.FAILED.value)),
                _lease_expired(lease_seconds),
            )
            .order_by(SagaIntentModel.id)
            .limit(limit)
            .with_for_update(skip_locked=True, of=SagaIntentModel)
        )
        models = list((await self._session.execute(claimable)).scalars())
        if not models:
            return []

        claimed = (
            update(SagaIntentModel)
            .where(
                SagaIntentModel.id.in_([model.id for model in models]),
                SagaIntentModel.status.in_((IntentStatus.PENDING.value, IntentStatus.FAILED.value)),
            )
            .values(claimed_at=func.now())
            .returning(SagaIntentModel)
        )
        rows = list((await self._session.execute(claimed)).scalars())
        return [
            IntentClaim(
                id=row.id,
                saga_id=row.saga_id,
                command_name=row.command_name,
                payload=row.payload,
                attempts=row.attempts,
                idempotency_key=row.idempotency_key,
                recorded_at=row.created_at,
            )
            for row in rows
        ]

    async def cancel_pending(self, idempotency_key: str) -> bool:
        """Cancel a recorded command that has not run yet."""
        cancelled = (
            await self._session.execute(
                update(SagaIntentModel)
                .where(
                    SagaIntentModel.idempotency_key == idempotency_key,
                    SagaIntentModel.status == IntentStatus.PENDING.value,
                )
                .values(status=IntentStatus.CANCELLED.value, claimed_at=None)
                .returning(SagaIntentModel.id)
            )
        ).scalar_one_or_none()
        return cancelled is not None

    async def mark_executed(self, intent_id: int) -> None:
        """Record that the owning context's handler committed its effect."""
        await self._session.execute(
            update(SagaIntentModel)
            .where(SagaIntentModel.id == intent_id)
            .values(
                status=IntentStatus.EXECUTED.value,
                executed_at=func.now(),
                last_error=None,
                claimed_at=None,
            )
        )

    async def mark_failed(self, intent_id: int, error: str, *, max_attempts: int) -> IntentStatus:
        """Count one failed execution, parking the intent once the budget is out."""
        attempt = (
            await self._session.execute(
                update(SagaIntentModel)
                .where(SagaIntentModel.id == intent_id)
                .values(
                    attempts=SagaIntentModel.attempts + 1,
                    last_error=error,
                    claimed_at=None,
                )
                .returning(SagaIntentModel.attempts)
            )
        ).scalar_one_or_none()
        if attempt is None:
            raise ValueError(f"saga intent {intent_id} does not exist")
        if attempt >= max_attempts:
            await self.park(intent_id, error)
            return IntentStatus.PARKED
        return IntentStatus.FAILED

    async def release_claim(self, intent_id: int) -> None:
        """Give the lease back after the outcome was recorded."""
        await self._session.execute(
            update(SagaIntentModel).where(SagaIntentModel.id == intent_id).values(claimed_at=None)
        )

    async def park(self, intent_id: int, error: str) -> None:
        """Park an intent nothing should run again but an operator."""
        await self._session.execute(
            update(SagaIntentModel)
            .where(SagaIntentModel.id == intent_id)
            .values(status=IntentStatus.PARKED.value, last_error=error, claimed_at=None)
        )

    async def pending_for_saga(self, saga_id: UUID) -> list[SagaIntent]:
        """Return every intent of one saga, oldest first, for inspection."""
        statement = (
            select(SagaIntentModel)
            .where(SagaIntentModel.saga_id == saga_id)
            .order_by(SagaIntentModel.id)
        )
        models = (await self._session.execute(statement)).scalars()
        return [saga_intent_to_domain(model) for model in models]


def _lease_expired(lease_seconds: int) -> ColumnElement[bool]:
    """Whether an intent's claim is old enough to be taken over.

    ``now() - claimed_at > lease`` rather than ``claimed_at < now() - lease``: the
    two say the same thing, and only the first keeps both sides of the comparison an
    interval. The lease length is passed through ``make_interval`` so it never
    becomes SQL text.
    """
    return or_(
        SagaIntentModel.claimed_at.is_(None),
        func.now() - SagaIntentModel.claimed_at
        > func.make_interval(0, 0, 0, 0, 0, 0, lease_seconds),
    )
