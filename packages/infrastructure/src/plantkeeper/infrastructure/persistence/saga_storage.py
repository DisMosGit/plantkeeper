"""Saga state storage over the project's own schema-qualified tables.

``python-cqrs`` ships its own ``SqlAlchemySagaStorage``, but it hard-codes
unqualified table names, reads them from environment variables *at import time*,
and declares a second declarative base. This adapter implements the same
``ISagaStorage`` protocol against ``write_shared.saga_state`` /
``write_shared.saga_log``, which Alembic owns like every other write table. See
``docs/adr/0005-orchestration-vs-choreography.md``.

The statement logic lives in :class:`_SagaStatements`, which never commits, and is
shared by the two entry points the protocol offers: the per-call methods (each
opening its own session and committing) and :meth:`create_run`, the checkpointed
run the engine prefers. Writing the queries twice would be the easy way for the
two paths to drift.

**Where a run commits is the whole point of this module.** The engine calls
``run.commit()`` at every checkpoint — after creating the saga, after each step,
after recording a failure — and those commits are what make a step durable. If the
run owns a session of its own, a step's effect (committed by the step handler
through the request's unit of work) and the checkpoint that records the step land
in *different* transactions, and a crash in between leaves the platform claiming
progress it never made. So a run opened over a bounded session commits **that**
session, through the unit of work, which is what puts the effect, the step-history
entry, the checkpoint and the lifecycle outbox row in one transaction. A run with
no bounded session — the recovery job, which has no request — opens its own and
behaves as before.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Literal, Protocol
from uuid import UUID

from cqrs.saga.storage.enums import SagaStatus, SagaStepStatus
from cqrs.saga.storage.models import SagaLogEntry
from cqrs.saga.storage.protocol import ISagaStorage, SagaStorageRun
from pydantic import JsonValue
from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from plantkeeper.infrastructure.persistence.models.shared import (
    SagaLogModel,
    SagaStateModel,
)

CRASHED_STATUSES = (SagaStatus.RUNNING.value, SagaStatus.COMPENSATING.value)
"""The two statuses a crash can leave behind.

A process in either may simply have stopped mid-step; the recovery job resumes it
from its step history.
"""

RETRYABLE_STATUSES = (SagaStatus.FAILED.value,)
"""A recorded failure the recovery job retries, within its budget.

The engine refuses to run a ``failed`` saga forward — it reads that status as
"compensation was completed, do not resume" — so the recovery job clears the status
before handing it back, and parks the record once the budget is spent
(``docs/sagas.md``).
"""


class SagaCommitter(Protocol):
    """The transaction a saga run commits into.

    Narrower than ``UnitOfWork`` on purpose: the storage needs to commit the
    caller's transaction and to know nothing else about it, and the per-call
    entry points have no unit of work at all.
    """

    async def commit(self) -> None:
        """Commit the transaction."""
        ...

    async def rollback(self) -> None:
        """Discard the transaction."""
        ...


class _SagaStatements:
    """Statement-level saga operations over one session. Never commits."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create_saga(self, saga_id: UUID, name: str, context: dict[str, JsonValue]) -> None:
        """Insert a saga execution in ``PENDING`` at version 1."""
        await self._session.execute(
            insert(SagaStateModel).values(
                id=saga_id,
                name=name,
                status=SagaStatus.PENDING.value,
                context=context,
                version=1,
                recovery_attempts=0,
            )
        )

    async def update_context(
        self,
        saga_id: UUID,
        context: dict[str, JsonValue],
        current_version: int | None = None,
    ) -> None:
        """Replace the persisted context and bump the optimistic-lock version."""
        statement = (
            update(SagaStateModel)
            .where(SagaStateModel.id == saga_id)
            .values(context=context, version=SagaStateModel.version + 1)
            .returning(SagaStateModel.id)
        )
        if current_version is not None:
            statement = statement.where(SagaStateModel.version == current_version)
        updated = (await self._session.execute(statement)).scalar_one_or_none()
        if updated is None and current_version is not None:
            raise ValueError(f"saga {saga_id} was modified concurrently or does not exist")

    async def update_status(self, saga_id: UUID, status: SagaStatus) -> None:
        """Set the saga's status and bump the optimistic-lock version."""
        statement = (
            update(SagaStateModel)
            .where(SagaStateModel.id == saga_id)
            .values(status=status.value, version=SagaStateModel.version + 1)
            .returning(SagaStateModel.id)
        )
        if (await self._session.execute(statement)).scalar_one_or_none() is None:
            raise ValueError(f"saga {saga_id} does not exist")

    async def log_step(
        self,
        saga_id: UUID,
        step_name: str,
        action: str,
        status: SagaStepStatus,
        details: str | None = None,
    ) -> None:
        """Append one step transition."""
        self._session.add(
            SagaLogModel(
                saga_id=saga_id,
                step_name=step_name,
                action=action,
                status=status.value,
                details=details,
            )
        )

    async def load_saga_state(
        self, saga_id: UUID, *, read_for_update: bool = False
    ) -> tuple[SagaStatus, dict[str, JsonValue], int]:
        """Return status, context and version; ``ValueError`` when unknown.

        The engine treats ``ValueError`` as "no such saga, create it", so the
        error type is part of the contract, not an implementation detail.
        """
        statement = select(SagaStateModel).where(SagaStateModel.id == saga_id)
        if read_for_update:
            statement = statement.with_for_update()
        model = (await self._session.execute(statement)).scalars().first()
        if model is None:
            raise ValueError(f"saga {saga_id} not found")
        return SagaStatus(model.status), model.context, model.version

    async def get_step_history(self, saga_id: UUID) -> list[SagaLogEntry]:
        """Return the saga's step transitions, oldest first.

        Ordered by ``(created_at, id)``: several transitions can share a
        timestamp, and the compensation order depends on the sequence being
        stable.
        """
        statement = (
            select(SagaLogModel)
            .where(SagaLogModel.saga_id == saga_id)
            .order_by(SagaLogModel.created_at, SagaLogModel.id)
        )
        models = (await self._session.execute(statement)).scalars()
        return [
            SagaLogEntry(
                saga_id=model.saga_id,
                step_name=model.step_name,
                action=_as_action(model.action),
                status=SagaStepStatus(model.status),
                timestamp=model.created_at,
                details=model.details,
            )
            for model in models
        ]

    async def get_sagas_for_recovery(
        self,
        limit: int,
        max_recovery_attempts: int = 5,
        stale_after_seconds: int | None = None,
        saga_name: str | None = None,
    ) -> list[UUID]:
        """Return sagas worth resuming: crashed ones, and failed ones with budget left."""
        statement = (
            select(SagaStateModel.id)
            .where(
                SagaStateModel.status.in_(CRASHED_STATUSES + RETRYABLE_STATUSES),
                SagaStateModel.recovery_attempts < max_recovery_attempts,
            )
            .order_by(SagaStateModel.updated_at, SagaStateModel.id)
            .limit(limit)
        )
        if saga_name is not None:
            statement = statement.where(SagaStateModel.name == saga_name)
        if stale_after_seconds is not None:
            threshold = datetime.now(UTC) - timedelta(seconds=stale_after_seconds)
            statement = statement.where(SagaStateModel.updated_at < threshold)
        return list((await self._session.execute(statement)).scalars())

    async def increment_recovery_attempts(
        self, saga_id: UUID, new_status: SagaStatus | None = None
    ) -> None:
        """Count one failed recovery, optionally moving the saga to a status."""
        values: dict[str, object] = {
            "recovery_attempts": SagaStateModel.recovery_attempts + 1,
            "version": SagaStateModel.version + 1,
        }
        if new_status is not None:
            values["status"] = new_status.value
        statement = (
            update(SagaStateModel)
            .where(SagaStateModel.id == saga_id)
            .values(**values)
            .returning(SagaStateModel.id)
        )
        if (await self._session.execute(statement)).scalar_one_or_none() is None:
            raise ValueError(f"saga {saga_id} not found")

    async def reset_for_retry(self, saga_id: UUID) -> None:
        """Clear the status and the recovery counter together."""
        statement = (
            update(SagaStateModel)
            .where(SagaStateModel.id == saga_id)
            .values(
                status=SagaStatus.RUNNING.value,
                recovery_attempts=0,
                version=SagaStateModel.version + 1,
            )
            .returning(SagaStateModel.id)
        )
        if (await self._session.execute(statement)).scalar_one_or_none() is None:
            raise ValueError(f"saga {saga_id} does not exist")

    async def set_recovery_attempts(self, saga_id: UUID, attempts: int) -> None:
        """Set the recovery counter to an explicit value."""
        statement = (
            update(SagaStateModel)
            .where(SagaStateModel.id == saga_id)
            .values(recovery_attempts=attempts, version=SagaStateModel.version + 1)
            .returning(SagaStateModel.id)
        )
        if (await self._session.execute(statement)).scalar_one_or_none() is None:
            raise ValueError(f"saga {saga_id} not found")


def _as_action(raw: str) -> Literal["act", "compensate"]:
    """Narrow the stored action; the log only ever holds the two literals."""
    return "compensate" if raw == "compensate" else "act"


class _SagaStorageRun:
    """The checkpointed run over one session.

    ``committer`` is what ``commit()`` delegates to. When the run is bound to a
    request's unit of work that is the unit of work, so the checkpoint travels
    with the step's effect and the events the aggregate raised. When it is not,
    the run owns its session and the committer is that session.

    Being bound changes one thing about ``commit()`` beyond *which* transaction it
    ends. The engine, on a step failure, logs the failure and then commits — it has
    no rollback on that path. With a session of its own that was harmless, because
    the step's half-finished work lived in a different transaction and vanished
    with it. Sharing the session, the same commit would *keep* half-finished work
    that the step never committed, so this run discards it instead: see
    :meth:`log_step` and :meth:`commit`.
    """

    def __init__(self, session: AsyncSession, committer: SagaCommitter) -> None:
        self._session = session
        self._committer = committer
        self._statements = _SagaStatements(session)
        self._failed_step = False

    async def create_saga(self, saga_id: UUID, name: str, context: dict[str, JsonValue]) -> None:
        """Stage a new saga execution."""
        await self._statements.create_saga(saga_id, name, context)

    async def update_context(
        self,
        saga_id: UUID,
        context: dict[str, JsonValue],
        current_version: int | None = None,
    ) -> None:
        """Stage a context update."""
        await self._statements.update_context(saga_id, context, current_version)

    async def update_status(self, saga_id: UUID, status: SagaStatus) -> None:
        """Stage a status change."""
        await self._statements.update_status(saga_id, status)

    async def log_step(
        self,
        saga_id: UUID,
        step_name: str,
        action: str,
        status: SagaStepStatus,
        details: str | None = None,
    ) -> None:
        """Stage a step transition, remembering that the step failed.

        A failed ``act`` means the step did not reach its own commit, so whatever
        it staged is still in this transaction and must not survive. The next
        :meth:`commit` — the engine's, taken to record the failure — discards it.
        """
        if action == "act" and status is SagaStepStatus.FAILED:
            self._failed_step = True
        await self._statements.log_step(saga_id, step_name, action, status, details)

    async def load_saga_state(
        self, saga_id: UUID, *, read_for_update: bool = False
    ) -> tuple[SagaStatus, dict[str, JsonValue], int]:
        """Load the saga's state within this run."""
        return await self._statements.load_saga_state(saga_id, read_for_update=read_for_update)

    async def get_step_history(self, saga_id: UUID) -> list[SagaLogEntry]:
        """Load the saga's step history within this run."""
        return await self._statements.get_step_history(saga_id)

    async def commit(self) -> None:
        """Make every checkpointed change durable, in the caller's transaction.

        Committing here is not a nested transaction and must not be attempted as
        one: the engine calls this at every checkpoint, and each call ends the
        transaction that carried the step's effect, its history entry, its
        checkpoint and its lifecycle outbox row. The next statement opens a new
        one — SQLAlchemy does that lazily — which is why a later step is again a
        single transaction of its own.

        The exception is the commit the engine takes after a step failed. There is
        nothing worth keeping in that transaction — the step never committed — so
        it is rolled back rather than committed. The saga's failure is still
        recorded: ``Saga._record_failure`` commits it afterwards, and the status it
        writes is the one the next reader and the recovery job act on.
        """
        if self._failed_step:
            self._failed_step = False
            await self._committer.rollback()
            return
        await self._committer.commit()

    async def rollback(self) -> None:
        """Discard the run's uncommitted changes."""
        self._failed_step = False
        await self._committer.rollback()


class SqlAlchemySagaStorage(ISagaStorage):
    """The ``ISagaStorage`` port over the project's saga tables.

    Two bindings, one class. Constructed with a session and a committer it is
    *request-bound*: every run it opens writes through the caller's unit of work,
    which is what makes a step and its checkpoint one transaction. Constructed with
    only a session factory it is *unbound*: each run opens and commits its own
    session, which is what the recovery job — which has no request and no unit of
    work — needs.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        session: AsyncSession | None = None,
        committer: SagaCommitter | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._session = session
        self._committer = committer

    @property
    def is_bound(self) -> bool:
        """Whether the runs this storage opens write through a caller's session."""
        return self._session is not None

    def create_run(self) -> contextlib.AbstractAsyncContextManager[SagaStorageRun]:
        """Open a scoped run with checkpointed commits.

        A bound storage yields a run over the caller's session and commits it
        through the caller's committer. An unbound one opens a session of its own
        and commits that. The run never rolls back a bound session on the way out:
        the request scope owns that decision, and a rollback here would discard
        work the caller has not yet had the chance to keep — a saga that failed
        still has to record its failure and compensation, which the base class does
        after the engine has unwound.
        """
        if self._session is not None and self._committer is not None:
            return self._bound_run()
        return self._unbound_run()

    @contextlib.asynccontextmanager
    async def _bound_run(self) -> AsyncIterator[SagaStorageRun]:
        """Yield a run over the request's session, committing the request's unit."""
        assert self._session is not None and self._committer is not None
        yield _SagaStorageRun(self._session, self._committer)

    @contextlib.asynccontextmanager
    async def _unbound_run(self) -> AsyncIterator[SagaStorageRun]:
        """Yield a run over a session of its own, rolling it back on failure."""
        async with self._session_factory() as session:
            run = _SagaStorageRun(session, session)
            try:
                yield run
            except BaseException:
                await run.rollback()
                raise

    async def create_saga(self, saga_id: UUID, name: str, context: dict[str, JsonValue]) -> None:
        """Insert a saga execution and commit it on its own."""
        async with self._session_factory() as session:
            await _SagaStatements(session).create_saga(saga_id, name, context)
            await session.commit()

    async def update_context(
        self,
        saga_id: UUID,
        context: dict[str, JsonValue],
        current_version: int | None = None,
    ) -> None:
        """Persist a context snapshot and commit it on its own."""
        async with self._session_factory() as session:
            await _SagaStatements(session).update_context(saga_id, context, current_version)
            await session.commit()

    async def update_status(self, saga_id: UUID, status: SagaStatus) -> None:
        """Persist a status change and commit it on its own."""
        async with self._session_factory() as session:
            await _SagaStatements(session).update_status(saga_id, status)
            await session.commit()

    async def log_step(
        self,
        saga_id: UUID,
        step_name: str,
        action: str,
        status: SagaStepStatus,
        details: str | None = None,
    ) -> None:
        """Append a step transition and commit it on its own."""
        async with self._session_factory() as session:
            await _SagaStatements(session).log_step(saga_id, step_name, action, status, details)
            await session.commit()

    async def load_saga_state(
        self, saga_id: UUID, *, read_for_update: bool = False
    ) -> tuple[SagaStatus, dict[str, JsonValue], int]:
        """Load the saga's state in a session of its own."""
        async with self._session_factory() as session:
            return await _SagaStatements(session).load_saga_state(
                saga_id, read_for_update=read_for_update
            )

    async def get_step_history(self, saga_id: UUID) -> list[SagaLogEntry]:
        """Load the saga's step history in a session of its own."""
        async with self._session_factory() as session:
            return await _SagaStatements(session).get_step_history(saga_id)

    async def get_sagas_for_recovery(
        self,
        limit: int,
        max_recovery_attempts: int = 5,
        stale_after_seconds: int | None = None,
        saga_name: str | None = None,
    ) -> list[UUID]:
        """Return unfinished sagas in a session of its own."""
        async with self._session_factory() as session:
            return await _SagaStatements(session).get_sagas_for_recovery(
                limit,
                max_recovery_attempts=max_recovery_attempts,
                stale_after_seconds=stale_after_seconds,
                saga_name=saga_name,
            )

    async def increment_recovery_attempts(
        self, saga_id: UUID, new_status: SagaStatus | None = None
    ) -> None:
        """Count a failed recovery in a session of its own."""
        async with self._session_factory() as session:
            await _SagaStatements(session).increment_recovery_attempts(saga_id, new_status)
            await session.commit()

    async def set_recovery_attempts(self, saga_id: UUID, attempts: int) -> None:
        """Set the recovery counter in a session of its own."""
        async with self._session_factory() as session:
            await _SagaStatements(session).set_recovery_attempts(saga_id, attempts)
            await session.commit()

    async def reset_for_retry(self, saga_id: UUID) -> None:
        """Clear a parked saga so the recovery job runs it again.

        An operator's only lever on a parked process, and both halves matter: the
        counter is zeroed — otherwise the next attempt would park the saga again
        immediately — and the status is cleared, because the engine refuses to run a
        ``failed`` saga forward.
        """
        async with self._session_factory() as session:
            await _SagaStatements(session).reset_for_retry(saga_id)
            await session.commit()
