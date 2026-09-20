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
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Literal
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

RECOVERABLE_STATUSES = (SagaStatus.RUNNING.value, SagaStatus.COMPENSATING.value)
"""The two statuses a crash can leave behind; everything else is terminal."""


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
        """Return unfinished sagas, least recently updated first."""
        statement = (
            select(SagaStateModel.id)
            .where(
                SagaStateModel.status.in_(RECOVERABLE_STATUSES),
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
    """The checkpointed run: one session, committed by the caller."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._statements = _SagaStatements(session)

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
        """Stage a step transition."""
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
        """Make every checkpointed change durable."""
        await self._session.commit()

    async def rollback(self) -> None:
        """Discard the run's uncommitted changes."""
        await self._session.rollback()


class SqlAlchemySagaStorage(ISagaStorage):
    """The ``ISagaStorage`` port over the project's saga tables."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    def create_run(self) -> contextlib.AbstractAsyncContextManager[SagaStorageRun]:
        """Open a scoped run with checkpointed commits."""

        @contextlib.asynccontextmanager
        async def _run() -> AsyncIterator[SagaStorageRun]:
            async with self._session_factory() as session:
                run = _SagaStorageRun(session)
                try:
                    yield run
                except BaseException:
                    await run.rollback()
                    raise

        return _run()

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
