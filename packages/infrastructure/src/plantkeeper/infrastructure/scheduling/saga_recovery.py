"""Recovery of sagas a crash left unfinished, and retry of the ones that failed.

Two different questions, one job, because both are "the process is not where it
should be" and both are answered from ``write_shared.saga_state``:

* a process stuck in ``running``/``compensating`` **crashed** — it may simply have
  stopped mid-step, so it is resumed from its step history;
* a process in ``failed`` **recorded a failure** — the cause may have been
  transient, so it is retried, up to a bounded budget. Past the budget it is parked
  for an operator, who can reset it.

The engine already knows how to resume: it skips the steps whose transitions are in
``saga_log`` and finishes compensation when the saga was interrupted while rolling
back. What it does not ship is a scheduler — or a way to run a ``failed`` saga
forward at all, which is why the retry path clears the status first.

Three details keep it safe:

* only sagas untouched for ``saga_recovery_stale_after_seconds`` are picked up, so
  a step that is merely slow is never raced by its own recovery;
* the query is per saga name, because the storage returns identifiers only and the
  class — and therefore the context — has to come from the registry;
* the budget is the engine's own ``recovery_attempts`` counter, so a crash-loop and
  a failure-loop spend the same allowance and a poison process cannot retry
  forever.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from uuid import UUID

from cqrs.saga.recovery import recover_saga
from cqrs.saga.storage.enums import SagaStatus
from cqrs.saga.storage.protocol import ISagaStorage
from dishka import AsyncContainer

from plantkeeper.application.ports.unit_of_work import UnitOfWork
from plantkeeper.application.sagas.base import Saga
from plantkeeper.application.sagas.registry import SAGA_TYPES
from plantkeeper.domain.saga.events import SagaParked, SagaRetrying
from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.di.cqrs import DishkaCQRSContainer

logger = logging.getLogger(__name__)

PARK_MARGIN = 1
"""The extra attempt the recovery query is asked for, so a spent budget is seen.

``get_sagas_for_recovery`` returns processes whose ``recovery_attempts`` is
*strictly* below the limit it is given, and the limit used to be the budget itself.
A process that had just spent its budget was therefore never returned again, so
``_park`` was unreachable and no process was ever parked — the budget quietly
became a mute exclusion instead of a decision an operator is told about.

Asking for one attempt more than the budget makes a spent budget *reachable*, and
:meth:`_park` spends that extra attempt so the parked process is not selected
again. An operator's reset drops the counter back to zero, which is what makes it
runnable once more.
"""

RECOVERY_BATCH_SIZE = 50
"""How many sagas of one type are picked up per tick."""

RETRYABLE_MARKER = "recovered in"
"""How the engine phrases "this saga is failed; forward execution is refused".

Matched rather than imported because the library raises a bare ``RuntimeError`` for
both this case and a genuine recovery failure, and the two need opposite handling:
one is the expected end of a compensation, the other is a failure worth counting.
"""


class SagaRecoveryJob:
    """Resumes crashed sagas and retries failed ones, until stopped."""

    def __init__(
        self,
        *,
        container: AsyncContainer,
        storage: ISagaStorage,
        settings: Settings,
        interval_seconds: float | None = None,
    ) -> None:
        self._container = container
        self._storage = storage
        self._settings = settings
        self._interval_seconds = (
            interval_seconds
            if interval_seconds is not None
            else settings.saga_recovery_interval_seconds
        )
        self._stopped = asyncio.Event()

    def stop(self) -> None:
        """Ask the loop to stop after the tick it is in."""
        self._stopped.set()

    async def run(self) -> None:
        """Recover, wait, repeat — until stopped."""
        while not self._stopped.is_set():
            try:
                recovered = await self.run_once()
                if recovered:
                    logger.info("recovered %d saga(s)", recovered)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("saga recovery tick failed; retrying next interval")
            await self._wait()

    async def run_once(self) -> int:
        """Recover every eligible saga, returning how many got further."""
        recovered = 0
        for saga_type in SAGA_TYPES:
            saga_ids = await self._storage.get_sagas_for_recovery(
                RECOVERY_BATCH_SIZE,
                max_recovery_attempts=self._settings.saga_recovery_max_attempts + PARK_MARGIN,
                stale_after_seconds=int(self._settings.saga_recovery_stale_after_seconds),
                saga_name=saga_type.__name__,
            )
            for saga_id in saga_ids:
                if await self._recover_one(saga_type, saga_id):
                    recovered += 1
        return recovered

    async def _recover_one(self, saga_type: type[Saga], saga_id: UUID) -> bool:
        """Resume or retry one saga in a request scope of its own."""
        async with self._container() as request_container:
            saga = await request_container.get(saga_type)
            if not await self._prepare_retry(request_container, saga_type, saga_id):
                # The budget was spent and the process is parked: this tick must
                # not run it, or the park would mean nothing.
                return False
            try:
                await recover_saga(
                    saga,
                    saga_id,
                    saga_type.context_type,
                    DishkaCQRSContainer(request_container),
                    self._storage,
                )
            except RuntimeError as error:
                if RETRYABLE_MARKER in str(error):
                    # The rollback finished, which is the outcome recovery wanted,
                    # and forward execution is refused on purpose.
                    logger.warning("saga %s finished compensation during recovery", saga_id)
                    return False
                logger.exception("saga %s could not be recovered", saga_id)
                return False
            except Exception:
                logger.exception("saga %s could not be recovered", saga_id)
                return False
            return True

    async def _prepare_retry(
        self, request_container: AsyncContainer, saga_type: type[Saga], saga_id: UUID
    ) -> bool:
        """Make a failed saga runnable again, or park it if its budget is spent.

        ``False`` means the caller must not run the process: its budget is spent
        and it has just been parked. ``True`` covers both a prepared retry and a
        crash-resume, neither of which is the caller's business to distinguish —
        the step history decides what actually runs.
        """
        try:
            status, _, _ = await self._storage.load_saga_state(saga_id)
        except ValueError:
            return True
        if status is not SagaStatus.FAILED:
            return True

        unit_of_work = await request_container.get(UnitOfWork)
        state = await unit_of_work.saga_states.get(saga_id)
        attempts = state.recovery_attempts if state is not None else 0
        if attempts >= self._settings.saga_recovery_max_attempts:
            await self._park(request_container, saga_type, saga_id, attempt=attempts)
            return False

        await self._announce_retry(
            request_container,
            saga_type,
            saga_id,
            attempt=attempts,
            error="the recorded failure is being retried",
        )
        # The engine refuses to run a ``failed`` saga forward, so the status is
        # cleared for this attempt. The budget still bounds how often that happens.
        await self._storage.update_status(saga_id, SagaStatus.RUNNING)
        return True

    async def _park(
        self,
        request_container: AsyncContainer,
        saga_type: type[Saga],
        saga_id: UUID,
        *,
        attempt: int,
    ) -> None:
        """Stop retrying a process whose budget is spent, and say so on the stream.

        The counter is pushed past the query's limit in the *same* transaction as
        the announcement, so the parking decision and the record of it cannot come
        apart: a crash before the commit leaves the process still selectable, and
        the next tick parks it instead of leaving it unattended.
        """
        unit_of_work = await request_container.get(UnitOfWork)
        storage = await request_container.get(ISagaStorage)
        await storage.set_recovery_attempts(saga_id, attempt + PARK_MARGIN)
        await unit_of_work.outbox.append(
            SagaParked(
                saga_id=saga_id,
                saga_name=saga_type.__name__,
                attempts=attempt,
                error=f"recovery budget of {self._settings.saga_recovery_max_attempts} spent",
            )
        )
        await unit_of_work.commit()
        logger.error(
            "saga %s parked after %d recovery attempts; an operator has to reset it",
            saga_id,
            attempt,
        )

    async def _announce_retry(
        self,
        request_container: AsyncContainer,
        saga_type: type[Saga],
        saga_id: UUID,
        *,
        attempt: int,
        error: str,
    ) -> None:
        """Publish that a recorded failure is being retried, and why."""
        unit_of_work = await request_container.get(UnitOfWork)
        await unit_of_work.outbox.append(
            SagaRetrying(
                saga_id=saga_id,
                saga_name=saga_type.__name__,
                attempt=attempt + 1,
                error=error,
            )
        )
        await unit_of_work.commit()

    async def _wait(self) -> None:
        """Sleep until the interval elapses or :meth:`stop` is called."""
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stopped.wait(), timeout=self._interval_seconds)
