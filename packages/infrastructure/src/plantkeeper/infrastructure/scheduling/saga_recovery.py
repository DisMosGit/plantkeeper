"""Recovery of sagas a crash left unfinished.

The saga engine already knows how to resume: it skips the steps whose transitions
are in ``saga_log`` and finishes compensation when the saga was interrupted while
rolling back. What it does not ship is a scheduler, so this job polls the
``running``/``compensating`` rows a crash leaves behind and hands each one to
``cqrs.saga.recovery.recover_saga``.

Two details keep it safe:

* only sagas untouched for ``saga_recovery_stale_after_seconds`` are picked up, so
  a step that is merely slow is never raced by its own recovery;
* the query is per saga name, because the storage returns identifiers only and the
  class — and therefore the context — has to come from the registry.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from uuid import UUID

from cqrs.saga.recovery import recover_saga
from cqrs.saga.storage.protocol import ISagaStorage
from dishka import AsyncContainer

from plantkeeper.application.sagas.base import Saga
from plantkeeper.application.sagas.registry import SAGA_TYPES
from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.di.cqrs import DishkaCQRSContainer

logger = logging.getLogger(__name__)

RECOVERY_BATCH_SIZE = 50
"""How many sagas of one type are picked up per tick."""


class SagaRecoveryJob:
    """Resumes crashed sagas until stopped."""

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
        """Recover every eligible saga, returning how many succeeded."""
        recovered = 0
        for saga_type in SAGA_TYPES:
            saga_ids = await self._storage.get_sagas_for_recovery(
                RECOVERY_BATCH_SIZE,
                max_recovery_attempts=self._settings.saga_recovery_max_attempts,
                stale_after_seconds=int(self._settings.saga_recovery_stale_after_seconds),
                saga_name=saga_type.__name__,
            )
            for saga_id in saga_ids:
                if await self._recover_one(saga_type, saga_id):
                    recovered += 1
        return recovered

    async def _recover_one(self, saga_type: type[Saga], saga_id: UUID) -> bool:
        """Resume one saga in a request scope of its own, tolerating terminal states."""
        async with self._container() as request_container:
            saga = await request_container.get(saga_type)
            try:
                await recover_saga(
                    saga,
                    saga_id,
                    saga_type.context_type,
                    DishkaCQRSContainer(request_container),
                    self._storage,
                )
            except RuntimeError:
                # Raised when a saga was recovered mid-compensation: the rollback
                # finished, which is the outcome recovery wanted, and forward
                # execution is refused on purpose.
                logger.warning("saga %s finished compensation during recovery", saga_id)
                return False
            except Exception:
                logger.exception("saga %s could not be recovered", saga_id)
                return False
        return True

    async def _wait(self) -> None:
        """Sleep until the interval elapses or :meth:`stop` is called."""
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stopped.wait(), timeout=self._interval_seconds)
