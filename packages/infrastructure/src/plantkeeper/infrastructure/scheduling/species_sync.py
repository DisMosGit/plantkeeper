"""The daily catalogue synchronisation trigger.

The manual path already exists: ``POST /api/v1/catalog/sync`` appends
``SpeciesSyncRequested`` to the outbox. This scheduler is the other trigger the
roadmap asks for, and it deliberately publishes *the same event* through *the same
port* rather than calling the saga — so "cron" and "manual" cannot drift into two
different synchronisations.

It waits one interval before the first tick: a cron does not fire at start-up, and
a fresh process replaying yesterday's catalogue on every restart would be noise.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging

from dishka import AsyncContainer

from plantkeeper.application.ports.clock import Clock
from plantkeeper.application.sagas.species_sync import SpeciesSyncTrigger
from plantkeeper.infrastructure.config import Settings

logger = logging.getLogger(__name__)


class SpeciesSyncScheduler:
    """Publishes the catalogue-sync trigger every interval until stopped."""

    def __init__(
        self,
        *,
        container: AsyncContainer,
        settings: Settings,
        interval_seconds: float | None = None,
    ) -> None:
        self._container = container
        self._interval_seconds = (
            interval_seconds
            if interval_seconds is not None
            else settings.species_sync_interval_seconds
        )
        self._stopped = asyncio.Event()

    def stop(self) -> None:
        """Ask the loop to stop after the wait it is in."""
        self._stopped.set()

    async def run(self) -> None:
        """Wait, publish, repeat — until stopped."""
        while not self._stopped.is_set():
            await self._wait()
            if self._stopped.is_set():
                return
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("catalogue sync trigger failed; retrying next interval")

    async def run_once(self) -> None:
        """Publish one ``SpeciesSyncRequested`` through the outbox."""
        async with self._container() as request_container:
            trigger = await request_container.get(SpeciesSyncTrigger)
            clock = await request_container.get(Clock)
            await trigger.request_sync(now=clock.now())
            logger.info("daily catalogue synchronisation requested")

    async def _wait(self) -> None:
        """Sleep until the interval elapses or :meth:`stop` is called."""
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stopped.wait(), timeout=self._interval_seconds)
