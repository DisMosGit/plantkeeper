"""The missed-care tick.

``MissedCareSaga`` is a choreography consumer: it reacts to ``WateringDue`` and
``WateringCompleted`` as they arrive. The other half of it is a timer — nothing
tells the saga that 24 hours have passed — so this job polls, in a request scope of
its own per tick, and asks the saga to escalate.

The interval is deliberately short compared to the grace period: the grace period
is a business rule (24 hours), while the tick is an operational knob, and a tick
that is late only delays the escalation, it never misses it.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging

from dishka import AsyncContainer

from plantkeeper.application.ports.clock import Clock
from plantkeeper.application.sagas.missed_care import MissedCareSaga
from plantkeeper.infrastructure.config import Settings

logger = logging.getLogger(__name__)


class MissedCareScheduler:
    """Runs one escalation tick per interval until stopped."""

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
            else settings.missed_care_check_interval_seconds
        )
        self._stopped = asyncio.Event()

    def stop(self) -> None:
        """Ask the loop to stop after the tick it is in."""
        self._stopped.set()

    async def run(self) -> None:
        """Tick every interval until stopped; a failed tick is logged, not fatal."""
        while not self._stopped.is_set():
            try:
                missed = await self.run_once()
                if missed:
                    logger.info("missed-care tick escalated %d deadline(s)", missed)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("missed-care tick failed; retrying next interval")
            await self._wait()

    async def run_once(self) -> int:
        """One tick: record what came due and escalate what expired.

        Public so a test can drive the timer with a clock it controls instead of
        waiting for the interval.
        """
        async with self._container() as request_container:
            saga = await request_container.get(MissedCareSaga)
            clock = await request_container.get(Clock)
            return await saga.escalate_overdue(now=clock.now())

    async def _wait(self) -> None:
        """Sleep until the interval elapses or :meth:`stop` is called."""
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stopped.wait(), timeout=self._interval_seconds)
