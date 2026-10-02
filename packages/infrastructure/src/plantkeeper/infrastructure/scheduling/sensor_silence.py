"""The silence timer: announce a sensor that has stopped reporting.

``SensorOffline`` has been in the catalogue since Phase 1 and could never fire,
because nothing inspected silence: the sensor registry did not persist when a sensor
last reported, and no job ever asked. Both halves now exist — the telemetry ingress
advances ``last_seen_at`` with each stored reading and clears the announcement — and
this job is the other half: it polls the registry for sensors silent past
:data:`~plantkeeper.domain.telemetry.sensor.OFFLINE_AFTER` and asks each one to
announce itself.

The aggregate decides. :meth:`Sensor.mark_offline` refuses a sensor that is still
reporting, refuses one that never reported, and refuses one whose current silence has
already been announced, so "at most once per silence" is a domain rule rather than a
property of this loop's query. The job only narrows the candidates; a duplicate tick
finds nothing to do, and a sensor that reports again and then goes quiet is a new
silence with a new announcement.

Each tick runs in a request scope of its own, exactly like the missed-care tick: the
events go to the outbox and commit in the same transaction as the sensor row that
records the announcement, so a crash cannot leave a sensor looking unannounced when
its ``SensorOffline`` is already on its way.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging

from dishka import AsyncContainer

from plantkeeper.application.ports.clock import Clock
from plantkeeper.application.ports.unit_of_work import UnitOfWork
from plantkeeper.domain.telemetry.sensor import OFFLINE_AFTER
from plantkeeper.infrastructure.config import Settings

logger = logging.getLogger(__name__)


class SensorSilenceJob:
    """Publishes ``SensorOffline`` for every sensor silent past the threshold."""

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
            else settings.sensor_silence_check_interval_seconds
        )
        self._stopped = asyncio.Event()

    def stop(self) -> None:
        """Ask the loop to stop after the tick it is in."""
        self._stopped.set()

    async def run(self) -> None:
        """Tick once, then every interval, until stopped."""
        while not self._stopped.is_set():
            try:
                announced = await self.run_once()
                if announced:
                    logger.info("announced %d silent sensor(s) offline", announced)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("sensor silence tick failed; retrying next interval")
            await self._wait()

    async def run_once(self) -> int:
        """One tick: announce every sensor that has gone quiet.

        Returns how many were announced, which is what a test asserts and what the
        log line reports. Public so the timer can be driven with a clock the test
        controls instead of waiting ten minutes for a real silence.
        """
        async with self._container() as request_container:
            unit_of_work = await request_container.get(UnitOfWork)
            clock = await request_container.get(Clock)
            now = clock.now()
            threshold = now - OFFLINE_AFTER
            async with unit_of_work:
                sensors = await unit_of_work.sensors.list_silent(threshold)
                for sensor in sensors:
                    sensor.mark_offline(now=now)
                    for event in sensor.collect_events():
                        await unit_of_work.outbox.append(event)
                    await unit_of_work.sensors.save(sensor)
                if sensors:
                    await unit_of_work.commit()
            return len(sensors)

    async def _wait(self) -> None:
        """Sleep until the interval elapses or :meth:`stop` is called."""
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stopped.wait(), timeout=self._interval_seconds)
