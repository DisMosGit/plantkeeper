"""The partition window of ``write_telemetry.sensor_readings``.

A partitioned table only accepts a row whose partition exists. Alembic ``0003``
creates the parent and the catch-all partition, and the telemetry ingress creates
nothing: this job keeps the monthly window ahead of the clock so that a reading
always lands in its own month rather than in the default.

The same window has a far end. Raw readings are kept for a documented retention
window (``Settings.telemetry_retention_months``, ``docs/telemetry.md``), and this job
drops the partition of every month that lies entirely beyond it. That is the point of
partitioning by month — retiring a month is one ``DROP TABLE`` rather than a
``DELETE`` of millions of rows — and the rollups computed from those readings are on
the read instance, so they outlive the rows.

The catch-all partition is the reason the schedule is not load-bearing: if this job
is stopped for a month, readings still arrive, they simply share one partition until
it runs again. That is a missing optimisation, never a failed write — which is why a
failed tick here is logged and retried instead of stopping the worker.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging

from dishka import AsyncContainer
from sqlalchemy.ext.asyncio import AsyncSession

from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.persistence.partitions import (
    drop_expired_partitions,
    ensure_telemetry_partitions,
)

logger = logging.getLogger(__name__)


class TelemetryPartitionJob:
    """Keeps the near future partitioned and retires the far past."""

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
            else settings.telemetry_partition_check_interval_seconds
        )
        self._months_ahead = settings.telemetry_partition_months_ahead
        self._retention_months = settings.telemetry_retention_months
        self._stopped = asyncio.Event()

    def stop(self) -> None:
        """Ask the loop to stop after the tick it is in."""
        self._stopped.set()

    async def run(self) -> None:
        """Tick once, then every interval, until stopped."""
        while not self._stopped.is_set():
            try:
                created = await self.run_once()
                if created:
                    logger.info("created %d telemetry partition(s): %s", len(created), created)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("telemetry partition tick failed; retrying next interval")
            await self._wait()

    async def run_once(self) -> list[str]:
        """One tick: create every missing partition, drop every expired one.

        Returns the names created; the dropped ones are logged, because a drop is
        destructive and belongs in the operator's log rather than in a return value
        the caller ignores. Public so a test can drive it without waiting for the
        interval.
        """
        async with self._container() as request_container:
            session = await request_container.get(AsyncSession)
            created = await ensure_telemetry_partitions(session, months_ahead=self._months_ahead)
            dropped = await drop_expired_partitions(
                session, retention_months=self._retention_months
            )
            await session.commit()
        if dropped:
            logger.info("dropped %d expired telemetry partition(s): %s", len(dropped), dropped)
        return created

    async def _wait(self) -> None:
        """Sleep until the interval elapses or :meth:`stop` is called."""
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stopped.wait(), timeout=self._interval_seconds)
