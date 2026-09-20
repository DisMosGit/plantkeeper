"""The background jobs' stop contract.

``workers/main.py`` runs the jobs as tasks and stops them with a signal. What
matters there is that ``stop`` really ends the loop: a job that kept running would
hold the process open, and one cancelled mid-tick could abandon a transaction. The
jobs are given a container that fails on use, so the tests exercise the loop and
its shutdown path without a database or a broker.
"""

from __future__ import annotations

import asyncio
from typing import cast
from uuid import UUID, uuid4

import pytest
from cqrs.saga.storage.protocol import ISagaStorage
from dishka import AsyncContainer

from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.scheduling.job import BackgroundJob
from plantkeeper.infrastructure.scheduling.missed_care import MissedCareScheduler
from plantkeeper.infrastructure.scheduling.saga_recovery import SagaRecoveryJob
from plantkeeper.infrastructure.scheduling.species_sync import SpeciesSyncScheduler

TICK = 0.01


class BrokenContainer:
    """Fails when a request scope is opened, which is exactly what a tick does.

    The jobs log a failed tick and try again, so this drives the loop down the
    failure path as well as the normal one.
    """

    def __call__(self) -> object:
        """Refuse to open a scope."""
        raise RuntimeError("no scope for you")


class BrokerlessStorage:
    """A saga storage reporting one unfinished saga, with no queries behind it."""

    async def get_sagas_for_recovery(
        self,
        limit: int,
        max_recovery_attempts: int = 5,
        stale_after_seconds: int | None = None,
        saga_name: str | None = None,
    ) -> list[UUID]:
        """Return one saga id, so the recovery path actually tries to run."""
        return [uuid4()]


def build_jobs() -> list[BackgroundJob]:
    """One of each job, all pointed at the failing container."""
    container = cast("AsyncContainer", BrokenContainer())
    settings = Settings()
    return [
        MissedCareScheduler(container=container, settings=settings, interval_seconds=TICK),
        SpeciesSyncScheduler(container=container, settings=settings, interval_seconds=TICK),
        SagaRecoveryJob(
            container=container,
            storage=cast("ISagaStorage", BrokerlessStorage()),
            settings=settings,
            interval_seconds=TICK,
        ),
    ]


@pytest.mark.parametrize("index", [0, 1, 2], ids=["missed-care", "species-sync", "saga-recovery"])
async def test_a_job_returns_once_it_is_stopped(index: int) -> None:
    job = build_jobs()[index]
    task = asyncio.create_task(job.run())
    await asyncio.sleep(TICK * 3)

    job.stop()

    await asyncio.wait_for(task, timeout=1.0)


async def test_stopping_before_the_first_tick_returns_immediately() -> None:
    """A shutdown during start-up must not wait a whole interval."""
    job = MissedCareScheduler(
        container=cast("AsyncContainer", BrokenContainer()),
        settings=Settings(),
        interval_seconds=3600.0,
    )
    job.stop()

    await asyncio.wait_for(job.run(), timeout=1.0)
