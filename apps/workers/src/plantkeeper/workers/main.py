"""The worker process: the outbox relay, the saga consumers and their timers.

The API writes events to the outbox and never talks to Kafka. This process is the
other half of that split:

* the **relay** publishes the outbox;
* the **saga consumers** subscribe to the topics the relay fills and run the four
  sagas (``docs/sagas.md``);
* three **background jobs** complete what an event cannot express — the 24-hour
  missed-care tick, the daily catalogue-sync trigger, and recovery of sagas a crash
  left unfinished.

They run as one ``asyncio.gather`` and are stopped in the reverse order they were
started: every job finishes its current tick, then the broker closes.
"""

from __future__ import annotations

import asyncio
import logging
import signal

from cqrs.saga.storage.protocol import ISagaStorage
from dishka import AsyncContainer, make_async_container
from faststream.kafka import KafkaBroker

from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.di.providers import worker_providers
from plantkeeper.infrastructure.messaging.relay import OutboxRelay
from plantkeeper.infrastructure.scheduling.job import BackgroundJob
from plantkeeper.infrastructure.scheduling.missed_care import MissedCareScheduler
from plantkeeper.infrastructure.scheduling.saga_recovery import SagaRecoveryJob
from plantkeeper.infrastructure.scheduling.species_sync import SpeciesSyncScheduler
from plantkeeper.workers.consumers import register_consumers

logger = logging.getLogger(__name__)


def configure_logging() -> None:
    """Configure the worker's own logging.

    The project has no logging framework yet (``CONTRIBUTING.md`` points at
    structlog for a later phase); until then one call to the standard library is
    enough to make the relay's and the sagas' warnings visible.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )


def _request_shutdown(relay: OutboxRelay, jobs: list[BackgroundJob]) -> None:
    """Flag every loop to stop after the step it is in.

    The signal handler deliberately does not cancel anything: a relay stopped
    mid-batch would either lose a message or republish one, and a scheduler
    cancelled between a write and its commit would do the same. Waiting one tick is
    always cheaper than reconciling the result.
    """
    logger.info("shutdown requested, finishing the current step")
    relay.stop()
    for job in jobs:
        job.stop()


def build_background_jobs(
    *,
    container: AsyncContainer,
    settings: Settings,
    storage: ISagaStorage,
) -> list[BackgroundJob]:
    """Build the jobs that run next to the relay.

    Built here rather than provided by Dishka because each one opens request
    scopes of its own; a job sharing the worker's container would keep one session
    open for the lifetime of the process.
    """
    return [
        MissedCareScheduler(container=container, settings=settings),
        SpeciesSyncScheduler(container=container, settings=settings),
        SagaRecoveryJob(container=container, storage=storage, settings=settings),
    ]


async def run(container: AsyncContainer) -> None:
    """Start the broker and the jobs, run until stopped, then stop both."""
    broker = await container.get(KafkaBroker)
    relay = await container.get(OutboxRelay)
    settings = await container.get(Settings)
    storage = await container.get(ISagaStorage)
    jobs = build_background_jobs(container=container, settings=settings, storage=storage)

    # Routes must exist before the broker starts: FastStream refuses to add them
    # to a running broker.
    register_consumers(broker, container=container, settings=settings)

    loop = asyncio.get_running_loop()
    for received in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(received, _request_shutdown, relay, jobs)

    await broker.start()
    logger.info("worker started: outbox relay, saga consumers, %d background job(s)", len(jobs))
    try:
        await asyncio.gather(relay.run(), *(job.run() for job in jobs))
    finally:
        relay.stop()
        for job in jobs:
            job.stop()
        await broker.stop()
        logger.info("worker stopped")


async def main_async() -> None:
    """Build the container, run the worker, and close the container.

    The container is closed on the same loop that created it: the engine's pool,
    the broker's producer and the saga storage's sessions are loop-bound, so
    closing them from a fresh loop would only produce errors on the way out.
    """
    container = make_async_container(*worker_providers())
    try:
        await run(container)
    finally:
        await container.close()


def main() -> None:
    """Entry point for ``python -m plantkeeper.workers``."""
    configure_logging()
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        # Only reached if the signal handler could not be installed (Windows).
        logger.info("interrupted")
