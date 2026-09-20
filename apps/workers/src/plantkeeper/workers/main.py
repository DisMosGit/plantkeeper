"""The outbox relay worker.

The API writes events to the outbox; nothing in the API process talks to Kafka.
This process is the other half of that split: it starts the broker, runs the
relay until it is asked to stop, and shuts both down in that order.
"""

from __future__ import annotations

import asyncio
import logging
import signal

from dishka import AsyncContainer, make_async_container
from faststream.kafka import KafkaBroker

from plantkeeper.infrastructure.di.providers import worker_providers
from plantkeeper.infrastructure.messaging.relay import OutboxRelay

logger = logging.getLogger(__name__)


def configure_logging() -> None:
    """Configure the worker's own logging.

    The project has no logging framework yet (``CONTRIBUTING.md`` points at
    structlog for a later phase); until then one call to the standard library is
    enough to make the relay's warnings visible.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )


def _request_shutdown(relay: OutboxRelay) -> None:
    """Flag the relay to stop after the poll it is in.

    The signal handler deliberately does not cancel anything: a relay stopped
    mid-batch would either lose a message or republish one, and both are worse
    than waiting for the current poll to finish.
    """
    logger.info("shutdown requested, finishing the current poll")
    relay.stop()


async def run(container: AsyncContainer) -> None:
    """Start the broker, run the relay until stopped, then stop both."""
    broker = await container.get(KafkaBroker)
    relay = await container.get(OutboxRelay)

    loop = asyncio.get_running_loop()
    for received in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(received, _request_shutdown, relay)

    await broker.start()
    logger.info("outbox relay started")
    try:
        await relay.run()
    finally:
        relay.stop()
        await broker.stop()
        logger.info("outbox relay stopped")


async def main_async() -> None:
    """Build the container, run the worker, and close the container.

    The container is closed on the same loop that created it: the engine's pool
    and the broker's producer are loop-bound, so closing them from a fresh loop
    would only produce errors on the way out.
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
