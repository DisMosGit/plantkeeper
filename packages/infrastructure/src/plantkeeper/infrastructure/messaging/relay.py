"""The outbox relay: the only component that talks to Kafka on the write side.

The relay polls the outbox with its **own** session, never with a request's
session: reading rows the write side has not committed yet would defeat the
atomicity the outbox exists for.

Failure handling has three layers, and they are deliberately different:

* ``tenacity`` smooths a transient broker blip inside one poll;
* the ``attempts`` column counts *failed polls*, so a process restart or a long
  broker outage does not lose the failure count;
* after ``outbox_max_attempts`` the message is copied to the dead-letter topic
  and the row is marked, which stops the relay retrying it forever. If the copy
  itself fails the row is counted as a failure instead, so a message is never
  dropped silently.

Delivery is at-least-once. A crash between Kafka accepting a message and the
``published_at`` update republishes it on the next poll; consumers deduplicate on
``(consumer_group, event_id)``, which is the contract ``AGENTS.md`` requires.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from tenacity import AsyncRetrying, stop_after_attempt, wait_exponential

from plantkeeper.application.ports.event_publisher import EventPublisher
from plantkeeper.application.ports.outbox import OutboxMessage, OutboxRepository
from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.persistence.repositories.outbox import (
    SqlAlchemyOutboxRepository,
)

logger = logging.getLogger(__name__)

PUBLISH_ATTEMPTS_PER_CYCLE = 3
"""How many times tenacity retries one message inside a single poll."""

MAX_BACKOFF_MULTIPLIER = 8
"""Ceiling on the exponential poll delay, in multiples of the poll interval."""

Committer = Callable[[], Awaitable[None]]
"""Commits the effect of one delivery; the relay's only transactional seam."""


class OutboxRelay:
    """Publishes unpublished outbox rows to Kafka, forever."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        publisher: EventPublisher,
        settings: Settings,
    ) -> None:
        self._session_factory = session_factory
        self._publisher = publisher
        self._settings = settings
        self._stopped = asyncio.Event()
        self._consecutive_failures = 0

    def stop(self) -> None:
        """Ask :meth:`run` to return after the current poll."""
        self._stopped.set()

    async def run(self) -> None:
        """Poll, publish, wait, until :meth:`stop` is called."""
        while not self._stopped.is_set():
            try:
                failed = await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                # One bad poll must not kill the relay.
                self._consecutive_failures += 1
                logger.exception("outbox poll failed; retrying")
            else:
                self._consecutive_failures = self._consecutive_failures + 1 if failed else 0
            await self._wait()

    async def run_once(self) -> int:
        """Publish one batch and return how many messages could not be published.

        Each message is committed on its own: in a shared batch, one poison
        message would either hold back the messages after it or make them be
        re-published after a restart.
        """
        failed = 0
        async with self._session_factory() as session:
            repository = SqlAlchemyOutboxRepository(session)
            messages = await repository.fetch_unpublished(self._settings.outbox_batch_size)
            for message in messages:
                if await self.deliver(repository, message, session.commit):
                    failed += 1
        return failed

    async def deliver(
        self,
        repository: OutboxRepository,
        message: OutboxMessage,
        commit: Committer,
    ) -> bool:
        """Publish one message, record the outcome, commit, and say if it failed.

        Public rather than private because this is where the retry and
        dead-letter policy lives, and a unit test should be able to drive it
        without a database.
        """
        failed = True
        try:
            await self._publish_with_retry(message)
        except Exception as exc:  # any broker error is a failed attempt
            await self._record_failure(repository, message, exc)
        else:
            failed = False
            await repository.mark_published(message.outbox_id)
            logger.debug("published outbox message %s to %s", message.event_id, message.topic)
        await commit()
        return failed

    async def _publish_with_retry(self, message: OutboxMessage) -> None:
        """Publish, retrying a transient broker error a few times."""
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(PUBLISH_ATTEMPTS_PER_CYCLE),
            wait=wait_exponential(
                multiplier=self._settings.outbox_retry_initial_wait_seconds,
                max=self._settings.outbox_retry_max_wait_seconds,
            ),
            reraise=True,
        ):
            with attempt:
                await self._publisher.publish(message)

    async def _record_failure(
        self,
        repository: OutboxRepository,
        message: OutboxMessage,
        exc: Exception,
    ) -> None:
        """Count a failed poll, or dead-letter the message if this was the last."""
        error = f"{type(exc).__name__}: {exc}"
        attempt = message.attempts + 1
        if attempt < self._settings.outbox_max_attempts:
            await repository.record_failure(message.outbox_id, error)
            logger.warning(
                "outbox message %s failed attempt %d/%d: %s",
                message.event_id,
                attempt,
                self._settings.outbox_max_attempts,
                error,
            )
            return

        try:
            await self._publisher.publish_dead_letter(message, error=error)
        except Exception as dlq_exc:  # keep the row for another try
            combined = f"{error}; dead-letter publish failed: {type(dlq_exc).__name__}: {dlq_exc}"
            await repository.record_failure(message.outbox_id, combined)
            logger.error(
                "outbox message %s could not be dead-lettered: %s", message.event_id, combined
            )
            return

        await repository.dead_letter(message.outbox_id, error)
        logger.error("outbox message %s dead-lettered after %d attempts", message.event_id, attempt)

    async def _wait(self) -> None:
        """Sleep between polls, backing off while publishing keeps failing.

        Waiting on the stop event rather than on ``asyncio.sleep`` means a
        shutdown request is honoured immediately instead of after the delay.
        """
        interval = self._settings.outbox_poll_interval_seconds
        delay = min(interval * 2**self._consecutive_failures, interval * MAX_BACKOFF_MULTIPLIER)
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stopped.wait(), timeout=delay)
