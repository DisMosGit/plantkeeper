"""Unit tests for the outbox relay's delivery policy.

The relay's loop needs a database, but its *policy* — retry a blip, count a
failed poll, dead-letter after the last attempt, never drop a row whose
dead-letter copy failed — does not. These tests drive :meth:`OutboxRelay.deliver`
with fakes so the policy is pinned without a container.
"""

from __future__ import annotations

import asyncio
from typing import cast
from uuid import uuid7

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from plantkeeper.application.ports.outbox import OutboxMessage
from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.messaging.relay import PUBLISH_ATTEMPTS_PER_CYCLE, OutboxRelay
from plantkeeper.infrastructure.messaging.topics import GARDEN_EVENTS

SETTINGS = Settings(
    outbox_max_attempts=5,
    outbox_poll_interval_seconds=0.0,
    outbox_retry_initial_wait_seconds=0.0,
    outbox_retry_max_wait_seconds=0.0,
)

UNUSED_SESSION_FACTORY = cast(async_sessionmaker[AsyncSession], None)
"""The delivery policy never touches the factory; only :meth:`OutboxRelay.run` does."""


def a_message(*, attempts: int = 0, outbox_id: int = 7) -> OutboxMessage:
    """Return a plausible outbox message."""
    return OutboxMessage(
        outbox_id=outbox_id,
        event_id=uuid7(),
        event_name="PlantAdded",
        topic=GARDEN_EVENTS,
        partition_key=str(uuid7()),
        payload={"name": "Fern"},
        attempts=attempts,
    )


class FakePublisher:
    """A publisher that fails a fixed number of times before it succeeds."""

    def __init__(self, *, failures: int = 0, dead_letter_fails: bool = False) -> None:
        self.remaining_failures = failures
        self.dead_letter_fails = dead_letter_fails
        self.published: list[OutboxMessage] = []
        self.dead_letters: list[tuple[OutboxMessage, str]] = []

    async def publish(self, message: OutboxMessage) -> None:
        """Fail while the failure budget lasts, then record the message."""
        if self.remaining_failures > 0:
            self.remaining_failures -= 1
            raise RuntimeError("broker unavailable")
        self.published.append(message)

    async def publish_dead_letter(self, message: OutboxMessage, *, error: str) -> None:
        """Record the dead-letter copy, or fail if the test asks for that."""
        if self.dead_letter_fails:
            raise RuntimeError("dead-letter topic unavailable")
        self.dead_letters.append((message, error))


class FakeOutboxRepository:
    """Records what the relay told the outbox to do."""

    def __init__(self) -> None:
        self.published_ids: list[int] = []
        self.failures: list[tuple[int, str]] = []
        self.dead_lettered: list[tuple[int, str]] = []

    async def append(self, event: object) -> None:
        """Unused by the relay."""
        raise NotImplementedError

    async def fetch_unpublished(self, limit: int) -> list[OutboxMessage]:
        """Unused by the delivery policy."""
        raise NotImplementedError

    async def mark_published(self, outbox_id: int) -> None:
        """Record a successful publish."""
        self.published_ids.append(outbox_id)

    async def record_failure(self, outbox_id: int, error: str) -> None:
        """Record a failed attempt."""
        self.failures.append((outbox_id, error))

    async def dead_letter(self, outbox_id: int, error: str) -> None:
        """Record that the row stopped being retried."""
        self.dead_lettered.append((outbox_id, error))


class FakeCommitter:
    """Counts how often the relay committed."""

    def __init__(self) -> None:
        self.calls = 0

    async def __call__(self) -> None:
        """Record one commit."""
        self.calls += 1


def relay_with(publisher: FakePublisher) -> OutboxRelay:
    """Return a relay wired to the given publisher and nothing else."""
    return OutboxRelay(
        session_factory=UNUSED_SESSION_FACTORY, publisher=publisher, settings=SETTINGS
    )


async def test_a_published_message_is_marked_and_committed() -> None:
    publisher = FakePublisher()
    repository = FakeOutboxRepository()
    committer = FakeCommitter()

    failed = await relay_with(publisher).deliver(repository, a_message(), committer)

    assert failed is False
    assert repository.published_ids == [7]
    assert repository.failures == []
    assert committer.calls == 1


async def test_a_transient_failure_is_retried_within_the_poll() -> None:
    publisher = FakePublisher(failures=PUBLISH_ATTEMPTS_PER_CYCLE - 1)
    repository = FakeOutboxRepository()

    failed = await relay_with(publisher).deliver(repository, a_message(), FakeCommitter())

    assert failed is False
    assert publisher.published
    assert repository.failures == []


async def test_an_exhausted_poll_counts_one_failed_attempt() -> None:
    publisher = FakePublisher(failures=PUBLISH_ATTEMPTS_PER_CYCLE)
    repository = FakeOutboxRepository()

    failed = await relay_with(publisher).deliver(repository, a_message(), FakeCommitter())

    assert failed is True
    assert repository.published_ids == []
    assert len(repository.failures) == 1
    assert repository.failures[0][0] == 7
    assert "broker unavailable" in repository.failures[0][1]
    assert repository.dead_lettered == []


async def test_the_last_attempt_dead_letters_the_message() -> None:
    publisher = FakePublisher(failures=PUBLISH_ATTEMPTS_PER_CYCLE)
    repository = FakeOutboxRepository()
    message = a_message(attempts=SETTINGS.outbox_max_attempts - 1)

    failed = await relay_with(publisher).deliver(repository, message, FakeCommitter())

    assert failed is True
    assert repository.failures == []
    assert repository.dead_lettered == [(7, "RuntimeError: broker unavailable")]
    assert publisher.dead_letters and publisher.dead_letters[0][0] == message


async def test_a_failed_dead_letter_copy_keeps_the_row_for_another_try() -> None:
    publisher = FakePublisher(failures=PUBLISH_ATTEMPTS_PER_CYCLE, dead_letter_fails=True)
    repository = FakeOutboxRepository()
    message = a_message(attempts=SETTINGS.outbox_max_attempts - 1)

    failed = await relay_with(publisher).deliver(repository, message, FakeCommitter())

    assert failed is True
    assert repository.dead_lettered == []
    assert len(repository.failures) == 1
    assert "dead-letter publish failed" in repository.failures[0][1]


async def test_run_returns_promptly_once_stopped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    relay = relay_with(FakePublisher())
    polls = 0

    async def counting_run_once() -> int:
        nonlocal polls
        polls += 1
        return 0

    monkeypatch.setattr(relay, "run_once", counting_run_once)

    task = asyncio.create_task(relay.run())
    await asyncio.sleep(0.01)
    relay.stop()
    await asyncio.wait_for(task, timeout=1)

    assert polls > 0
    assert task.done()
