"""Unit tests for the failure policy every consumer runs a delivery under.

The policy is three decisions, and each of them is invisible until it is wrong:
what is worth another attempt, how long to wait before it, and what happens when
the budget is spent. These tests drive
:func:`~plantkeeper.application.delivery.run_with_failure_policy` with fakes so all
three are pinned without a broker, a database or a subscriber.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

import pytest

from plantkeeper.application.delivery import (
    Delivery,
    FailurePolicy,
    MoveAside,
    describe_failure,
    is_terminal,
    run_with_failure_policy,
)
from plantkeeper.domain.base import DomainError
from plantkeeper.domain.garden.errors import PlantAlreadyRemovedError

POLICY = FailurePolicy(max_attempts=3, initial_wait_seconds=0.5, max_wait_seconds=4.0)


class BrokerOutageError(Exception):
    """A failure that may succeed later: an unreachable database, say."""


class Recorder:
    """A delivery that fails ``failures`` times, then answers ``answer``.

    Records every attempt, so a test can assert the retry *schedule* rather than
    only the outcome.
    """

    def __init__(self, *, failures: int = 0, answer: bool = True) -> None:
        self.remaining = failures
        self.answer = answer
        self.attempts = 0

    async def __call__(self) -> bool:
        """Take one attempt."""
        self.attempts += 1
        if self.remaining > 0:
            self.remaining -= 1
            raise BrokerOutageError("the database went away")
        return self.answer


@dataclass
class MoveAsideRecorder:
    """Records the failures the policy moved aside, or refuses them like a broker."""

    fails: bool = False
    moved: list[Exception] = field(default_factory=list)

    async def __call__(self, error: Exception) -> None:
        """Record ``error``, or raise as a refused dead-letter copy would."""
        if self.fails:
            raise BrokerOutageError("dead-letter topic unavailable")
        self.moved.append(error)


@dataclass
class SleepRecorder:
    """The waits a run asked for, and the sleep to hand the policy."""

    waits: list[float] = field(default_factory=list)

    async def __call__(self, seconds: float) -> None:
        """Record a wait instead of taking it."""
        self.waits.append(seconds)


async def run_delivery(
    delivery: Delivery,
    move_aside: MoveAside,
    sleep: Callable[[float], Awaitable[None]],
    *,
    policy: FailurePolicy = POLICY,
) -> bool:
    """Run one delivery through the policy the way a subscriber does."""
    return await run_with_failure_policy(
        delivery, move_aside=move_aside, policy=policy, sleep=sleep
    )


async def test_a_delivery_that_works_is_attempted_once() -> None:
    delivery = Recorder()
    moved = MoveAsideRecorder()
    sleep = SleepRecorder()

    assert await run_delivery(delivery, moved, sleep) is True
    assert delivery.attempts == 1
    assert moved.moved == []
    assert sleep.waits == []


async def test_a_delivery_that_is_not_this_consumers_is_not_retried() -> None:
    """``False`` is an answer, not a failure: it must not spend the budget."""
    delivery = Recorder(answer=False)
    moved = MoveAsideRecorder()
    sleep = SleepRecorder()

    assert await run_delivery(delivery, moved, sleep) is False
    assert delivery.attempts == 1
    assert moved.moved == []
    assert sleep.waits == []


async def test_a_transient_failure_is_retried_with_backoff_until_it_works() -> None:
    """Two failures then success: three attempts, waits doubling from the base."""
    delivery = Recorder(failures=2)
    moved = MoveAsideRecorder()
    sleep = SleepRecorder()

    assert await run_delivery(delivery, moved, sleep) is True
    assert delivery.attempts == 3
    assert sleep.waits == [0.5, 1.0]
    assert moved.moved == []


async def test_a_delivery_that_keeps_failing_is_moved_aside_once() -> None:
    """Three allowed attempts, then the copy — never a fourth attempt."""
    delivery = Recorder(failures=99)
    moved = MoveAsideRecorder()
    sleep = SleepRecorder()

    assert await run_delivery(delivery, moved, sleep) is True
    assert delivery.attempts == 3
    assert sleep.waits == [0.5, 1.0]
    assert len(moved.moved) == 1
    assert isinstance(moved.moved[0], BrokerOutageError)


async def test_a_broken_domain_rule_is_moved_aside_at_once() -> None:
    """A rule cannot be satisfied by repeating the delivery, so nothing waits."""

    async def deliver() -> bool:
        raise PlantAlreadyRemovedError("this plant is gone")

    moved = MoveAsideRecorder()
    sleep = SleepRecorder()

    assert await run_delivery(deliver, moved, sleep) is True
    assert sleep.waits == []
    assert len(moved.moved) == 1
    assert isinstance(moved.moved[0], DomainError)


async def test_a_refused_copy_is_raised_rather_than_swallowed() -> None:
    """The delivery was neither handled nor stored: the caller must offer it again."""
    delivery = Recorder(failures=99)
    moved = MoveAsideRecorder(fails=True)
    sleep = SleepRecorder()

    with pytest.raises(BrokerOutageError, match="dead-letter topic unavailable"):
        await run_delivery(delivery, moved, sleep)


def test_the_backoff_is_capped_by_the_configured_ceiling() -> None:
    policy = FailurePolicy(max_attempts=10, initial_wait_seconds=1.0, max_wait_seconds=4.0)

    waits = [policy.wait_before_retry(attempt) for attempt in range(1, 6)]

    assert waits == [1.0, 2.0, 4.0, 4.0, 4.0]


def test_a_budget_of_zero_attempts_is_refused() -> None:
    """A delivery that can never be attempted is a configuration mistake, not a policy."""
    with pytest.raises(ValueError, match="at least 1"):
        FailurePolicy(max_attempts=0, initial_wait_seconds=0.5, max_wait_seconds=4.0)


def test_a_domain_error_is_terminal_and_nothing_else_is() -> None:
    assert is_terminal(PlantAlreadyRemovedError("gone")) is True
    assert is_terminal(BrokerOutageError("the database went away")) is False


def test_the_recorded_cause_names_the_failure_type() -> None:
    """A reader of the dead-letter topic has to tell a rule from an outage."""
    assert describe_failure(PlantAlreadyRemovedError("gone")) == ("PlantAlreadyRemovedError: gone")
    assert describe_failure(BrokerOutageError("timeout")) == "BrokerOutageError: timeout"
