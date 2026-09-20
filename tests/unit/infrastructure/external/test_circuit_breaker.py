"""Behaviour tests for the project's circuit breaker.

The breaker reads the ``Clock`` port rather than a monotonic timer, so these
tests move time by hand: no monkeypatching, and the reset timeout is exercised
rather than slept through.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from plantkeeper.infrastructure.external.circuit_breaker import (
    AsyncCircuitBreaker,
    CircuitOpenError,
    CircuitState,
)

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
RESET = timedelta(seconds=60)


class MutableClock:
    """A clock the test advances."""

    def __init__(self, now: datetime) -> None:
        self._now = now

    def now(self) -> datetime:
        """Return the instant the test has set."""
        return self._now

    def advance(self, delta: timedelta) -> None:
        """Move the clock forward."""
        self._now += delta


def assert_state(breaker: AsyncCircuitBreaker, expected: CircuitState) -> None:
    """Compare through a helper: mypy pins a property's narrowed value otherwise."""
    assert breaker.state is expected


@pytest.fixture
def clock() -> MutableClock:
    return MutableClock(NOW)


@pytest.fixture
def breaker(clock: MutableClock) -> AsyncCircuitBreaker:
    return AsyncCircuitBreaker(name="trefle", failure_threshold=3, reset_timeout=RESET, clock=clock)


async def failing() -> None:
    """An operation that always fails."""
    raise RuntimeError("down")


async def succeeding() -> str:
    """An operation that always answers."""
    return "ok"


@pytest.mark.parametrize(
    ("threshold", "reset"),
    [(0, RESET), (-1, RESET), (1, timedelta(0)), (1, timedelta(seconds=-1))],
)
def test_invalid_configuration_is_refused(threshold: int, reset: timedelta) -> None:
    with pytest.raises(ValueError, match="must be"):
        AsyncCircuitBreaker(
            name="trefle",
            failure_threshold=threshold,
            reset_timeout=reset,
            clock=MutableClock(NOW),
        )


async def test_a_success_keeps_the_breaker_closed(breaker: AsyncCircuitBreaker) -> None:
    assert await breaker.call(succeeding) == "ok"

    assert_state(breaker, CircuitState.CLOSED)
    assert breaker.failure_count == 0


async def test_consecutive_failures_open_the_breaker(breaker: AsyncCircuitBreaker) -> None:
    for _ in range(2):
        with pytest.raises(RuntimeError, match="down"):
            await breaker.call(failing)

    assert_state(breaker, CircuitState.CLOSED)
    assert breaker.failure_count == 2

    with pytest.raises(RuntimeError, match="down"):
        await breaker.call(failing)

    assert_state(breaker, CircuitState.OPEN)
    assert breaker.failure_count == 3


async def test_a_success_resets_the_failure_count(breaker: AsyncCircuitBreaker) -> None:
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await breaker.call(failing)

    await breaker.call(succeeding)

    assert breaker.failure_count == 0
    # Two more failures are not enough any more: the count started over.
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await breaker.call(failing)
    assert_state(breaker, CircuitState.CLOSED)


async def test_an_open_breaker_does_not_call_the_operation(
    breaker: AsyncCircuitBreaker,
) -> None:
    for _ in range(3):
        with pytest.raises(RuntimeError):
            await breaker.call(failing)

    calls = 0

    async def counted() -> None:
        nonlocal calls
        calls += 1

    with pytest.raises(CircuitOpenError, match="trefle"):
        await breaker.call(counted)

    assert calls == 0


async def test_the_reset_timeout_promotes_the_breaker_to_half_open(
    breaker: AsyncCircuitBreaker, clock: MutableClock
) -> None:
    for _ in range(3):
        with pytest.raises(RuntimeError):
            await breaker.call(failing)

    clock.advance(RESET - timedelta(seconds=1))
    assert_state(breaker, CircuitState.OPEN)

    clock.advance(timedelta(seconds=1))
    assert_state(breaker, CircuitState.HALF_OPEN)


async def test_a_successful_trial_closes_the_breaker(
    breaker: AsyncCircuitBreaker, clock: MutableClock
) -> None:
    for _ in range(3):
        with pytest.raises(RuntimeError):
            await breaker.call(failing)
    clock.advance(RESET)

    assert await breaker.call(succeeding) == "ok"
    assert_state(breaker, CircuitState.CLOSED)
    assert breaker.failure_count == 0


async def test_a_failed_trial_reopens_the_breaker(
    breaker: AsyncCircuitBreaker, clock: MutableClock
) -> None:
    for _ in range(3):
        with pytest.raises(RuntimeError):
            await breaker.call(failing)
    clock.advance(RESET)

    with pytest.raises(RuntimeError):
        await breaker.call(failing)

    assert_state(breaker, CircuitState.OPEN)
    # The reset timeout started over with the failed trial.
    clock.advance(RESET - timedelta(seconds=1))
    assert_state(breaker, CircuitState.OPEN)
    clock.advance(timedelta(seconds=1))
    assert_state(breaker, CircuitState.HALF_OPEN)
