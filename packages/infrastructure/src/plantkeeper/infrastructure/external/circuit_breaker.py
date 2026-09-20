"""A small asynchronous circuit breaker, built on the project's ``Clock`` port.

The breaker exists so a Trefle outage fails fast instead of spending the whole
sync on timeouts. It is project-owned rather than ``pybreaker`` for the same
reason the rest of this project owns its infrastructure seams: its state
transitions read the ``Clock`` port, so a test advances time instead of
monkeypatching a library's monotonic timer, and the call site stays fully typed.

State machine — the three states every breaker documents:

* ``CLOSED``: calls go through; ``failure_threshold`` *consecutive* failures open
  the breaker. A success resets the count.
* ``OPEN``: calls fail immediately with :class:`CircuitOpenError` and the
  operation is never attempted. After ``reset_timeout`` the next call is allowed
  through as a trial.
* ``HALF_OPEN``: one trial is in flight; other callers fail fast. The trial's
  outcome either closes the breaker or reopens it for another ``reset_timeout``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from enum import StrEnum

from plantkeeper.application.ports.clock import Clock


class CircuitState(StrEnum):
    """Where a breaker is in its life cycle."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(Exception):
    """A call was rejected because the breaker is open.

    Raised *instead of* the operation: the caller uses it to switch to whatever
    fallback it has, and the external system is never contacted.
    """

    def __init__(self, name: str) -> None:
        super().__init__(f"circuit {name!r} is open")
        self.name = name


class AsyncCircuitBreaker:
    """Counts consecutive failures of one named external dependency.

    One instance per dependency, held for the process's lifetime (``APP`` scope
    in the container): the count is only meaningful if every call of that
    dependency shares it.
    """

    def __init__(
        self,
        *,
        name: str,
        failure_threshold: int,
        reset_timeout: timedelta,
        clock: Clock,
    ) -> None:
        """Configure the breaker; the operation is passed per call."""
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be at least 1")
        if reset_timeout <= timedelta(0):
            raise ValueError("reset_timeout must be positive")
        self._name = name
        self._failure_threshold = failure_threshold
        self._reset_timeout = reset_timeout
        self._clock = clock
        self._state = CircuitState.CLOSED
        self._failures = 0
        self._opened_at: datetime | None = None
        self._trial_in_flight = False
        self._lock = asyncio.Lock()

    @property
    def name(self) -> str:
        """The dependency this breaker guards, for logs."""
        return self._name

    @property
    def state(self) -> CircuitState:
        """The current state, with an elapsed reset timeout already applied.

        An ``OPEN`` breaker whose ``reset_timeout`` has elapsed reports
        ``HALF_OPEN``: the next call is the trial, without a background timer.
        """
        return self._effective_state()

    @property
    def failure_count(self) -> int:
        """How many consecutive failures have been recorded."""
        return self._failures

    async def call[ResultT](self, operation: Callable[[], Awaitable[ResultT]]) -> ResultT:
        """Run ``operation`` unless the breaker is open.

        Raises :class:`CircuitOpenError` without calling ``operation`` when the
        breaker is open and its reset timeout has not elapsed, or when a
        half-open trial is already running. Any exception from the operation is
        recorded and re-raised unchanged.
        """
        async with self._lock:
            self._before_call()
        try:
            result = await operation()
        except Exception:
            # ``CancelledError`` is a ``BaseException`` and is deliberately not a
            # failure: a shutdown is not evidence that Trefle is down.
            async with self._lock:
                self._record_failure()
            raise
        async with self._lock:
            self._record_success()
        return result

    def _effective_state(self) -> CircuitState:
        """Return the state, promoting an expired ``OPEN`` to ``HALF_OPEN``."""
        if (
            self._state is CircuitState.OPEN
            and self._opened_at is not None
            and self._clock.now() - self._opened_at >= self._reset_timeout
        ):
            return CircuitState.HALF_OPEN
        return self._state

    def _before_call(self) -> None:
        """Admit or reject the call; the caller holds the lock."""
        state = self._effective_state()
        if state is CircuitState.OPEN:
            raise CircuitOpenError(self._name)
        self._state = state
        if state is CircuitState.HALF_OPEN:
            if self._trial_in_flight:
                raise CircuitOpenError(self._name)
            self._trial_in_flight = True

    def _record_success(self) -> None:
        """A success closes the breaker and clears the failure count."""
        self._state = CircuitState.CLOSED
        self._failures = 0
        self._opened_at = None
        self._trial_in_flight = False

    def _record_failure(self) -> None:
        """Count one failure, opening or reopening the breaker as required."""
        self._trial_in_flight = False
        if self._state is CircuitState.HALF_OPEN:
            self._open()
            return
        self._failures += 1
        if self._failures >= self._failure_threshold:
            self._open()

    def _open(self) -> None:
        """Open the breaker and start the reset timeout from now."""
        self._state = CircuitState.OPEN
        self._opened_at = self._clock.now()
        self._failures = self._failure_threshold
