"""The failure policy every Kafka consumer runs a delivery under.

A consumer that raises has to answer one question about the delivery: *is this
worth another attempt?* The project gives the same answer here as it gives a
write request — a broken domain rule cannot succeed by being repeated, so it is
moved aside at once, while anything else (a database that went away, a broker
timeout) is retried a bounded number of times with backoff and moved aside when
the budget is spent.

Moving a delivery aside is not dropping it. Whoever moves it writes a copy to the
dead-letter topic and records the delivery's claim in the same breath, so the
partition moves on and the delivery is neither retried forever nor handled twice
(``docs/adr/0011-consumer-failure-policy.md``).

Both subscriber sets share this module — the worker's consumers and Django's
projections — because one policy written twice is two policies. It knows nothing
about Kafka, the database or what a delivery looks like: the caller hands it the
two operations it owns, doing the work and moving a failure aside.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from plantkeeper.domain.base import DomainError

logger = logging.getLogger(__name__)

Delivery = Callable[[], Awaitable[bool]]
"""One attempt at a delivery: ``True`` when it was handled, ``False`` when it was
not this consumer's or had already been handled."""

MoveAside = Callable[[Exception], Awaitable[None]]
"""Store a delivery that cannot be handled: copy it out and claim it."""


@dataclass(frozen=True)
class FailurePolicy:
    """How often a delivery is retried before it is moved aside."""

    max_attempts: int
    """Total attempts, including the first one. At least one."""

    initial_wait_seconds: float
    """How long to wait after the first failure; doubled after each one."""

    max_wait_seconds: float
    """The ceiling on that wait, so a long budget does not sleep for hours."""

    def __post_init__(self) -> None:
        """Refuse a budget that could never attempt the delivery at all."""
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")

    def wait_before_retry(self, attempt: int) -> float:
        """Return how long to wait after failure number ``attempt`` (1-based)."""
        return min(self.initial_wait_seconds * 2.0 ** (attempt - 1), self.max_wait_seconds)


def describe_failure(error: BaseException) -> str:
    """Return the one-line cause recorded for a failed delivery.

    The type is part of it because the classification is: a reader of the
    dead-letter topic has to be able to tell a domain rule that was broken from a
    database that was unreachable.
    """
    return f"{type(error).__name__}: {error}"


def is_terminal(error: BaseException) -> bool:
    """Whether repeating ``error`` cannot change its outcome.

    A :class:`~plantkeeper.domain.base.DomainError` is a rule an aggregate
    enforced: the same delivery against the same state fails again, so retrying it
    only delays the move-aside and holds the partition while it does.
    """
    return isinstance(error, DomainError)


async def _sleep(seconds: float) -> None:
    """Wait between attempts; a seam so a test need not wait at all."""
    await asyncio.sleep(seconds)


async def run_with_failure_policy(
    deliver: Delivery,
    *,
    move_aside: MoveAside,
    policy: FailurePolicy,
    sleep: Callable[[float], Awaitable[None]] = _sleep,
) -> bool:
    """Run one delivery under ``policy``, moving it aside if it cannot succeed.

    Returns what ``deliver`` answered: ``True`` when the delivery was handled —
    including when it was moved aside, which is a decision rather than a
    duplicate — and ``False`` when it was not this consumer's to handle or had
    already been handled.

    A failure that ``move_aside`` cannot record propagates. That is deliberate:
    the delivery was neither handled nor copied anywhere, so the caller's
    transport has to offer it again rather than let it disappear, and a consumer
    that swallows this would be the silent loss the policy exists to prevent.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            return await deliver()
        except DomainError as error:
            logger.warning(
                "delivery breaks a domain rule (%s); moving it aside",
                describe_failure(error),
            )
            await move_aside(error)
            return True
        except Exception as error:
            if attempt >= policy.max_attempts:
                logger.error(
                    "delivery failed %d time(s) (%s); moving it aside",
                    attempt,
                    describe_failure(error),
                )
                await move_aside(error)
                return True
            wait = policy.wait_before_retry(attempt)
            logger.warning(
                "delivery failed (%s); retrying in %.1fs (attempt %d of %d)",
                describe_failure(error),
                wait,
                attempt + 1,
                policy.max_attempts,
            )
            await sleep(wait)
