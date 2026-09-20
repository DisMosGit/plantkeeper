"""The system clock: the production implementation of the clock port."""

from __future__ import annotations

from datetime import UTC, datetime


class SystemClock:
    """Reads the wall clock.

    The domain never does this — aggregates take ``now`` as an argument — so
    replacing this one object with a fixed clock makes a whole test suite
    deterministic without patching the clock module.
    """

    def now(self) -> datetime:
        """Return the current UTC instant, timezone-aware."""
        return datetime.now(UTC)
