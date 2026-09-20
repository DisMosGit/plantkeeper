"""The clock port.

Domain aggregates never read the wall clock: they take ``now`` as an argument so
that behaviour is reproducible. The application layer is where that argument
comes from, and it asks this port, which tests replace with a fixed clock.
"""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable


@runtime_checkable
class Clock(Protocol):
    """Source of the current time for use cases."""

    def now(self) -> datetime:
        """Return the current timezone-aware instant."""
        ...
