"""The contract a worker's background loop implements.

``OutboxRelay`` already has this shape, and the schedulers below repeat it on
purpose: a long-running coroutine with an idempotent ``stop`` that the signal
handler can call from a different task. ``workers/main.py`` runs them together and
stops them all before it closes the broker.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class BackgroundJob(Protocol):
    """A coroutine that runs until :meth:`stop` is called."""

    async def run(self) -> None:
        """Run until stopped; must return once :meth:`stop` has been requested."""
        ...

    def stop(self) -> None:
        """Ask the loop to finish its current tick and return."""
        ...
