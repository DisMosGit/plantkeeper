"""Shared pytest fixtures.

pytest-asyncio runs in ``asyncio_mode = "auto"`` (see the root ``pyproject.toml``),
so async tests need no decorator. ``anyio_backend`` is declared here for the
anyio-based tests that arrive with the FastAPI/Starlette code in later phases.

The ``event_loop`` fixture is deliberately *not* overridden: pytest-asyncio >= 1.0
removed that override point, so redefining it would only be dead, misleading code.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest


@pytest.fixture
def anyio_backend() -> str:
    """Run anyio-based tests on the asyncio backend only."""
    return "asyncio"


@pytest.fixture
def now() -> datetime:
    """A fixed, timezone-aware instant used as the domain clock in tests.

    Domain code never reads the wall clock itself: aggregates take ``now`` as an
    argument, so tests stay deterministic without freezing time.
    """
    return datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
