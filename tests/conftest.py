"""Shared pytest fixtures.

pytest-asyncio runs in ``asyncio_mode = "auto"`` (see the root ``pyproject.toml``),
so async tests need no decorator. ``anyio_backend`` is declared here for the
anyio-based tests that arrive with the FastAPI/Starlette code in later phases.

The ``event_loop`` fixture is deliberately *not* overridden: pytest-asyncio >= 1.0
removed that override point, so redefining it would only be dead, misleading code.
"""

from __future__ import annotations

import pytest


@pytest.fixture
def anyio_backend() -> str:
    """Run anyio-based tests on the asyncio backend only."""
    return "asyncio"
