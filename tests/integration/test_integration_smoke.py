"""Smoke test: the integration suite is collected (no container is started)."""

from __future__ import annotations

import pytest


@pytest.mark.integration
def test_integration_harness_is_collected() -> None:
    assert True
