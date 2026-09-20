"""Smoke test: the e2e suite is collected (no service is started)."""

from __future__ import annotations

import pytest


@pytest.mark.slow
def test_e2e_harness_is_collected() -> None:
    assert True
