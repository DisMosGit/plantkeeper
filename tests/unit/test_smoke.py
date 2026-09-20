"""Smoke tests for the skeleton: pytest collects, and every workspace member is
importable from the shared ``plantkeeper`` namespace."""

from __future__ import annotations

import importlib

import pytest

NAMESPACE_MODULES = [
    "plantkeeper.domain",
    "plantkeeper.application",
    "plantkeeper.infrastructure",
    "plantkeeper.api",
    "plantkeeper.admin",
    "plantkeeper.workers",
    "plantkeeper.iot_simulator",
]


def test_smoke() -> None:
    assert True


@pytest.mark.parametrize("module_name", NAMESPACE_MODULES)
def test_workspace_member_is_importable(module_name: str) -> None:
    importlib.import_module(module_name)
