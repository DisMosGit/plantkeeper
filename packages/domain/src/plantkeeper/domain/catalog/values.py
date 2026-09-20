"""Catalog value objects."""

from __future__ import annotations

from enum import StrEnum


class LightRequirement(StrEnum):
    """How much light a species needs, as reported by the catalogue."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
