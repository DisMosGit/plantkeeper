"""Catalog invariant violations."""

from __future__ import annotations

from plantkeeper.domain.base import DomainError


class CatalogError(DomainError):
    """Base class for every Catalog rule violation."""


class SpeciesNameEmptyError(CatalogError):
    """A species must have non-blank scientific and common names."""


class SpeciesVersionConflictError(CatalogError):
    """The species changed since the caller read it (optimistic locking)."""
