"""Identity invariant violations."""

from __future__ import annotations

from plantkeeper.domain.base import DomainError


class IdentityError(DomainError):
    """Base class for every Identity rule violation."""


class UserNameEmptyError(IdentityError):
    """A member must have a display name that is not blank."""
