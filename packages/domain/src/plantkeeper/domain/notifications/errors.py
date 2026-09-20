"""Notifications invariant violations."""

from __future__ import annotations

from plantkeeper.domain.base import DomainError


class NotificationError(DomainError):
    """Base class for every Notifications rule violation."""


class NotificationAlreadyReadError(NotificationError):
    """The notification was acknowledged before."""
