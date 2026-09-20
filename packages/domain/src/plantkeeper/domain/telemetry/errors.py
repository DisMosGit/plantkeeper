"""Telemetry invariant violations."""

from __future__ import annotations

from plantkeeper.domain.base import DomainError


class TelemetryError(DomainError):
    """Base class for every Telemetry rule violation."""


class SensorReadingMismatchError(TelemetryError):
    """A reading was submitted for a different sensor."""


class SensorNeverSeenError(TelemetryError):
    """The sensor never reported, so it cannot be declared offline."""


class SensorStillOnlineError(TelemetryError):
    """The sensor reported more recently than the offline threshold."""
