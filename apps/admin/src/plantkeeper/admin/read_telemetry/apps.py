"""The Django app that owns the telemetry read models."""

from __future__ import annotations

from django.apps import AppConfig


class ReadTelemetryConfig(AppConfig):
    """Configuration for the telemetry rollup tables.

    A separate app from ``read_models`` because it owns a separate schema and a
    separate consumer group: the rollups are rebuilt on their own schedule, and
    their migration must not be entangled with the domain read models'. ``label``
    is short because it appears in migration dependencies.
    """

    name = "plantkeeper.admin.read_telemetry"
    label = "read_telemetry"
    default_auto_field = "django.db.models.BigAutoField"
    verbose_name = "Telemetry read models"
