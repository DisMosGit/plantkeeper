"""Django Admin for the telemetry read side.

Read-only for the same reason the domain read models are: a projection owns every
row here, so an edit would be overwritten by the next event and a deletion would
come back on the next replay. What the admin is for is *inspection* — which windows
a sensor has, what its latest reading was, and when its silence was announced.
"""

from __future__ import annotations

from django.contrib import admin

from plantkeeper.admin.read_models.admin import ReadOnlyMixin
from plantkeeper.admin.read_telemetry.models import SensorLatest, TelemetryRollup


@admin.register(TelemetryRollup)
class TelemetryRollupAdmin(ReadOnlyMixin, admin.ModelAdmin):
    """The per-window aggregates of every sensor the projection has seen."""

    list_display = (
        "sensor_id",
        "bucket_start",
        "plant_id",
        "moisture_min",
        "moisture_avg",
        "moisture_max",
        "temperature_avg",
        "sample_count",
    )
    list_filter = ("plant_id",)
    date_hierarchy = "bucket_start"
    search_fields = ("sensor_id", "plant_id")
    fields = (
        "sensor_id",
        "bucket_start",
        "plant_id",
        "moisture_min",
        "moisture_avg",
        "moisture_max",
        "temperature_min",
        "temperature_avg",
        "temperature_max",
        "sample_count",
        "updated_at",
    )
    readonly_fields = fields


@admin.register(SensorLatest)
class SensorLatestAdmin(ReadOnlyMixin, admin.ModelAdmin):
    """The newest reading of every sensor, its last alert and its last silence."""

    list_display = (
        "sensor_id",
        "plant_id",
        "recorded_at",
        "moisture",
        "temperature",
        "last_alert_type",
        "offline_at",
    )
    list_filter = ("plant_id", "last_alert_type")
    date_hierarchy = "recorded_at"
    search_fields = ("sensor_id", "plant_id")
    fields = (
        "sensor_id",
        "plant_id",
        "recorded_at",
        "moisture",
        "temperature",
        "light",
        "last_alert_at",
        "last_alert_type",
        "offline_at",
        "updated_at",
    )
    readonly_fields = fields
