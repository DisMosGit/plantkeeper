"""The telemetry read models Django Admin shows and the rollup projection writes.

Telemetry is the platform's highest-volume stream, and the raw readings are the
write side's detail record. These tables are not a copy of that record: they are
the answers a client asks about it. ``telemetry_rollups`` keeps one row per sensor
per fixed window with the extremes, the mean and the sample count, so "how has this
plant's soil been doing" is one read instead of a scan; ``sensor_latest`` keeps the
newest reading of each sensor, so "what is this sensor saying now" is one row.

Both are upserted by ``TelemetryRollupProjection`` (``plantkeeper.admin.projections
.telemetry``) under a consumer group of its own, and both are rebuildable by
replaying ``telemetry.events`` after the ledger rows and the rollups are removed
(``docs/telemetry.md``).

The window is fixed and named here rather than per row: a rollup's ``bucket_start``
is the window's start, and every row of a sensor whose ``recorded_at`` falls in
``[bucket_start, bucket_start + ROLLUP_WINDOW)`` contributes to it. Nothing stores
the window length, so changing it starts new buckets rather than rewriting old
ones — which is the rebuild's business, not this table's.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from uuid import UUID

from django.db import models

from plantkeeper.infrastructure.persistence.schemas import READ_TELEMETRY

ROLLUP_WINDOW: timedelta = timedelta(hours=1)
"""The fixed width of one rollup window, in UTC."""


def read_table(name: str) -> str:
    """Return the schema-qualified table name a model should use.

    The same trick as ``plantkeeper.admin.read_models.models.read_table``: a
    ``Meta.db_table`` already quoted is left alone by ``quote_name``, so a Django
    model can live in a named schema without a database router.
    """
    return f'"{READ_TELEMETRY}"."{name}"'


class TelemetryRollup(models.Model):
    """One sensor's readings inside one fixed window.

    ``plant_id`` is denormalised from the sensor registry — the event carries it —
    so a reader answering "how has this plant been doing" never joins the write
    side. The three extremes and the mean are updated by a single upsert, which is
    what makes an out-of-order reading recompute the window rather than create a
    second row: the mean is re-derived from the count and the mean already stored.
    """

    sensor_id: UUID = models.UUIDField()
    bucket_start: datetime = models.DateTimeField()
    plant_id: UUID = models.UUIDField()
    moisture_min: float = models.FloatField()
    moisture_max: float = models.FloatField()
    moisture_avg: float = models.FloatField()
    temperature_min: float = models.FloatField()
    temperature_max: float = models.FloatField()
    temperature_avg: float = models.FloatField()
    sample_count: int = models.IntegerField(default=1)
    updated_at: datetime = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = read_table("telemetry_rollups")
        # The upsert's conflict target, and the identity of a window: one row per
        # sensor per bucket.
        constraints = [
            models.UniqueConstraint(
                fields=["sensor_id", "bucket_start"],
                name="uq_telemetry_rollups_sensor_id_bucket_start",
            )
        ]
        indexes = [
            models.Index(fields=["plant_id", "bucket_start"], name="ix_rollups_plant_bucket"),
            models.Index(fields=["bucket_start"], name="ix_rollups_bucket_start"),
        ]
        ordering = ["-bucket_start"]

    def __str__(self) -> str:
        """Identify the window the way the admin lists it."""
        return f"{self.sensor_id} at {self.bucket_start:%Y-%m-%d %H:%M}"


class SensorLatest(models.Model):
    """The newest reading of one sensor, its last alert, and its last silence.

    ``recorded_at`` only ever moves forward: a replayed or late reading older than
    the stored one is absorbed by the upsert's ``WHERE`` clause, so the row really
    is the latest. The two non-reading facts live here because the read side
    projects *every* telemetry event and this is the row that means "this sensor's
    latest known state": ``offline_at`` is the moment the silence timer announced
    the sensor, and ``last_alert_at``/``last_alert_type`` are the newest threshold
    event. Neither is folded into ``telemetry_rollups``: the reading that crossed
    the threshold is already a sample there, and counting the alert too would
    double the sample count and skew the mean.
    """

    sensor_id: UUID = models.UUIDField(primary_key=True)
    plant_id: UUID = models.UUIDField()
    recorded_at: datetime = models.DateTimeField()
    moisture: float = models.FloatField()
    temperature: float = models.FloatField()
    light: float = models.FloatField()
    offline_at: datetime | None = models.DateTimeField(null=True, blank=True)
    last_alert_at: datetime | None = models.DateTimeField(null=True, blank=True)
    last_alert_type: str | None = models.CharField(max_length=40, null=True, blank=True)
    updated_at: datetime = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = read_table("sensor_latest")
        indexes = [models.Index(fields=["plant_id"], name="ix_sensor_latest_plant_id")]
        ordering = ["-recorded_at"]
        verbose_name_plural = "sensor latest"

    def __str__(self) -> str:
        """Identify the sensor the way the admin lists it."""
        return f"{self.sensor_id} at {self.recorded_at:%Y-%m-%d %H:%M}"
