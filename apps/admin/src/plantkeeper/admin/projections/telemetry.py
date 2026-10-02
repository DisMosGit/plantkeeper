"""The telemetry read side's writer: readings in, rollups and a latest row out.

Telemetry is projected by a consumer group of its own (``<prefix>-telemetry-rollup``)
with its own ledger rows, for the same reason every other projection has one: each
one owns its offsets and its idempotency domain, so a replay or a rebuild of the
rollups cannot disturb the domain read models. It consumes ``telemetry.events`` — the
*domain* facts the ingress published after resolving and validating the raw stream —
rather than ``telemetry.raw``, so it never repeats the ingress's validation,
sensor-to-plant resolution or deduplication (``docs/telemetry.md``).

Two tables, two shapes of write:

* **``telemetry_rollups``** — one row per sensor per fixed window. The upsert
  inserts a one-sample rollup and folds it into the existing row on conflict, so a
  reading that arrives out of order *recomputes* the window it belongs to rather
  than creating a second row or being dropped. The mean is re-derived from the
  stored mean and count, which is what keeps it correct when the window is revisited
  in any order.
* **``sensor_latest``** — the newest reading of each sensor, moved forward only. A
  late reading older than the stored one leaves the row alone, so "latest" keeps
  meaning latest. A reading also clears ``offline_at``: the sensor is reporting
  again, so the last silence announcement no longer describes it. The row is where
  the *non-reading* telemetry events land as well — the newest threshold alert in
  ``last_alert_at``/``last_alert_type``, the silence in ``offline_at`` — because the
  read side projects every telemetry event and this is the row that means "this
  sensor's latest known state". They are deliberately not folded into the rollup: the
  reading that crossed the threshold is already a sample in its window, so recording
  the alert as a second sample would double ``sample_count`` and skew the mean.

Every write is one SQL statement rather than an ORM round trip, because every one of
them is an upsert whose correctness is the database's atomic ``ON CONFLICT`` — an
existence check followed by an insert would race a second consumer of the same
group. They run inside :meth:`Projection.apply`'s transaction, so the ledger claim
and the projected rows commit together.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import ClassVar, Final

from django.db import connection

from plantkeeper.admin.projections.base import Projection, ProjectionHandler
from plantkeeper.admin.read_telemetry.models import ROLLUP_WINDOW
from plantkeeper.domain.base import DomainEvent
from plantkeeper.domain.telemetry.events import (
    SensorOffline,
    SoilMoistureHigh,
    SoilMoistureLow,
    TelemetryReceived,
    TemperatureAnomaly,
)
from plantkeeper.infrastructure.messaging.topics import TELEMETRY_EVENTS

TelemetryAlert = SoilMoistureLow | SoilMoistureHigh | TemperatureAnomaly
"""The telemetry events that report a threshold rather than a new measurement."""

UPSERT_ROLLUP: Final = """
    INSERT INTO "read_telemetry"."telemetry_rollups" (
        sensor_id, bucket_start, plant_id,
        moisture_min, moisture_max, moisture_avg,
        temperature_min, temperature_max, temperature_avg,
        sample_count, updated_at
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 1, now())
    ON CONFLICT (sensor_id, bucket_start) DO UPDATE SET
        plant_id = EXCLUDED.plant_id,
        moisture_min = LEAST("read_telemetry"."telemetry_rollups".moisture_min,
                             EXCLUDED.moisture_min),
        moisture_max = GREATEST("read_telemetry"."telemetry_rollups".moisture_max,
                                EXCLUDED.moisture_max),
        moisture_avg = ("read_telemetry"."telemetry_rollups".moisture_avg
                        * "read_telemetry"."telemetry_rollups".sample_count
                        + EXCLUDED.moisture_avg)
                       / ("read_telemetry"."telemetry_rollups".sample_count + 1),
        temperature_min = LEAST("read_telemetry"."telemetry_rollups".temperature_min,
                                EXCLUDED.temperature_min),
        temperature_max = GREATEST("read_telemetry"."telemetry_rollups".temperature_max,
                                   EXCLUDED.temperature_max),
        temperature_avg = ("read_telemetry"."telemetry_rollups".temperature_avg
                           * "read_telemetry"."telemetry_rollups".sample_count
                           + EXCLUDED.temperature_avg)
                          / ("read_telemetry"."telemetry_rollups".sample_count + 1),
        sample_count = "read_telemetry"."telemetry_rollups".sample_count + 1,
        updated_at = now()
"""
"""Fold one reading into its window, or start the window with it.

The inserted row is a rollup of exactly one sample — ``min = max = avg =`` the
reading — so the conflict clause can treat it as "the new sample" and fold it into
whatever is already stored. That is what makes the write idempotent per *reading*
rather than per *window*: the ledger already guarantees each event is projected once,
and this clause guarantees the aggregate is exact whichever order the readings came
in.
"""

UPSERT_LATEST: Final = """
    INSERT INTO "read_telemetry"."sensor_latest" (
        sensor_id, plant_id, recorded_at, moisture, temperature, light, offline_at, updated_at
    )
    VALUES (%s, %s, %s, %s, %s, %s, NULL, now())
    ON CONFLICT (sensor_id) DO UPDATE SET
        plant_id = EXCLUDED.plant_id,
        recorded_at = EXCLUDED.recorded_at,
        moisture = EXCLUDED.moisture,
        temperature = EXCLUDED.temperature,
        light = EXCLUDED.light,
        offline_at = NULL,
        updated_at = now()
    WHERE EXCLUDED.recorded_at > "read_telemetry"."sensor_latest".recorded_at
"""
"""Move the sensor's latest row forward, or leave it alone.

``WHERE`` rather than an unconditional update: a replayed or late reading older than
the stored one must not walk "latest" backwards. Because the update only runs for a
strictly newer reading, clearing ``offline_at`` in the same clause means "the sensor
reported again", which is exactly when a silence announcement stops applying.
"""

ANNOUNCE_OFFLINE: Final = """
    UPDATE "read_telemetry"."sensor_latest"
    SET offline_at = %s, updated_at = now()
    WHERE sensor_id = %s
"""
"""Record the silence announcement on the sensor's latest row.

The reading that began the silence always reaches this projection first — the
ingress appends ``TelemetryReceived`` and, much later, the timer appends
``SensorOffline``; both travel the same topic and the same partition key, so the
relay keeps them in order. The ``UPDATE`` therefore finds its row; a rebuild replays
the topic in that same order. A row that is missing (only possible if the ledger was
cleared without replaying the readings) leaves the announcement unrecorded rather
than inventing a reading, and the rebuild is what puts it back.
"""

RECORD_ALERT: Final = """
    UPDATE "read_telemetry"."sensor_latest"
    SET last_alert_at = %s, last_alert_type = %s, updated_at = now()
    WHERE sensor_id = %s
      AND (last_alert_at IS NULL OR last_alert_at <= %s)
"""
"""Record the newest threshold alert on the sensor's latest row.

``<=`` rather than ``<`` so a replay of the same alert rewrites the same values, and
an *older* alert that arrives after a newer one is ignored rather than walking the
row backwards. The alert's own instant is used, not the delivery time: the row
answers "what did this sensor last complain about", which is a fact about the sensor.
"""

ALERT_TYPE_NAMES: Final[dict[type[DomainEvent], str]] = {
    SoilMoistureLow: "soil_moisture_low",
    SoilMoistureHigh: "soil_moisture_high",
    TemperatureAnomaly: "temperature_anomaly",
}
"""How an alert event names itself in ``sensor_latest.last_alert_type``.

The same vocabulary as the notification types the alerts become
(``domain/notifications/values.py``), so the read side and the household are told
about one event in one language — but spelled here rather than imported, because the
Telemetry context does not depend on Notifications and the read side must not either.
"""


class TelemetryRollupProjection(Projection):
    """The telemetry read side: windows of readings, and the latest per sensor."""

    name = "telemetry-rollup"
    topics = (TELEMETRY_EVENTS,)

    def on_telemetry_received(self, event: TelemetryReceived) -> None:
        """Fold one reading into its window and move the sensor's latest row on."""
        bucket_start = window_start(event.recorded_at)
        with connection.cursor() as cursor:
            cursor.execute(
                UPSERT_ROLLUP,
                (
                    event.sensor_id.value,
                    bucket_start,
                    event.plant_id.value,
                    event.moisture.value,
                    event.moisture.value,
                    event.moisture.value,
                    event.temperature.value,
                    event.temperature.value,
                    event.temperature.value,
                ),
            )
            cursor.execute(
                UPSERT_LATEST,
                (
                    event.sensor_id.value,
                    event.plant_id.value,
                    event.recorded_at,
                    event.moisture.value,
                    event.temperature.value,
                    event.light.value,
                ),
            )

    def on_sensor_offline(self, event: SensorOffline) -> None:
        """Record the silence announcement on the sensor's latest row."""
        with connection.cursor() as cursor:
            cursor.execute(ANNOUNCE_OFFLINE, (event.occurred_at, event.sensor_id.value))

    def on_alert(self, event: TelemetryAlert) -> None:
        """Record the newest threshold alert on the sensor's latest row.

        The three alert events share one handler because they share one effect: which
        band was crossed changes the label, not the write. The reading they were
        raised from is projected separately, so this must not touch the rollups.
        """
        alert_type = ALERT_TYPE_NAMES[type(event)]
        with connection.cursor() as cursor:
            cursor.execute(
                RECORD_ALERT,
                (
                    event.occurred_at,
                    alert_type,
                    event.sensor_id.value,
                    event.occurred_at,
                ),
            )

    handlers: ClassVar[dict[type[DomainEvent], ProjectionHandler]] = {
        TelemetryReceived: on_telemetry_received,
        SensorOffline: on_sensor_offline,
        SoilMoistureLow: on_alert,
        SoilMoistureHigh: on_alert,
        TemperatureAnomaly: on_alert,
    }


def window_start(recorded_at: datetime) -> datetime:
    """Return the start of the fixed window ``recorded_at`` falls in, in UTC.

    Windows are cut in UTC whatever offset the event was written with: a reading
    from a sensor that reports with a local offset still belongs to the same hour
    as its neighbours, and ``timestamptz`` compares instants, not offsets.
    """
    return recorded_at.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


__all__ = ["ROLLUP_WINDOW", "TelemetryRollupProjection", "window_start"]
