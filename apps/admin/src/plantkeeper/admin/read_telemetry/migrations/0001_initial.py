"""Telemetry read side schema.

Creates the ``read_telemetry`` schema the rollup projection writes and Django Admin
reads, and its two tables. It is a schema and an app of its own — not a corner of
``read_analytics`` — so the rollups' migration and rebuild story stay separate from
the domain read models'.

The schema is created here as well as in ``docker/postgres-read/init`` because that
init script only runs when the Postgres volume is first initialised, and because the
test suite starts an empty Postgres container with no init script at all.

Generated with ``manage.py makemigrations read_telemetry``, then edited: the
``RunSQL`` is prepended so the schema exists before the first ``CREATE TABLE``.
"""

from __future__ import annotations

from django.db import migrations, models


class Migration(migrations.Migration):
    """Create the telemetry read models."""

    initial = True

    dependencies: list[tuple[str, str]] = []

    operations = [
        migrations.RunSQL(
            sql="CREATE SCHEMA IF NOT EXISTS read_telemetry",
            # The schema is shared with ``docker/postgres-read/init``, exactly like
            # read_models 0001 leaves its own in place on a downgrade.
            reverse_sql=migrations.RunSQL.noop,
        ),
        migrations.CreateModel(
            name="SensorLatest",
            fields=[
                ("sensor_id", models.UUIDField(primary_key=True, serialize=False)),
                ("plant_id", models.UUIDField()),
                ("recorded_at", models.DateTimeField()),
                ("moisture", models.FloatField()),
                ("temperature", models.FloatField()),
                ("light", models.FloatField()),
                ("offline_at", models.DateTimeField(blank=True, null=True)),
                ("last_alert_at", models.DateTimeField(blank=True, null=True)),
                ("last_alert_type", models.CharField(blank=True, max_length=40, null=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "verbose_name_plural": "sensor latest",
                "db_table": '"read_telemetry"."sensor_latest"',
                "ordering": ["-recorded_at"],
                "indexes": [models.Index(fields=["plant_id"], name="ix_sensor_latest_plant_id")],
            },
        ),
        migrations.CreateModel(
            name="TelemetryRollup",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("sensor_id", models.UUIDField()),
                ("bucket_start", models.DateTimeField()),
                ("plant_id", models.UUIDField()),
                ("moisture_min", models.FloatField()),
                ("moisture_max", models.FloatField()),
                ("moisture_avg", models.FloatField()),
                ("temperature_min", models.FloatField()),
                ("temperature_max", models.FloatField()),
                ("temperature_avg", models.FloatField()),
                ("sample_count", models.IntegerField(default=1)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": '"read_telemetry"."telemetry_rollups"',
                "ordering": ["-bucket_start"],
                "indexes": [
                    models.Index(
                        fields=["plant_id", "bucket_start"], name="ix_rollups_plant_bucket"
                    ),
                    models.Index(fields=["bucket_start"], name="ix_rollups_bucket_start"),
                ],
                "constraints": [
                    models.UniqueConstraint(
                        fields=("sensor_id", "bucket_start"),
                        name="uq_telemetry_rollups_sensor_id_bucket_start",
                    )
                ],
            },
        ),
    ]
