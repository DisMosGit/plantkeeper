"""Read side schema.

Creates the ``read_analytics`` schema the projections write and Django Admin
reads, and every table in it. The write side's ``write_*`` schemas are Alembic's;
this one is Django's, because the read models are Django models.

The schema is created here as well as in ``docker/postgres/init`` because that
init script only runs when the Postgres volume is first initialised, and because
the test suite starts an empty Postgres container with no init script at all.

Generated with ``manage.py makemigrations read_models``, then edited: the
``RunSQL`` is prepended so the schema exists before the first ``CREATE TABLE``.
"""

from __future__ import annotations

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    """Create the read models."""

    initial = True

    dependencies: list[tuple[str, str]] = []

    operations = [
        migrations.RunSQL(
            sql="CREATE SCHEMA IF NOT EXISTS read_analytics",
            # The schema is shared with ``docker/postgres/init``, exactly like
            # Alembic 0001 leaves the write schemas in place on a downgrade.
            reverse_sql=migrations.RunSQL.noop,
        ),
        migrations.CreateModel(
            name="SpeciesReadModel",
            fields=[
                ("species_id", models.UUIDField(primary_key=True, serialize=False)),
                ("scientific_name", models.CharField(max_length=200)),
                ("common_name", models.CharField(max_length=200)),
                ("watering_interval", models.DurationField()),
                ("light_requirement", models.CharField(max_length=20)),
                ("version", models.IntegerField(default=1)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "verbose_name_plural": "species",
                "db_table": '"read_analytics"."species"',
                "ordering": ["common_name"],
            },
        ),
        migrations.CreateModel(
            name="CareReadModel",
            fields=[
                ("plant_id", models.UUIDField(primary_key=True, serialize=False)),
                ("watering_interval", models.DurationField()),
                ("next_watering_at", models.DateTimeField()),
                ("last_watered_at", models.DateTimeField(blank=True, null=True)),
                ("version", models.IntegerField(default=1)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": '"read_analytics"."care_schedules"',
                "ordering": ["next_watering_at"],
                "indexes": [
                    models.Index(fields=["next_watering_at"], name="ix_care_next_watering_at")
                ],
            },
        ),
        migrations.CreateModel(
            name="NotificationReadModel",
            fields=[
                ("notification_id", models.UUIDField(primary_key=True, serialize=False)),
                ("household_id", models.UUIDField()),
                ("notification_type", models.CharField(max_length=40)),
                ("payload", models.JSONField(default=dict)),
                ("created_at", models.DateTimeField()),
                ("read_at", models.DateTimeField(blank=True, null=True)),
            ],
            options={
                "db_table": '"read_analytics"."notifications"',
                "ordering": ["-created_at"],
                "indexes": [
                    models.Index(
                        fields=["household_id", "read_at"], name="ix_notifications_house_read"
                    )
                ],
            },
        ),
        migrations.CreateModel(
            name="PlantReadModel",
            fields=[
                ("plant_id", models.UUIDField(primary_key=True, serialize=False)),
                ("household_id", models.UUIDField(blank=True, null=True)),
                ("species_id", models.UUIDField(blank=True, null=True)),
                ("name", models.CharField(blank=True, max_length=200, null=True)),
                ("species_name", models.CharField(blank=True, max_length=200, null=True)),
                ("location", models.CharField(blank=True, max_length=100, null=True)),
                ("added_at", models.DateTimeField(blank=True, null=True)),
                ("removed", models.BooleanField(default=False)),
                ("next_watering_at", models.DateTimeField(blank=True, null=True)),
                ("onboarded_at", models.DateTimeField(blank=True, null=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": '"read_analytics"."plants"',
                "ordering": ["added_at"],
                "indexes": [
                    models.Index(
                        fields=["household_id", "removed"], name="ix_plants_household_id_removed"
                    )
                ],
            },
        ),
        migrations.CreateModel(
            name="ProcessedEvent",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("consumer_group", models.CharField(max_length=100)),
                ("event_id", models.UUIDField()),
                ("processed_at", models.DateTimeField(auto_now_add=True)),
            ],
            options={
                "db_table": '"read_analytics"."processed_events"',
                "indexes": [models.Index(fields=["event_id"], name="ix_processed_events_event_id")],
                "constraints": [
                    models.UniqueConstraint(
                        fields=("consumer_group", "event_id"),
                        name="uq_processed_events_consumer_group_event_id",
                    )
                ],
            },
        ),
        migrations.CreateModel(
            name="JournalReadModel",
            fields=[
                ("entry_id", models.UUIDField(primary_key=True, serialize=False)),
                ("entry_type", models.CharField(max_length=20)),
                ("note", models.TextField(blank=True, null=True)),
                ("occurred_at", models.DateTimeField()),
                ("recorded_at", models.DateTimeField()),
                (
                    "plant",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="journal_entries",
                        to="read_models.plantreadmodel",
                    ),
                ),
            ],
            options={
                "verbose_name_plural": "journal entries",
                "db_table": '"read_analytics"."journal_entries"',
                "ordering": ["-occurred_at"],
                "indexes": [
                    models.Index(
                        fields=["plant", "occurred_at"], name="ix_journal_plant_occurred_at"
                    )
                ],
            },
        ),
    ]
