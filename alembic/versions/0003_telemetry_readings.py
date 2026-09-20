"""Telemetry readings and their monthly partitions.

Phase 5 turns ``telemetry.raw`` into facts, and the facts need somewhere to live.
``write_telemetry.sensor_readings`` is the project's only high-volume,
strictly time-ordered table, so it is RANGE-partitioned by month on
``recorded_at``:

* the primary key is ``(sensor_id, recorded_at)`` — PostgreSQL requires every
  unique constraint of a partitioned table to contain the partition key, and that
  pair is exactly the idempotency key the telemetry ingress inserts with
  (``INSERT ... ON CONFLICT (sensor_id, recorded_at) DO NOTHING``);
* ``plant_id`` is denormalised from the sensor registry so a reader never joins to
  ask what a plant's soil has been doing;
* there is no foreign key to ``sensors``: a reading is a log entry, and deleting a
  sensor must not rewrite the history of what it reported.

The partition DDL is written out here rather than generated from the models:
migrations have to keep working after the models move on. The maintenance job
(``plantkeeper.infrastructure.scheduling.telemetry_partitions``) uses the same
`CREATE TABLE IF NOT EXISTS` form to keep the window ahead of the clock, and
``tests/integration/test_telemetry_ingest.py`` pins the two spellings together.

Revision ID: 0003
Revises: 0002
Create Date: 2026-09-22

"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TABLE = "write_telemetry.sensor_readings"
DEFAULT_PARTITION = f"{TABLE}_default"

# Inlined rather than imported from ``plantkeeper``: a migration is a point in
# time, and it must not change meaning when the code it was written against does.
DEFAULT_PARTITION_DDL = (
    f"CREATE TABLE IF NOT EXISTS {DEFAULT_PARTITION} PARTITION OF {TABLE} DEFAULT"
)


def upgrade() -> None:
    """Create the readings table, its indexes and its catch-all partition.

    The default partition is created here and left empty in the happy path: it is
    what makes an out-of-window ``recorded_at`` — a replayed capture, a badly set
    clock — land somewhere instead of failing the write that carries it. The
    monthly partitions are created by the maintenance job, because PostgreSQL
    refuses to create one that would overlap rows already sitting in the default.
    """
    op.execute(
        f"""
        CREATE TABLE {TABLE} (
            sensor_id uuid NOT NULL,
            recorded_at timestamp with time zone NOT NULL,
            plant_id uuid NOT NULL,
            moisture double precision NOT NULL,
            temperature double precision NOT NULL,
            light double precision NOT NULL,
            created_at timestamp with time zone DEFAULT now() NOT NULL,
            CONSTRAINT pk_sensor_readings PRIMARY KEY (sensor_id, recorded_at)
        ) PARTITION BY RANGE (recorded_at)
        """
    )
    op.execute(
        f"CREATE INDEX ix_sensor_readings_plant_id_recorded_at ON {TABLE} (plant_id, recorded_at)"
    )
    op.execute(f"CREATE INDEX ix_sensor_readings_recorded_at ON {TABLE} (recorded_at)")
    op.execute(DEFAULT_PARTITION_DDL)


def downgrade() -> None:
    """Drop the readings table.

    Dropping the parent drops every partition with it, including the ones the
    maintenance job created, so this needs no list of names to work from.
    """
    op.execute(f"DROP TABLE IF EXISTS {TABLE}")
