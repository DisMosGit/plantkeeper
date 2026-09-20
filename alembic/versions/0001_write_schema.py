"""Write side schema.

Creates the eight Postgres schemas the write side owns and every table in them:
one schema per bounded context plus ``write_shared`` for the transactional
outbox and the HTTP idempotency keys.

The schemas are created here as well as in ``docker/postgres/init`` because that
init script only runs when the Postgres volume is first initialised: a database
whose volume already existed would otherwise have to be recreated by hand.

Revision ID: 0001
Revises:
Create Date: 2026-09-20

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateSchema

from plantkeeper.infrastructure.persistence.schemas import ALL_WRITE_SCHEMAS

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the schemas and all write-side tables."""
    for schema in ALL_WRITE_SCHEMAS:
        op.execute(CreateSchema(schema, if_not_exists=True))

    # --- Garden ---------------------------------------------------------------
    op.create_table(
        "households",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_households")),
        schema="write_garden",
    )
    op.create_table(
        "plants",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("household_id", sa.Uuid(), nullable=False),
        sa.Column("species_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("location", sa.String(length=100), nullable=False),
        sa.Column("added_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_watered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_repotted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("removed", sa.Boolean(), nullable=False),
        sa.ForeignKeyConstraint(
            ["household_id"],
            ["write_garden.households.id"],
            name=op.f("fk_plants_household_id_households"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_plants")),
        schema="write_garden",
    )
    op.create_index(
        "ix_plants_household_id_removed",
        "plants",
        ["household_id", "removed"],
        schema="write_garden",
    )

    # --- Care -----------------------------------------------------------------
    op.create_table(
        "care_schedules",
        sa.Column("plant_id", sa.Uuid(), nullable=False),
        sa.Column("watering_interval", sa.Interval(), nullable=False),
        sa.Column("next_watering_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("plant_id", name=op.f("pk_care_schedules")),
        schema="write_care",
    )
    op.create_index(
        "ix_care_schedules_next_watering_at",
        "care_schedules",
        ["next_watering_at"],
        schema="write_care",
    )

    # --- Catalog --------------------------------------------------------------
    op.create_table(
        "species",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("scientific_name", sa.String(length=200), nullable=False),
        sa.Column("common_name", sa.String(length=200), nullable=False),
        sa.Column("watering_interval", sa.Interval(), nullable=False),
        sa.Column("light_requirement", sa.String(length=20), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_species")),
        schema="write_catalog",
    )

    # --- Journal --------------------------------------------------------------
    op.create_table(
        "journal_entries",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("plant_id", sa.Uuid(), nullable=False),
        sa.Column("entry_type", sa.String(length=20), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("note", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_journal_entries")),
        schema="write_journal",
    )
    op.create_index(
        "ix_journal_entries_plant_id_occurred_at",
        "journal_entries",
        ["plant_id", "occurred_at"],
        schema="write_journal",
    )

    # --- Telemetry ------------------------------------------------------------
    op.create_table(
        "sensors",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("plant_id", sa.Uuid(), nullable=False),
        sa.Column("added_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_sensors")),
        schema="write_telemetry",
    )
    op.create_index("ix_sensors_plant_id", "sensors", ["plant_id"], schema="write_telemetry")

    # --- Notifications --------------------------------------------------------
    op.create_table(
        "notifications",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("household_id", sa.Uuid(), nullable=False),
        sa.Column("notification_type", sa.String(length=40), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("read_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_notifications")),
        schema="write_notifications",
    )
    op.create_index(
        "ix_notifications_household_id_read_at",
        "notifications",
        ["household_id", "read_at"],
        schema="write_notifications",
    )

    # --- Shared ---------------------------------------------------------------
    op.create_table(
        "outbox",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("event_name", sa.String(length=255), nullable=False),
        sa.Column("topic", sa.String(length=255), nullable=False),
        sa.Column("partition_key", sa.String(length=255), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("dead_lettered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempts", sa.SmallInteger(), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_outbox")),
        sa.UniqueConstraint("event_id", name=op.f("uq_outbox_event_id")),
        schema="write_shared",
    )
    op.create_index(
        "ix_outbox_unpublished",
        "outbox",
        ["id"],
        schema="write_shared",
        # The relay only ever asks for rows that are neither published nor
        # dead-lettered, so the index covers exactly those.
        postgresql_where=sa.text("published_at IS NULL AND dead_lettered_at IS NULL"),
    )
    op.create_table(
        "idempotency_keys",
        sa.Column("key", sa.String(length=255), nullable=False),
        sa.Column("request_hash", sa.String(length=64), nullable=False),
        sa.Column("status_code", sa.Integer(), nullable=False),
        sa.Column("response", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("key", name=op.f("pk_idempotency_keys")),
        schema="write_shared",
    )
    op.create_index(
        "ix_idempotency_keys_created_at",
        "idempotency_keys",
        ["created_at"],
        schema="write_shared",
    )


def downgrade() -> None:
    """Drop every write-side table.

    The schemas themselves are left in place: ``docker/postgres/init`` created
    some of them before this migration ran, so dropping them would delete
    objects this migration does not own.
    """
    op.drop_index("ix_idempotency_keys_created_at", "idempotency_keys", schema="write_shared")
    op.drop_table("idempotency_keys", schema="write_shared")
    op.drop_index("ix_outbox_unpublished", "outbox", schema="write_shared")
    op.drop_table("outbox", schema="write_shared")

    op.drop_index(
        "ix_notifications_household_id_read_at", "notifications", schema="write_notifications"
    )
    op.drop_table("notifications", schema="write_notifications")

    op.drop_index("ix_sensors_plant_id", "sensors", schema="write_telemetry")
    op.drop_table("sensors", schema="write_telemetry")

    op.drop_index(
        "ix_journal_entries_plant_id_occurred_at", "journal_entries", schema="write_journal"
    )
    op.drop_table("journal_entries", schema="write_journal")

    op.drop_table("species", schema="write_catalog")

    op.drop_index("ix_care_schedules_next_watering_at", "care_schedules", schema="write_care")
    op.drop_table("care_schedules", schema="write_care")

    op.drop_index("ix_plants_household_id_removed", "plants", schema="write_garden")
    op.drop_table("plants", schema="write_garden")
    op.drop_table("households", schema="write_garden")
