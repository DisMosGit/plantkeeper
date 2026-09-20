"""Saga state, the consumer ledger and the missed-care window.

Phase 4's process managers need three things the write schema did not have:

* ``write_shared.saga_state`` and ``write_shared.saga_log`` — the orchestration
  sagas' executions and their append-only step history. The library's own
  SQLAlchemy storage hard-codes unqualified table names and a second declarative
  base, so this project owns the tables and the storage adapter
  (``docs/adr/0005-orchestration-vs-choreography.md``);
* ``write_shared.processed_events`` — the write-side consumer ledger, the mirror
  of ``read_analytics.processed_events``. ``AGENTS.md`` requires every consumer to
  be idempotent on ``(consumer_group, event_id)`` and the unique constraint is
  where that rule actually holds;
* ``write_care.missed_care_windows`` — the timer state of the choreography
  MissedCareSaga, which has no central process manager to keep it.

Revision ID: 0002
Revises: 0001
Create Date: 2026-09-21

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the saga tables, the consumer ledger and the care window."""
    op.create_table(
        "saga_state",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=30), nullable=False),
        sa.Column("context", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("recovery_attempts", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_saga_state")),
        schema="write_shared",
    )
    op.create_index(
        "ix_saga_state_status_updated_at",
        "saga_state",
        ["name", "status", "updated_at"],
        schema="write_shared",
    )
    op.create_table(
        "saga_log",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("saga_id", sa.Uuid(), nullable=False),
        sa.Column("step_name", sa.String(length=255), nullable=False),
        sa.Column("action", sa.String(length=20), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("details", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["saga_id"],
            ["write_shared.saga_state.id"],
            name=op.f("fk_saga_log_saga_id_saga_state"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_saga_log")),
        schema="write_shared",
    )
    op.create_index(
        "ix_saga_log_saga_id_created_at",
        "saga_log",
        ["saga_id", "created_at", "id"],
        schema="write_shared",
    )
    op.create_table(
        "processed_events",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("consumer_group", sa.String(length=100), nullable=False),
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column(
            "processed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_processed_events")),
        sa.UniqueConstraint(
            "consumer_group",
            "event_id",
            name="uq_processed_events_consumer_group_event_id",
        ),
        schema="write_shared",
    )
    op.create_index(
        "ix_processed_events_event_id",
        "processed_events",
        ["event_id"],
        schema="write_shared",
    )
    op.create_table(
        "missed_care_windows",
        sa.Column("plant_id", sa.Uuid(), nullable=False),
        sa.Column("household_id", sa.Uuid(), nullable=False),
        sa.Column("due_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("grace_deadline", sa.DateTime(timezone=True), nullable=False),
        sa.Column("state", sa.String(length=20), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("plant_id", name=op.f("pk_missed_care_windows")),
        schema="write_care",
    )
    op.create_index(
        "ix_missed_care_windows_state_grace_deadline",
        "missed_care_windows",
        ["state", "grace_deadline"],
        schema="write_care",
    )


def downgrade() -> None:
    """Drop the Phase 4 tables, newest first.

    As in ``0001``, the schemas themselves stay: ``docker/postgres/init`` created
    them, so dropping them would delete objects this migration does not own.
    """
    op.drop_index(
        "ix_missed_care_windows_state_grace_deadline",
        "missed_care_windows",
        schema="write_care",
    )
    op.drop_table("missed_care_windows", schema="write_care")

    op.drop_index("ix_processed_events_event_id", "processed_events", schema="write_shared")
    op.drop_table("processed_events", schema="write_shared")

    op.drop_index("ix_saga_log_saga_id_created_at", "saga_log", schema="write_shared")
    op.drop_table("saga_log", schema="write_shared")

    op.drop_index("ix_saga_state_status_updated_at", "saga_state", schema="write_shared")
    op.drop_table("saga_state", schema="write_shared")
