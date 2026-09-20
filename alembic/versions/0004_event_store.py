"""The Journal's event store.

Phase 6 makes the journal an event-sourced aggregate: the facts live in a stream,
and ``(stream_id, version)`` is the optimistic lock that makes "two writers, one
version" impossible rather than merely unlikely.

* ``stream_id`` is the plant id, because a journal is per plant and a stream holds
  nothing but ``JournalEntryAdded`` (``docs/event-sourcing.md``);
* ``payload`` is the event document exactly as the outbox stores it, so a stored
  event and a published one are the same JSON;
* ``global_position`` is the store's own append order across every stream, which is
  what gives ``load_all`` a deterministic order without a timestamp tie-break;
* ``event_id`` is unique as well: appending the same event twice cannot yield two
  rows, whatever version it lands on.

The DDL is written out rather than generated from the models: a migration has to keep
working after the models move on, and the two unique keys plus the version check are
the parts a reader needs to see.

Revision ID: 0004
Revises: 0003
Create Date: 2026-09-23

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the event store."""
    op.create_table(
        "event_store",
        sa.Column("global_position", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("stream_id", sa.Uuid(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.String(length=255), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("version > 0", name=op.f("ck_event_store_version_positive")),
        sa.PrimaryKeyConstraint("global_position", name=op.f("pk_event_store")),
        sa.UniqueConstraint("stream_id", "version", name="uq_event_store_stream_id_version"),
        sa.UniqueConstraint("event_id", name="uq_event_store_event_id"),
        schema="write_journal",
    )


def downgrade() -> None:
    """Drop the event store.

    The ``write_journal`` schema itself stays: ``docker/postgres/init`` created it,
    so dropping it would delete an object this migration does not own.
    """
    op.drop_table("event_store", schema="write_journal")
