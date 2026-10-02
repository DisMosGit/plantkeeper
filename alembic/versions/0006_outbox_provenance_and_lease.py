"""Provenance on the outbox, and the relay's lease.

Two changes to ``write_shared.outbox`` that have to travel together because the
claim query reads both:

* **provenance** — ``correlation_id``, ``causation_id``, ``raised_by``,
  ``schema_version`` and ``traceparent``. Stored on the row rather than derived by
  the relay, for the same reason the topic and the partition key already are: the
  write side is the only component that knows the answer, and a relay that
  re-derived it would guess (ADR 0010, ``docs/events.md``).
* **the lease** — ``claimed_at``. The relay claims the rows it works on with
  ``FOR UPDATE SKIP LOCKED`` so several relays can run at once during a deploy,
  and a claim that stops being refreshed is reclaimable by the next poll. Plus the
  ``(partition_key, id)`` partial index the ordering barrier filters on.

Every column is additive and nullable, so the migration is safe on a live table
and a relay running the previous version simply ignores them. ``schema_version``
is ``NOT NULL DEFAULT 1`` because ``1`` is what the policy calls every event
published before the field existed.

Revision ID: 0006
Revises: 0005
Create Date: 2026-10-02

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add the provenance columns, the lease and the ordering index."""
    op.add_column(
        "outbox",
        sa.Column("schema_version", sa.SmallInteger(), server_default=sa.text("1"), nullable=False),
        schema="write_shared",
    )
    op.add_column(
        "outbox",
        sa.Column("raised_by", sa.String(length=255), nullable=True),
        schema="write_shared",
    )
    op.add_column(
        "outbox",
        sa.Column("correlation_id", sa.Uuid(), nullable=True),
        schema="write_shared",
    )
    op.add_column(
        "outbox",
        sa.Column("causation_id", sa.Uuid(), nullable=True),
        schema="write_shared",
    )
    op.add_column(
        "outbox",
        sa.Column("traceparent", sa.String(length=255), nullable=True),
        schema="write_shared",
    )
    op.add_column(
        "outbox",
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        schema="write_shared",
    )
    op.create_index(
        "ix_outbox_partition_key_pending",
        "outbox",
        ["partition_key", "id"],
        unique=False,
        schema="write_shared",
        postgresql_where=sa.text("published_at IS NULL AND dead_lettered_at IS NULL"),
    )


def downgrade() -> None:
    """Drop the ordering index and the columns it and the relay added."""
    op.drop_index(
        "ix_outbox_partition_key_pending",
        table_name="outbox",
        schema="write_shared",
    )
    for column in (
        "claimed_at",
        "traceparent",
        "causation_id",
        "correlation_id",
        "raised_by",
        "schema_version",
    ):
        op.drop_column("outbox", column, schema="write_shared")
