"""Checkpoints of the journal streams.

Replaying a long journal from version 1 is correct but wasteful, so every
``SNAPSHOT_EVERY`` events the appending transaction writes the stream's serialised
:class:`~plantkeeper.domain.journal.state.JournalState` here. A reader asks for the
newest row (``ORDER BY version DESC LIMIT 1``, which the composite primary key's
index serves) and replays only the events after it.

``(stream_id, version)`` rather than ``stream_id`` alone: a stream keeps its
checkpoint history, and ``version`` is what says where the next replay starts.
Retention and pruning are deliberately out of scope (``docs/event-sourcing.md``).

Revision ID: 0005
Revises: 0004
Create Date: 2026-09-23

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the journal checkpoints table."""
    op.create_table(
        "journal_snapshots",
        sa.Column("stream_id", sa.Uuid(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("state", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("stream_id", "version", name=op.f("pk_journal_snapshots")),
        schema="write_journal",
    )


def downgrade() -> None:
    """Drop the checkpoints, leaving the streams and the schema alone."""
    op.drop_table("journal_snapshots", schema="write_journal")
