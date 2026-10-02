"""Recorded cross-context commands.

A process manager coordinates contexts, but it must not write one it does not own.
The onboarding saga used to write a care schedule and a notification directly
through the shared session; it now records a command instead, in the same
transaction that checkpoints the process, and the command dispatcher runs it
through the owning context's own handler and unit of work.

The table is therefore the hand-off between a saga and a context, which is why

* ``idempotency_key`` is unique — ``(saga_id, step_no)`` derived — so a
  re-execution of the same recorded command cannot produce a second effect;
* ``(status, id)`` is indexed partially on the two retryable statuses, which is
  exactly the dispatcher's claim query;
* ``claimed_at`` is a lease, so a dispatcher that dies mid-execution does not
  strand its intents.

Revision ID: 0007
Revises: 0006
Create Date: 2026-10-02

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0007"
down_revision: str | None = "0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the recorded-command table."""
    op.create_table(
        "saga_intents",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("saga_id", sa.Uuid(), nullable=False),
        sa.Column("step_no", sa.Integer(), nullable=False),
        sa.Column("command_name", sa.String(length=255), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "status",
            sa.String(length=20),
            server_default=sa.text("'pending'"),
            nullable=False,
        ),
        sa.Column("attempts", sa.SmallInteger(), server_default=sa.text("0"), nullable=False),
        sa.Column("idempotency_key", sa.String(length=255), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("executed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_saga_intents")),
        sa.UniqueConstraint("idempotency_key", name="uq_saga_intents_idempotency_key"),
        schema="write_shared",
    )
    op.create_index(
        "ix_saga_intents_pending",
        "saga_intents",
        ["id"],
        unique=False,
        schema="write_shared",
        postgresql_where=sa.text("status IN ('pending', 'failed')"),
    )
    op.create_index(
        "ix_saga_intents_saga_id",
        "saga_intents",
        ["saga_id", "step_no"],
        unique=False,
        schema="write_shared",
    )


def downgrade() -> None:
    """Drop the recorded-command table and its indexes."""
    op.drop_index("ix_saga_intents_saga_id", table_name="saga_intents", schema="write_shared")
    op.drop_index("ix_saga_intents_pending", table_name="saga_intents", schema="write_shared")
    op.drop_table("saga_intents", schema="write_shared")
