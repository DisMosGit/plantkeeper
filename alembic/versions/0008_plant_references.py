"""Event-maintained plant references, one per consuming context.

The notification and journal consumers each needed one fact from the Garden context:
which household a plant belongs to. They read it out of ``write_garden``, which made
them a second reader of another context's table — forbidden by ``event-transport``
and invisible to the import rules, because the coupling was a repository call rather
than an import.

Each context now keeps its own row, filled from ``PlantAdded``/``PlantMoved``/
``PlantRemoved`` under its own consumer group. One writer per table, and the fact
travels as an event the consumer already reacts to.

Two tables in two schemas rather than one shared table: a shared table would be the
same coupling under a different name, which is the whole thing being removed.

Revision ID: 0008
Revises: 0007
Create Date: 2026-10-02

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0008"
down_revision: str | None = "0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLES: tuple[tuple[str, str], ...] = (
    ("write_notifications", "the notifications context's view of a plant"),
    ("write_journal", "the journal context's view of a plant"),
)


def upgrade() -> None:
    """Create one ``plant_refs`` table per consuming context."""
    for schema, _purpose in _TABLES:
        op.create_table(
            "plant_refs",
            sa.Column("plant_id", sa.Uuid(), nullable=False),
            sa.Column("household_id", sa.Uuid(), nullable=False),
            sa.Column("name", sa.String(length=255), nullable=False),
            sa.Column("location", sa.String(length=255), nullable=False),
            sa.Column("seen_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column(
                "updated_at",
                sa.DateTime(timezone=True),
                server_default=sa.text("now()"),
                nullable=False,
            ),
            sa.PrimaryKeyConstraint("plant_id", name=op.f("pk_plant_refs")),
            schema=schema,
        )
        op.create_index(
            "ix_plant_refs_household_id",
            "plant_refs",
            ["household_id"],
            unique=False,
            schema=schema,
        )


def downgrade() -> None:
    """Drop both reference tables."""
    for schema, _purpose in _TABLES:
        op.drop_index("ix_plant_refs_household_id", table_name="plant_refs", schema=schema)
        op.drop_table("plant_refs", schema=schema)
