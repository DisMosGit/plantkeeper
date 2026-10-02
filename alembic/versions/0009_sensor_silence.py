"""The sensor's silence state: when it last reported, and when that silence was announced.

``SensorOffline`` is in the catalogue but could never fire: no timer inspected
silence, and the sensor's last-seen instant was not persisted with its readings. The
gateway chose to leave ``sensors.last_seen_at`` alone, on the grounds that the
readings table already answered "when did this sensor last report" — true for a
single sensor, false for the question the timer asks, which is "which sensors have
gone quiet", over every registered sensor, every tick.

So the ingress now advances ``last_seen_at`` with each stored reading, and
``offline_announced_at`` records when the current silence was announced (``None``
while the sensor reports, cleared by the next reading). The two columns make the
timer's query an index-free scan of the registry rather than a scan of the readings.

Both columns are additive and nullable, so the migration is safe on a populated
database: an existing sensor simply has no silence state until its next reading.

Revision ID: 0009
Revises: 0008
Create Date: 2026-10-02

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0009"
down_revision: str | None = "0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add the silence state to the sensor registry."""
    op.add_column(
        "sensors",
        sa.Column("offline_announced_at", sa.DateTime(timezone=True), nullable=True),
        schema="write_telemetry",
    )


def downgrade() -> None:
    """Drop the silence announcement; ``last_seen_at`` predates this change."""
    op.drop_column("sensors", "offline_announced_at", schema="write_telemetry")
