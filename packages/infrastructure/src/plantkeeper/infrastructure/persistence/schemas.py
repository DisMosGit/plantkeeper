"""The Postgres schemas the write side owns.

One schema per bounded context plus one for the tables that serve *every*
context: the transactional outbox and the HTTP idempotency keys. The names match
``docker/postgres/init/01-schemas.sql``; Alembic creates them too, so a database
whose volume already existed is migrated just as well as a fresh one.

The read side adds schemas of its own, owned by Django and created by
``apps/admin``'s migrations: ``read_analytics`` for the domain read models and
``read_telemetry`` for the telemetry rollups. They are named here so the read
models and the schema-creating migrations cannot drift from the schema the
infrastructure already provisions.
"""

from __future__ import annotations

from typing import Final

WRITE_IDENTITY: Final = "write_identity"
WRITE_CATALOG: Final = "write_catalog"
WRITE_GARDEN: Final = "write_garden"
WRITE_CARE: Final = "write_care"
WRITE_JOURNAL: Final = "write_journal"
WRITE_TELEMETRY: Final = "write_telemetry"
WRITE_NOTIFICATIONS: Final = "write_notifications"
WRITE_SHARED: Final = "write_shared"

ALL_WRITE_SCHEMAS: Final = (
    WRITE_IDENTITY,
    WRITE_CATALOG,
    WRITE_GARDEN,
    WRITE_CARE,
    WRITE_JOURNAL,
    WRITE_TELEMETRY,
    WRITE_NOTIFICATIONS,
    WRITE_SHARED,
)

READ_ANALYTICS: Final = "read_analytics"
"""Where the domain projections write and Django Admin reads; Django owns its migrations."""

READ_TELEMETRY: Final = "read_telemetry"
"""Where the telemetry rollup projection writes; Django owns its migrations.

A schema of its own rather than a corner of ``read_analytics``: the rollups are the
platform's only high-volume read model, they are rebuilt on a schedule of their own
from ``telemetry.events``, and keeping them apart leaves the domain read models'
shape, one-writer rule and rebuild story untouched.
"""

ALL_READ_SCHEMAS: Final = (READ_ANALYTICS, READ_TELEMETRY)
