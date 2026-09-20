"""The Postgres schemas the write side owns.

One schema per bounded context plus one for the tables that serve *every*
context: the transactional outbox and the HTTP idempotency keys. The names match
``docker/postgres/init/01-schemas.sql``; Alembic creates them too, so a database
whose volume already existed is migrated just as well as a fresh one.

The read side adds one schema of its own, ``read_analytics``, owned by Django and
created by ``apps/admin``'s migration. It is named here so the read models and
the schema-creating migration cannot drift from the schema the infrastructure
already provisions.
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
"""Where the projections write and Django Admin reads; Django owns its migrations."""

ALL_READ_SCHEMAS: Final = (READ_ANALYTICS,)
