"""The Postgres schemas the write side owns.

One schema per bounded context plus one for the tables that serve *every*
context: the transactional outbox and the HTTP idempotency keys. The names match
``docker/postgres/init/01-schemas.sql``; Alembic creates them too, so a database
whose volume already existed is migrated just as well as a fresh one.
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
