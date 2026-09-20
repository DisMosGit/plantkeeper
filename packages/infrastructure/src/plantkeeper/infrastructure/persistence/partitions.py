"""Monthly partitions of ``write_telemetry.sensor_readings``.

The table is RANGE-partitioned on ``recorded_at`` because telemetry is the only
high-volume, strictly time-ordered data in the project: a month per partition
keeps each index small, and a month's worth of readings can be dropped as a unit.

The partition bounds have to exist before a row that needs them arrives, so this
module owns the arithmetic and the DDL:

* Alembic ``0003`` calls :func:`ensure_telemetry_partitions` while it creates the
  table, so a fresh database (and a migrated one) starts with the current month;
* :class:`~plantkeeper.infrastructure.scheduling.telemetry_partitions.TelemetryPartitionJob`
  calls it regularly so the window keeps moving;
* a ``DEFAULT`` partition is always created, so a row for an out-of-window instant
  — a replayed capture, a badly set clock — lands somewhere rather than failing
  the write that carries it.

Every statement is ``CREATE TABLE IF NOT EXISTS``: the job runs on a schedule, and
running the same creation twice must be a no-op rather than an error.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Final

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from plantkeeper.infrastructure.persistence.models.telemetry import SensorReadingModel
from plantkeeper.infrastructure.persistence.schemas import WRITE_TELEMETRY

TELEMETRY_PARTITIONED_TABLE: Final = f"{WRITE_TELEMETRY}.{SensorReadingModel.__tablename__}"

DEFAULT_PARTITION_SUFFIX: Final = "default"
"""The catch-all partition's suffix; it has no bounds to name it by."""

MONTHS_AHEAD: Final = 3
"""How many months past the current one the job keeps created."""

MONTHS_PER_YEAR: Final = 12


def month_of(instant: date | datetime) -> date:
    """Return the first day of the month ``instant`` falls in."""
    day = instant.date() if isinstance(instant, datetime) else instant
    return day.replace(day=1)


def next_month(month: date) -> date:
    """Return the first day of the month after ``month``."""
    year, number = month.year, month.month
    if number == MONTHS_PER_YEAR:
        return date(year + 1, 1, 1)
    return date(year, number + 1, 1)


def add_months(month: date, count: int) -> date:
    """Return the first day of the month ``count`` months after ``month``."""
    result = month_of(month)
    for _ in range(count):
        result = next_month(result)
    return result


def partition_name(month: date, *, table: str = TELEMETRY_PARTITIONED_TABLE) -> str:
    """Return the qualified name of the partition holding ``month``."""
    return f"{table}_{month.year:04d}{month.month:02d}"


def months_to_create(*, today: date | None = None, months_ahead: int = MONTHS_AHEAD) -> list[date]:
    """Return the months from the current one through ``months_ahead`` months on.

    The current month is included even during its last days: creating it twice is
    a no-op, and skipping it would make the function's contract depend on the day
    of the month.
    """
    if months_ahead < 0:
        raise ValueError("months_ahead cannot be negative")
    start = month_of(today if today is not None else datetime.now(UTC).date())
    return [add_months(start, offset) for offset in range(months_ahead + 1)]


def partition_statement(month: date, *, table: str = TELEMETRY_PARTITIONED_TABLE) -> str:
    """Return the ``CREATE TABLE IF NOT EXISTS`` that creates one month's partition."""
    return (
        f"CREATE TABLE IF NOT EXISTS {partition_name(month, table=table)} "
        f"PARTITION OF {table} FOR VALUES FROM ('{month.isoformat()}') "
        f"TO ('{next_month(month).isoformat()}')"
    )


def default_partition_name(*, table: str = TELEMETRY_PARTITIONED_TABLE) -> str:
    """Return the qualified name of the catch-all partition."""
    return f"{table}_{DEFAULT_PARTITION_SUFFIX}"


def default_partition_statement(*, table: str = TELEMETRY_PARTITIONED_TABLE) -> str:
    """Return the ``CREATE TABLE IF NOT EXISTS`` for the catch-all partition."""
    name = default_partition_name(table=table)
    return f"CREATE TABLE IF NOT EXISTS {name} PARTITION OF {table} DEFAULT"


def statements_for(
    *, today: date | None = None, months_ahead: int = MONTHS_AHEAD
) -> list[tuple[str, str]]:
    """Return ``(name, statement)`` for every partition a healthy window needs.

    The default partition comes first so a single call on a fresh table leaves it
    usable even if a later statement fails.
    """
    statements = [(default_partition_name(), default_partition_statement())]
    for month in months_to_create(today=today, months_ahead=months_ahead):
        statements.append((partition_name(month), partition_statement(month)))
    return statements


async def existing_partitions(session: AsyncSession) -> set[str]:
    """Return the partitions of ``sensor_readings`` that already exist.

    Read from the catalogue rather than inferred from a DDL result: whether
    ``CREATE TABLE IF NOT EXISTS`` created anything is not reported in a way a
    driver consistently exposes, while ``pg_class`` answers the question exactly.
    The join is on ``pg_inherits``, which is what makes a table a partition of its
    parent rather than merely a table in the same schema.
    """
    statement = text(
        """
        SELECT child.relname
        FROM pg_inherits
        JOIN pg_class AS parent ON parent.oid = pg_inherits.inhparent
        JOIN pg_class AS child ON child.oid = pg_inherits.inhrelid
        JOIN pg_namespace AS namespace ON namespace.oid = parent.relnamespace
        WHERE namespace.nspname = :schema AND parent.relname = :table
        """
    )
    result = await session.execute(
        statement, {"schema": WRITE_TELEMETRY, "table": SensorReadingModel.__tablename__}
    )
    return {row[0] for row in result}


async def parent_exists(session: AsyncSession) -> bool:
    """Whether ``write_telemetry.sensor_readings`` has been created.

    The job runs on a timer, and a worker started before ``make migrate`` must
    report that there is nothing to partition rather than failing on a table that
    does not exist yet.
    """
    statement = text(
        "SELECT 1 FROM pg_class AS c JOIN pg_namespace AS n ON n.oid = c.relnamespace "
        "WHERE n.nspname = :schema AND c.relname = :table"
    )
    result = await session.execute(
        statement, {"schema": WRITE_TELEMETRY, "table": SensorReadingModel.__tablename__}
    )
    return result.first() is not None


async def ensure_telemetry_partitions(
    session: AsyncSession,
    *,
    today: date | None = None,
    months_ahead: int = MONTHS_AHEAD,
) -> list[str]:
    """Create whatever is missing and return the names that were created.

    The caller owns the transaction; ``session.commit()`` is deliberately not
    called here so a later caller can decide when the DDL lands. An absent parent
    table is not an error but produces no partitions, which is what makes the first
    run of the worker against an unmigrated database harmless.
    """
    if not await parent_exists(session):
        return []
    present = await existing_partitions(session)
    created: list[str] = []
    for name, statement in statements_for(today=today, months_ahead=months_ahead):
        unqualified = name.rpartition(".")[2]
        if unqualified in present:
            continue
        await session.execute(text(statement))
        created.append(name)
    return created
