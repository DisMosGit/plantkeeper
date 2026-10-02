"""The partition-window arithmetic and the DDL it produces.

The table itself is created by Alembic ``0003`` and exercised in
``tests/integration/test_telemetry_ingest.py``; what is checked here is the part
that has to be right without a database — the month arithmetic at year and leap
boundaries, and the fact that every statement is the idempotent
``CREATE TABLE IF NOT EXISTS`` form.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import pytest

from plantkeeper.infrastructure.persistence.partitions import (
    TELEMETRY_PARTITIONED_TABLE,
    add_months,
    default_partition_name,
    default_partition_statement,
    drop_expired_partitions,
    drop_statement,
    ensure_telemetry_partitions,
    expired_partitions,
    month_from_partition_name,
    month_of,
    months_to_create,
    next_month,
    partition_name,
    partition_statement,
    previous_month,
    retention_cutoff,
    statements_for,
)


class FakeSession:
    """A session that answers the catalogue query and records the DDL."""

    def __init__(self, *, parent: bool, partitions: set[str] | None = None) -> None:
        self._parent = parent
        self._partitions = partitions or set()
        self.executed: list[str] = []

    async def execute(self, statement: object, params: dict[str, Any] | None = None) -> Any:
        """Answer the parent lookup, then record every statement issued."""
        text = str(statement)
        if "pg_inherits" in text:
            return FakeResult([(name,) for name in self._partitions])
        if "pg_class" in text:
            return FakeResult([(1,)] if self._parent else [])
        self.executed.append(text)
        return FakeResult([])


class FakeResult:
    """The single thing the callers ask of a result: its rows."""

    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self._rows = rows

    def __iter__(self) -> Any:
        return iter(self._rows)

    def first(self) -> tuple[object, ...] | None:
        """Return the first row, or ``None``."""
        return self._rows[0] if self._rows else None


def test_a_month_is_the_first_day_of_that_month() -> None:
    assert month_of(date(2026, 9, 20)) == date(2026, 9, 1)
    assert month_of(datetime(2026, 9, 20, 12, 0, tzinfo=UTC)) == date(2026, 9, 1)


def test_the_next_month_rolls_the_year_over() -> None:
    assert next_month(date(2026, 9, 1)) == date(2026, 10, 1)
    assert next_month(date(2026, 12, 1)) == date(2027, 1, 1)
    assert next_month(date(2024, 2, 1)) == date(2024, 3, 1)


def test_adding_months_walks_one_month_at_a_time() -> None:
    assert add_months(date(2026, 11, 1), 3) == date(2027, 2, 1)
    assert add_months(date(2026, 9, 1), 0) == date(2026, 9, 1)


def test_the_window_starts_at_the_current_month_and_runs_ahead() -> None:
    months = months_to_create(today=date(2026, 9, 20), months_ahead=3)

    assert months == [date(2026, 9, 1), date(2026, 10, 1), date(2026, 11, 1), date(2026, 12, 1)]


def test_the_window_of_a_window_can_be_empty() -> None:
    assert months_to_create(today=date(2026, 9, 20), months_ahead=0) == [date(2026, 9, 1)]


def test_a_negative_window_is_refused() -> None:
    with pytest.raises(ValueError, match="months_ahead"):
        months_to_create(today=date(2026, 9, 20), months_ahead=-1)
    with pytest.raises(ValueError, match="months_ahead"):
        statements_for(today=date(2026, 9, 20), months_ahead=-1)


def test_a_partition_is_named_after_its_month() -> None:
    assert partition_name(date(2026, 9, 1)) == f"{TELEMETRY_PARTITIONED_TABLE}_202609"
    assert partition_name(date(2026, 12, 1)) == f"{TELEMETRY_PARTITIONED_TABLE}_202612"


def test_a_partition_statement_is_bounded_and_idempotent() -> None:
    statement = partition_statement(date(2026, 9, 1))

    assert statement.startswith("CREATE TABLE IF NOT EXISTS ")
    assert f"PARTITION OF {TELEMETRY_PARTITIONED_TABLE}" in statement
    assert "FOR VALUES FROM ('2026-09-01') TO ('2026-10-01')" in statement


def test_the_catch_all_partition_has_no_bounds() -> None:
    statement = default_partition_statement()

    assert default_partition_name() == f"{TELEMETRY_PARTITIONED_TABLE}_default"
    assert statement == (
        f"CREATE TABLE IF NOT EXISTS {TELEMETRY_PARTITIONED_TABLE}_default "
        f"PARTITION OF {TELEMETRY_PARTITIONED_TABLE} DEFAULT"
    )


def test_the_statements_cover_the_default_partition_and_the_window() -> None:
    statements = statements_for(today=date(2026, 9, 20), months_ahead=2)

    names = [name for name, _ in statements]
    assert names == [
        f"{TELEMETRY_PARTITIONED_TABLE}_default",
        f"{TELEMETRY_PARTITIONED_TABLE}_202609",
        f"{TELEMETRY_PARTITIONED_TABLE}_202610",
        f"{TELEMETRY_PARTITIONED_TABLE}_202611",
    ]
    assert all(statement.startswith("CREATE TABLE IF NOT EXISTS ") for _, statement in statements)


async def test_an_unmigrated_database_has_nothing_to_partition() -> None:
    """The job runs on a timer; a worker started before ``make migrate`` must not fail."""
    session = FakeSession(parent=False)

    created = await ensure_telemetry_partitions(
        session,  # type: ignore[arg-type]
        today=date(2026, 9, 20),
        months_ahead=3,
    )

    assert created == []
    assert session.executed == []


async def test_only_the_missing_partitions_are_created() -> None:
    session = FakeSession(
        parent=True,
        # The catalogue reports child table names unqualified.
        partitions={"sensor_readings_default"},
    )

    created = await ensure_telemetry_partitions(
        session,  # type: ignore[arg-type]
        today=date(2026, 9, 20),
        months_ahead=1,
    )

    assert created == [
        f"{TELEMETRY_PARTITIONED_TABLE}_202609",
        f"{TELEMETRY_PARTITIONED_TABLE}_202610",
    ]
    assert len(session.executed) == 2


def test_the_previous_month_rolls_the_year_back() -> None:
    assert previous_month(date(2026, 1, 1)) == date(2025, 12, 1)
    assert previous_month(date(2026, 9, 1)) == date(2026, 8, 1)


def test_adding_months_can_walk_backwards() -> None:
    assert add_months(date(2026, 2, 1), -3) == date(2025, 11, 1)
    assert add_months(date(2026, 1, 1), -12) == date(2025, 1, 1)
    assert add_months(date(2026, 9, 20), 0) == date(2026, 9, 1)


def test_the_retention_cutoff_is_whole_months_before_the_current_one() -> None:
    assert retention_cutoff(today=date(2026, 7, 15), retention_months=12) == date(2025, 7, 1)
    assert retention_cutoff(today=date(2026, 1, 3), retention_months=1) == date(2025, 12, 1)
    assert retention_cutoff(today=date(2026, 1, 3), retention_months=0) == date(2026, 1, 1)


def test_a_negative_retention_window_is_refused() -> None:
    with pytest.raises(ValueError, match="retention_months"):
        retention_cutoff(today=date(2026, 7, 15), retention_months=-1)


def test_a_partition_name_is_read_back_as_its_month() -> None:
    assert month_from_partition_name("sensor_readings_202609") == date(2026, 9, 1)
    assert month_from_partition_name("sensor_readings_default") is None
    assert month_from_partition_name("sensor_readings") is None
    assert month_from_partition_name("sensor_readings_20260") is None
    assert month_from_partition_name("something_else_202609") is None


def test_only_a_partition_entirely_beyond_the_window_expires() -> None:
    present = {
        "sensor_readings_202505",
        "sensor_readings_202506",
        "sensor_readings_202507",
        "sensor_readings_202607",
        "sensor_readings_default",
    }

    expired = expired_partitions(present, today=date(2026, 7, 15), retention_months=12)

    # June 2025 ends exactly at the cutoff and is entirely outside; July 2025
    # overlaps the window and stays.
    assert expired == [
        (date(2025, 5, 1), f"{TELEMETRY_PARTITIONED_TABLE}_202505"),
        (date(2025, 6, 1), f"{TELEMETRY_PARTITIONED_TABLE}_202506"),
    ]


def test_a_drop_statement_is_idempotent() -> None:
    assert drop_statement(date(2026, 9, 1)) == (
        f"DROP TABLE IF EXISTS {TELEMETRY_PARTITIONED_TABLE}_202609"
    )


async def test_an_unmigrated_database_has_nothing_to_drop() -> None:
    session = FakeSession(parent=False)

    dropped = await drop_expired_partitions(
        session,  # type: ignore[arg-type]
        today=date(2026, 7, 15),
        retention_months=12,
    )

    assert dropped == []
    assert session.executed == []


async def test_only_the_expired_partitions_are_dropped() -> None:
    session = FakeSession(
        parent=True,
        partitions={"sensor_readings_default", "sensor_readings_202505", "sensor_readings_202506"},
    )

    dropped = await drop_expired_partitions(
        session,  # type: ignore[arg-type]
        today=date(2026, 7, 15),
        retention_months=12,
    )

    assert dropped == [
        f"{TELEMETRY_PARTITIONED_TABLE}_202505",
        f"{TELEMETRY_PARTITIONED_TABLE}_202506",
    ]
    # The catch-all partition is never dropped: it holds readings whose month never
    # had a partition of its own.
    assert all("default" not in statement for statement in session.executed)
