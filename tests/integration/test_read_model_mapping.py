"""The read side is written by Django and read by SQLAlchemy, so the two must agree.

Every read table has two declarations: a Django model the projections write
through, and a read-only SQLAlchemy mapping the query side reads through. Nothing at
runtime makes them agree — a renamed column, a widened type or a flipped nullability
shows up as a query that returns the wrong thing or fails in production — so the two
are compared here, column by column, for every table the read side maps.

The comparison is deliberately a function of two *shapes* rather than of the models
themselves, so the negative controls at the bottom can hand it a disagreement and
show that it is caught. A parity test that cannot fail is not evidence.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, cast

import pytest
from django.apps import apps
from django.db.models import Field, ForeignKey
from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    Integer,
    Interval,
    String,
    Text,
    Uuid,
)
from sqlalchemy.types import TypeEngine

# Imported for the side effect of registering every read mapping on
# ``ReadModelBase.metadata``; the models themselves are looked up by table.
import plantkeeper.infrastructure.persistence.models.read_telemetry  # noqa: F401
from plantkeeper.infrastructure.persistence.models.read_models import ReadModelBase
from plantkeeper.infrastructure.persistence.schemas import ALL_READ_SCHEMAS

pytestmark = pytest.mark.integration

DJANGO_FAMILIES: dict[str, str] = {
    "UUIDField": "uuid",
    "CharField": "str",
    "TextField": "text",
    "DurationField": "interval",
    "IntegerField": "int",
    "BigIntegerField": "int",
    "SmallIntegerField": "int",
    "BigAutoField": "int",
    "AutoField": "int",
    "FloatField": "float",
    "DateTimeField": "datetime",
    "BooleanField": "bool",
    "JSONField": "json",
}
"""Django internal type names, folded to the families both ORMs can express."""


@dataclass(frozen=True)
class ColumnShape:
    """What one column has to be for the two declarations to agree."""

    family: str
    nullable: bool
    primary_key: bool


def _orm_family(type_: TypeEngine[Any]) -> str:
    """Fold a SQLAlchemy type to the same family vocabulary as Django's."""
    if isinstance(type_, Uuid):
        return "uuid"
    if isinstance(type_, Text):
        return "text"
    if isinstance(type_, String):
        return "str"
    if isinstance(type_, Interval):
        return "interval"
    if isinstance(type_, Boolean):
        return "bool"
    if isinstance(type_, DateTime):
        return "datetime"
    if isinstance(type_, BigInteger):
        return "int"
    if isinstance(type_, Integer):
        return "int"
    if isinstance(type_, Float):
        return "float"
    if isinstance(type_, JSON):
        return "json"
    raise AssertionError(f"unmapped SQLAlchemy type {type_!r}; teach the parity test about it")


def django_shape(field: Field[Any, Any]) -> ColumnShape:
    """Describe one Django field the way the comparison needs it.

    A foreign key carries its target's type, not ``ForeignKey``: the SQLAlchemy side
    declares ``plant_id`` as a UUID with a foreign key, and the two must agree on the
    column, not on how the relationship is spelled.
    """
    internal = field.get_internal_type()
    if isinstance(field, ForeignKey):
        internal = field.target_field.get_internal_type()
    assert internal in DJANGO_FAMILIES, f"unmapped Django field {internal}"
    return ColumnShape(
        family=DJANGO_FAMILIES[internal],
        nullable=field.null,
        primary_key=field.primary_key,
    )


def orm_shape(column: Column[Any]) -> ColumnShape:
    """Describe one SQLAlchemy column."""
    return ColumnShape(
        family=_orm_family(column.type),
        nullable=bool(column.nullable),
        primary_key=column.primary_key,
    )


def django_columns(model: type[Any]) -> dict[str, ColumnShape]:
    """Return the Django model's columns, by database column name."""
    return {field.column: django_shape(field) for field in model._meta.concrete_fields}


def orm_columns(table_name: str) -> dict[str, ColumnShape]:
    """Return one mapped table's columns, by name."""
    table = ReadModelBase.metadata.tables[table_name]
    return {column.name: orm_shape(column) for column in table.columns}


def django_model_for(schema: str, table: str) -> type[Any]:
    """Return the Django model that declares ``schema.table``."""
    qualified = f'"{schema}"."{table}"'
    for model in apps.get_models():
        if model._meta.db_table == qualified:
            return cast("type[Any]", model)
    raise AssertionError(f"no Django model declares {qualified}")


def disagreements(django: Mapping[str, ColumnShape], orm: Mapping[str, ColumnShape]) -> list[str]:
    """Return a human-readable line for every way the two shapes differ."""
    problems: list[str] = []
    for name in sorted(set(django) | set(orm)):
        if name not in django:
            problems.append(f"{name}: the ORM maps it, Django does not")
            continue
        if name not in orm:
            problems.append(f"{name}: Django declares it, the ORM does not")
            continue
        left, right = django[name], orm[name]
        if left.family != right.family:
            problems.append(f"{name}: Django is {left.family}, the ORM is {right.family}")
        if left.nullable != right.nullable:
            problems.append(
                f"{name}: Django nullable={left.nullable}, ORM nullable={right.nullable}"
            )
        if left.primary_key != right.primary_key:
            problems.append(
                f"{name}: Django primary_key={left.primary_key}, "
                f"ORM primary_key={right.primary_key}"
            )
    return problems


MAPPED_TABLES: list[tuple[str, str]] = sorted(
    (table.schema or "", table.name) for table in ReadModelBase.metadata.tables.values()
)


def test_the_read_side_maps_something() -> None:
    """A parity test over an empty registry would pass forever."""
    assert MAPPED_TABLES


@pytest.mark.parametrize(("schema", "table"), MAPPED_TABLES, ids=lambda value: value)
def test_every_read_mapping_agrees_with_its_django_model(schema: str, table: str) -> None:
    """Column for column, for every table the read side maps."""
    model = django_model_for(schema, table)
    problems = disagreements(django_columns(model), orm_columns(f"{schema}.{table}"))
    assert problems == [], f"{schema}.{table}:\n" + "\n".join(problems)


@pytest.mark.parametrize(("schema", "table"), MAPPED_TABLES, ids=lambda value: value)
def test_every_mapped_table_lives_in_a_read_schema(schema: str, table: str) -> None:
    """Both ORMs must agree on where the table lives, not only on its columns."""
    assert schema in ALL_READ_SCHEMAS, table


# --- The negative controls: a comparator that cannot fail is not evidence -------


def test_a_column_only_one_side_declares_is_a_difference() -> None:
    only_django = {"a": _shape()}
    only_orm = {"a": _shape(), "b": _shape()}

    assert disagreements(only_django, only_orm) == ["b: the ORM maps it, Django does not"]
    assert disagreements(only_orm, only_django) == ["b: Django declares it, the ORM does not"]


def test_a_type_difference_is_a_difference() -> None:
    assert disagreements({"a": _shape("float")}, {"a": _shape("int")}) == [
        "a: Django is float, the ORM is int"
    ]


def test_a_nullability_difference_is_a_difference() -> None:
    assert disagreements({"a": _shape(nullable=True)}, {"a": _shape()}) == [
        "a: Django nullable=True, ORM nullable=False"
    ]


def test_a_primary_key_difference_is_a_difference() -> None:
    assert disagreements({"a": _shape(primary_key=True)}, {"a": _shape()}) == [
        "a: Django primary_key=True, ORM primary_key=False"
    ]


def _shape(
    family: str = "uuid", *, nullable: bool = False, primary_key: bool = False
) -> ColumnShape:
    """A column shape for the negative controls."""
    return ColumnShape(family=family, nullable=nullable, primary_key=primary_key)
