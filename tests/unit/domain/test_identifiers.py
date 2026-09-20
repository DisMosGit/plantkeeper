"""Boundary tests for the typed UUID identifiers."""

from __future__ import annotations

from uuid import RFC_4122, UUID, uuid4

import pytest
from pydantic import ValidationError

from plantkeeper.domain.identifiers import (
    HouseholdId,
    JournalEntryId,
    NotificationId,
    PlantId,
    SensorId,
    SpeciesId,
    UserId,
    UuidIdentifier,
)

IDENTIFIER_TYPES: list[type[UuidIdentifier]] = [
    HouseholdId,
    JournalEntryId,
    NotificationId,
    PlantId,
    SensorId,
    SpeciesId,
    UserId,
]


@pytest.mark.parametrize("identifier_type", IDENTIFIER_TYPES)
def test_identifier_wraps_a_uuid(identifier_type: type[UuidIdentifier]) -> None:
    raw = uuid4()

    identifier = identifier_type(raw)

    assert identifier.value == raw
    assert identifier.root == raw
    assert str(identifier) == str(raw)


@pytest.mark.parametrize("identifier_type", IDENTIFIER_TYPES)
def test_new_generates_a_unique_uuid7(identifier_type: type[UuidIdentifier]) -> None:
    first = identifier_type.new()
    second = identifier_type.new()

    assert first != second
    assert first.value.version == 7
    assert first.value.variant == RFC_4122


@pytest.mark.parametrize("identifier_type", IDENTIFIER_TYPES)
def test_identifier_rejects_a_non_uuid(identifier_type: type[UuidIdentifier]) -> None:
    with pytest.raises(ValidationError):
        identifier_type.model_validate("not-a-uuid")


@pytest.mark.parametrize("identifier_type", IDENTIFIER_TYPES)
def test_identifier_is_frozen(identifier_type: type[UuidIdentifier]) -> None:
    identifier = identifier_type(uuid4())

    # ``__setattr__`` is called directly because the frozen field is read-only
    # for the type checker; the test asserts Pydantic rejects the write.
    with pytest.raises(ValidationError):
        identifier.__setattr__("root", uuid4())


def test_equal_values_are_equal_and_hashable() -> None:
    raw = uuid4()

    first = PlantId(raw)
    second = PlantId(raw)

    assert first == second
    assert hash(first) == hash(second)
    assert len({first, second}) == 1


def test_different_identifier_types_never_compare_equal() -> None:
    raw = uuid4()

    assert PlantId(raw) != SensorId(raw)
    assert HouseholdId(raw) != UserId(raw)


def test_identifier_survives_a_json_round_trip() -> None:
    plant_id = PlantId(uuid4())

    restored = PlantId.model_validate_json(plant_id.model_dump_json())

    assert restored == plant_id
    assert isinstance(plant_id.value, UUID)
