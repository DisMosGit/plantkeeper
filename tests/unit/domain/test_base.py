"""Tests for the domain base classes."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from plantkeeper.domain.base import AggregateRoot, DomainError, DomainEvent, Entity
from plantkeeper.domain.identifiers import PlantId, SensorId


class SampleEvent(DomainEvent):
    """Concrete event used to exercise the base class."""

    plant_id: PlantId


class SampleEntity(Entity[PlantId]):
    """Concrete entity used to exercise the base class."""


class SensorEntity(Entity[SensorId]):
    """Second entity type, to prove identity includes the concrete class."""


class SampleAggregate(AggregateRoot[PlantId]):
    """Concrete aggregate used to exercise the base class."""

    def record(self, event: DomainEvent) -> None:
        self._record(event)


def _sample_event(**overrides: object) -> SampleEvent:
    payload: dict[str, object] = {"plant_id": PlantId(uuid4())}
    payload.update(overrides)
    return SampleEvent.model_validate(payload)


def test_domain_event_fills_identifier_and_timestamp() -> None:
    event = _sample_event()

    assert isinstance(event.event_id, UUID)
    assert event.event_id.version == 7
    assert event.occurred_at.tzinfo is not None
    assert abs((event.occurred_at - datetime.now(UTC)).total_seconds()) < 5


def test_domain_event_accepts_an_explicit_timestamp() -> None:
    moment = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)

    event = _sample_event(occurred_at=moment)

    assert event.occurred_at == moment


def test_domain_event_rejects_a_naive_timestamp() -> None:
    with pytest.raises(ValidationError):
        _sample_event(occurred_at=datetime(2026, 1, 1, 12, 0))


def test_domain_event_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        _sample_event(bogus=1)


def test_domain_event_is_frozen() -> None:
    event = _sample_event()

    with pytest.raises(ValidationError):
        event.__setattr__("occurred_at", datetime.now(UTC))


def test_domain_event_survives_a_json_round_trip() -> None:
    event = _sample_event()

    assert SampleEvent.model_validate_json(event.model_dump_json()) == event


def test_entities_with_the_same_identity_are_equal_and_hashable() -> None:
    plant_id = PlantId(uuid4())

    first = SampleEntity(plant_id)
    second = SampleEntity(plant_id)

    assert first == second
    assert hash(first) == hash(second)
    assert first.id == plant_id


def test_entities_with_different_identities_are_not_equal() -> None:
    assert SampleEntity(PlantId(uuid4())) != SampleEntity(PlantId(uuid4()))


def test_identity_includes_the_concrete_entity_type() -> None:
    # Compared as ``object``: mypy would (correctly) reject comparing two
    # unrelated entity classes directly.
    plant: object = SampleEntity(PlantId(uuid4()))
    sensor: object = SensorEntity(SensorId(uuid4()))

    assert plant != sensor


def test_entity_repr_mentions_the_type_and_identifier() -> None:
    plant_id = PlantId(uuid4())

    assert repr(SampleEntity(plant_id)) == f"SampleEntity(id={plant_id!r})"


def test_aggregate_drains_recorded_events() -> None:
    aggregate = SampleAggregate(PlantId(uuid4()))
    first = _sample_event()
    second = _sample_event()
    aggregate.record(first)
    aggregate.record(second)

    assert aggregate.collect_events() == [first, second]
    assert aggregate.collect_events() == []


def test_domain_error_is_an_exception() -> None:
    assert issubclass(DomainError, Exception)

    with pytest.raises(DomainError):
        raise DomainError("invariant violated")
