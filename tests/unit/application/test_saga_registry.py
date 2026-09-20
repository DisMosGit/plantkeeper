"""Contract tests for the saga registry.

The registry is the one place that says which consumers and sagas exist, and the
worker builds its subscriptions from it. These tests keep the three things that
can drift apart in step: the saga map the dispatcher resolves, the events every
consumer claims to handle (which must all have a Kafka topic), and the names that
become consumer groups.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from plantkeeper.application.sagas.base import Saga
from plantkeeper.application.sagas.contexts import OnboardPlantContext
from plantkeeper.application.sagas.onboard import OnboardPlantSaga
from plantkeeper.application.sagas.registry import (
    SAGA_TYPES,
    WORKER_CONSUMER_TYPES,
    build_saga_map,
    saga_type_named,
)
from plantkeeper.domain.catalog.events import SpeciesSyncRequested
from plantkeeper.domain.garden.events import PlantAdded
from plantkeeper.domain.identifiers import HouseholdId, PlantId, SpeciesId
from plantkeeper.domain.values import Location
from plantkeeper.infrastructure.messaging.topics import EVENT_TOPICS

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


class StubStorage:
    """The smallest thing a saga can be built with; nothing here is persisted."""

    async def load_saga_state(
        self, saga_id: object, *, read_for_update: bool = False
    ) -> tuple[object, dict[str, object], int]:
        """Report every saga as unknown, which is the fresh-start path."""
        raise ValueError(f"saga {saga_id} not found")

    async def get_step_history(self, saga_id: object) -> list[object]:
        """An untouched saga has no history."""
        return []


def a_plant_added(plant_id: PlantId | None = None) -> PlantAdded:
    """A valid ``PlantAdded`` to drive the onboarding saga with."""
    return PlantAdded(
        plant_id=plant_id or PlantId.new(),
        household_id=HouseholdId.new(),
        species_id=SpeciesId.new(),
        name="Fern",
        location=Location(value="Shelf"),
        added_at=NOW,
        occurred_at=NOW,
    )


def test_the_saga_map_binds_one_saga_per_context() -> None:
    saga_map = build_saga_map()

    assert set(saga_map) == {saga_type.context_type for saga_type in SAGA_TYPES}
    assert len(saga_map) == len(SAGA_TYPES)


def test_every_saga_is_addressable_by_its_name() -> None:
    for saga_type in SAGA_TYPES:
        assert saga_type_named(saga_type.__name__) is saga_type
    assert saga_type_named("InventedSaga") is None


def test_saga_names_are_unique() -> None:
    names = [saga_type.__name__ for saga_type in SAGA_TYPES]
    assert len(names) == len(set(names))


def test_every_consumer_group_name_is_unique_and_url_safe() -> None:
    names = [consumer_type.name for consumer_type in WORKER_CONSUMER_TYPES]

    assert len(names) == len(set(names))
    assert all(name and name == name.lower() and " " not in name for name in names)


def test_every_handled_event_has_a_kafka_topic() -> None:
    """The worker derives its subscriptions from these events; an unmapped one
    would make the consumer silently unreachable."""
    for consumer_type in WORKER_CONSUMER_TYPES:
        for event_type in consumer_type.handled_types:
            assert event_type in EVENT_TOPICS, event_type.__name__
    for saga_type in SAGA_TYPES:
        for event_type in saga_type.trigger_events:
            assert event_type in EVENT_TOPICS, event_type.__name__


def test_a_saga_advertises_the_event_it_starts_from() -> None:
    saga = OnboardPlantSaga(StubStorage())
    event = a_plant_added()

    assert isinstance(saga, Saga)
    assert saga.handles(event)
    # A different trigger is another saga's business.
    assert not saga.handles(SpeciesSyncRequested())


def test_a_saga_id_is_deterministic_per_correlation() -> None:
    saga = OnboardPlantSaga(StubStorage())
    plant_id = PlantId.new()
    context = OnboardPlantContext(
        plant_id=str(plant_id),
        household_id=str(HouseholdId.new()),
        species_id=str(SpeciesId.new()),
    )

    assert saga.saga_id_for(context) == saga.saga_id_for(context)
    # A second instance is the same process manager, so it addresses the same saga.
    assert saga.saga_id_for(context) == OnboardPlantSaga(StubStorage()).saga_id_for(context)
    other = OnboardPlantContext(
        plant_id=str(uuid4()),
        household_id=context.household_id,
        species_id=context.species_id,
    )
    assert saga.saga_id_for(other) != saga.saga_id_for(context)


def test_the_context_comes_from_the_trigger_event() -> None:
    saga = OnboardPlantSaga(StubStorage())
    event = a_plant_added()

    context = saga.context_from_event(event)

    assert isinstance(context, OnboardPlantContext)
    assert context.plant_id == str(event.plant_id)
    assert context.household_id == str(event.household_id)
    assert context.species_id == str(event.species_id)
