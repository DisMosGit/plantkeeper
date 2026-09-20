"""Integration tests for the two orchestration sagas.

The sagas are driven the way the worker drives them: a real
:class:`~plantkeeper.infrastructure.persistence.saga_storage.SqlAlchemySagaStorage`
over the testcontainer Postgres, real step handlers over a real unit of work, and
the library's ``SagaDispatcher`` resolving the steps from a tiny test container.
What is under test is the thing a fake cannot show — that each step's commit is a
durable checkpoint, that a failure compensates exactly the steps that ran, and
that the saga's execution and its step log end in the state the recovery job will
look for.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import UUID, uuid4

import pytest
from cqrs.dispatcher.saga import SagaDispatcher
from cqrs.saga.storage.enums import SagaStatus
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from plantkeeper.application.ports.catalog import SpeciesRecord
from plantkeeper.application.sagas.adapters import OutboxSpeciesCache, RepositorySpeciesCatalog
from plantkeeper.application.sagas.contexts import OnboardPlantContext
from plantkeeper.application.sagas.onboard import (
    CreateCareScheduleStep,
    CreateOnboardingNotificationStep,
    OnboardPlantSaga,
    PublishPlantOnboardedStep,
    ResolveSpeciesStep,
)
from plantkeeper.application.sagas.registry import build_saga_map
from plantkeeper.application.sagas.species_sync import (
    ApplySpeciesUpdatesStep,
    FetchSpeciesStep,
    InvalidateSpeciesCacheStep,
    SpeciesSyncSaga,
    SpeciesSyncTrigger,
)
from plantkeeper.domain.catalog.events import SpeciesSyncRequested
from plantkeeper.domain.catalog.species import Species
from plantkeeper.domain.catalog.values import LightRequirement
from plantkeeper.domain.garden.events import PlantAdded
from plantkeeper.domain.garden.household import Household
from plantkeeper.domain.garden.plant import Plant
from plantkeeper.domain.identifiers import HouseholdId, PlantId, SpeciesId
from plantkeeper.domain.values import Location, WateringInterval
from plantkeeper.infrastructure.persistence.mappers.care import care_schedule_to_domain
from plantkeeper.infrastructure.persistence.models.care import CareScheduleModel
from plantkeeper.infrastructure.persistence.models.notifications import NotificationModel
from plantkeeper.infrastructure.persistence.models.shared import (
    OutboxModel,
    SagaLogModel,
    SagaStateModel,
)
from plantkeeper.infrastructure.persistence.repositories.catalog import (
    SqlAlchemySpeciesRepository,
)
from plantkeeper.infrastructure.persistence.saga_storage import SqlAlchemySagaStorage
from plantkeeper.infrastructure.persistence.tracking import AggregateTracker
from plantkeeper.infrastructure.persistence.uow import SqlAlchemyUnitOfWork

pytestmark = pytest.mark.integration

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
WEEK = timedelta(days=7)


class FakeClock:
    """A clock the test moves by hand."""

    def __init__(self, now: datetime) -> None:
        self._now = now

    def now(self) -> datetime:
        """Return the instant the test has set."""
        return self._now


class DictContainer:
    """The smallest cqrs ``Container``: a mapping from type to instance.

    The engine only ever calls ``resolve(step_type)``, so a dict is enough to pin
    which instance each step resolves to — including a deliberately failing one.
    """

    def __init__(self, instances: dict[type, object]) -> None:
        self._instances = instances

    async def resolve[ResolvedT](self, type_: type[ResolvedT]) -> ResolvedT:
        """Return the instance registered for ``type_``."""
        return cast("ResolvedT", self._instances[type_])


class StubSource:
    """A ``SpeciesSource`` that answers with a fixed snapshot."""

    def __init__(self, records: list[SpeciesRecord]) -> None:
        self._records = records

    async def fetch_all(self) -> list[SpeciesRecord]:
        """Return the records the test built."""
        return self._records


class FailingCache:
    """A ``SpeciesCache`` whose invalidation always fails."""

    async def invalidate(self, species_ids: object) -> None:
        """Fail the step, which is what the compensation test needs."""
        raise RuntimeError("cache is down")


async def seed_species(
    session_factory: async_sessionmaker[AsyncSession],
    species_id: SpeciesId,
    *,
    common_name: str = "Boston fern",
    interval: timedelta = WEEK,
) -> None:
    """Insert one catalogue entry."""
    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        async with uow:
            await uow.species.add(
                Species.create(
                    scientific_name="Nephrolepis exaltata",
                    common_name=common_name,
                    watering_interval=WateringInterval(value=interval),
                    light_requirement=LightRequirement.MEDIUM,
                    species_id=species_id,
                )
            )
            await uow.commit()


async def seed_plant(
    session_factory: async_sessionmaker[AsyncSession], species_id: SpeciesId
) -> tuple[HouseholdId, PlantId]:
    """Insert a household with one plant and return their identifiers."""
    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        async with uow:
            household = Household.create(name="Home")
            await uow.households.add(household)
            plant = Plant.add(
                household_id=household.id,
                species_id=species_id,
                name="Fern",
                location=Location(value="Shelf"),
                now=NOW,
            )
            household.add_plant(plant.id)
            await uow.plants.add(plant)
            await uow.commit()
            return household.id, plant.id


async def read_species(
    session_factory: async_sessionmaker[AsyncSession], species_id: SpeciesId
) -> Species | None:
    """Read a catalogue entry back through its repository."""
    async with session_factory() as session:
        return await SqlAlchemySpeciesRepository(session, AggregateTracker()).get(species_id)


async def outbox_event_names(session_factory: async_sessionmaker[AsyncSession]) -> list[str]:
    """Return the outbox in insertion order, as event names."""
    async with session_factory() as session:
        statement = select(OutboxModel).order_by(OutboxModel.id)
        models = (await session.execute(statement)).scalars()
        return [model.event_name for model in models]


async def saga_status(
    session_factory: async_sessionmaker[AsyncSession], saga_id: UUID
) -> SagaStatus:
    """Return the persisted status of one saga."""
    async with session_factory() as session:
        model = await session.get(SagaStateModel, saga_id)
        assert model is not None
        return SagaStatus(model.status)


async def saga_step_actions(
    session_factory: async_sessionmaker[AsyncSession], saga_id: UUID
) -> list[tuple[str, str]]:
    """Return ``(action, status)`` for every step transition, in order."""
    async with session_factory() as session:
        statement = (
            select(SagaLogModel)
            .where(SagaLogModel.saga_id == saga_id)
            .order_by(SagaLogModel.created_at, SagaLogModel.id)
        )
        models = (await session.execute(statement)).scalars()
        return [(model.action, model.status) for model in models]


def a_dispatcher(saga: object, steps: dict[type, object], storage: object) -> SagaDispatcher:
    """A dispatcher that resolves the saga and each step from the test's mapping.

    The engine resolves the saga instance as well as the step handlers, so both are
    registered here; the real worker gets them from Dishka instead.
    """
    return SagaDispatcher(build_saga_map(), DictContainer({type(saga): saga, **steps}), storage)


def a_plant_added(
    household_id: HouseholdId, plant_id: PlantId, species_id: SpeciesId
) -> PlantAdded:
    """The event the onboarding saga starts from."""
    return PlantAdded(
        plant_id=plant_id,
        household_id=household_id,
        species_id=species_id,
        name="Fern",
        location=Location(value="Shelf"),
        added_at=NOW,
        occurred_at=NOW,
    )


# --- OnboardPlantSaga ---------------------------------------------------------


async def test_onboarding_creates_the_schedule_the_reminder_and_the_event(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    species_id = SpeciesId.new()
    await seed_species(session_factory, species_id)
    household_id, plant_id = await seed_plant(session_factory, species_id)
    # The seed's own ``PlantAdded`` is already in the outbox; the saga adds a tail.
    seeded = len(await outbox_event_names(session_factory))
    storage = SqlAlchemySagaStorage(session_factory)
    saga = OnboardPlantSaga(storage)
    clock = FakeClock(NOW)

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        steps: dict[type, object] = {
            ResolveSpeciesStep: ResolveSpeciesStep(RepositorySpeciesCatalog(uow.species)),
            CreateCareScheduleStep: CreateCareScheduleStep(uow, clock),
            CreateOnboardingNotificationStep: CreateOnboardingNotificationStep(uow, clock),
            PublishPlantOnboardedStep: PublishPlantOnboardedStep(uow, clock),
        }
        dispatcher = a_dispatcher(saga, steps, storage)
        event = a_plant_added(household_id, plant_id, species_id)
        handled = await saga.handle_event(event, dispatcher=dispatcher, unit_of_work=uow)
        saga_id = saga.saga_id_for(saga.context_from_event(event))

    assert handled
    assert await saga_status(session_factory, saga_id) is SagaStatus.COMPLETED
    # The engine logs every transition, so each of the four steps leaves a
    # started/completed pair, in step order.
    assert (
        await saga_step_actions(session_factory, saga_id)
        == [
            ("act", "started"),
            ("act", "completed"),
        ]
        * 4
    )

    async with session_factory() as session:
        schedule_model = await session.get(CareScheduleModel, plant_id.value)
        assert schedule_model is not None
        schedule = care_schedule_to_domain(schedule_model)
        assert schedule.next_watering_at == NOW + WEEK
        notifications = (await session.execute(select(NotificationModel))).scalars().all()
        assert len(notifications) == 1
        assert notifications[0].notification_type == "plant_onboarded"

    assert (await outbox_event_names(session_factory))[seeded:] == [
        "SagaStarted",
        "CareScheduleCreated",
        "NotificationCreated",
        "PlantOnboarded",
        "SagaCompleted",
    ]


async def test_a_completed_onboarding_is_a_no_op_when_redelivered(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    species_id = SpeciesId.new()
    await seed_species(session_factory, species_id)
    household_id, plant_id = await seed_plant(session_factory, species_id)
    storage = SqlAlchemySagaStorage(session_factory)
    saga = OnboardPlantSaga(storage)
    clock = FakeClock(NOW)

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        steps: dict[type, object] = {
            ResolveSpeciesStep: ResolveSpeciesStep(RepositorySpeciesCatalog(uow.species)),
            CreateCareScheduleStep: CreateCareScheduleStep(uow, clock),
            CreateOnboardingNotificationStep: CreateOnboardingNotificationStep(uow, clock),
            PublishPlantOnboardedStep: PublishPlantOnboardedStep(uow, clock),
        }
        dispatcher = a_dispatcher(saga, steps, storage)
        event = a_plant_added(household_id, plant_id, species_id)
        assert await saga.handle_event(event, dispatcher=dispatcher, unit_of_work=uow)

    first_pass = await outbox_event_names(session_factory)

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        steps = {
            ResolveSpeciesStep: ResolveSpeciesStep(RepositorySpeciesCatalog(uow.species)),
            CreateCareScheduleStep: CreateCareScheduleStep(uow, clock),
            CreateOnboardingNotificationStep: CreateOnboardingNotificationStep(uow, clock),
            PublishPlantOnboardedStep: PublishPlantOnboardedStep(uow, clock),
        }
        dispatcher = a_dispatcher(saga, steps, storage)
        handled = await saga.handle_event(event, dispatcher=dispatcher, unit_of_work=uow)

    assert handled is False
    assert await outbox_event_names(session_factory) == first_pass


class ExplodingNotificationStep(CreateOnboardingNotificationStep):
    """Step 3 fails, so step 2's schedule has to be rolled back."""

    async def act(self, context: OnboardPlantContext) -> object:
        """Fail before anything is written."""
        raise RuntimeError("notification channel is down")


async def test_a_failing_step_compensates_what_the_earlier_steps_created(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    species_id = SpeciesId.new()
    await seed_species(session_factory, species_id)
    household_id, plant_id = await seed_plant(session_factory, species_id)
    storage = SqlAlchemySagaStorage(session_factory)
    saga = OnboardPlantSaga(storage)
    clock = FakeClock(NOW)

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        steps: dict[type, object] = {
            ResolveSpeciesStep: ResolveSpeciesStep(RepositorySpeciesCatalog(uow.species)),
            CreateCareScheduleStep: CreateCareScheduleStep(uow, clock),
            CreateOnboardingNotificationStep: ExplodingNotificationStep(uow, clock),
            PublishPlantOnboardedStep: PublishPlantOnboardedStep(uow, clock),
        }
        dispatcher = a_dispatcher(saga, steps, storage)
        event = a_plant_added(household_id, plant_id, species_id)
        saga_id = saga.saga_id_for(saga.context_from_event(event))
        with pytest.raises(RuntimeError, match="notification channel is down"):
            await saga.handle_event(event, dispatcher=dispatcher, unit_of_work=uow)

    assert await saga_status(session_factory, saga_id) is SagaStatus.FAILED
    actions = await saga_step_actions(session_factory, saga_id)
    assert ("compensate", "completed") in actions

    async with session_factory() as session:
        assert await session.get(CareScheduleModel, plant_id.value) is None
        assert (await session.execute(select(NotificationModel))).scalars().all() == []

    names = await outbox_event_names(session_factory)
    assert "SagaFailed" in names
    assert "SagaCompensated" in names
    assert "SagaCompleted" not in names


class ExplodingPublishStep(PublishPlantOnboardedStep):
    """Step 4 fails *after* step 3 wrote a notification."""

    async def act(self, context: OnboardPlantContext) -> object:
        """Fail once the earlier steps have committed their writes."""
        raise RuntimeError("outbox is unavailable")


async def test_a_failing_last_step_compensates_the_notification_as_well(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """The roadmap's compensation is *both* writes, so both are rolled back here.

    ``ExplodingNotificationStep`` fails before the notification exists and can only
    prove the schedule's rollback; this one fails at the last step, where the
    notification is already committed and its compensation has real work to do.
    """
    species_id = SpeciesId.new()
    await seed_species(session_factory, species_id)
    household_id, plant_id = await seed_plant(session_factory, species_id)
    storage = SqlAlchemySagaStorage(session_factory)
    saga = OnboardPlantSaga(storage)
    clock = FakeClock(NOW)

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        steps: dict[type, object] = {
            ResolveSpeciesStep: ResolveSpeciesStep(RepositorySpeciesCatalog(uow.species)),
            CreateCareScheduleStep: CreateCareScheduleStep(uow, clock),
            CreateOnboardingNotificationStep: CreateOnboardingNotificationStep(uow, clock),
            PublishPlantOnboardedStep: ExplodingPublishStep(uow, clock),
        }
        dispatcher = a_dispatcher(saga, steps, storage)
        event = a_plant_added(household_id, plant_id, species_id)
        saga_id = saga.saga_id_for(saga.context_from_event(event))
        with pytest.raises(RuntimeError, match="outbox is unavailable"):
            await saga.handle_event(event, dispatcher=dispatcher, unit_of_work=uow)

    assert await saga_status(session_factory, saga_id) is SagaStatus.FAILED
    actions = await saga_step_actions(session_factory, saga_id)
    assert ("compensate", "completed") in actions

    async with session_factory() as session:
        assert await session.get(CareScheduleModel, plant_id.value) is None
        assert (await session.execute(select(NotificationModel))).scalars().all() == []

    names = await outbox_event_names(session_factory)
    assert "SagaFailed" in names
    assert "SagaCompensated" in names
    # The last step never ran, so nothing was announced.
    assert "PlantOnboarded" not in names


# --- the saga state a reader sees ---------------------------------------------


async def test_the_saga_state_repository_reads_what_recovery_needs(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """`SagaStateRepository` is the read side of `saga_state` (ROADMAP 4.1).

    It returns the rows the engine writes, so a reader can tell a crashed saga from
    a finished one, from one another worker is still touching, and from one that has
    used up its recovery attempts.
    """
    running_id, completed_id, unknown_id = uuid4(), uuid4(), uuid4()
    stale_at = NOW - timedelta(days=1)
    async with session_factory() as session:
        session.add_all(
            [
                SagaStateModel(
                    id=running_id,
                    name="OnboardPlantSaga",
                    status=SagaStatus.RUNNING.value,
                    context={},
                    version=1,
                    recovery_attempts=0,
                    created_at=stale_at,
                    updated_at=stale_at,
                ),
                SagaStateModel(
                    id=completed_id,
                    name="SpeciesSyncSaga",
                    status=SagaStatus.COMPLETED.value,
                    context={},
                    version=3,
                    recovery_attempts=0,
                    created_at=NOW,
                    updated_at=NOW,
                ),
            ]
        )
        await session.commit()

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        state = await uow.saga_states.get(running_id)
        assert state is not None
        assert state.saga_name == "OnboardPlantSaga"
        assert state.status is SagaStatus.RUNNING
        assert state.version == 1
        assert state.updated_at == stale_at
        assert await uow.saga_states.get(unknown_id) is None

        # Only the unfinished saga is a candidate; the completed one is filtered out.
        recoverable = await uow.saga_states.list_recoverable(limit=10, max_attempts=5)
        assert [row.saga_id for row in recoverable] == [running_id]
        # Only once it has stopped moving: a fresh row is not stale yet...
        assert (
            await uow.saga_states.list_recoverable(
                limit=10, max_attempts=5, stale_before=NOW - timedelta(days=2)
            )
            == []
        )
        # ...and the same row is stale once the boundary is far enough back.
        stale = await uow.saga_states.list_recoverable(limit=10, max_attempts=5, stale_before=NOW)
        assert [row.saga_id for row in stale] == [running_id]
        # A saga that exhausted its attempts is left for an operator.
        assert await uow.saga_states.list_recoverable(limit=10, max_attempts=0) == []


# --- SpeciesSyncSaga ----------------------------------------------------------


def a_record(species_id: SpeciesId, *, common_name: str, interval: timedelta) -> SpeciesRecord:
    """One upstream catalogue entry."""
    return SpeciesRecord(
        species_id=species_id,
        scientific_name="Nephrolepis exaltata",
        common_name=common_name,
        watering_interval=WateringInterval(value=interval),
        light_requirement=LightRequirement.HIGH,
    )


def species_sync_requested() -> SpeciesSyncRequested:
    """A fresh trigger event, so each test addresses its own saga."""
    return SpeciesSyncRequested(occurred_at=NOW)


async def test_species_sync_updates_changed_entries_and_invalidates_their_cache(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    species_id = SpeciesId.new()
    await seed_species(session_factory, species_id)
    storage = SqlAlchemySagaStorage(session_factory)
    saga = SpeciesSyncSaga(storage)
    clock = FakeClock(NOW)
    source = StubSource(
        [a_record(species_id, common_name="Sword fern", interval=timedelta(days=3))]
    )

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        steps: dict[type, object] = {
            FetchSpeciesStep: FetchSpeciesStep(source),
            ApplySpeciesUpdatesStep: ApplySpeciesUpdatesStep(uow, clock),
            InvalidateSpeciesCacheStep: InvalidateSpeciesCacheStep(uow, OutboxSpeciesCache(uow)),
        }
        dispatcher = a_dispatcher(saga, steps, storage)
        event = species_sync_requested()
        handled = await saga.handle_event(event, dispatcher=dispatcher, unit_of_work=uow)
        saga_id = saga.saga_id_for(saga.context_from_event(event))

    assert handled
    assert await saga_status(session_factory, saga_id) is SagaStatus.COMPLETED
    updated = await read_species(session_factory, species_id)
    assert updated is not None
    assert updated.common_name == "Sword fern"
    assert updated.watering_interval == WateringInterval(value=timedelta(days=3))
    assert updated.version == 2
    names = await outbox_event_names(session_factory)
    assert names.count("SpeciesUpdated") == 1
    assert names.count("SpeciesCacheInvalidated") == 1


async def test_a_failing_cache_step_restores_the_previous_species_versions(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    species_id = SpeciesId.new()
    await seed_species(session_factory, species_id)
    storage = SqlAlchemySagaStorage(session_factory)
    saga = SpeciesSyncSaga(storage)
    clock = FakeClock(NOW)
    source = StubSource(
        [a_record(species_id, common_name="Sword fern", interval=timedelta(days=3))]
    )

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        steps: dict[type, object] = {
            FetchSpeciesStep: FetchSpeciesStep(source),
            ApplySpeciesUpdatesStep: ApplySpeciesUpdatesStep(uow, clock),
            InvalidateSpeciesCacheStep: InvalidateSpeciesCacheStep(uow, FailingCache()),
        }
        dispatcher = a_dispatcher(saga, steps, storage)
        event = species_sync_requested()
        saga_id = saga.saga_id_for(saga.context_from_event(event))
        with pytest.raises(RuntimeError, match="cache is down"):
            await saga.handle_event(event, dispatcher=dispatcher, unit_of_work=uow)

    assert await saga_status(session_factory, saga_id) is SagaStatus.FAILED
    restored = await read_species(session_factory, species_id)
    assert restored is not None
    assert restored.common_name == "Boston fern"
    assert restored.watering_interval == WateringInterval(value=WEEK)
    # The restore is itself a catalogue change, so it records an update and bumps
    # the version: consumers must see the rollback.
    assert restored.version == 3
    names = await outbox_event_names(session_factory)
    assert names.count("SpeciesUpdated") == 2
    assert "SagaCompensated" in names


async def test_the_sync_trigger_consumer_runs_the_saga_once(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """The consumer half of 4.5's trigger: cron and `POST /catalog/sync` both end here.

    The daily scheduler and the REST endpoint publish the same
    ``SpeciesSyncRequested``; this drives the consumer group that turns that event
    into the saga, and shows the ledger making a redelivery a no-op.
    """
    species_id = SpeciesId.new()
    await seed_species(session_factory, species_id)
    storage = SqlAlchemySagaStorage(session_factory)
    saga = SpeciesSyncSaga(storage)
    clock = FakeClock(NOW)
    source = StubSource(
        [a_record(species_id, common_name="Sword fern", interval=timedelta(days=3))]
    )
    event = species_sync_requested()
    group = "plantkeeper-worker-species-sync"

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        trigger = SpeciesSyncTrigger(uow, saga)
        steps: dict[type, object] = {
            FetchSpeciesStep: FetchSpeciesStep(source),
            ApplySpeciesUpdatesStep: ApplySpeciesUpdatesStep(uow, clock),
            InvalidateSpeciesCacheStep: InvalidateSpeciesCacheStep(uow, OutboxSpeciesCache(uow)),
        }
        assert await trigger.consume(
            event, consumer_group=group, dispatcher=a_dispatcher(saga, steps, storage)
        )

    updated = await read_species(session_factory, species_id)
    assert updated is not None
    assert updated.common_name == "Sword fern"

    # The same delivery in the same group stops at the ledger: no second update.
    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        trigger = SpeciesSyncTrigger(uow, saga)
        steps = {
            FetchSpeciesStep: FetchSpeciesStep(source),
            ApplySpeciesUpdatesStep: ApplySpeciesUpdatesStep(uow, clock),
            InvalidateSpeciesCacheStep: InvalidateSpeciesCacheStep(uow, OutboxSpeciesCache(uow)),
        }
        assert not await trigger.consume(
            event, consumer_group=group, dispatcher=a_dispatcher(saga, steps, storage)
        )

    names = await outbox_event_names(session_factory)
    assert names.count("SpeciesCacheInvalidated") == 1
    assert names.count("SagaStarted") == 1
