"""Integration tests for the worker's background jobs.

These run the jobs the way ``apps/workers`` does: the real
``worker_providers()`` container (pointed at the testcontainer database) with only
the clock overridden, and one explicit ``run_once`` per tick so the timer is driven
by the test instead of slept through. What is under test is the wiring — that the
job reaches the saga through a request scope of its own — not the event loop.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from urllib.parse import urlparse
from uuid import uuid4

import pytest
from cqrs.saga.storage.enums import SagaStatus
from dishka import AsyncContainer, Provider, Scope, make_async_container, provide
from pydantic import JsonValue
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from plantkeeper.application.ports.clock import Clock
from plantkeeper.application.ports.sagas import MissedCareState
from plantkeeper.application.sagas.missed_care import GRACE_PERIOD
from plantkeeper.application.sagas.onboard import OnboardPlantSaga
from plantkeeper.domain.care.schedule import CareSchedule
from plantkeeper.domain.garden.household import Household
from plantkeeper.domain.garden.plant import Plant
from plantkeeper.domain.identifiers import PlantId, SpeciesId
from plantkeeper.domain.values import Location, WateringInterval
from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.di.providers import worker_providers
from plantkeeper.infrastructure.persistence.models.shared import (
    OutboxModel,
    SagaStateModel,
)
from plantkeeper.infrastructure.persistence.repositories.sagas import (
    SqlAlchemyMissedCareWindowRepository,
)
from plantkeeper.infrastructure.persistence.saga_storage import SqlAlchemySagaStorage
from plantkeeper.infrastructure.persistence.uow import SqlAlchemyUnitOfWork
from plantkeeper.infrastructure.scheduling.missed_care import MissedCareScheduler
from plantkeeper.infrastructure.scheduling.saga_recovery import SagaRecoveryJob
from plantkeeper.infrastructure.scheduling.species_sync import SpeciesSyncScheduler

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

    def advance(self, delta: timedelta) -> None:
        """Move the clock forward."""
        self._now += delta


class FakeClockProvider(Provider):
    """Replaces the container's wall clock; later providers win in Dishka."""

    def __init__(self, clock: Clock) -> None:
        super().__init__()
        self._clock = clock

    @provide(scope=Scope.APP)
    def clock(self) -> Clock:
        """Return the test's clock instead of ``SystemClock``."""
        return self._clock


def point_settings_at(database: str, monkeypatch: pytest.MonkeyPatch) -> Settings:
    """Point ``Settings()`` at the testcontainer before the container is built."""
    parsed = urlparse(database)
    assert parsed.username and parsed.password and parsed.hostname and parsed.port
    monkeypatch.setenv("POSTGRES_USER", parsed.username)
    monkeypatch.setenv("POSTGRES_PASSWORD", parsed.password)
    monkeypatch.setenv("POSTGRES_DB", parsed.path.lstrip("/"))
    monkeypatch.setenv("POSTGRES_HOST", parsed.hostname)
    monkeypatch.setenv("POSTGRES_PORT", str(parsed.port))
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    return Settings()


async def seed_due_plant(
    session_factory: async_sessionmaker[AsyncSession], *, next_watering_at: datetime
) -> PlantId:
    """Insert a household with one plant whose schedule is due at the given moment."""
    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        async with uow:
            household = Household.create(name="Home")
            await uow.households.add(household)
            plant = Plant.add(
                household_id=household.id,
                species_id=SpeciesId.new(),
                name="Fern",
                location=Location(value="Shelf"),
                now=NOW,
            )
            household.add_plant(plant.id)
            await uow.plants.add(plant)
            await uow.care_schedules.add(
                CareSchedule.create(
                    plant_id=plant.id,
                    watering_interval=WateringInterval(value=WEEK),
                    starts_at=next_watering_at,
                    now=NOW,
                )
            )
            await uow.commit()
            return plant.id


async def outbox_event_names(session_factory: async_sessionmaker[AsyncSession]) -> list[str]:
    """Return the outbox in insertion order, as event names."""
    async with session_factory() as session:
        models = (await session.execute(select(OutboxModel).order_by(OutboxModel.id))).scalars()
        return [model.event_name for model in models]


@pytest.fixture
def worker_settings(database: str, monkeypatch: pytest.MonkeyPatch) -> Settings:
    """``Settings`` pointed at the testcontainer, with fast timers for tests."""
    return point_settings_at(database, monkeypatch)


@pytest.fixture
def clock() -> FakeClock:
    """The clock the container is overridden with."""
    return FakeClock(NOW)


@pytest.fixture
async def container(worker_settings: Settings, clock: FakeClock) -> AsyncIterator[AsyncContainer]:
    """The worker's container with only the clock replaced."""
    container = make_async_container(*worker_providers(), FakeClockProvider(clock))
    try:
        yield container
    finally:
        await container.close()


async def test_the_missed_care_scheduler_escalates_an_expired_window(
    session_factory: async_sessionmaker[AsyncSession],
    container: AsyncContainer,
    worker_settings: Settings,
    clock: FakeClock,
) -> None:
    plant_id = await seed_due_plant(session_factory, next_watering_at=NOW)
    scheduler = MissedCareScheduler(
        container=container, settings=worker_settings, interval_seconds=1
    )

    assert await scheduler.run_once() == 0

    clock.advance(GRACE_PERIOD + timedelta(minutes=1))
    assert await scheduler.run_once() == 1

    async with session_factory() as session:
        window = await SqlAlchemyMissedCareWindowRepository(session).get(plant_id)
    assert window is not None
    assert window.state is MissedCareState.MISSED
    names = await outbox_event_names(session_factory)
    assert names.count("WateringDue") == 1
    assert names.count("CareMissed") == 1


async def test_the_species_sync_scheduler_publishes_one_trigger_per_tick(
    session_factory: async_sessionmaker[AsyncSession],
    worker_settings: Settings,
    container: AsyncContainer,
) -> None:
    scheduler = SpeciesSyncScheduler(
        container=container, settings=worker_settings, interval_seconds=1
    )

    await scheduler.run_once()

    names = await outbox_event_names(session_factory)
    assert names.count("SpeciesSyncRequested") == 1


async def test_the_recovery_job_ignores_a_saga_that_is_not_stale(
    session_factory: async_sessionmaker[AsyncSession],
    container: AsyncContainer,
    worker_settings: Settings,
) -> None:
    saga_id = uuid4()
    storage = SqlAlchemySagaStorage(session_factory)
    await storage.create_saga(saga_id, OnboardPlantSaga.__name__, {})
    await storage.update_status(saga_id, SagaStatus.RUNNING)
    settings = worker_settings.model_copy(update={"saga_recovery_stale_after_seconds": 3600.0})
    job = SagaRecoveryJob(container=container, storage=storage, settings=settings)

    assert await job.run_once() == 0

    async with session_factory() as session:
        model = await session.get(SagaStateModel, saga_id)
    assert model is not None
    assert model.recovery_attempts == 0
    assert model.status == SagaStatus.RUNNING.value


async def test_the_recovery_job_counts_a_failed_recovery(
    session_factory: async_sessionmaker[AsyncSession],
    container: AsyncContainer,
    worker_settings: Settings,
) -> None:
    saga_id = uuid4()
    storage = SqlAlchemySagaStorage(session_factory)
    # The context refers to a plant that was never created, so the first step
    # fails and the job has to record the attempt without crashing.
    context: dict[str, JsonValue] = {
        "plant_id": str(uuid4()),
        "household_id": str(uuid4()),
        "species_id": str(uuid4()),
    }
    await storage.create_saga(saga_id, OnboardPlantSaga.__name__, context)
    await storage.update_status(saga_id, SagaStatus.RUNNING)
    settings = worker_settings.model_copy(update={"saga_recovery_stale_after_seconds": 0.0})
    job = SagaRecoveryJob(container=container, storage=storage, settings=settings)

    assert await job.run_once() == 0

    async with session_factory() as session:
        model = await session.get(SagaStateModel, saga_id)
    assert model is not None
    assert model.recovery_attempts == 1
    assert model.status == SagaStatus.FAILED.value
