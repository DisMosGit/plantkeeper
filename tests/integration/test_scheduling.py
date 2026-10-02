"""Integration tests for the worker's background jobs.

These run the jobs the way ``apps/workers`` does: the real
``worker_providers()`` container (pointed at the testcontainer database) with only
the clock overridden, and one explicit ``run_once`` per tick so the timer is driven
by the test instead of slept through. What is under test is the wiring — that the
job reaches the saga through a request scope of its own — not the event loop.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from urllib.parse import urlparse
from uuid import UUID, uuid4

import pytest
from cqrs.saga.storage.enums import SagaStatus
from dishka import AsyncContainer, Provider, Scope, make_async_container, provide
from pydantic import JsonValue
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from plantkeeper.application.ports.clock import Clock
from plantkeeper.application.ports.sagas import MissedCareState
from plantkeeper.application.sagas.missed_care import GRACE_PERIOD
from plantkeeper.application.sagas.onboard import OnboardPlantSaga
from plantkeeper.application.telemetry.ingest import TelemetryIngestConsumer
from plantkeeper.domain.care.schedule import CareSchedule
from plantkeeper.domain.catalog.species import Species
from plantkeeper.domain.catalog.values import LightRequirement
from plantkeeper.domain.garden.household import Household
from plantkeeper.domain.garden.plant import Plant
from plantkeeper.domain.identifiers import PlantId, SensorId, SpeciesId
from plantkeeper.domain.telemetry.sensor import OFFLINE_AFTER, Sensor
from plantkeeper.domain.values import Location, WateringInterval
from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.di.providers import worker_providers
from plantkeeper.infrastructure.persistence.models.shared import (
    OutboxModel,
    SagaStateModel,
)
from plantkeeper.infrastructure.persistence.models.telemetry import SensorModel
from plantkeeper.infrastructure.persistence.repositories.sagas import (
    SqlAlchemyMissedCareWindowRepository,
)
from plantkeeper.infrastructure.persistence.repositories.telemetry import (
    SqlAlchemySensorRepository,
)
from plantkeeper.infrastructure.persistence.saga_storage import SqlAlchemySagaStorage
from plantkeeper.infrastructure.persistence.tracking import AggregateTracker
from plantkeeper.infrastructure.persistence.uow import SqlAlchemyUnitOfWork
from plantkeeper.infrastructure.scheduling.missed_care import MissedCareScheduler
from plantkeeper.infrastructure.scheduling.saga_recovery import SagaRecoveryJob
from plantkeeper.infrastructure.scheduling.sensor_silence import SensorSilenceJob
from plantkeeper.infrastructure.scheduling.species_sync import SpeciesSyncScheduler

pytestmark = pytest.mark.integration

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
WEEK = timedelta(days=7)

SPECIES_ID = SpeciesId.new()
"""The catalogue entry the recovery tests' sagas onboard against."""


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


async def seed_species(
    session_factory: async_sessionmaker[AsyncSession], species_id: SpeciesId
) -> None:
    """Insert one catalogue entry, so the onboarding saga's first step can read it."""
    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        async with uow:
            await uow.species.add(
                Species.create(
                    scientific_name="Nephrolepis exaltata",
                    common_name="Boston fern",
                    watering_interval=WateringInterval(value=WEEK),
                    light_requirement=LightRequirement.MEDIUM,
                    species_id=species_id,
                )
            )
            await uow.commit()


async def seed_saga(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    status: SagaStatus,
    attempts: int,
) -> UUID:
    """Store an onboarding saga in a chosen state, as if it had already run.

    Written through the storage rather than by dispatching it: what these tests are
    about is what the recovery job does with a process it finds, not how the
    process got there. ``saga_id`` is in the context because the steps record it
    with the commands they delegate, and a recovered run reads it back out.
    """
    saga_id = uuid4()
    storage = SqlAlchemySagaStorage(session_factory)
    await storage.create_saga(
        saga_id,
        OnboardPlantSaga.__name__,
        {
            "saga_id": str(saga_id),
            "plant_id": str(uuid4()),
            "household_id": str(uuid4()),
            "species_id": str(SPECIES_ID),
        },
    )
    await storage.update_status(saga_id, status)
    async with session_factory() as session:
        await session.execute(
            update(SagaStateModel)
            .where(SagaStateModel.id == saga_id)
            .values(recovery_attempts=attempts)
        )
        await session.commit()
    return saga_id


async def saga_row(
    session_factory: async_sessionmaker[AsyncSession], saga_id: UUID
) -> SagaStateModel:
    """Return the stored state of one saga."""
    async with session_factory() as session:
        model = await session.get(SagaStateModel, saga_id)
    assert model is not None
    return model


async def test_the_recovery_job_retries_a_recorded_failure(
    session_factory: async_sessionmaker[AsyncSession],
    container: AsyncContainer,
    worker_settings: Settings,
) -> None:
    """A ``failed`` process under budget is cleared and run forward again.

    The engine refuses to run a ``failed`` saga, so the retry only works because the
    job clears the status first; the species is in the catalogue this time, so the
    re-run gets all the way to the announcement.
    """
    await seed_species(session_factory, SPECIES_ID)
    saga_id = await seed_saga(session_factory, status=SagaStatus.FAILED, attempts=1)
    settings = worker_settings.model_copy(update={"saga_recovery_stale_after_seconds": 0.0})
    job = SagaRecoveryJob(
        container=container, storage=SqlAlchemySagaStorage(session_factory), settings=settings
    )

    assert await job.run_once() == 1

    names = await outbox_event_names(session_factory)
    assert "SagaRetrying" in names
    assert "PlantOnboarded" in names
    assert (await saga_row(session_factory, saga_id)).status == SagaStatus.COMPLETED.value


async def test_the_recovery_job_parks_a_process_whose_budget_is_spent(
    session_factory: async_sessionmaker[AsyncSession],
    container: AsyncContainer,
    worker_settings: Settings,
) -> None:
    """Past the budget the process is parked, announced, and left alone.

    The second tick is the point of the test: parking has to mean something, so the
    job must neither run the process again nor announce the same park twice.
    """
    saga_id = await seed_saga(
        session_factory,
        status=SagaStatus.FAILED,
        attempts=worker_settings.saga_recovery_max_attempts,
    )
    settings = worker_settings.model_copy(update={"saga_recovery_stale_after_seconds": 0.0})
    job = SagaRecoveryJob(
        container=container, storage=SqlAlchemySagaStorage(session_factory), settings=settings
    )

    assert await job.run_once() == 0

    names = await outbox_event_names(session_factory)
    assert names.count("SagaParked") == 1
    assert "SagaRetrying" not in names
    assert (await saga_row(session_factory, saga_id)).status == SagaStatus.FAILED.value

    assert await job.run_once() == 0
    assert (await outbox_event_names(session_factory)).count("SagaParked") == 1


async def test_an_operator_reset_lets_a_parked_process_run_again(
    session_factory: async_sessionmaker[AsyncSession],
    container: AsyncContainer,
    worker_settings: Settings,
) -> None:
    """``reset_for_retry`` clears the budget, and the next tick acts on it.

    Zeroing only the counter would leave the status ``failed`` and the process
    unrunnable; clearing only the status would leave it over budget. The two are
    cleared together, and what the next tick finds is a process it may resume from
    its recorded history — a resume, not a retry, so no ``SagaRetrying`` is
    announced: nothing failed this time.
    """
    await seed_species(session_factory, SPECIES_ID)
    saga_id = await seed_saga(
        session_factory,
        status=SagaStatus.FAILED,
        attempts=worker_settings.saga_recovery_max_attempts,
    )
    settings = worker_settings.model_copy(update={"saga_recovery_stale_after_seconds": 0.0})
    storage = SqlAlchemySagaStorage(session_factory)
    job = SagaRecoveryJob(container=container, storage=storage, settings=settings)

    assert await job.run_once() == 0
    assert (await saga_row(session_factory, saga_id)).status == SagaStatus.FAILED.value

    await storage.reset_for_retry(saga_id)
    reset = await saga_row(session_factory, saga_id)
    assert reset.status == SagaStatus.RUNNING.value
    assert reset.recovery_attempts == 0

    assert await job.run_once() == 1
    names = await outbox_event_names(session_factory)
    assert "PlantOnboarded" in names
    assert (await saga_row(session_factory, saga_id)).status == SagaStatus.COMPLETED.value


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


# --- The silence timer --------------------------------------------------------
#
# ``SensorOffline`` is in the catalogue and until now could never fire. The timer and
# the persisted ``last_seen_at`` are the two halves that make it real, and what these
# tests pin down is the "at most once per silence" rule end to end: nothing inside the
# period, one announcement past it, nothing on a second tick, and a new announcement
# only after the sensor has reported again.


async def seed_sensor(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    last_seen_at: datetime | None = None,
    sensor_id: SensorId | None = None,
) -> SensorId:
    """Register one sensor, optionally one that has already reported."""
    sensor = Sensor(
        sensor_id or SensorId.new(),
        plant_id=PlantId.new(),
        added_at=NOW,
        last_seen_at=last_seen_at,
    )
    async with session_factory() as session:
        await SqlAlchemySensorRepository(session, AggregateTracker()).add(sensor)
        await session.commit()
    return sensor.id


async def ingest_a_reading(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    sensor_id: SensorId,
    recorded_at: datetime,
    clock: Clock,
) -> None:
    """Store one raw reading through the production ingress.

    The real consumer, not a helper: ``last_seen_at`` is persisted by the ingress's
    own write, so a test that set the column by hand would not be testing the half of
    the feature that was missing.
    """
    payload = json.dumps(
        {
            "sensor_id": str(sensor_id.value),
            "recorded_at": recorded_at.isoformat(),
            "moisture": 12.5,
            "temperature": 21.0,
            "light": 900.0,
        }
    ).encode("utf-8")
    async with session_factory() as session:
        consumer = TelemetryIngestConsumer(SqlAlchemyUnitOfWork(session), clock)
        assert await consumer.ingest(payload) is True


async def stored_sensor(
    session_factory: async_sessionmaker[AsyncSession], sensor_id: SensorId
) -> SensorModel:
    """Return the stored registry row of one sensor."""
    async with session_factory() as session:
        model = await session.get(SensorModel, sensor_id.value)
    assert model is not None
    return model


async def test_a_sensor_that_never_reported_is_not_announced_offline(
    session_factory: async_sessionmaker[AsyncSession],
    container: AsyncContainer,
    worker_settings: Settings,
    clock: FakeClock,
) -> None:
    await seed_sensor(session_factory)
    job = SensorSilenceJob(container=container, settings=worker_settings, interval_seconds=1)

    clock.advance(OFFLINE_AFTER * 10)

    assert await job.run_once() == 0
    assert await outbox_event_names(session_factory) == []


async def test_the_silence_job_announces_a_sensor_once_per_silence(
    session_factory: async_sessionmaker[AsyncSession],
    container: AsyncContainer,
    worker_settings: Settings,
    clock: FakeClock,
) -> None:
    sensor_id = await seed_sensor(session_factory)
    job = SensorSilenceJob(container=container, settings=worker_settings, interval_seconds=1)

    await ingest_a_reading(session_factory, sensor_id=sensor_id, recorded_at=NOW, clock=clock)

    # Inside the period: nothing, however often the timer runs.
    assert await job.run_once() == 0
    clock.advance(OFFLINE_AFTER - timedelta(seconds=1))
    assert await job.run_once() == 0
    assert (await outbox_event_names(session_factory)).count("SensorOffline") == 0

    # Past the period: one announcement, and the row records it.
    clock.advance(timedelta(seconds=1))
    assert await job.run_once() == 1
    assert (await outbox_event_names(session_factory)).count("SensorOffline") == 1
    assert (await stored_sensor(session_factory, sensor_id)).offline_announced_at == clock.now()

    # The same silence, several more ticks: still one.
    clock.advance(timedelta(hours=1))
    assert await job.run_once() == 0
    assert (await outbox_event_names(session_factory)).count("SensorOffline") == 1

    # Reporting again clears the announcement, so the next silence is a new one.
    await ingest_a_reading(
        session_factory, sensor_id=sensor_id, recorded_at=clock.now(), clock=clock
    )
    assert (await stored_sensor(session_factory, sensor_id)).offline_announced_at is None
    assert await job.run_once() == 0

    clock.advance(OFFLINE_AFTER)
    assert await job.run_once() == 1
    assert (await outbox_event_names(session_factory)).count("SensorOffline") == 2
