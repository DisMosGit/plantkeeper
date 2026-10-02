"""Integration tests for recorded cross-context commands and their dispatcher.

The interesting property is not that a command runs — it is *when* it becomes
durable and *how often* it can run. A saga step records the command in the same
transaction as its own checkpoint, so the record and the decision cannot come
apart; the dispatcher executes it through the owning context's own handler and
marks it done in the same transaction as the effect, so the effect and the record
of it cannot come apart either.

Everything here needs a real Postgres: the claims are ``FOR UPDATE SKIP LOCKED``,
the idempotency is a unique constraint, and the transaction boundaries are the
subject of the test rather than a detail behind it.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from urllib.parse import urlparse
from uuid import UUID, uuid4

import pytest
from dishka import make_async_container
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from plantkeeper.application.commands.care import (
    CreateCareScheduleCommand,
    DeleteCareScheduleCommand,
)
from plantkeeper.application.commands.notifications import (
    CreateOnboardingNotificationCommand,
    DeleteNotificationCommand,
)
from plantkeeper.application.ports.saga_intents import IntentStatus, SagaIntent, recorded_intent
from plantkeeper.domain.garden.household import Household
from plantkeeper.domain.garden.plant import Plant
from plantkeeper.domain.identifiers import HouseholdId, NotificationId, PlantId, SpeciesId
from plantkeeper.domain.values import Location
from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.di.providers import worker_providers
from plantkeeper.infrastructure.persistence.models.care import CareScheduleModel
from plantkeeper.infrastructure.persistence.models.notifications import NotificationModel
from plantkeeper.infrastructure.persistence.repositories.sagas import (
    SqlAlchemySagaIntentRepository,
)
from plantkeeper.infrastructure.persistence.uow import SqlAlchemyUnitOfWork
from plantkeeper.infrastructure.scheduling.command_dispatch import CommandDispatcher

pytestmark = pytest.mark.integration

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
WEEK = timedelta(days=7)

STEP_CARE = 2
STEP_NOTIFICATION = 3
"""The step numbers the onboarding saga records against.

Duplicated from the saga rather than imported: the numbers are part of the intent's
identity, so a test that read them from the code could not notice them changing.
"""


def settings_for(**overrides: object) -> Settings:
    """Dispatcher settings with a zero interval, so a test never sleeps."""
    return Settings(
        intent_dispatch_interval_seconds=0.0,
        intent_dispatch_batch_size=50,
        intent_max_attempts=3,
        intent_claim_lease_seconds=60,
    ).model_copy(update=overrides)


async def _truncate_intents(engine: AsyncEngine) -> None:
    """Remove every recorded command."""
    async with engine.begin() as connection:
        await connection.execute(text("TRUNCATE write_shared.saga_intents CASCADE"))


@pytest.fixture(autouse=True)
async def point_settings_at_the_database(database: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Point the application's settings at this session's container, and empty it.

    The dispatcher builds its own engine from ``Settings``, which reads the
    environment — that is how ``make workers`` is configured — so a test that only
    passed the DSN to its own fixtures would have the container talking to whatever
    database the developer's environment names.

    Asynchronous because asyncpg binds its connections to the running loop: an
    engine created and disposed inside ``asyncio.run`` leaves a driver that cannot
    be used from the test's own loop.
    """
    parsed = urlparse(database)
    assert parsed.username and parsed.password and parsed.hostname and parsed.port
    monkeypatch.setenv("POSTGRES_USER", parsed.username)
    monkeypatch.setenv("POSTGRES_PASSWORD", parsed.password)
    monkeypatch.setenv("POSTGRES_HOST", parsed.hostname)
    monkeypatch.setenv("POSTGRES_PORT", str(parsed.port))
    monkeypatch.setenv("POSTGRES_DB", parsed.path.lstrip("/"))
    # The dispatcher claims *every* pending intent, so one test's leftover command
    # would be executed by the next test's batch. Each test starts from none.
    engine = create_async_engine(database)
    try:
        await _truncate_intents(engine)
    finally:
        await engine.dispose()


async def record(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    saga_id: UUID,
    step_no: int,
    command_name: str,
    payload: dict[str, object],
) -> None:
    """Record one command and commit it, as a saga step does inside its transaction."""
    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        await uow.saga_intents.record(
            recorded_intent(
                saga_id=saga_id,
                step_no=step_no,
                command_name=command_name,
                payload=payload,  # type: ignore[arg-type]  # a JSON-shaped literal
            )
        )
        await uow.commit()


async def intents_of(
    session_factory: async_sessionmaker[AsyncSession], saga_id: UUID
) -> list[SagaIntent]:
    """Return one saga's recorded commands, read through the repository."""
    async with session_factory() as session:
        return await SqlAlchemySagaIntentRepository(session).pending_for_saga(saga_id)


async def seed_plant(session_factory: async_sessionmaker[AsyncSession]) -> PlantId:
    """Insert a household with one plant, so a care command has something to act on."""
    plant_id = PlantId.new()
    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        household = Household.create(name="Home", household_id=HouseholdId.new())
        await uow.households.add(household)
        await uow.plants.add(
            Plant.add(
                household_id=household.id,
                species_id=SpeciesId.new(),
                name="Fern",
                location=Location(value="Shelf"),
                now=NOW,
                plant_id=plant_id,
            )
        )
        await uow.commit()
    return plant_id


async def care_payload(
    session_factory: async_sessionmaker[AsyncSession], *, interval: timedelta = WEEK
) -> tuple[dict[str, object], PlantId]:
    """A care-context command payload for a plant that exists."""
    plant_id = await seed_plant(session_factory)
    command = CreateCareScheduleCommand(
        plant_id=plant_id, watering_interval_seconds=interval.total_seconds()
    )
    return command.model_dump(mode="json"), plant_id


async def run_dispatch(
    session_factory: async_sessionmaker[AsyncSession], **overrides: object
) -> int:
    """Build a worker container, run one dispatch batch, and return how many ran."""
    container = make_async_container(*worker_providers())
    try:
        dispatcher = CommandDispatcher(container=container, settings=settings_for(**overrides))
        return await dispatcher.run_once()
    finally:
        await container.close()


async def test_a_recorded_command_is_executed_by_the_owning_context(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """The happy path: recorded, claimed, executed, and the effect exists."""
    payload, plant_id = await care_payload(session_factory)
    saga_id = uuid4()
    await record(
        session_factory,
        saga_id=saga_id,
        step_no=STEP_CARE,
        command_name="CreateCareScheduleCommand",
        payload=payload,
    )

    assert await run_dispatch(session_factory) == 1

    (intent,) = await intents_of(session_factory, saga_id)
    assert intent.status is IntentStatus.EXECUTED
    async with session_factory() as session:
        schedule = await session.get(CareScheduleModel, plant_id.value)
    assert schedule is not None
    # The handler runs on the system clock — the dispatcher is production code and
    # has no test clock — so the interval, not the instant, is what is asserted.
    assert datetime.now(UTC) < schedule.next_watering_at <= datetime.now(UTC) + WEEK


async def test_executing_one_intent_twice_produces_one_effect(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """A dispatcher that died after the effect but before the record cannot double it.

    This is what the derived ``(saga_id, step_no)`` idempotency key buys: the
    second execution is recognised by the owning context's handler as a replay of
    the first request rather than as a new one.
    """
    payload, plant_id = await care_payload(session_factory)
    saga_id = uuid4()
    await record(
        session_factory,
        saga_id=saga_id,
        step_no=STEP_CARE,
        command_name="CreateCareScheduleCommand",
        payload=payload,
    )
    assert await run_dispatch(session_factory) == 1

    # Put the intent back exactly as that crash would leave it.
    async with session_factory() as session:
        await session.execute(
            text("UPDATE write_shared.saga_intents SET status = 'pending', executed_at = NULL")
        )
        await session.commit()

    assert await run_dispatch(session_factory) == 1

    async with session_factory() as session:
        schedules = (await session.execute(select(CareScheduleModel))).scalars().all()
    assert len(schedules) == 1
    assert schedules[0].plant_id == plant_id.value


async def test_a_crash_after_recording_is_recovered_by_the_dispatcher(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Recording and executing are separate transactions, so a crash between them
    leaves exactly one pending command and exactly one effect when it is retried."""
    payload, plant_id = await care_payload(session_factory)
    saga_id = uuid4()
    await record(
        session_factory,
        saga_id=saga_id,
        step_no=STEP_CARE,
        command_name="CreateCareScheduleCommand",
        payload=payload,
    )

    (before,) = await intents_of(session_factory, saga_id)
    assert before.status is IntentStatus.PENDING
    async with session_factory() as session:
        assert await session.get(CareScheduleModel, plant_id.value) is None

    assert await run_dispatch(session_factory) == 1

    async with session_factory() as session:
        assert await session.get(CareScheduleModel, plant_id.value) is not None


async def test_a_failing_command_is_retried_and_then_parked(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """A command that cannot succeed stops being retried at its budget.

    Parking is the terminal path: past the budget nothing runs the intent again, so
    a poison command cannot spin forever, and its row says why it stopped.
    """
    # A cadence that no longer satisfies the domain's ``timedelta > 0`` rule, so
    # every attempt fails the same way. A plant that merely does not exist will not
    # do: the schema has no cross-context foreign key, so creating a schedule for
    # one succeeds — which is the point of the split, and useless as a poison pill.
    command = CreateCareScheduleCommand(plant_id=PlantId.new(), watering_interval_seconds=0.0)
    saga_id = uuid4()
    await record(
        session_factory,
        saga_id=saga_id,
        step_no=STEP_CARE,
        command_name="CreateCareScheduleCommand",
        payload=command.model_dump(mode="json"),
    )

    assert await run_dispatch(session_factory, intent_max_attempts=2) == 0
    assert await run_dispatch(session_factory, intent_max_attempts=2) == 0
    # Parked, so a third batch picks nothing up.
    assert await run_dispatch(session_factory, intent_max_attempts=2) == 0

    (intent,) = await intents_of(session_factory, saga_id)
    assert intent.status is IntentStatus.PARKED
    assert intent.attempts == 2


async def test_a_leased_intent_is_not_claimed_by_a_second_dispatcher(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """The lease is what stops two dispatchers executing the same command."""
    payload, _ = await care_payload(session_factory)
    saga_id = uuid4()
    await record(
        session_factory,
        saga_id=saga_id,
        step_no=STEP_CARE,
        command_name="CreateCareScheduleCommand",
        payload=payload,
    )

    async with session_factory() as session:
        repository = SqlAlchemySagaIntentRepository(session)
        assert len(await repository.claim_pending(10, lease_seconds=60)) == 1
        await session.commit()
    async with session_factory() as session:
        repository = SqlAlchemySagaIntentRepository(session)
        assert await repository.claim_pending(10, lease_seconds=60) == []
        await session.rollback()


async def test_a_stale_lease_is_reclaimed(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """A dispatcher that died mid-execution does not strand its intents."""
    payload, _ = await care_payload(session_factory)
    saga_id = uuid4()
    await record(
        session_factory,
        saga_id=saga_id,
        step_no=STEP_CARE,
        command_name="CreateCareScheduleCommand",
        payload=payload,
    )

    async with session_factory() as session:
        repository = SqlAlchemySagaIntentRepository(session)
        (claimed,) = await repository.claim_pending(10, lease_seconds=60)
        await session.execute(
            text(
                "UPDATE write_shared.saga_intents"
                " SET claimed_at = now() - interval '10 minutes' WHERE id = :id"
            ),
            {"id": claimed.id},
        )
        await session.commit()

    async with session_factory() as session:
        repository = SqlAlchemySagaIntentRepository(session)
        reclaimed = await repository.claim_pending(10, lease_seconds=60)
        assert [intent.id for intent in reclaimed] == [claimed.id]
        await session.rollback()


async def test_cancelling_a_pending_command_prevents_its_effect(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """A compensation withdraws a command that has not run, and nothing happens."""
    payload, plant_id = await care_payload(session_factory)
    saga_id = uuid4()
    await record(
        session_factory,
        saga_id=saga_id,
        step_no=STEP_CARE,
        command_name="CreateCareScheduleCommand",
        payload=payload,
    )

    async with session_factory() as session:
        repository = SqlAlchemySagaIntentRepository(session)
        cancelled = await repository.cancel_pending(f"saga-intent:{saga_id}:{STEP_CARE}")
        await session.commit()
    assert cancelled is True

    assert await run_dispatch(session_factory) == 0

    (intent,) = await intents_of(session_factory, saga_id)
    assert intent.status is IntentStatus.CANCELLED
    async with session_factory() as session:
        assert await session.get(CareScheduleModel, plant_id.value) is None


async def test_cancelling_an_executed_command_reports_that_it_already_ran(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """The signal a compensating step needs to choose between withdraw and undo."""
    payload, _ = await care_payload(session_factory)
    saga_id = uuid4()
    await record(
        session_factory,
        saga_id=saga_id,
        step_no=STEP_CARE,
        command_name="CreateCareScheduleCommand",
        payload=payload,
    )
    await run_dispatch(session_factory)

    async with session_factory() as session:
        repository = SqlAlchemySagaIntentRepository(session)
        assert await repository.cancel_pending(f"saga-intent:{saga_id}:{STEP_CARE}") is False
        await session.rollback()


async def test_the_undo_of_an_executed_command_removes_the_effect(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """The compensating command is recorded and dispatched like any other."""
    payload, plant_id = await care_payload(session_factory)
    saga_id = uuid4()
    await record(
        session_factory,
        saga_id=saga_id,
        step_no=STEP_CARE,
        command_name="CreateCareScheduleCommand",
        payload=payload,
    )
    await run_dispatch(session_factory)

    undo = DeleteCareScheduleCommand(plant_id=plant_id)
    await record(
        session_factory,
        saga_id=saga_id,
        step_no=STEP_CARE + 1000,
        command_name="DeleteCareScheduleCommand",
        payload=undo.model_dump(mode="json"),
    )
    assert await run_dispatch(session_factory) == 1

    async with session_factory() as session:
        assert await session.get(CareScheduleModel, plant_id.value) is None


async def test_an_unknown_command_name_is_not_silently_skipped(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """A recorded command nobody can execute must not be marked done.

    Marking it executed would lose the effect silently — the worst outcome an
    outbox-shaped pattern can produce — so the intent is left for an operator with
    the reason recorded instead.
    """
    saga_id = uuid4()
    await record(
        session_factory,
        saga_id=saga_id,
        step_no=9,
        command_name="NotACommandThisPlatformHas",
        payload={"anything": True},
    )

    assert await run_dispatch(session_factory) == 0

    (intent,) = await intents_of(session_factory, saga_id)
    assert intent.status is not IntentStatus.EXECUTED
    assert "NotACommandThisPlatformHas" in (intent.last_error or "")


async def test_the_notifications_context_can_undo_its_own_command(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """The delete command belongs to the notifications context, not to the saga."""
    notification_id = NotificationId.new()
    command = CreateOnboardingNotificationCommand(
        household_id=HouseholdId.new(),
        plant_id=PlantId.new(),
        next_watering_at=NOW + WEEK,
        notification_id=notification_id,
    )
    saga_id = uuid4()
    await record(
        session_factory,
        saga_id=saga_id,
        step_no=STEP_NOTIFICATION,
        command_name="CreateOnboardingNotificationCommand",
        payload=command.model_dump(mode="json"),
    )
    assert await run_dispatch(session_factory) == 1

    async with session_factory() as session:
        stored = await session.get(NotificationModel, notification_id.value)
    assert stored is not None
    assert stored.notification_type == "plant_onboarded"

    undo = DeleteNotificationCommand(notification_id=notification_id)
    await record(
        session_factory,
        saga_id=saga_id,
        step_no=STEP_NOTIFICATION + 1000,
        command_name="DeleteNotificationCommand",
        payload=undo.model_dump(mode="json"),
    )
    assert await run_dispatch(session_factory) == 1

    async with session_factory() as session:
        assert await session.get(NotificationModel, notification_id.value) is None


async def test_every_command_the_onboarding_saga_records_has_a_handler() -> None:
    """The dispatcher's registry and the saga's vocabulary must not drift.

    The saga names its commands as strings, so a rename on one side and not the
    other would only fail at dispatch time in production. This fails here instead.
    """
    from plantkeeper.application.registry import build_request_map

    registered = {request_type.__name__ for request_type in build_request_map()}
    assert {
        CreateCareScheduleCommand.__name__,
        CreateOnboardingNotificationCommand.__name__,
        DeleteCareScheduleCommand.__name__,
        DeleteNotificationCommand.__name__,
    } <= registered
