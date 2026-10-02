"""Integration tests for the reference rows a consuming context keeps about a plant.

``NotificationConsumer`` and ``JournalEntryConsumer`` both need one fact the Garden
context owns — which household a plant belongs to — and neither may read
``write_garden`` for it (``event-transport``). Each keeps its own ``plant_refs`` row
instead, filled from ``PlantAdded``/``PlantMoved``/``PlantRemoved`` under its own
consumer group.

Everything here goes through the worker's own container rather than a hand-built
consumer. The two tables have the same shape behind two names, so the wiring that
hands each consumer *its* table is half of what these tests are about; the other half
is the documented rule for a fact that arrives before the plant it concerns. That
rule is *dropped*: this context holds no household for the plant, and inventing one —
or holding the fact aside until the introduction turns up — would make the reference
table a queue rather than a copy of a fact another context owns.

No ``write_garden`` row is inserted anywhere in this file. A watering that is
journalled, or a reminder that is addressed, from a reference row alone therefore
cannot have come from the Garden context's tables — which is what makes the absence
of those rows an assertion rather than a coincidence.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from urllib.parse import urlparse

import pytest
from dishka import AsyncContainer, make_async_container
from sqlalchemy import Connection, event, func, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from plantkeeper.application.journal.consumer import JournalEntryConsumer
from plantkeeper.application.notifications.consumer import NotificationConsumer
from plantkeeper.application.ports.plant_references import PlantReference
from plantkeeper.application.sagas.consumer import Consumer
from plantkeeper.domain.base import DomainEvent
from plantkeeper.domain.care.events import WateringCompleted, WateringDue
from plantkeeper.domain.garden.events import PlantAdded, PlantMoved, PlantRemoved
from plantkeeper.domain.identifiers import HouseholdId, PlantId, SpeciesId
from plantkeeper.domain.values import Location
from plantkeeper.infrastructure.di.providers import worker_providers
from plantkeeper.infrastructure.persistence.models.garden import PlantModel
from plantkeeper.infrastructure.persistence.models.notifications import NotificationModel
from plantkeeper.infrastructure.persistence.models.plant_refs import (
    JournalPlantReferenceModel,
    NotificationPlantReferenceModel,
)
from plantkeeper.infrastructure.persistence.repositories.plant_references import (
    SqlAlchemyPlantReferenceRepository,
)
from plantkeeper.infrastructure.persistence.uow import SqlAlchemyUnitOfWork
from plantkeeper.workers.consumers import build_saga_dispatcher

pytestmark = pytest.mark.integration

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
LATER = NOW + timedelta(hours=1)
EARLIER = NOW + timedelta(minutes=30)
WEEK = timedelta(days=7)

NOTIFICATION_GROUP = "test-notifications"
JOURNAL_GROUP = "test-journal-entries"


def point_settings_at(database: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Point ``Settings()`` at the testcontainer before the container is built.

    The container builds its own engine from the environment — that is how
    ``make workers`` is configured — so a test that only passed the DSN to its own
    fixtures would have the consumers talking to whatever database the developer's
    environment names.
    """
    parsed = urlparse(database)
    assert parsed.username and parsed.password and parsed.hostname and parsed.port
    monkeypatch.setenv("POSTGRES_USER", parsed.username)
    monkeypatch.setenv("POSTGRES_PASSWORD", parsed.password)
    monkeypatch.setenv("POSTGRES_DB", parsed.path.lstrip("/"))
    monkeypatch.setenv("POSTGRES_HOST", parsed.hostname)
    monkeypatch.setenv("POSTGRES_PORT", str(parsed.port))


@pytest.fixture(autouse=True)
def worker_settings(database: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Point the environment at this session's container."""
    point_settings_at(database, monkeypatch)


@pytest.fixture
async def container() -> AsyncIterator[AsyncContainer]:
    """The worker's container, wired exactly as ``make workers`` wires it."""
    container = make_async_container(*worker_providers())
    try:
        yield container
    finally:
        await container.close()


@contextmanager
def statements_on(engine: AsyncEngine) -> Iterator[list[str]]:
    """Record every statement the engine runs while the block is open.

    The listener is on the sync engine the async one wraps, which is where
    SQLAlchemy emits ``before_cursor_execute`` for both.
    """
    recorded: list[str] = []

    def record(
        conn: Connection,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: bool,
    ) -> None:
        recorded.append(statement)

    event.listen(engine.sync_engine, "before_cursor_execute", record)
    try:
        yield recorded
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", record)


async def consume(
    container: AsyncContainer,
    consumer_type: type[Consumer],
    event: DomainEvent,
    *,
    group: str,
) -> bool:
    """Run one delivery the way the worker's handler does.

    One request scope, the consumer and the saga dispatcher resolved from it, and
    the consumer's own ``consume`` — the same three steps
    ``plantkeeper.workers.consumers.build_consumer_handler`` performs, minus the
    Kafka delivery that carried the event.
    """
    async with container() as request_container:
        consumer = await request_container.get(consumer_type)
        dispatcher = await build_saga_dispatcher(request_container)
        return await consumer.consume(event, consumer_group=group, dispatcher=dispatcher)


def a_plant_added(
    plant_id: PlantId,
    household_id: HouseholdId,
    *,
    name: str = "Fern",
    location: str = "Shelf",
    occurred_at: datetime = NOW,
) -> PlantAdded:
    """One introduction of a plant, as the Garden context publishes it."""
    return PlantAdded(
        plant_id=plant_id,
        household_id=household_id,
        species_id=SpeciesId.new(),
        name=name,
        location=Location(value=location),
        added_at=occurred_at,
        occurred_at=occurred_at,
    )


def a_plant_moved(plant_id: PlantId, *, location: str, occurred_at: datetime) -> PlantMoved:
    """One move of a plant."""
    return PlantMoved(
        plant_id=plant_id,
        previous_location=Location(value="Shelf"),
        location=Location(value=location),
        occurred_at=occurred_at,
    )


def a_plant_removed(plant_id: PlantId) -> PlantRemoved:
    """One removal of a plant."""
    return PlantRemoved(plant_id=plant_id, removed_at=LATER, occurred_at=LATER)


def a_watering_completed(plant_id: PlantId) -> WateringCompleted:
    """One care fact the journal should record."""
    return WateringCompleted(
        plant_id=plant_id,
        completed_at=NOW,
        next_watering_at=NOW + WEEK,
        occurred_at=NOW,
    )


def a_watering_due(plant_id: PlantId) -> WateringDue:
    """One care fact a reminder should be addressed from."""
    return WateringDue(plant_id=plant_id, due_at=NOW, occurred_at=NOW)


async def notification_reference(
    session_factory: async_sessionmaker[AsyncSession], plant_id: PlantId
) -> PlantReference | None:
    """Read a plant out of the notifications context's own table."""
    async with session_factory() as session:
        return await SqlAlchemyPlantReferenceRepository(
            session, NotificationPlantReferenceModel
        ).get(plant_id)


async def journal_reference(
    session_factory: async_sessionmaker[AsyncSession], plant_id: PlantId
) -> PlantReference | None:
    """Read a plant out of the journal's own table."""
    async with session_factory() as session:
        return await SqlAlchemyPlantReferenceRepository(session, JournalPlantReferenceModel).get(
            plant_id
        )


async def garden_plants(session_factory: async_sessionmaker[AsyncSession]) -> int:
    """How many plants the Garden context actually holds."""
    async with session_factory() as session:
        result = await session.execute(select(func.count()).select_from(PlantModel))
        return int(result.scalar_one())


async def journal_entries(
    session_factory: async_sessionmaker[AsyncSession], plant_id: PlantId
) -> int:
    """How many entries the plant's journal holds."""
    async with session_factory() as session:
        return len(await SqlAlchemyUnitOfWork(session).journal_entries.list_by_plant(plant_id))


async def notifications(
    session_factory: async_sessionmaker[AsyncSession], household_id: HouseholdId
) -> list[NotificationModel]:
    """The household's notifications, read straight from the table."""
    async with session_factory() as session:
        statement = select(NotificationModel).where(
            NotificationModel.household_id == household_id.value
        )
        return list((await session.execute(statement)).scalars().all())


async def test_each_context_records_the_plant_in_its_own_table(
    container: AsyncContainer, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    """The same ``PlantAdded`` fills two tables, one per consuming context.

    Both contexts name the same port and hold the same shape of row, so this is
    also the wiring test: a container that handed one consumer the other's table
    would leave one schema empty and put two rows in the other.
    """
    plant_id, household_id = PlantId.new(), HouseholdId.new()
    event = a_plant_added(plant_id, household_id)

    assert await consume(container, NotificationConsumer, event, group=NOTIFICATION_GROUP)
    assert await consume(container, JournalEntryConsumer, event, group=JOURNAL_GROUP)

    notification = await notification_reference(session_factory, plant_id)
    journal = await journal_reference(session_factory, plant_id)
    assert notification is not None and journal is not None
    assert notification.household_id == household_id
    assert journal.household_id == household_id
    assert notification.name == journal.name == "Fern"
    assert notification.location == journal.location == "Shelf"


async def test_a_move_refreshes_the_row_the_context_already_holds(
    container: AsyncContainer, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    """A move carries only the new location; the rest survives it."""
    plant_id, household_id = PlantId.new(), HouseholdId.new()
    await consume(
        container,
        JournalEntryConsumer,
        a_plant_added(plant_id, household_id),
        group=JOURNAL_GROUP,
    )

    await consume(
        container,
        JournalEntryConsumer,
        a_plant_moved(plant_id, location="Window", occurred_at=LATER),
        group=JOURNAL_GROUP,
    )

    reference = await journal_reference(session_factory, plant_id)
    assert reference is not None
    assert reference.location == "Window"
    assert reference.name == "Fern"
    assert reference.household_id == household_id


async def test_an_out_of_order_move_does_not_revert_the_row(
    container: AsyncContainer, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    """A rebuilt group replays the topic, and a replay must not undo a later move."""
    plant_id, household_id = PlantId.new(), HouseholdId.new()
    for delivery in (
        a_plant_added(plant_id, household_id),
        a_plant_moved(plant_id, location="Window", occurred_at=LATER),
        a_plant_moved(plant_id, location="Attic", occurred_at=EARLIER),
    ):
        await consume(container, JournalEntryConsumer, delivery, group=JOURNAL_GROUP)

    reference = await journal_reference(session_factory, plant_id)
    assert reference is not None
    assert reference.location == "Window"
    assert reference.seen_at == LATER


async def test_a_move_for_a_plant_the_context_has_not_seen_is_dropped(
    container: AsyncContainer, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    """Without the introduction there is no household to record the move against."""
    plant_id = PlantId.new()

    await consume(
        container,
        JournalEntryConsumer,
        a_plant_moved(plant_id, location="Window", occurred_at=LATER),
        group=JOURNAL_GROUP,
    )

    assert await journal_reference(session_factory, plant_id) is None


async def test_a_removal_forgets_the_plant(
    container: AsyncContainer, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    """The reference is a copy, and a copy of a removed plant is not kept."""
    plant_id, household_id = PlantId.new(), HouseholdId.new()
    await consume(
        container,
        JournalEntryConsumer,
        a_plant_added(plant_id, household_id),
        group=JOURNAL_GROUP,
    )

    await consume(container, JournalEntryConsumer, a_plant_removed(plant_id), group=JOURNAL_GROUP)

    assert await journal_reference(session_factory, plant_id) is None


async def test_a_watering_for_a_plant_the_journal_has_not_seen_is_dropped(
    container: AsyncContainer, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    """The documented rule: no household, no entry — not a held fact."""
    plant_id = PlantId.new()

    assert await consume(
        container, JournalEntryConsumer, a_watering_completed(plant_id), group=JOURNAL_GROUP
    )

    assert await journal_entries(session_factory, plant_id) == 0


async def test_a_reminder_for_a_plant_the_notifications_context_has_not_seen_is_dropped(
    container: AsyncContainer, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    """Addressing a reminder needs a household, and an unknown plant names none."""
    plant_id, household_id = PlantId.new(), HouseholdId.new()

    assert await consume(
        container, NotificationConsumer, a_watering_due(plant_id), group=NOTIFICATION_GROUP
    )

    assert await notifications(session_factory, household_id) == []


async def test_the_journal_journals_a_watering_from_its_own_row_alone(
    container: AsyncContainer, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    """The watering is recorded although ``write_garden`` holds no plant at all."""
    plant_id, household_id = PlantId.new(), HouseholdId.new()
    await consume(
        container,
        JournalEntryConsumer,
        a_plant_added(plant_id, household_id),
        group=JOURNAL_GROUP,
    )
    assert await garden_plants(session_factory) == 0

    assert await consume(
        container, JournalEntryConsumer, a_watering_completed(plant_id), group=JOURNAL_GROUP
    )

    assert await journal_entries(session_factory, plant_id) == 1


async def test_a_reminder_is_addressed_from_the_notifications_own_row_alone(
    container: AsyncContainer, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    """The reminder reaches the household the reference row names, and no other."""
    plant_id, household_id = PlantId.new(), HouseholdId.new()
    await consume(
        container,
        NotificationConsumer,
        a_plant_added(plant_id, household_id),
        group=NOTIFICATION_GROUP,
    )
    assert await garden_plants(session_factory) == 0

    assert await consume(
        container, NotificationConsumer, a_watering_due(plant_id), group=NOTIFICATION_GROUP
    )

    stored = await notifications(session_factory, household_id)
    assert [notification.payload["plant_id"] for notification in stored] == [str(plant_id)]


async def test_the_reference_consumers_issue_no_query_against_the_garden_schema(
    container: AsyncContainer, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    """The rule is about the session, so it is checked on the statements it runs.

    A behavioural test can show that a care fact was handled; only the statements
    can show that no query reached another context's tables on the way — including
    one whose answer happened not to be needed this time.
    """
    plant_id, household_id = PlantId.new(), HouseholdId.new()

    async with container() as request_container:
        engine = await request_container.get(AsyncEngine)
        with statements_on(engine) as statements:
            dispatcher = await build_saga_dispatcher(request_container)
            journal = await request_container.get(JournalEntryConsumer)
            assert await journal.consume(
                a_plant_added(plant_id, household_id),
                consumer_group=JOURNAL_GROUP,
                dispatcher=dispatcher,
            )
            notifications = await request_container.get(NotificationConsumer)
            assert await notifications.consume(
                a_watering_due(plant_id),
                consumer_group=NOTIFICATION_GROUP,
                dispatcher=dispatcher,
            )

    assert statements, "no statement was recorded, so the check below proves nothing"
    assert [statement for statement in statements if "write_garden" in statement] == []
