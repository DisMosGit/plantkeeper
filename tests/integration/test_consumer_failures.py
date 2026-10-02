"""Integration tests for the failure policy a consumer's delivery runs under.

Task 6.1's three cases live here, driven through the *production* handler
(``build_consumer_handler``) rather than a hand-rolled call: one poison delivery
whose rule cannot be satisfied, one transient failure that recovers, and one
delivery that keeps failing while the next delivery on the same subscription is
still handled. A fourth pins the rule that makes the policy trustworthy — a
dead-letter copy the broker refuses leaves the delivery unclaimed, so the
transport offers it again instead of the message disappearing.

The consumer under test is the test's own, because no production consumer can be
made to fail on demand: the plan it fails from is shared across the request scopes
the handler opens. Everything else is real — the worker's container, the request
scope, the ledger, the unit of work and the row its handler writes.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, cast
from urllib.parse import urlparse
from uuid import UUID

import pytest
from aiokafka import ConsumerRecord
from cqrs.dispatcher.saga import SagaDispatcher
from dishka import AsyncContainer, Provider, Scope, make_async_container, provide
from faststream.kafka import KafkaMessage
from faststream.kafka.message import ConsumerProtocol
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from plantkeeper.application.ports.dead_letter import DeadLetterPublisher, DeliveryDeadLetter
from plantkeeper.application.ports.unit_of_work import UnitOfWork
from plantkeeper.application.sagas.consumer import Consumer
from plantkeeper.domain.base import DomainEvent
from plantkeeper.domain.care.events import WateringDue
from plantkeeper.domain.garden.errors import PlantAlreadyRemovedError
from plantkeeper.domain.identifiers import HouseholdId, PlantId
from plantkeeper.domain.notifications.notification import Notification
from plantkeeper.domain.notifications.values import NotificationType
from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.di.providers import worker_providers
from plantkeeper.infrastructure.messaging.topics import (
    CARE_EVENTS,
    HEADER_EVENT_ID,
    HEADER_EVENT_NAME,
)
from plantkeeper.infrastructure.persistence.models.notifications import NotificationModel
from plantkeeper.infrastructure.persistence.models.shared import ProcessedEventModel
from plantkeeper.workers.consumers import build_consumer_handler

pytestmark = pytest.mark.integration

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
GROUP = "test-consumer-failures"

ConsumerHandler = Callable[[KafkaMessage], Awaitable[None]]


@dataclass
class FailurePlan:
    """How the consumer under test fails, shared across the scopes it runs in.

    A consumer instance is created per request scope, so the countdown cannot live
    on it: the plan is app-scoped and handed to every instance.
    """

    household_id: HouseholdId
    transient_failures: int = 0
    """How many deliveries fail the retryable way before one succeeds."""

    terminal_failures: int = 0
    """How many deliveries break a domain rule and cannot succeed at all."""

    attempts: int = 0
    """Every attempt taken, retries included."""

    def take_attempt(self) -> None:
        """Count this attempt, and fail it if the plan says so."""
        self.attempts += 1
        if self.terminal_failures > 0:
            self.terminal_failures -= 1
            raise PlantAlreadyRemovedError("this plant is gone")
        if self.transient_failures > 0:
            self.transient_failures -= 1
            raise RuntimeError("the database went away")


class FlakyConsumer(Consumer):
    """A consumer that writes one notification, or fails trying."""

    name = "flaky"
    handled_types = (WateringDue,)

    def __init__(self, unit_of_work: UnitOfWork, plan: FailurePlan) -> None:
        super().__init__(unit_of_work)
        self._plan = plan

    async def handle(self, event: DomainEvent, dispatcher: SagaDispatcher) -> None:
        """Record the reminder, unless this attempt is a planned failure."""
        del event, dispatcher  # this consumer reacts on its own, to one event type
        self._plan.take_attempt()
        await self.unit_of_work.notifications.add(
            Notification.create(
                household_id=self._plan.household_id,
                notification_type=NotificationType.WATERING_DUE,
                now=NOW,
            )
        )


class RecorderPublisher:
    """A dead-letter publisher that keeps what it was asked to copy."""

    def __init__(self, *, refuse: bool = False) -> None:
        self.refuse = refuse
        self.copies: list[DeliveryDeadLetter] = []

    async def publish_moved_aside(self, delivery: DeliveryDeadLetter) -> None:
        """Record the copy, or fail like a broker that will not take it."""
        if self.refuse:
            raise RuntimeError("dead-letter topic unavailable")
        self.copies.append(delivery)


class HarnessProvider(Provider):
    """The three things this test substitutes for the worker's own wiring."""

    def __init__(self, settings: Settings, plan: FailurePlan, publisher: RecorderPublisher) -> None:
        super().__init__()
        self._settings = settings
        self._plan = plan
        self._publisher = publisher

    @provide(scope=Scope.APP)
    def settings(self) -> Settings:
        """Point the worker's engines at this session's container."""
        return self._settings

    @provide(scope=Scope.APP)
    def plan(self) -> FailurePlan:
        """The failure plan every consumer instance shares."""
        return self._plan

    @provide(scope=Scope.APP)
    def dead_letter_publisher(self) -> DeadLetterPublisher:
        """Stand in for the Kafka publisher, so no broker is needed."""
        return self._publisher

    @provide(scope=Scope.REQUEST)
    def flaky(self, unit_of_work: UnitOfWork) -> FlakyConsumer:
        """Build the consumer the handler resolves, as the real one would be."""
        return FlakyConsumer(unit_of_work, self._plan)


def settings_for(database: str) -> Settings:
    """Build the worker's settings against the test's Postgres container.

    Replacing the ``Settings`` factory rather than the environment keeps the test
    off the developer's ``.env``; everything else is the default configuration,
    because Postgres is the only real dependency this test exercises.
    """
    parsed = urlparse(database)
    assert parsed.username and parsed.password and parsed.hostname and parsed.port
    return Settings(
        postgres_user=parsed.username,
        postgres_password=parsed.password,
        postgres_db=parsed.path.lstrip("/"),
        postgres_host=parsed.hostname,
        postgres_port=parsed.port,
        # No real waiting: the schedule itself is pinned in the unit tests, and a
        # sleeping suite is a slow one.
        consumer_retry_initial_wait_seconds=0.0,
        consumer_retry_max_wait_seconds=0.0,
    )


def a_message_for(event: DomainEvent, *, plant_id: PlantId) -> KafkaMessage:
    """Build the delivery the worker's handler receives for ``event``."""
    body = event.model_dump_json().encode("utf-8")
    record = ConsumerRecord(
        topic=CARE_EVENTS,
        partition=0,
        offset=0,
        timestamp=0,
        timestamp_type=0,
        key=str(plant_id).encode("utf-8"),
        value=body,
        headers=[],
        checksum=None,
        serialized_key_size=-1,
        serialized_value_size=-1,
    )
    return KafkaMessage(
        cast("Any", record),
        body,
        headers={HEADER_EVENT_NAME: type(event).__name__, HEADER_EVENT_ID: str(event.event_id)},
        consumer=cast("ConsumerProtocol", None),
    )


@dataclass
class Harness:
    """The delivered-at handler plus what the tests read the outcome from."""

    handler: ConsumerHandler
    session_factory: async_sessionmaker[AsyncSession]
    plan: FailurePlan
    publisher: RecorderPublisher
    plant_id: PlantId = field(default_factory=PlantId.new)

    async def deliver(self, event: DomainEvent) -> None:
        """Deliver one event to the same subscription, as Kafka would."""
        await self.handler(a_message_for(event, plant_id=self.plant_id))

    def a_watering_due(self) -> WateringDue:
        """One care fact for this harness's plant."""
        return WateringDue(plant_id=self.plant_id, due_at=NOW, occurred_at=NOW)


@pytest.fixture
async def harness(
    database: str, session_factory: async_sessionmaker[AsyncSession]
) -> AsyncIterator[Harness]:
    """The worker's container with the flaky consumer and a recording publisher."""
    plan = FailurePlan(household_id=HouseholdId.new())
    publisher = RecorderPublisher()
    settings = settings_for(database)
    container: AsyncContainer = make_async_container(
        *worker_providers(),
        # Later registrations win in Dishka: these replace the worker's own
        # settings, the consumer the handler resolves, and the dead-letter
        # publisher — the Kafka one would need a broker.
        HarnessProvider(settings, plan, publisher),
    )
    try:
        yield Harness(
            handler=build_consumer_handler(
                FlakyConsumer,
                container=container,
                consumer_group=GROUP,
                settings=settings,
            ),
            session_factory=session_factory,
            plan=plan,
            publisher=publisher,
        )
    finally:
        await container.close()


async def claimed_events(
    session_factory: async_sessionmaker[AsyncSession], *, group: str = GROUP
) -> set[UUID]:
    """The delivery identifiers this consumer group has claimed."""
    async with session_factory() as session:
        rows = await session.execute(
            select(ProcessedEventModel.event_id).where(ProcessedEventModel.consumer_group == group)
        )
        return set(rows.scalars().all())


async def notifications(
    session_factory: async_sessionmaker[AsyncSession], household_id: HouseholdId
) -> int:
    """How many notifications the household holds."""
    async with session_factory() as session:
        rows = await session.execute(
            select(NotificationModel.id).where(NotificationModel.household_id == household_id.value)
        )
        return len(rows.scalars().all())


async def test_a_broken_domain_rule_is_moved_aside_at_once(harness: Harness) -> None:
    """A rule cannot be satisfied by repeating the delivery: one attempt, one copy."""
    harness.plan.terminal_failures = 1
    event = harness.a_watering_due()

    await harness.deliver(event)

    assert harness.plan.attempts == 1
    assert len(harness.publisher.copies) == 1
    copy = harness.publisher.copies[0]
    assert copy.consumer_group == GROUP
    assert copy.original_topic == CARE_EVENTS
    assert copy.partition_key == str(harness.plant_id)
    assert copy.headers[HEADER_EVENT_ID] == str(event.event_id)
    assert copy.error.startswith("PlantAlreadyRemovedError")
    assert copy.body == event.model_dump_json()
    # The claim is the other half of the copy: the delivery is dealt with, not
    # pending, so the redelivery Kafka may still make changes nothing.
    assert await claimed_events(harness.session_factory) == {event.event_id}
    assert await notifications(harness.session_factory, harness.plan.household_id) == 0


async def test_a_redelivery_of_a_moved_aside_event_is_neither_retried_nor_copied(
    harness: Harness,
) -> None:
    """The claim is what stops a redelivery from being handled — or copied — twice."""
    harness.plan.terminal_failures = 1
    event = harness.a_watering_due()
    await harness.deliver(event)
    attempts_before = harness.plan.attempts

    await harness.deliver(event)

    assert harness.plan.attempts == attempts_before
    assert len(harness.publisher.copies) == 1


async def test_a_transient_failure_is_retried_until_the_delivery_works(
    harness: Harness,
) -> None:
    """One failure then success: two attempts, one effect, nothing moved aside."""
    harness.plan.transient_failures = 1
    event = harness.a_watering_due()

    await harness.deliver(event)

    assert harness.plan.attempts == 2
    assert harness.publisher.copies == []
    assert await claimed_events(harness.session_factory) == {event.event_id}
    # Exactly one: the failed attempt's write was rolled back with its claim, so
    # the retry started from nothing rather than from half the work.
    assert await notifications(harness.session_factory, harness.plan.household_id) == 1


async def test_the_next_delivery_is_handled_after_one_is_moved_aside(harness: Harness) -> None:
    """A poison delivery must not stop what comes after it on the same key.

    The handler returns normally once the copy is stored, which is what lets the
    subscriber commit the offset and take the next delivery — and the claim keeps
    the poison itself from being handled if Kafka offers it again.
    """
    harness.plan.terminal_failures = 1
    poison = harness.a_watering_due()
    following = harness.a_watering_due()

    await harness.deliver(poison)
    await harness.deliver(following)

    assert len(harness.publisher.copies) == 1
    assert harness.publisher.copies[0].headers[HEADER_EVENT_ID] == str(poison.event_id)
    assert await claimed_events(harness.session_factory) == {poison.event_id, following.event_id}
    assert await notifications(harness.session_factory, harness.plan.household_id) == 1


async def test_a_refused_copy_leaves_the_delivery_unclaimed(harness: Harness) -> None:
    """Nothing was handled and nothing was stored: the broker must offer it again.

    The exception propagates out of the handler, which with the subscribers'
    ``NACK_ON_ERROR`` policy means "seek back and redeliver". Swallowing it would
    be the silent loss the policy exists to prevent.
    """
    harness.plan.terminal_failures = 1
    harness.publisher.refuse = True
    event = harness.a_watering_due()

    with pytest.raises(RuntimeError, match="dead-letter topic unavailable"):
        await harness.deliver(event)

    assert await claimed_events(harness.session_factory) == set()
    assert await notifications(harness.session_factory, harness.plan.household_id) == 0
    # The delivery is still in flight rather than lost: once the copy can be
    # stored, the attempt the redelivery makes handles it.
    harness.publisher.refuse = False
    await harness.deliver(event)
    assert await claimed_events(harness.session_factory) == {event.event_id}
    assert await notifications(harness.session_factory, harness.plan.household_id) == 1
