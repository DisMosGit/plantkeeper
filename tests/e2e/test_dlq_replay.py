"""End-to-end test of the dead-letter path and the operator's replay command.

The whole loop runs against the real containers: a delivery a worker's consumer
cannot handle is retried the configured number of times, copied to
``plantkeeper.dlq.v1`` with its consumer group, origin and cause, and claimed; the
cause is then fixed and the documented replay command puts the message back, after
which the same running consumer handles it.

The failure is real rather than simulated. The journal's consumer cannot resolve
the plant of a watering while its own reference table is renamed out from under it,
which is exactly the shape of the failure the policy is for: it may succeed later,
so it is retried; it does not, so it is moved aside. Nothing about it is a broken
domain rule, and nothing about it is swallowed.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest
from aiokafka import AIOKafkaConsumer
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from plantkeeper.application.ports.plant_references import PlantReference
from plantkeeper.domain.care.events import WateringCompleted
from plantkeeper.domain.identifiers import HouseholdId, PlantId
from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.messaging.topics import (
    CARE_EVENTS,
    DLQ_TOPIC,
    HEADER_CONSUMER_GROUP,
    HEADER_ERROR,
    HEADER_EVENT_ID,
    HEADER_ORIGINAL_TOPIC,
)
from plantkeeper.infrastructure.persistence.models.plant_refs import (
    JournalPlantReferenceModel,
)
from plantkeeper.infrastructure.persistence.models.shared import ProcessedEventModel
from plantkeeper.infrastructure.persistence.repositories.plant_references import (
    SqlAlchemyPlantReferenceRepository,
)
from plantkeeper.infrastructure.persistence.uow import SqlAlchemyUnitOfWork

pytestmark = pytest.mark.slow

ROOT = Path(__file__).resolve().parents[2]
REPLAY_COMMAND = ROOT / "tools" / "dlq.py"

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
NEXT_WATERING = NOW + timedelta(days=7)
JOURNAL_PLANTS = "write_journal.plant_refs"
HIDDEN_JOURNAL_PLANTS = "write_journal.plant_refs_hidden"

WAIT_SECONDS = 60.0
"""Long enough for the relay, the retry budget and a redelivery; a broken pipeline
fails the test instead of hanging."""


@pytest.fixture
def worker_settings(worker_settings: Settings) -> Settings:
    """The suite's worker settings, with the failure policy's waiting removed.

    The delivery under test fails on purpose, and what this test is about is what
    happens *after* the attempts are spent — the copy, the claim, the replay — not
    the schedule between them, which the unit tests cover. Narrowing the fixture the
    shared harness runs on is how the worker it starts gets the same settings.
    """
    return worker_settings.model_copy(
        update={
            "consumer_retry_initial_wait_seconds": 0.0,
            "consumer_retry_max_wait_seconds": 0.0,
        }
    )


async def rename_journal_plants(database: str, *, rename: str) -> None:
    """Rename the journal's reference table, either way, or do nothing if absent.

    ``IF EXISTS`` on both directions so the test's cleanup can run whether or not
    the table was hidden when it started.
    """
    engine = create_async_engine(database)
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f"ALTER TABLE IF EXISTS {rename}"))
    finally:
        await engine.dispose()


async def hide_journal_plants(database: str) -> None:
    """Take the journal's reference table away, as a broken deployment would."""
    await rename_journal_plants(database, rename=f"{JOURNAL_PLANTS} RENAME TO plant_refs_hidden")


async def restore_journal_plants(database: str) -> None:
    """Put the journal's reference table back."""
    await rename_journal_plants(database, rename=f"{HIDDEN_JOURNAL_PLANTS} RENAME TO plant_refs")


async def seed_journal_reference(
    session_factory: async_sessionmaker[AsyncSession], plant_id: PlantId
) -> HouseholdId:
    """Give the journal context a reference row, as an event would have."""
    household_id = HouseholdId.new()
    async with session_factory() as session:
        await SqlAlchemyPlantReferenceRepository(session, JournalPlantReferenceModel).upsert(
            PlantReference(
                plant_id=plant_id,
                household_id=household_id,
                name="Fern",
                location="Shelf",
                seen_at=NOW,
            )
        )
        await session.commit()
    return household_id


async def publish_a_watering(
    session_factory: async_sessionmaker[AsyncSession], plant_id: PlantId
) -> WateringCompleted:
    """Append the care fact to the outbox, as the care context's own commit does."""
    event = WateringCompleted(
        plant_id=plant_id,
        completed_at=NOW,
        next_watering_at=NEXT_WATERING,
        occurred_at=NOW,
    )
    async with session_factory() as session:
        unit_of_work = SqlAlchemyUnitOfWork(session)
        async with unit_of_work:
            await unit_of_work.outbox.append(event)
            await unit_of_work.commit()
    return event


async def journal_entries(
    session_factory: async_sessionmaker[AsyncSession], plant_id: PlantId
) -> int:
    """How many entries the plant's journal holds."""
    async with session_factory() as session:
        return len(await SqlAlchemyUnitOfWork(session).journal_entries.list_by_plant(plant_id))


async def claim_exists(
    session_factory: async_sessionmaker[AsyncSession], *, consumer_group: str, event_id: UUID
) -> bool:
    """Whether the group claimed this delivery — the other half of the move-aside."""
    async with session_factory() as session:
        rows = await session.execute(
            select(ProcessedEventModel.id).where(
                ProcessedEventModel.consumer_group == consumer_group,
                ProcessedEventModel.event_id == event_id,
            )
        )
        return rows.scalar_one_or_none() is not None


async def wait_for_claim(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    consumer_group: str,
    event_id: UUID,
    expected: bool = True,
) -> bool:
    """Wait until the ledger says what the test expects, and report what it says.

    A wait rather than a read: the copy reaches Kafka *before* the claim is
    committed — that is the order that makes a lost delivery impossible — so a
    consumer of the dead-letter topic can see the copy while the claim is still in
    flight.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + WAIT_SECONDS
    claimed = await claim_exists(session_factory, consumer_group=consumer_group, event_id=event_id)
    while claimed is not expected and loop.time() < deadline:
        await asyncio.sleep(0.2)
        claimed = await claim_exists(
            session_factory, consumer_group=consumer_group, event_id=event_id
        )
    return claimed


async def dead_letter_copy(bootstrap_servers: str, event_id: UUID) -> dict[str, str] | None:
    """Return the headers of the dead-lettered copy of ``event_id``, if it is there.

    A consumer with its own group and its own offsets: the suite shares one broker
    with the read-side tests, so the copy this test made is picked out by its
    ``event_id`` rather than assumed to be the first one on the topic.
    """
    consumer = AIOKafkaConsumer(
        DLQ_TOPIC,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        group_id=f"e2e-dlq-{event_id}",
    )
    loop = asyncio.get_running_loop()
    deadline = loop.time() + WAIT_SECONDS
    await consumer.start()
    try:
        while (remaining := deadline - loop.time()) > 0:
            batches = await consumer.getmany(timeout_ms=min(int(remaining * 1000), 1_000))
            for group in batches.values():
                for message in group:
                    headers = {name: value.decode() for name, value in message.headers}
                    if headers.get(HEADER_EVENT_ID) == str(event_id):
                        return headers
    finally:
        await consumer.stop()
    return None


async def count_dead_letter_copies(bootstrap_servers: str, event_id: UUID) -> int:
    """How many copies of ``event_id`` the dead-letter topic holds.

    One is the whole point of claiming the delivery with the copy: a redelivery the
    broker made before the claim was visible would be copied a second time. The
    drain runs until two polls in a row find nothing, so a copy still in flight is
    counted rather than raced past.
    """
    consumer = AIOKafkaConsumer(
        DLQ_TOPIC,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        group_id=f"e2e-dlq-count-{event_id}",
    )
    await consumer.start()
    try:
        matches = 0
        idle_polls = 0
        while idle_polls < 2:
            batches = await consumer.getmany(timeout_ms=1_000)
            if not batches:
                idle_polls += 1
                continue
            idle_polls = 0
            for group in batches.values():
                for message in group:
                    headers = {name: value.decode() for name, value in message.headers}
                    if headers.get(HEADER_EVENT_ID) == str(event_id):
                        matches += 1
        return matches
    finally:
        await consumer.stop()


async def wait_for_journal_entry(
    session_factory: async_sessionmaker[AsyncSession], plant_id: PlantId
) -> int:
    """Wait until the plant's journal holds an entry, and return how many."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + WAIT_SECONDS
    while loop.time() < deadline:
        entries = await journal_entries(session_factory, plant_id)
        if entries:
            return entries
        await asyncio.sleep(0.5)
    return await journal_entries(session_factory, plant_id)


def run_replay_command(
    event_id: UUID, *, dry_run: bool = False
) -> subprocess.CompletedProcess[str]:
    """Run the documented replay command against the running stack.

    A subprocess rather than an import, so the test proves the command an operator
    types: the same interpreter ``uv run python tools/dlq.py`` uses, with the
    environment the fixtures pointed at the test's containers.
    """
    arguments = [sys.executable, str(REPLAY_COMMAND), "replay", "--event-id", str(event_id)]
    if dry_run:
        arguments.append("--dry-run")
    # A fixed argument list, never a shell.
    return subprocess.run(
        arguments,
        cwd=ROOT,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


async def test_a_failing_delivery_is_dead_lettered_and_replayed(
    database: str,
    kafka_bootstrap_servers: str,
    worker_settings: Settings,
    running_worker: None,
) -> None:
    """Poison, copy, fix, replay, handled — the operator's whole loop."""
    group = f"{worker_settings.worker_consumer_group_prefix}-journal-entries"
    engine = create_async_engine(database)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    plant_id = PlantId.new()
    await seed_journal_reference(session_factory, plant_id)

    try:
        await hide_journal_plants(database)
        event = await publish_a_watering(session_factory, plant_id)

        copy = await dead_letter_copy(kafka_bootstrap_servers, event.event_id)
        assert copy is not None, "the failing delivery never reached the dead-letter topic"
        assert copy[HEADER_CONSUMER_GROUP] == group
        assert copy[HEADER_ORIGINAL_TOPIC] == CARE_EVENTS
        assert "plant_refs" in copy[HEADER_ERROR]
        # The claim is what makes the copy terminal rather than pending.
        assert await wait_for_claim(session_factory, consumer_group=group, event_id=event.event_id)
        assert await journal_entries(session_factory, plant_id) == 0

        # The cause is fixed. Replaying is an operator's decision, and the
        # command refuses to guess: the dry run changes nothing.
        await restore_journal_plants(database)
        dry_run = run_replay_command(event.event_id, dry_run=True)
        assert dry_run.returncode == 0, dry_run.stderr
        assert "would replay" in dry_run.stdout
        assert await wait_for_claim(session_factory, consumer_group=group, event_id=event.event_id)

        replayed = run_replay_command(event.event_id)
        assert replayed.returncode == 0, replayed.stderr
        assert "replayed offset" in replayed.stdout
        assert "write_shared.processed_events" in replayed.stdout
        assert not await wait_for_claim(
            session_factory,
            consumer_group=group,
            event_id=event.event_id,
            expected=False,
        )

        assert await wait_for_journal_entry(session_factory, plant_id) == 1
        # Exactly one copy, ever: the claim committed with it, so a redelivery
        # the broker made in between was recognised as already dealt with.
        assert await count_dead_letter_copies(kafka_bootstrap_servers, event.event_id) == 1
    finally:
        # Passed or failed, the schema goes back the way it was: the container is
        # shared with the rest of the suite.
        await restore_journal_plants(database)
        await engine.dispose()
