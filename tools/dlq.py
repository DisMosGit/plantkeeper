"""Inspect and replay what is waiting on ``plantkeeper.dlq.v1``.

Two writers put messages there: the relay abandons an outbox row it could not
publish, and a consumer copies aside a delivery it could not handle
(``docs/adr/0011-consumer-failure-policy.md``). Neither is dropped silently, and
both need an operator to put them back once the cause is fixed — which is what this
command does:

* ``list`` shows what is waiting: the consumer group that gave up, the event, the
  topic it came from and the cause it recorded;
* ``replay`` republishes the selected messages to the topic they came from — body,
  key and headers as they were — and deletes the ledger claim of every delivery
  whose group had one. The two steps belong to one command because a delivery
  whose claim survived a replay would be recognised as a duplicate and ignored.

The claim lives in the ledger of the process that moved the delivery aside:
``write_shared.processed_events`` for a worker's consumer group, Django's
``read_analytics.processed_events`` for a projection's. Whichever it is, a replay
clears the one that holds the row and leaves the other alone, so the operator does
not have to know which side owns the group.

Nothing is replayed by accident: a run needs ``--all`` or ``--event-id``, and
``--dry-run`` prints the plan without touching either store. Replaying appends the
message to the end of its topic, so it is handled after everything that arrived in
the meantime rather than in its original position — which is the price of putting
it back at all.

Usage::

    uv run python tools/dlq.py list
    uv run python tools/dlq.py replay --event-id 0199c0ffee... --dry-run
    uv run python tools/dlq.py replay --event-id 0199c0ffee...
    uv run python tools/dlq.py replay --all --consumer-group plantkeeper-worker-journal-entries
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from collections.abc import Sequence
from dataclasses import dataclass, field
from uuid import UUID, uuid4

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.messaging.topics import (
    DLQ_TOPIC,
    HEADER_CONSUMER_GROUP,
    HEADER_ERROR,
    HEADER_EVENT_ID,
    HEADER_EVENT_NAME,
    HEADER_ORIGINAL_TOPIC,
)

DLQ_HEADERS: tuple[str, ...] = (
    HEADER_CONSUMER_GROUP,
    HEADER_ORIGINAL_TOPIC,
    HEADER_ERROR,
)
"""The headers a dead-letter copy carries beside the message's own.

Stripped before a replay: the topic they name is where the message goes, and the
group and the cause belong to the copy rather than to the message put back.
"""

DRAIN_TIMEOUT_MS = 1_000
"""How long one poll waits for a message before the topic counts as drained."""

MAX_RECORDS = 200
"""How many messages one poll takes; this topic is an operator's queue, not a stream."""

IDLE_POLLS_BEFORE_DRAINED = 2
"""Empty polls in a row that mean "the topic has nothing more for this run"."""

DLQ_REPLAY_GROUP_PREFIX = "plantkeeper-dlq-replay"
"""The group a reading run joins; the run suffix keeps it from ever committing."""

WRITE_LEDGER = "write_shared.processed_events"
READ_LEDGER = "read_analytics.processed_events"

RELAY_COPY = "no ledger claim: the outbox relay wrote it, not a consumer"
INGRESS_COPY = "no ledger claim: the telemetry ingress keeps none"


class DeadLetterError(Exception):
    """A copy cannot be replayed as it stands."""


@dataclass(frozen=True)
class Copy:
    """One message as it sits on the dead-letter topic."""

    offset: int
    key: str
    body: str
    headers: dict[str, str]

    @property
    def consumer_group(self) -> str | None:
        """Which consumer group moved the delivery aside; ``None`` for a relay copy."""
        return self.headers.get(HEADER_CONSUMER_GROUP)

    @property
    def original_topic(self) -> str:
        """Where the message came from — and where a replay puts it back."""
        return self.headers.get(HEADER_ORIGINAL_TOPIC, "")

    @property
    def error(self) -> str:
        """The cause the writer recorded."""
        return self.headers.get(HEADER_ERROR, "")

    @property
    def event_name(self) -> str:
        """The event it carried, when the copy has one (a raw reading has not)."""
        return self.headers.get(HEADER_EVENT_NAME, "")

    @property
    def event_id(self) -> UUID | None:
        """The event's identifier, when the copy has one."""
        raw = self.headers.get(HEADER_EVENT_ID)
        if raw is None:
            return None
        try:
            return UUID(raw)
        except ValueError:
            return None

    def replay_headers(self) -> dict[str, str]:
        """The message's own headers, without the dead-letter ones."""
        return {name: value for name, value in self.headers.items() if name not in DLQ_HEADERS}

    def describe(self) -> str:
        """Return the two-line summary ``list`` prints for this copy."""
        origin = self.consumer_group or "the outbox relay"
        event = self.event_name or "raw message"
        identifier = "" if self.event_id is None else f" {self.event_id}"
        return (
            f"offset {self.offset:>5}  {origin}  {event}{identifier}\n"
            f"              from {self.original_topic}  cause: {self.error}"
        )


@dataclass
class ReplayReport:
    """What a replay did, per message, so the command can be read as a log."""

    replayed: list[tuple[Copy, str]] = field(default_factory=list)
    skipped: list[tuple[Copy, str]] = field(default_factory=list)


def settings_from_environment() -> Settings:
    """Read the same ``.env`` the services read, so the tool addresses their stores."""
    return Settings()


async def dead_letter_topic_exists(settings: Settings) -> bool:
    """Whether the broker has the dead-letter topic at all.

    Asked before subscribing: a topic nobody has written to yet does not exist,
    and joining a group for it would log a metadata error and wait out a rebalance
    to learn that there is nothing there.
    """
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap_servers)
    await admin.start()
    try:
        return DLQ_TOPIC in await admin.list_topics()
    finally:
        await admin.close()


async def read_copies(settings: Settings) -> list[Copy]:
    """Drain the dead-letter topic as it is now, oldest first.

    A snapshot rather than a subscription: the tool keeps no offsets, and an
    operator asking what is waiting wants the whole topic rather than the part some
    earlier run has already seen. So the group identifier is fresh on every run and
    nothing is ever committed — the topic is read from its oldest retained message
    every time.
    """
    if not await dead_letter_topic_exists(settings):
        return []
    consumer = AIOKafkaConsumer(
        DLQ_TOPIC,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=f"{DLQ_REPLAY_GROUP_PREFIX}-{uuid4().hex}",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        copies: list[Copy] = []
        idle_polls = 0
        # Two idle polls rather than one: the first can come back empty while the
        # group is still joining, which would report a topic with messages in it
        # as drained.
        while idle_polls < IDLE_POLLS_BEFORE_DRAINED:
            batches = await consumer.getmany(timeout_ms=DRAIN_TIMEOUT_MS, max_records=MAX_RECORDS)
            if not batches:
                idle_polls += 1
                continue
            idle_polls = 0
            for records in batches.values():
                for record in records:
                    copies.append(_as_copy(record.key, record.value, record.headers, record.offset))
        return copies
    finally:
        await consumer.stop()


def _as_copy(
    key: bytes | None,
    value: bytes | None,
    headers: Sequence[tuple[str, bytes]] | None,
    offset: int,
) -> Copy:
    """Turn one consumed record into the copy an operator sees."""
    return Copy(
        offset=offset,
        key="" if key is None else key.decode("utf-8", errors="replace"),
        body="" if value is None else value.decode("utf-8", errors="replace"),
        headers={name: raw.decode("utf-8", errors="replace") for name, raw in headers or []},
    )


def select(copies: list[Copy], arguments: argparse.Namespace) -> list[Copy]:
    """Return the copies the operator asked for."""
    selected = copies
    if arguments.event_id is not None:
        wanted = UUID(arguments.event_id)
        selected = [copy for copy in selected if copy.event_id == wanted]
    if arguments.consumer_group is not None:
        selected = [copy for copy in selected if copy.consumer_group == arguments.consumer_group]
    return selected


async def clear_write_claim(settings: Settings, *, consumer_group: str, event_id: UUID) -> bool:
    """Delete the claim a worker's consumer group holds, answering whether it had one.

    A plain statement rather than a repository: this is an operator undoing a
    decision, not a use case, and it has to work whatever the application code
    currently looks like.
    """
    engine = create_async_engine(settings.postgres_dsn)
    try:
        async with engine.begin() as connection:
            # The table name is this module's own constant, never an argument.
            result = await connection.execute(
                text(
                    f"DELETE FROM {WRITE_LEDGER} "
                    "WHERE consumer_group = :consumer_group AND event_id = :event_id"
                ),
                {"consumer_group": consumer_group, "event_id": event_id},
            )
            return bool(result.rowcount)
    finally:
        await engine.dispose()


def clear_read_claim(*, consumer_group: str, event_id: UUID) -> bool:
    """Delete the claim a projection's consumer group holds, if that is where it is.

    Through Django's model rather than by naming the table: the read side owns its
    schema, and its migrations decide where its ledger lives. Called on a thread —
    Django's ORM may not run inside the event loop that drains the topic.
    """
    _configure_django()
    from plantkeeper.admin.read_models.models import ProcessedEvent

    deleted, _ = ProcessedEvent.objects.filter(
        consumer_group=consumer_group, event_id=event_id
    ).delete()
    return bool(deleted)


def _configure_django() -> None:
    """Configure Django unless the process already did."""
    from django.apps import apps

    if apps.apps_ready:
        return
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "plantkeeper.admin.settings")
    os.environ.setdefault("DJANGO_DEBUG", "true")

    import django

    django.setup()


async def clear_claim(settings: Settings, copy: Copy) -> str:
    """Clear whichever ledger holds this copy's claim, and say which it was."""
    if copy.consumer_group is None:
        return RELAY_COPY
    if copy.event_id is None:
        return INGRESS_COPY
    group, event_id = copy.consumer_group, copy.event_id
    if await clear_write_claim(settings, consumer_group=group, event_id=event_id):
        return WRITE_LEDGER
    cleared = await asyncio.to_thread(clear_read_claim, consumer_group=group, event_id=event_id)
    return READ_LEDGER if cleared else f"no claim in {WRITE_LEDGER} or {READ_LEDGER}"


async def replay_copy(settings: Settings, copy: Copy, producer: AIOKafkaProducer) -> str:
    """Put one copy back and clear its claim, returning a line for the operator."""
    if not copy.original_topic:
        raise DeadLetterError("the copy names no original_topic header; nothing to replay to")

    claim = await clear_claim(settings, copy)
    await producer.send_and_wait(
        copy.original_topic,
        copy.body.encode("utf-8"),
        key=copy.key.encode("utf-8") if copy.key else None,
        headers=[(name, value.encode("utf-8")) for name, value in copy.replay_headers().items()],
    )
    event = copy.event_name or "raw message"
    return f"replayed offset {copy.offset} to {copy.original_topic} ({event}); claim: {claim}"


async def run_replay(settings: Settings, copies: list[Copy], *, dry_run: bool) -> ReplayReport:
    """Replay every selected copy, or describe what a run would do."""
    report = ReplayReport()
    if dry_run:
        for copy in copies:
            report.replayed.append(
                (copy, f"would replay offset {copy.offset} to {copy.original_topic}")
            )
        return report

    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await producer.start()
    try:
        for copy in copies:
            try:
                report.replayed.append((copy, await replay_copy(settings, copy, producer)))
            except DeadLetterError as error:
                report.skipped.append((copy, str(error)))
    finally:
        await producer.stop()
    return report


def build_parser() -> argparse.ArgumentParser:
    """Return the command-line parser."""
    summary = (__doc__ or "Dead-letter inspection").splitlines()[0]
    parser = argparse.ArgumentParser(description=summary)
    subcommands = parser.add_subparsers(dest="command", required=True)

    listing = subcommands.add_parser("list", help="show what is waiting on the dead-letter topic")
    listing.add_argument("--limit", type=int, default=50, help="how many copies to show")
    listing.add_argument("--verbose", action="store_true", help="let the Kafka client log too")

    replay = subcommands.add_parser("replay", help="put selected copies back on their topic")
    replay.add_argument("--all", action="store_true", help="select every copy on the topic")
    replay.add_argument("--event-id", help="select the copy of this event identifier")
    replay.add_argument("--consumer-group", help="restrict the selection to this group")
    replay.add_argument(
        "--dry-run", action="store_true", help="print the plan without changing anything"
    )
    replay.add_argument("--verbose", action="store_true", help="let the Kafka client log too")
    return parser


def configure_logging(*, verbose: bool) -> None:
    """Keep the Kafka client's own warnings out of an operator's output.

    The client warns about a coordinator it is about to reconnect to, which is
    normal on a group's first join and reads like a failure next to the command's
    own lines. ``--verbose`` puts it back.
    """
    if not verbose:
        logging.getLogger("aiokafka").setLevel(logging.ERROR)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the command and return a process exit code."""
    arguments = build_parser().parse_args(argv)
    configure_logging(verbose=bool(arguments.verbose))
    if arguments.command == "replay" and arguments.event_id is None and not arguments.all:
        print("select what to replay: --event-id <id>, or --all", file=sys.stderr)
        return 2

    settings = settings_from_environment()
    copies = asyncio.run(read_copies(settings))

    if arguments.command == "list":
        shown = copies[: arguments.limit]
        for copy in shown:
            print(copy.describe())
        if len(copies) > len(shown):
            print(f"… and {len(copies) - len(shown)} more (raise --limit)")
        if not copies:
            print("nothing is waiting on the dead-letter topic")
        return 0

    selected = select(copies, arguments)
    if not selected:
        print("nothing matched; run `list` to see what is waiting")
        return 1
    report = asyncio.run(run_replay(settings, selected, dry_run=arguments.dry_run))
    for _copy, line in report.replayed:
        print(line)
    for copy, reason in report.skipped:
        print(f"offset {copy.offset}: {reason}", file=sys.stderr)
    return 1 if report.skipped else 0


if __name__ == "__main__":
    raise SystemExit(main())
