"""The simulator's command line.

Four things can be decided from the command line: how many sensors, how often
each one reports, which scenario they live in, and where the readings go. The
fifth — the seed — is what makes any of it reproducible: the same seed, sensor
count, scenario and step reproduce the same stream, and ``--replay`` turns a
``--dry-run`` capture back into that stream on demand.

The loop itself (:func:`drive`) takes its sleeping, its stop condition and its
publisher as arguments, which is what lets the unit tests check the timetable and
the batching without a broker or a wall clock.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import os
import signal
import sys
from collections.abc import Awaitable, Callable, Iterator, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Final, TextIO
from uuid import UUID

from plantkeeper.iot_simulator.publisher import KafkaPublisher, Publisher, build_publisher
from plantkeeper.iot_simulator.scenarios import DEFAULT_SCENARIO, SCENARIOS
from plantkeeper.iot_simulator.simulator import (
    DEFAULT_SENSOR_COUNT,
    DEFAULT_STEP_SECONDS,
    JSON_TOPIC,
    Reading,
    SensorSimulator,
    SensorSimulatorConfig,
    default_sensor_base_id,
)

LOGGER: Final = logging.getLogger("plantkeeper.iot_simulator")

DEFAULT_BOOTSTRAP_SERVERS: Final = "localhost:9092"

BOOTSTRAP_ENV_VAR: Final = "KAFKA_BOOTSTRAP_SERVERS"

EXIT_OK: Final = 0
EXIT_INTERRUPTED: Final = 130
"""What a shell reports for ``^C``; the run is stopped, not failed."""


def build_parser() -> argparse.ArgumentParser:
    """Return the argument parser for the simulator's CLI."""
    parser = argparse.ArgumentParser(
        prog="python -m plantkeeper.iot_simulator",
        description="Publish simulated sensor telemetry to Kafka's telemetry.raw topic.",
    )
    parser.add_argument(
        "--sensors",
        type=int,
        default=DEFAULT_SENSOR_COUNT,
        help=f"how many virtual sensors to run (default: {DEFAULT_SENSOR_COUNT})",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_STEP_SECONDS,
        help=f"seconds between two readings of one sensor (default: {DEFAULT_STEP_SECONDS})",
    )
    parser.add_argument(
        "--scenario",
        choices=sorted(SCENARIOS),
        default=DEFAULT_SCENARIO.name,
        help=f"the environment the sensors live in (default: {DEFAULT_SCENARIO.name})",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=0,
        help="seed for the noise and the initial soil, for reproducibility (default: 0)",
    )
    parser.add_argument(
        "--sensor-base-id",
        type=UUID,
        default=None,
        help=(
            "base identifier the sensor ids are derived from; pass the one a "
            "previous run printed to reproduce its exact sensors"
        ),
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get(BOOTSTRAP_ENV_VAR, DEFAULT_BOOTSTRAP_SERVERS),
        help=(
            f"Kafka bootstrap servers (default: ${BOOTSTRAP_ENV_VAR} "
            f"or {DEFAULT_BOOTSTRAP_SERVERS})"
        ),
    )
    parser.add_argument(
        "--replay",
        type=Path,
        default=None,
        help="replay a JSONL file written by --dry-run instead of simulating",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print readings as JSON lines to stdout instead of publishing to Kafka",
    )
    parser.add_argument(
        "--max-ticks",
        type=int,
        default=None,
        help="stop after this many ticks (default: run until interrupted)",
    )
    return parser


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse the simulator's arguments, validating what argparse cannot.

    The range checks live here so an invalid run fails before it connects to
    Kafka, with a message about the argument rather than about a producer.
    """
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.sensors < 1:
        parser.error("--sensors must be at least 1")
    if args.interval <= 0:
        parser.error("--interval must be positive")
    if args.max_ticks is not None and args.max_ticks < 1:
        parser.error("--max-ticks must be at least 1")
    if args.replay is not None and not args.replay.is_file():
        parser.error(f"--replay file does not exist: {args.replay}")
    return args


def simulator_for(args: argparse.Namespace) -> SensorSimulator:
    """Build the simulator the arguments describe."""
    return SensorSimulator(
        SensorSimulatorConfig(
            sensor_count=args.sensors,
            base_id=args.sensor_base_id or default_sensor_base_id(),
            scenario=SCENARIOS[args.scenario],
            seed=args.seed,
            step_seconds=args.interval,
        )
    )


def load_replay(path: Path) -> list[Reading]:
    """Read a JSONL capture back into readings, in file order.

    A malformed line is rejected rather than skipped: a replay that quietly drops
    lines would look like it worked while producing a different stream.
    """
    readings: list[Reading] = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            document = json.loads(line)
            readings.append(
                Reading(
                    sensor_id=UUID(document["sensor_id"]),
                    recorded_at=datetime.fromisoformat(document["recorded_at"]),
                    moisture=float(document["moisture"]),
                    temperature=float(document["temperature"]),
                    light=float(document["light"]),
                )
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"{path}:{number} is not a telemetry reading: {exc}") from exc
    if not readings:
        raise ValueError(f"{path} holds no readings")
    return readings


async def _sleep(seconds: float) -> None:
    """Wait for ``seconds``; the loop's default pacing."""
    await asyncio.sleep(seconds)


def _never() -> bool:
    """The stop condition of a run that ends by itself."""
    return False


async def drive(
    simulator: SensorSimulator,
    publisher: Publisher,
    *,
    interval: float,
    stop: Callable[[], bool] = _never,
    delay: Callable[[float], Awaitable[None]] = _sleep,
    wall_clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    replay: Sequence[Reading] | None = None,
    dry_run: bool = False,
    journal: TextIO | None = None,
    max_ticks: int | None = None,
) -> tuple[int, int]:
    """Run the stream until stopped, and return ``(ticks, readings)``.

    Each tick reports what every sensor currently holds, then waits for the
    interval. The loops are injectable: a test passes a stop that fires after one
    tick and a delay that does not sleep, so the timetable is asserted without
    waiting on it.
    """
    started_at = wall_clock() if replay is None else datetime.now(UTC)
    journal_stream = sys.stdout if journal is None else journal
    ticks = 0
    sent = 0
    stopped = stop()
    while not stopped and (max_ticks is None or ticks < max_ticks):
        if replay is None:
            instant = started_at + timedelta(seconds=ticks * interval)
            readings: Sequence[Reading] = simulator.readings(instant)
        else:
            readings = replay[ticks : ticks + 1]
            if not readings:
                break
        for reading in readings:
            await publisher.send(reading)
            sent += 1
        ticks += 1
        stopped = stop()
        # Nothing to wait for after the last tick: not when ``max_ticks`` is
        # reached, not at the end of a replay, and not once a signal has asked the
        # loop to stop. The check happens before the sleep rather than after it.
        if stopped or (max_ticks is not None and ticks >= max_ticks):
            break
        await delay(interval)
    await publisher.flush()
    await publisher.stop()

    if dry_run:
        journal_stream.flush()
    LOGGER.info(
        "simulator stopped: %d tick(s), %d reading(s) from %d sensor(s), scenario %s",
        ticks,
        sent,
        simulator.config.sensor_count,
        simulator.scenario.name,
    )
    return ticks, sent


@contextlib.contextmanager
def _stop_on_signals() -> Iterator[Callable[[], bool]]:
    """Install ``SIGINT``/``SIGTERM`` handlers and yield the stop condition.

    The handlers only set a flag: the loop finishes the tick it is in and then
    flushes, so a ``^C`` never leaves a half-sent batch behind. On a platform
    without ``add_signal_handler`` the flag is set by ``KeyboardInterrupt``
    instead, which :func:`main` catches.
    """
    stopped = False

    def request_stop(*_args: object) -> None:
        nonlocal stopped
        if not stopped:
            LOGGER.info("shutdown requested, finishing the current tick")
        stopped = True

    loop = asyncio.get_running_loop()
    installed: list[signal.Signals] = []
    for received in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(received, request_stop)
        except NotImplementedError, RuntimeError:  # pragma: no cover - Windows
            continue
        installed.append(received)
    try:
        yield lambda: stopped
    finally:
        for received in installed:
            with contextlib.suppress(NotImplementedError, RuntimeError):
                loop.remove_signal_handler(received)


async def run(args: argparse.Namespace, *, stream: TextIO | None = None) -> int:
    """Run one simulator session from parsed arguments.

    ``stream`` defaults to stdout *at call time* rather than at import time, so the
    dry-run capture follows a caller that redirected stdout — which is exactly what
    ``python -m plantkeeper.iot_simulator --dry-run > capture.jsonl`` does.
    """
    stream = sys.stdout if stream is None else stream
    simulator = simulator_for(args)
    publisher = build_publisher(
        dry_run=args.dry_run,
        bootstrap_servers=args.bootstrap_servers,
        topic=JSON_TOPIC,
        stream=stream,
    )
    if isinstance(publisher, KafkaPublisher):
        await publisher.start()

    LOGGER.info(
        "simulator starting: %d sensor(s) every %.1fs, scenario %s, base id %s, topic %s",
        simulator.config.sensor_count,
        args.interval,
        simulator.scenario.name,
        simulator.config.base_id,
        JSON_TOPIC if not args.dry_run else "stdout",
    )
    replay = load_replay(args.replay) if args.replay is not None else None
    with _stop_on_signals() as stop:
        await drive(
            simulator,
            publisher,
            interval=args.interval,
            stop=stop,
            replay=replay,
            dry_run=args.dry_run,
            journal=stream,
            max_ticks=args.max_ticks,
        )
    return EXIT_OK


def configure_logging() -> None:
    """Make the simulator's own progress visible on stderr.

    Diagnostics go to stderr so ``--dry-run``'s stdout stays a clean JSONL
    capture that can be piped straight into ``--replay``.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
        stream=sys.stderr,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point for ``python -m plantkeeper.iot_simulator``."""
    configure_logging()
    args = parse_args(argv)
    try:
        return asyncio.run(run(args))
    except KeyboardInterrupt:  # pragma: no cover - only when no signal handler could be installed
        LOGGER.info("interrupted")
        return EXIT_INTERRUPTED
    except ValueError as exc:
        LOGGER.error("%s", exc)
        return 2
