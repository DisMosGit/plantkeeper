"""The CLI: argument handling, the dry-run capture, replay and the loop.

``drive`` takes its own clock, its own sleep and its own stop condition, so the
timetable is asserted without waiting for it, and the publisher is a fake — the
only thing under test here is what the simulator asks for and when.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from io import StringIO
from pathlib import Path
from uuid import UUID

import pytest

from plantkeeper.iot_simulator.cli import (
    build_parser,
    drive,
    load_replay,
    main,
    parse_args,
    run,
    simulator_for,
)
from plantkeeper.iot_simulator.publisher import JournalPublisher
from plantkeeper.iot_simulator.scenarios import DROUGHT, NORMAL, SCENARIOS, scenario_named
from plantkeeper.iot_simulator.simulator import (
    Reading,
    SensorSimulator,
    SensorSimulatorConfig,
)

START = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
INTERVAL = 10.0


def a_simulator(*, sensor_count: int = 2, scenario: str = NORMAL) -> SensorSimulator:
    """A simulator the tests can replay and compare against."""
    return SensorSimulator(
        SensorSimulatorConfig(
            sensor_count=sensor_count,
            base_id=UUID("00000000-0000-0000-0000-0000000000dd"),
            scenario=scenario_named(scenario),
            seed=42,
            step_seconds=INTERVAL,
        )
    )


class RecordingPublisher:
    """Keeps every reading handed to it, and counts flushes and stops."""

    def __init__(self) -> None:
        self.sent: list[tuple[datetime, float]] = []
        self.flushes = 0
        self.stops = 0

    async def send(self, reading: Reading) -> None:
        """Record one reading."""
        self.sent.append((reading.recorded_at, reading.moisture))

    async def flush(self) -> None:
        """Count a flush."""
        self.flushes += 1

    async def stop(self) -> None:
        """Count a stop."""
        self.stops += 1


async def no_delay(_seconds: float) -> None:
    """A sleep that returns at once, so the loop runs as fast as the test wants."""
    return None


class StopAfter:
    """A stop condition that lets the loop run ``ticks`` times and then stops."""

    def __init__(self, ticks: int) -> None:
        self._ticks = ticks
        self._calls = 0

    def __call__(self) -> bool:
        stopped = self._calls >= self._ticks
        self._calls += 1
        return stopped


# --- arguments ----------------------------------------------------------------


def test_the_defaults_are_the_roadmaps() -> None:
    args = parse_args([])

    assert args.sensors == 20
    assert args.interval == 10.0
    assert args.scenario == NORMAL
    assert args.seed == 0
    assert args.dry_run is False
    assert args.max_ticks is None
    assert args.replay is None
    assert args.sensor_base_id is None


def test_every_flag_is_honoured() -> None:
    args = parse_args(
        [
            "--sensors",
            "3",
            "--interval",
            "2.5",
            "--scenario",
            DROUGHT,
            "--seed",
            "42",
            "--bootstrap-servers",
            "broker:9092",
            "--sensor-base-id",
            "00000000-0000-0000-0000-0000000000ee",
            "--dry-run",
            "--max-ticks",
            "5",
        ]
    )

    assert (args.sensors, args.interval, args.scenario) == (3, 2.5, DROUGHT)
    assert (args.seed, args.bootstrap_servers) == (42, "broker:9092")
    assert args.sensor_base_id == UUID("00000000-0000-0000-0000-0000000000ee")
    assert args.dry_run is True
    assert args.max_ticks == 5


def test_the_cli_accepts_exactly_the_registered_scenarios() -> None:
    for name in SCENARIOS:
        assert parse_args(["--scenario", name]).scenario == name

    with pytest.raises(SystemExit):
        parse_args(["--scenario", "monsoon"])


def test_the_help_names_every_scenario() -> None:
    help_text = build_parser().format_help()

    for name in SCENARIOS:
        assert name in help_text


@pytest.mark.parametrize("argv", [["--sensors", "0"], ["--interval", "0"], ["--max-ticks", "0"]])
def test_an_argument_that_cannot_mean_anything_is_refused(argv: list[str]) -> None:
    with pytest.raises(SystemExit):
        parse_args(argv)


def test_a_replay_file_that_does_not_exist_is_refused() -> None:
    with pytest.raises(SystemExit):
        parse_args(["--replay", "/nowhere/telemetry.jsonl"])


def test_the_arguments_build_the_simulator_they_describe() -> None:
    args = parse_args(["--sensors", "4", "--scenario", DROUGHT, "--seed", "7", "--interval", "5"])

    simulator = simulator_for(args)

    assert simulator.config.sensor_count == 4
    assert simulator.config.seed == 7
    assert simulator.config.step_seconds == 5.0
    assert simulator.scenario.name == DROUGHT


# --- the loop -----------------------------------------------------------------


async def test_a_tick_reports_every_sensor_and_then_sleeps_once() -> None:
    publisher = RecordingPublisher()
    delays: list[float] = []

    async def record_delay(seconds: float) -> None:
        delays.append(seconds)

    ticks, sent = await drive(
        a_simulator(sensor_count=3),
        publisher,
        interval=INTERVAL,
        stop=StopAfter(2),
        delay=record_delay,
        wall_clock=lambda: START,
    )

    assert (ticks, sent) == (2, 6)
    assert publisher.flushes == 1
    assert publisher.stops == 1
    # Two ticks, one wait: there is nothing to wait for after the last one.
    assert delays == [INTERVAL]
    assert [moment for moment, _ in publisher.sent[:3]] == [START] * 3
    assert publisher.sent[3][0] == START + timedelta(seconds=INTERVAL)


async def test_max_ticks_ends_the_run() -> None:
    publisher = RecordingPublisher()

    ticks, sent = await drive(
        a_simulator(sensor_count=1),
        publisher,
        interval=INTERVAL,
        delay=no_delay,
        wall_clock=lambda: START,
        max_ticks=3,
    )

    assert (ticks, sent) == (3, 3)


async def test_a_journal_run_writes_what_a_replay_reads_back(tmp_path: Path) -> None:
    path = tmp_path / "capture.jsonl"
    with path.open("w", encoding="utf-8") as stream:
        await drive(
            a_simulator(sensor_count=2),
            JournalPublisher(stream),
            interval=INTERVAL,
            delay=no_delay,
            wall_clock=lambda: START,
            dry_run=True,
            max_ticks=3,
        )

    replay = load_replay(path)

    assert len(replay) == 6
    assert replay[0].sensor_id == replay[2].sensor_id
    assert replay[0].recorded_at == START


async def test_a_replay_publishes_the_recorded_readings_and_then_stops(tmp_path: Path) -> None:
    path = tmp_path / "capture.jsonl"
    with path.open("w", encoding="utf-8") as stream:
        await drive(
            a_simulator(scenario=DROUGHT),
            JournalPublisher(stream),
            interval=INTERVAL,
            delay=no_delay,
            wall_clock=lambda: START,
            dry_run=True,
            max_ticks=3,
        )
    recorded = load_replay(path)

    publisher = RecordingPublisher()
    ticks, sent = await drive(
        a_simulator(sensor_count=99),
        publisher,
        interval=INTERVAL,
        delay=no_delay,
        replay=recorded,
    )

    # One reading per tick, replayed in the order they were recorded.
    assert (ticks, sent) == (len(recorded), len(recorded))
    assert [moisture for _, moisture in publisher.sent] == [
        reading.moisture for reading in recorded
    ]


def test_a_capture_with_a_bad_line_is_rejected_rather_than_half_read(tmp_path: Path) -> None:
    path = tmp_path / "broken.jsonl"
    path.write_text('{"sensor_id": "not-a-uuid"}\n', encoding="utf-8")

    with pytest.raises(ValueError, match="is not a telemetry reading"):
        load_replay(path)


def test_an_empty_capture_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "empty.jsonl"
    path.write_text("\n\n", encoding="utf-8")

    with pytest.raises(ValueError, match="holds no readings"):
        load_replay(path)


# --- the entry points ---------------------------------------------------------


async def test_run_without_a_broker_prints_the_stream() -> None:
    stream = StringIO()
    args = parse_args(
        ["--dry-run", "--sensors", "2", "--seed", "5", "--max-ticks", "2", "--interval", "1"]
    )

    result = await run(args, stream=stream)

    lines = [line for line in stream.getvalue().splitlines() if line]
    assert result == 0
    assert len(lines) == 4
    assert json.loads(lines[0])["sensor_id"]


def test_main_returns_ok_for_a_short_dry_run(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """``main`` is the module entry point: it parses, runs and reports its exit code."""
    capture = tmp_path / "stdout.jsonl"
    with capture.open("w", encoding="utf-8") as stream:
        monkeypatch.setattr("plantkeeper.iot_simulator.cli.sys.stdout", stream)
        exit_code = main(["--dry-run", "--sensors", "1", "--max-ticks", "1"])

    assert exit_code == 0
    assert json.loads(capture.read_text(encoding="utf-8").strip())["moisture"] >= 0.0
