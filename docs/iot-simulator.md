# IoT simulator

> **Status:** Phase 5. The package is `tools/iot-simulator`; the process is
> `uv run python -m plantkeeper.iot_simulator`. Its output is the input to
> [`docs/telemetry.md`](telemetry.md).

## What it is, and what it deliberately is not

The simulator is **not** a bounded context and **not** a producer of domain events. It
knows about sensors, soil and daylight, and nothing about plants, households or
schedules — which is why `pyproject.toml` builds it on its own, why an `import-linter`
contract forbids it from importing `plantkeeper.domain`, `plantkeeper.application` or
`plantkeeper.infrastructure`, and why the raw message it publishes has no `plant_id`.

That places one job on the write side: a sensor's telemetry is only meaningful once
somebody has registered the sensor for a plant (`POST /api/v1/sensors`). The simulator
prints the sensor ids it derives, precisely so that an operator can register the ones
they intend to watch; readings from a sensor nobody registered are dropped by the
ingress, with a warning.

## The physical model

Three behaviours (`plantkeeper.iot_simulator.physics`), all pure functions of their
arguments:

| Behaviour | Model |
|-----------|-------|
| Daylight | A half-sine from 06:00 to 20:00, zero at night, peaking at 13:00 (`light_at`) |
| Evaporation | `m - floor = (m0 - floor) · 0.5^(Δh / half_life)`, where the half-life shortens as the air warms above freezing and as the light gets brighter (`dry_moisture`) |
| Sensor noise | Gaussian noise on every reading, plus a rare outlier, both clamped to what a sensor can report (`with_noise`) |

A **half-life** rather than a per-hour evaporation rate is the load-bearing choice: the
same number describes a ten-second tick and an hour-long one, so the soil dries to the
same place however often the caller looks at it. The default is 12 hours at 22 °C and
full light, which is a plant that wants water about once a week — the timescale the
project's care context is built around.

Air temperature is seasonal (a yearly sine) plus daily (warmer in the afternoon), and
each sensor carries its own small, deterministic offset: two sensors on one shelf
disagree by a fraction of a degree, but they disagree the same way on every run.

## Scenarios

| Scenario | What changes | Why it exists |
|----------|--------------|---------------|
| `normal` | nothing | the baseline: soil that dries slowly over days |
| `drought` | 31 % starting moisture, a 3-hour half-life, 31 °C, brighter | crosses the domain's low-moisture threshold immediately, so `AdaptiveWateringSaga` pulls a watering forward |
| `overwatering` | a 92 % moisture floor, a 48-hour half-life | stays above the high-moisture threshold, so overwatering is unmistakable |
| `cold_snap` | −2 °C ambient | below the temperature-anomaly threshold, and slows drying to a crawl |
| `sensor_failure` | sensors go silent for five minutes, staggered per sensor | the stream stops for a key and then resumes, as a restarting sensor does |

The thresholds the scenarios are built around are the domain's own
(`MOISTURE_LOW_THRESHOLD`, `MOISTURE_HIGH_THRESHOLD`, the temperature band), so a
scenario cannot drift away from what the consumer reacts to. `AdaptiveWateringSaga`
keeps its own two-day horizon: `drought` trips the low threshold at once, but the saga
still only reschedules a watering that is more than two days away.

## Command line

```bash
# see the stream without a broker; stdout is a replayable capture
uv run python -m plantkeeper.iot_simulator --dry-run --sensors 2 --scenario normal --seed 42

# publish twenty sensors every ten seconds until interrupted
uv run python -m plantkeeper.iot_simulator --scenario normal --seed 42

# replay a capture exactly as it was recorded
uv run python -m plantkeeper.iot_simulator --replay capture.jsonl
```

| Flag | Default | Meaning |
|------|---------|---------|
| `--sensors N` | `20` | how many virtual sensors to run |
| `--interval SEC` | `10` | seconds between two readings of one sensor |
| `--scenario NAME` | `normal` | one of the five above |
| `--seed INT` | `0` | seed for the noise and the initial soil |
| `--sensor-base-id UUID` | random | the base the sensor ids are derived from |
| `--bootstrap-servers` | `$KAFKA_BOOTSTRAP_SERVERS`, else `localhost:9092` | the Kafka to publish to |
| `--replay FILE.jsonl` | — | replay a `--dry-run` capture instead of simulating |
| `--dry-run` | off | print JSON lines to stdout instead of publishing |
| `--max-ticks N` | unlimited | stop after N ticks |

Logs go to stderr, so `--dry-run`'s stdout stays a clean capture that can be piped
straight into `--replay`.

## Determinism

A run is a pure function of `(seed, sensors, scenario, base id, step length)`. The
sensor ids are `uuid5(<fixed namespace>, "<sensor-base-id>:<index>")`, so they are stable
across runs and machines given the same base id; the noise comes from one
`random.Random(seed + index)` per sensor, so one sensor's draws cannot shift another's.
Timestamps come from the wall clock, because a stream whose readings are all in the past
is not useful to a running system.

## Publishing

`aiokafka` to `telemetry.raw`, one JSON object per message, keyed by `sensor_id` so a
sensor's readings stay in one partition and therefore in order. Messages are queued and
flushed every 100 records or every second, whichever comes first, and `SIGINT`/`SIGTERM`
flush what is pending before the process exits.

## Quickstart

```bash
make dev                                   # Kafka, Postgres, Valkey
make migrate                               # write schema (incl. the readings table)
make api &                                 # the write side, to register sensors
make workers &                             # the telemetry ingress and the sagas
make iot-drought                           # a stream that trips AdaptiveWateringSaga
```

To watch a simulated plant react, register the sensors the simulator announces (its
`--sensor-base-id` is printed at startup) for a plant that has a care schedule, then
start the `drought` scenario: the reading reaches `write_telemetry.sensor_readings`, the
ingress appends `TelemetryReceived`, and `AdaptiveWateringSaga` moves the watering to
now.

## Tests

`tests/unit/iot/` covers the model, the scenarios, the publishers and the CLI loop
without a broker; `tests/e2e/test_iot_flow.py` runs the real pipeline — a simulated
reading through Kafka, the worker's ingress registration, the relay and the saga — over
containerised Kafka and Postgres.
