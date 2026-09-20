# IoT simulator

Virtual sensors with a physical model, publishing to Kafka's `telemetry.raw` topic.
`ROADMAP.md` Phase 5 describes the behaviour; `docs/iot-simulator.md` documents the
model and the runbook.

```bash
# see the stream without a broker
uv run python -m plantkeeper.iot_simulator --dry-run --sensors 2 --seed 42

# publish twenty sensors every ten seconds until interrupted
uv run python -m plantkeeper.iot_simulator --scenario normal --seed 42
```

| Flag | Default | Meaning |
|------|---------|---------|
| `--sensors N` | `20` | how many virtual sensors to run |
| `--interval SEC` | `10` | seconds between two readings of one sensor |
| `--scenario NAME` | `normal` | `normal`, `drought`, `overwatering`, `cold_snap`, `sensor_failure` |
| `--seed INT` | `0` | seed for the noise and the initial soil |
| `--sensor-base-id UUID` | random | base the sensor ids are derived from, so a known set can be reproduced |
| `--bootstrap-servers` | `$KAFKA_BOOTSTRAP_SERVERS` or `localhost:9092` | Kafka to publish to |
| `--replay FILE.jsonl` | — | replay a dry-run capture instead of simulating |
| `--dry-run` | off | print JSON lines to stdout instead of publishing |
| `--max-ticks N` | unlimited | stop after N ticks |

Readings reach `AdaptiveWateringSaga` only for sensors the write side knows about,
so register the sensors you intend to watch (`POST /api/v1/sensors`, with the
`sensor_id` the simulator printed) before expecting care to react. Diagnostics go
to stderr, which keeps `--dry-run`'s stdout a clean capture.
