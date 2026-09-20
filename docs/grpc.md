# gRPC API

> **Status:** Phase 7. The `CareService` and `GardenService` are served over gRPC
> by `apps/api`, next to the REST API and over the same application layer. See
> [`ROADMAP.md`](../ROADMAP.md) §7 and
> [`docs/adr/0006-rest-and-grpc.md`](adr/0006-rest-and-grpc.md).

PlantKeeper speaks two protocols on purpose: REST is the general-purpose,
browser-friendly write API, and gRPC is the typed, reflection-discoverable surface
for the calls a client makes often — what is due today, what plants exist,
complete a watering. Both dispatch through the same `python-cqrs` mediator, the
same command and query handlers, and the same `Dishka` container, so there is one
behaviour with two wire formats rather than two implementations.

## Running it

```bash
make proto   # generate the Python stubs from proto/ (they are not committed)
make grpc    # gRPC server on 0.0.0.0:50051
```

The server is a process of its own, like `make api` and `make admin`. It uses the
same `Settings` — in particular the Postgres connection — and, like the REST
process, it never talks to Kafka: commands commit their events into the outbox and
the worker's relay publishes them.

`make lint`, `make test`, `make test-unit`, `make test-e2e` and `make grpc` depend
on `make proto`, so a fresh checkout does not need a manual generation step before
those. A bare `uv run pytest` does, because the tests import the generated stubs.

## Services

| Service | RPC | What it does |
|---------|-----|--------------|
| `plantkeeper.v1.CareService` | `GetTodayCare` | The household's schedules that come due before midnight UTC. |
| `plantkeeper.v1.CareService` | `CompleteWatering` | Complete a watering and answer with the schedule for the next one. |
| `plantkeeper.v1.GardenService` | `ListPlants` | The household's plants, oldest first. |
| `plantkeeper.v1.GardenService` | `GetPlant` | One plant. |

Health (`grpc.health.v1.Health`) and server reflection
(`grpc.reflection.v1alpha.ServerReflection`) are registered too, which is what
makes `grpcurl` usable without the `.proto` files.

### Message conventions

- **Identifiers are wrapper messages** (`PlantId`, `HouseholdId`, `SpeciesId`,
  `SensorId`, each with a `string value`), mirroring the domain's typed UUID
  wrappers: a `PlantId` cannot be passed where a `SensorId` is expected.
- **Instants are `google.protobuf.Timestamp`**, **intervals
  `google.protobuf.Duration`** — the standard well-known types, not epoch integers
  a client would have to interpret.
- **Optional fields are `optional`** in the proto3 sense, so presence is explicit:
  a plant that was never watered has no `last_watered_at`, and
  `CompleteWateringRequest` without `expected_version` means "use the schedule's
  current version".
- `SensorReading` in `plantkeeper/v1/common.proto` is the shared shape of a raw
  telemetry measurement. Phase 7 has no RPC that returns it; it is there for the
  telemetry surface a later phase can expose, and the stub contract test keeps it
  usable meanwhile.

## Calling it with grpcurl

```bash
# What does the server expose?
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext localhost:50051 list plantkeeper.v1.CareService

# Care: what is due today
grpcurl -plaintext \
  -d '{"household_id": {"value": "<household-uuid>"}}' \
  localhost:50051 plantkeeper.v1.CareService/GetTodayCare

# Care: complete a watering
grpcurl -plaintext \
  -d '{"plant_id": {"value": "<plant-uuid>"}}' \
  localhost:50051 plantkeeper.v1.CareService/CompleteWatering

# Garden: one plant, and the whole household
grpcurl -plaintext \
  -d '{"plant_id": {"value": "<plant-uuid>"}}' \
  localhost:50051 plantkeeper.v1.GardenService/GetPlant

grpcurl -plaintext \
  -d '{"household_id": {"value": "<household-uuid>"}, "include_removed": false}' \
  localhost:50051 plantkeeper.v1.GardenService/ListPlants

# Health
grpcurl -plaintext localhost:50051 grpc.health.v1.Health/Check
```

## Errors

Every failure is a gRPC status, not a generic `UNKNOWN`:

| Failure | Status |
|---------|--------|
| The plant or household does not exist (`NotFoundError`) | `NOT_FOUND` |
| A broken invariant — watering twice within the hour, a schedule at the wrong version (`DomainError`) | `FAILED_PRECONDITION` |
| A unique key lost a race (`ConcurrentWriteError`, including the event store's version race) | `ABORTED` |
| An idempotency key reused with a different body (`IdempotencyKeyConflictError`) | `ALREADY_EXISTS` |
| A value that cannot become a domain value — a malformed UUID (`pydantic.ValidationError`) | `INVALID_ARGUMENT` |
| Anything else | `INTERNAL` |

The REST API maps the same application errors onto HTTP statuses; the two tables
live side by side in `plantkeeper.api.errors` and `plantkeeper.api.grpc.errors`.

## Generating the stubs

The stubs are **not committed**. They are build products of the `.proto` files,
and a committed copy would drift from the contract it was generated from; they are
ignored by `.gitignore` under
`apps/api/src/plantkeeper/api/grpc/generated/`.

```bash
make proto   # every proto/**/*.proto → apps/api/src/plantkeeper/api/grpc/generated/
```

`tools/protogen.py` runs `grpc_tools.protoc` with `--python_out`,
`--grpc_python_out` and `--pyi_out`, then rewrites one thing: protoc derives a
module's import path from the proto file's path relative to the include root, so
`plantkeeper/v1/common.proto` becomes `plantkeeper.v1.common_pb2` and the other
generated modules import it as `from plantkeeper.v1 import common_pb2`. The stubs
live under `plantkeeper.api.grpc.generated`, so those imports are rewritten to
that package path. protoc has no option for the mapping (`--python_opt=M...` is
rejected by `grpc_tools.protoc`), and a runtime `sys.modules` alias would hide the
indirection; a textual rewrite of a generated file is the honest, small way.

Adding a proto file is therefore: write it under `proto/plantkeeper/v1/`, run
`make proto`, and wire the new service into
`plantkeeper/api/grpc/server.py`.
