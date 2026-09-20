# 0006. REST and gRPC over one application layer

## Status

Accepted

## Date

2026-09-21

## Context

Phase 2 gave PlantKeeper a REST write API: FastAPI routers map an HTTP request onto
a command or a query, hand it to the `python-cqrs` mediator, and map the answer —
or the failure — back onto HTTP. The handlers, the aggregates, the transactional
outbox and the error semantics are all the application layer's, and the router is
a thin adapter over them.

Phase 7 adds a second protocol for the calls a client makes most often: what is due
today, complete a watering, read the household's plants. gRPC is attractive there
because the contract is a `.proto` file rather than prose, the generated client is
typed, and server reflection lets tooling such as `grpcurl` call the server without
the proto files in hand. The risk in adding it is obvious: a second entry point
that re-implements what the first one does, drifts from it, and turns one behaviour
into two.

The forces are therefore:

- **One application layer.** `AGENTS.md` keeps business logic out of routers; it
  must stay out of servicers too. Whatever a gRPC call does, the REST call that
  shares its use case must do exactly the same.
- **Failure semantics already exist.** The application raises `NotFoundError`,
  `DomainError`, `ConcurrentWriteError` and `IdempotencyKeyConflictError`; the REST
  layer translates them into HTTP statuses. gRPC needs its own vocabulary for the
  same set, or clients of the second protocol get `UNKNOWN` for everything.
- **Generated code is a build product.** protobuf stubs are derived from the
  `.proto` files. Committing them invites a copy that no longer matches its source,
  which is the failure mode a contract is supposed to prevent.
- **The project has no CI/CD** (`AGENTS.md` forbids adding it), so generation has
  to be reproducible from a `make` target that developers and tests already run.

## Decision

We will expose the Care and Garden read/command surface over gRPC **from the same
`apps/api` package, dispatching through the same mediator as REST**, and we will
not generate a second implementation of anything.

Specifically:

- **`plantkeeper.api.mediator` is shared.** Both the FastAPI dependency
  (`deps.get_mediator`) and the gRPC `BaseServicer.rpc` build a `RequestMediator`
  from the same function, over a request-scoped Dishka container. `view_of`, which
  checks the response type a handler promised, moved there too so both protocols
  narrow a handler's answer the same way.
- **One request scope per RPC**, opened with `async with container()` exactly as
  Dishka's FastAPI integration opens one per HTTP request: a handler and its
  repositories must share one session and one transaction.
- **A separate process** (`make grpc`, `python -m plantkeeper.api.grpc`), because
  the two protocols have different operational shapes — HTTP wants a reverse proxy
  and reloads, gRPC wants HTTP/2 and long-lived streams — while sharing the same
  `Settings` and the same `api_providers()`. Like the REST process, the gRPC one
  builds no Kafka broker.
- **The error table is explicit** (`plantkeeper.api.grpc.errors`): `NotFoundError`
  → `NOT_FOUND`, `IdempotencyKeyConflictError` → `ALREADY_EXISTS`,
  `ConcurrentWriteError` → `ABORTED`, `DomainError` → `FAILED_PRECONDITION`,
  `pydantic.ValidationError` → `INVALID_ARGUMENT`, anything else → `INTERNAL`.
- **Stubs are generated, not committed.** `make proto` runs
  `tools/protogen.py`, which calls `grpc_tools.protoc` and rewrites the generated
  modules' absolute proto-package imports to the package the stubs live in
  (`plantkeeper.api.grpc.generated`). Every target that imports the stubs depends
  on `make proto`, and `.gitignore` keeps them out of the tree.
- **`buf` is not used.** It is a second binary to install and pin, and the project
  already depends on Python tooling that can run `protoc`; `grpcio-tools` is a dev
  dependency, like Alembic.

## Consequences

What becomes easier:

- A use case has one implementation and two adapters, and the shared mediator means
  a change to a handler is picked up by both protocols without touching either.
- The proto files are the contract: a client generates its stub from them, and the
  reflection registry is derived from the generated descriptors, so a service
  cannot be registered without appearing to `grpcurl`.
- `make proto` is the whole regeneration story — there is no checked-in artifact to
  update by hand.

What becomes harder, and the constraints this creates:

- **`make proto` is a prerequisite of the tests and the linter** (it is wired into
  the Makefile), because the stubs do not exist in a fresh checkout. A bare
  `uv run pytest` without it fails at import; that is deliberate visibility rather
  than a checked-in copy.
- **The generated modules are not type-checked as project code.** `grpcio` and
  `protobuf` ship no `py.typed` marker, so mypy treats their boundaries as `Any`
  and the generated modules carry `ignore_errors`. The annotations the servicers
  and the mapping module put around those boundaries are hand-written — which is
  why `proto/` stays the contract rather than the stubs.
- **protoc's import paths need one rewrite** after generation. It is mechanical,
  lives in `tools/protogen.py`, and is covered by the stub contract test; any
  future proto package needs no new special case.
- Adding a service is a proto file plus wiring in `grpc/server.py`; adding a method
  to an existing service touches the servicer, whose methods must be named exactly
  as the RPC.
- The gRPC surface deliberately does not expose everything REST does. Writes that
  need idempotency keys, the catalogue, sensors, notifications and the journal stay
  REST for now; a future phase that moves one of them has this pattern to follow.

## References

- [`docs/grpc.md`](../grpc.md) — the services, message conventions, error table and
  grpcurl usage.
- [`docs/adr/0003-write-side-outbox.md`](0003-write-side-outbox.md) — why the write
  side owns its dispatcher and outbox rather than the framework's.
- [`docs/events.md`](../events.md) — the Kafka contract the two API processes share
  through the outbox.
- `proto/plantkeeper/v1/`, `apps/api/src/plantkeeper/api/grpc/`,
  `tools/protogen.py`.
