# 2. Bounded contexts

## Status

Accepted (Phase 1)

## Date

2026-09-20

## Context

PlantKeeper is a household plant tracker fed by simulated IoT telemetry. Two very
different clocks meet in it: care happens once a week, telemetry arrives every ten
seconds. Modelling that as one large object graph with a single schema would couple
the slow, heavily validated care rules to a high-volume stream, and would make the
storage and consistency requirements of each concern impossible to state separately.

At the same time the project is a portfolio-scale monorepo, not a fleet of
deployables. Whatever context split we choose has to stay usable by one person: it
must be visible in the directory layout, checkable by tooling, and must not make a
simple change require touching five packages.

## Decision

We will model the domain as **eight bounded contexts**, seven with aggregates and one
read-side context:

| Context | Owns | Aggregate roots |
|---------|------|-----------------|
| Identity | Household members (no authentication) | `User` |
| Catalog | Species and their ideal care, synchronised from Trefle | `Species` |
| Garden | The plants a household owns | `Plant`, `Household` |
| Care | When each plant should be watered next | `CareSchedule` |
| Journal | Append-only record of care performed | `JournalEntry`, `JournalStream` |
| Telemetry | Sensor readings and threshold alerts | `Sensor` |
| Notifications | Messages for the household | `Notification` |
| Analytics | Read models for Django Admin and reports | *(none — projections)* |

Each context is a package under `packages/domain/src/plantkeeper/domain/` and owns its
own errors and events. The decision rests on three rules:

1. **A shared kernel, not shared models.** Contexts may import
   `plantkeeper.domain.base`, `plantkeeper.domain.identifiers` and
   `plantkeeper.domain.values`. They never import each other. The rule is enforced
   mechanically by the `independence` contract in the root `pyproject.toml`
   (`make lint`), so a cross-context import fails the build rather than review.
2. **Kafka events are the only channel.** A context reacts to another context's
   published event (through the transactional outbox in Phase 2), never through a
   direct method call, an import or a synchronous HTTP request. This keeps the write
   path of the slow domain independent of the fast telemetry stream.
3. **Analytics has no domain logic.** It is the read side of CQRS: the only context
   with no aggregates, no invariants and no events of its own. Its tables
   (`read_analytics`) are projections of the other contexts' events (Phase 3).

Identifiers (`PlantId`, `SensorId`, …) are part of the shared kernel precisely so
that contexts can refer to each other's aggregates in their own events without
importing the owning context's code.

## Consequences

- **Easier:** each context can change its internal model, its storage schema and its
  tests on its own; the event catalogue in [`docs/events.md`](../events.md) is the
  complete integration surface; the import-linter contract makes violations visible
  during `make lint`.
- **Easier:** the per-context invariants are small enough to test exhaustively
  (Phase 1 has one unit-test module per context).
- **Harder:** any workflow that spans contexts needs an event or a saga, not a method
  call — the onboarding flow becomes `OnboardPlantSaga` with compensation in Phase 4.
- **Harder:** eventual consistency is now a real property: a plant exists in Garden
  before its care schedule and read model do, so clients must tolerate it (the REST
  API returns 202-style semantics where relevant in Phase 2).
- **Constrained:** adding a context is cheap (a package plus an import-linter entry),
  but moving a concept between contexts requires a new ADR, because the event
  catalogue and the read models change with it.
- **Follow-up:** Phases 2–9 each realise one context's infrastructure and consumers;
  `docs/architecture.md` carries the runtime view, and the generated
  [`docs/diagrams/event-flow.md`](../diagrams/event-flow.md) the complete event
  topology.

## References

- [`docs/domain.md`](../domain.md) — ubiquitous language, aggregates, invariants
- [`docs/events.md`](../events.md) — the event catalogue that spans the contexts
- [`docs/architecture.md`](../architecture.md) — layers, write path, read path
- [`docs/adr/0001-record-architecture-decisions.md`](0001-record-architecture-decisions.md)
