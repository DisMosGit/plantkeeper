# 1. Record architecture decisions

## Status

Accepted (Phase 0)

## Context

PlantKeeper makes a number of architectural choices that are not obvious from the code:
CQRS with separate write/read schemas, a transactional outbox, event sourcing for the
Journal only, Kafka in KRaft mode, Dishka as the DI container, and so on. Without a
written record, the reasoning behind those choices is lost as soon as the commit message
scrolls out of view, and the same debates get repeated.

## Decision

Every architecturally significant decision is recorded as an Architecture Decision
Record under `docs/adr/`, using the format of Michael Nygard ("Documenting Architecture
Decisions", 2011):

- one file per decision, named `NNNN-short-title.md`, numbered sequentially;
- the status starts at `Proposed` and becomes `Accepted`, `Deprecated` or `Superseded by
  ADR-NNNN`;
- ADRs are immutable: a change of mind is a **new** ADR that supersedes the old one;
- the template lives in [`template.md`](template.md).

`AGENTS.md` and `CONTRIBUTING.md` both require an ADR for architectural changes.

## Consequences

- The reasoning behind the architecture is reviewable next to the code.
- Reviewers can point at an ADR instead of re-arguing a decision.
- A small, ongoing writing cost: any significant change is not "done" until its ADR
  exists.

## References

- [Documenting Architecture Decisions — Michael Nygard](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions)
- [`template.md`](template.md)
