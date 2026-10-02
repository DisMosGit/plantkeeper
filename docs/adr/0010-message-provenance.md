# 0010. Message provenance, schema versioning and the accepted trust model

## Status

Accepted. Extends [ADR 0003](0003-write-side-outbox.md) and
[ADR 0008](0008-outbox-pattern.md), which describe *when* a message is published;
this one describes *what travels with it*.

## Date

2026-10-02

## Context

The envelope this platform published was two headers: `event_name` and `event_id`.
That is enough to deserialise a message and to deduplicate it, and it is all
`AGENTS.md`'s idempotency rule needs. It is not enough to answer three questions an
operator asks the moment something goes wrong in an event-driven system:

1. **Which events belong together?** One HTTP request can cause an event, which
   causes a saga, which causes four more events on three topics. Nothing in the
   message says they are related, so reconstructing "what happened because of this
   request" means correlating timestamps by hand.
2. **What caused this?** Even with a conversation identified, a message does not
   name its immediate predecessor, so a chain can only be read forwards from its
   head — and the head is often not known.
3. **Who or what raised it?** `event_id` is a UUID with no author. With sagas,
   consumers, timers and the API all publishing, "who sent this?" had no answer in
   the message at all.

There is a fourth question the platform cannot answer and will not pretend to:
**which user** caused this. PlantKeeper has no authentication — `AGENTS.md` forbids
adding it, and `developer-workflow` records the absence as a deliberate scope
boundary. `raised_by` can therefore only ever name a *component* or a *claimed*
identity, never a verified one. That is a real limitation and it belongs in a
recorded decision rather than in a comment.

Two further forces shaped the shape of the answer:

- **The body is the event.** `docs/events.md` documents an envelope-less message,
  and every event model is `extra="forbid"`. Adding an envelope or new payload
  fields would break every consumer and turn each payload model into a migration
  hazard. Provenance must therefore travel in the headers.
- **A schema needs a version, or it cannot change.** There was no way to say "this
  body is the new shape". Without one, the first incompatible payload change is a
  flag day: every consumer must be updated and deployed in lockstep with the
  producer.

## Decision

We will carry provenance and a schema version as message headers, record them on
the outbox row when the event is appended, and propagate them across every hop.

**The fields.** `correlation_id` (the conversation), `causation_id` (the
`event_id` of the causing delivery), `raised_by` (a tagged actor),
`schema_version` (the revision of the payload document) and `traceparent` (W3C
trace context, carried opaquely). `occurred_at` joins them as the instant the
raising edge observed the event, distinct from the body's own. The set is frozen on
`write_shared.outbox` beside the topic and the partition key, so the relay emits
what the write side decided and re-derives nothing.

**Where they come from.** The edge that knows binds them, and the outbox adapter
stamps them; no handler threads a correlation identifier through its own signature.
The API binds one per HTTP request (honouring a client's `x-correlation-id`), a
consumer binds one per delivery from the headers it arrived with, and a timer binds
one per tick. A saga re-attributes to `saga:<id>` while keeping the conversation it
was triggered on.

**The actor vocabulary.** `user:<id>`, `system:<job>`, `saga:<id>`,
`service:<name>` — a tagged string, so a reader can tell a claimed human identity
from a component without a lookup.

**Schema versioning.** `schema_version` starts at `1` for every event that exists,
is a class variable on the event model rather than a payload field, and is bumped
**only** for an incompatible change: a removed or renamed field, or a narrowed
type. A compatible addition does not bump it. Consumers ignore a version they do
not know; the version exists so they can *choose* to, not so they must fail.

**Absence is tolerated, never fatal.** A message with no provenance headers is
handled normally and attributed to `system:unknown`; a malformed identifier is
repaired (the message starts its own conversation) rather than rejected. Refusing
an old message would turn a metadata gap into data loss, and the gap closes on its
own because every new event carries the fields.

**The trust model, recorded.** There is no authentication and no authorization:
every endpoint is open to anyone who can reach it, `raised_by` records a claim
rather than a verified identity, and the platform is not safe to expose to an
untrusted network. That is accepted, not overlooked. The envelope is deliberately
the *smallest* change that makes adding authentication later non-breaking: when a
verified principal exists it becomes the `raised_by` value, and no event shape,
topic or consumer has to change.

**Scope.** This applies to every process that appends to the outbox and to the
relay that publishes it — `apps/api`, `apps/workers` and `apps/admin`'s
consumers — and to the `write_shared.outbox` table.

## Consequences

**Easier.** A chain of reactions is reconstructable from any link, forwards or
backwards, because every message names both its conversation and its cause. An
operator can answer "who raised this?" from the message alone. A payload change can
ship without a flag day. Trace context survives the broker, so a deployment that
adds a tracer does not have to re-instrument the messaging layer.

**Harder.** Five more columns on the hottest write table, and a header set every
producer must fill. A component that forgets to bind provenance produces
`system:unknown` rows that are quiet and legal — the failure mode is invisible
metadata, not an error, so a reviewer has to look for it. The tolerance policy
means old and new messages are indistinguishable in kind, only in completeness.

**Follow-up this creates.** Authentication remains the platform's largest
un-addressed risk; this ADR reduces the cost of adding it but does not add it. A
tracer is still not wired up — `traceparent` is carried and dropped. And
`schema_version` is a promise no consumer exercises yet: nothing branches on it
today, which is correct while every event is at version 1, and becomes a real
obligation at the first bump.

## References

- [`docs/events.md`](../events.md) — the header table and the tolerance policy
- [ADR 0003](0003-write-side-outbox.md) — the outbox, the relay and the first
  dead-letter path
- [ADR 0008](0008-outbox-pattern.md) — the consumer half of delivery
- [ADR 0011](0011-consumer-failure-policy.md) — the consumer failure policy, which
  reuses these headers for a moved-aside delivery
- `plantkeeper.application.provenance` — the fields and the ambient binding
