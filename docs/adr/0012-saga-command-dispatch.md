# 0012. Cross-context saga effects are recorded commands

## Status

Accepted. Amends [ADR 0005](0005-orchestration-vs-choreography.md), which chose the
saga engine and where saga state lives; this one decides how a saga changes a
context it does not own.

## Date

2026-10-02

## Context

The onboarding saga coordinates three contexts: it resolves a species from the
catalogue, creates a care schedule, and creates the household's first notification
before announcing the plant. It ran inside one shared unit of work, so those steps
simply wrote `write_care` and `write_notifications` through the same session the
garden command had used.

That worked, and it was wrong twice over:

1. **It made the platform's central claim false.** The README says the bounded
   contexts "communicate only through Kafka events"; a process manager writing
   another context's tables is neither communication by events nor one writer per
   table. `event-transport` forbids exactly this, and `docs/architecture.md`
   recorded the cross-context reads as a known exception rather than fixing them.
2. **It coupled the contexts through the database.** A care table rename would
   break the onboarding saga, and the care context could not change its schema
   without auditing another context's process manager. The import rules could not
   catch it: the coupling was a repository call, not an import.

The obvious repair — call the care context's command handler directly — trades one
problem for another. A synchronous in-process call is still a synchronous
dependency between contexts, and it puts the saga's checkpoint and the other
context's write back into a question about transaction boundaries: whose commit is
it, and what happens if only one of them lands? ADR 0005's own amendment closed the
window between a step and its checkpoint; introducing a second writer in the same
transaction would reopen a version of it.

The platform already has the tool for "make this effect happen exactly once,
without a synchronous call": the transactional outbox. The question was whether the
same idea applies *inside* a saga, where the "message" is a command and the
"broker" is the saga's own database.

## Decision

We will make a cross-context saga effect a **recorded command** — a row in
`write_shared.saga_intents` — written in the step's own transaction and executed
afterwards by a command dispatcher.

**Recording.** A step that changes a context the process does not own inserts
`(saga_id, step_no, command_name, payload, status, attempts, idempotency_key)` in
the same transaction as its checkpoint and its lifecycle event. The intent's
idempotency key is derived — `saga-intent:<saga_id>:<step_no>` — so the effect the
step delegates has a stable identity that a retry and a duplicate delivery agree on.

**Dispatching.** A `CommandDispatcher` background job in the `make workers` process
claims pending intents (`FOR UPDATE SKIP LOCKED`, with a lease so a dispatcher that
dies does not strand them), resolves each command name through the same
`RequestMap` the API uses, resolves that handler from a request scope of its own,
and runs it. The handler's effect and the intent's new status commit **together**,
because they share the unit of work: one transaction decides both.

**Compensating.** A step whose effect was delegated cannot undo it directly either.
Its compensation cancels the recorded command when the dispatcher has not run it
yet — cancelling is preferred, because an effect that never happened needs no
undo — and otherwise records the matching *delete* command, issued to the same
context through the same hand-off. Deleting something absent is defined as a
success, so a compensation delivered twice is harmless.

**Failure.** A command that raises is retried within a bounded budget and then
parked with its cause recorded. A parked intent is visible and terminal until an
operator acts; nothing runs it again.

**Scope.** This applies to every orchestration saga step, and to the onboarding
saga first: its care-schedule and notification steps now record commands instead of
writing those schemas.

**What we rejected.** Kafka command topics — commands are a context's private
vocabulary, not a public contract, and putting them on the broker would double the
topic surface, re-add the ordering problem the outbox already solves, and break the
platform's "every message on Kafka is a domain event" rule that the generated
catalogue guards. Direct handler calls — the synchronous coupling being removed.
A shared kernel table of identifiers — a shared database under another name.

## Consequences

**Easier.** The README's claim is now true: a context's tables have exactly one
writer, and the coupling is a command name in a payload rather than an import of
another context's repositories. The dispatch is durable — a crash between recording
and executing leaves one pending row and produces one effect on retry — and it is
inspectable, because the intent table says what was asked for, whether it ran, and
why it did not. Cross-context effects became *asynchronous*, which is honest about
what they always were.

**Harder.** Onboarding's notification and schedule now appear one dispatcher cycle
after the plant, so an end-to-end test asserts eventual effects rather than
immediate ones — a real latency the platform did not have before, bounded by the
dispatcher's interval. There is a second background job to run and monitor, and a
new table whose rows an operator has to understand when something is stuck. The
compensation path is genuinely more complex: two commands per delegated step, a
cancel-versus-undo decision, and a `+1000` step-number offset that exists only to
keep the two keys distinct.

**Follow-ups this creates.** The dispatcher's throughput and lag are unmeasured;
a queue-depth metric would say whether the interval is right. A parked intent has
no operator interface yet — `SqlAlchemySagaIntentRepository.pending_for_saga` and
the row itself are the whole story. And the saga's steps still read other contexts
through ports (`SpeciesCatalog`): reads across a boundary are the next thing to
reconsider, and the answer there is event-carried state rather than a command.

## References

- [`docs/sagas.md`](../sagas.md) — the onboarding sequence and the dispatcher
- [`docs/events.md`](../events.md) — the envelope the lifecycle events travel in
- [ADR 0005](0005-orchestration-vs-choreography.md) — the saga engine and its
  amended transaction boundary
- [ADR 0003](0003-write-side-outbox.md) — the outbox idea this applies inside a saga
- `plantkeeper.application.ports.saga_intents` — the port
- `plantkeeper.infrastructure.scheduling.command_dispatch` — the dispatcher
