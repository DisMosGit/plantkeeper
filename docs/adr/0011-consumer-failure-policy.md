# 0011. A consumer retries a delivery a bounded number of times, then moves it aside

## Status

Accepted. Completes the failure handling [ADR 0003](0003-write-side-outbox.md) and
[ADR 0008](0008-outbox-pattern.md) described for the publishing side; this one
decides what a *consumer* does when its handler raises.

## Date

2026-10-02

## Context

The platform's delivery story had two halves and only one of them was finished.
The relay claims outbox rows, retries a transient broker failure, and after
`OUTBOX_MAX_ATTEMPTS` copies the message to `plantkeeper.dlq.v1` and marks the row.
A consumer had nothing of the kind. Its handler raised into FastStream, the
exception was logged, and what happened next was recorded in `docs/events.md` as
"the error propagates and the broker redelivers it".

That sentence was not true, and the truth was worse. The subscribers used
FastStream's default acknowledgement policy, `ACK_FIRST`, which configures the
Kafka client with `enable_auto_commit=True` and lets *its* timer commit offsets
every five seconds regardless of what the handler did. So:

1. **A failed delivery was silently lost.** The handler's transaction rolled back
   — including the `(consumer_group, event_id)` claim that made a redelivery safe —
   but the offset advanced anyway. Nothing was handled, nothing was retried, and
   nothing said so. The at-least-once guarantee the ledger was built for did not
   exist.
2. **A crash mid-handler lost the delivery too.** Same mechanism: the client had
   already committed an offset for a delivery whose work never landed.
3. **There was no terminal path at all.** A *poison* delivery — one that can never
   succeed, because it breaks a domain rule or because the data it needs is
   genuinely gone — had no state to end in. With redelivery it would have blocked
   its partition forever; with auto-commit it vanished.

The forces on the answer are the ones the relay already faced. Retrying is right
for a failure that may pass — a database that went away, a broker that timed out —
and wrong for one that cannot: repeating a delivery that breaks an aggregate's rule
produces the same rule violation, only later. Whatever a consumer cannot handle has
to end up somewhere an operator can find it, and the claim has to travel with it so
it is neither retried forever nor handled twice.

## Decision

We will give every consumer the same three-outcome policy the relay has, in one
implementation shared by both subscriber sets.

**Classification.** A `DomainError` — a rule an aggregate enforced — is terminal:
the delivery is moved aside at once, with no retry. Anything else is transient by
default and is retried `CONSUMER_MAX_ATTEMPTS` (3) times in total, waiting
`CONSUMER_RETRY_INITIAL_WAIT_SECONDS` (0.5 s) after the first failure and doubling
to a ceiling of `CONSUMER_RETRY_MAX_WAIT_SECONDS` (10 s).

**Move aside.** A delivery that breaks a rule, or that fails its last attempt, is
copied to `plantkeeper.dlq.v1` with the message as it arrived — body, key and
headers — plus `consumer_group`, `original_topic` and `error`. Its
`processed_events` claim is committed with the copy: the write side does both in one
transaction (claim, publish, commit), Django's side publishes first and records the
claim immediately after, because its synchronous ORM cannot join an async publish.
Of the two orders only this one is safe — a claim recorded for a copy that never
reached Kafka drops the delivery, while a copy without a claim is replayed twice at
worst. If the copy itself cannot be stored the exception propagates, the claim stays
unwritten, and the broker offers the delivery again.

**Redelivery is made real.** Every subscriber is registered with
`ack_policy=NACK_ON_ERROR`: the offset is committed once the handler returned, and a
handler that raised seeks the consumer back to its delivery. Without this the retry
budget above would only ever run inside one process, and a crash between handler and
commit would still lose the message.

**Decoding never raises.** An unknown `event_name`, a body that does not validate
and a body that is not JSON at all are contract violations the consumer logs and
acknowledges. This is the same policy the decoder already documented, and it is now
load-bearing: with redelivery enabled, anything that raises *outside* the policy
would be offered forever.

**Scope.** The worker's saga and choreography consumers, the telemetry ingress and
the read side's Django projections. One exception is deliberate: an orchestration
trigger whose saga failed has already recorded the failure — `SagaFailed` plus the
delivery's claim, committed on purpose — so the move-aside finds the claim taken and
copies nothing. That failure belongs to the saga's own retry budget and park state
([`docs/sagas.md`](../sagas.md)), not to the consumer's. The telemetry ingress keeps
no ledger (`AGENTS.md`), so its copy records no claim and a replay is absorbed by
the readings table's `(sensor_id, recorded_at)` key.

**Replay is an operator's command, not a background job.** `tools/dlq.py list`
shows what is waiting; `tools/dlq.py replay --event-id <id>` clears the claim in
whichever ledger holds it — `write_shared.processed_events` or
`read_analytics.processed_events` — and republishes the message to its
`original_topic` with its key and headers. A replay needs `--event-id` or `--all`,
and `--dry-run` prints the plan without changing either store.

**What we rejected.** FastStream's built-in retry alone — no terminal path, no claim
with the copy, no record for an operator. Dropping a delivery that fails — silent
data loss. Redelivering forever — one poison delivery blocks its partition for every
other aggregate on it. Replaying without clearing the claim — the redelivery would
be recognised as a duplicate and ignored, so the operator's action would appear to
work and change nothing. A separate dead-letter topic per context — the topic
carries its own origin header, and one topic is one thing to watch.

## Consequences

**Easier.** The ledger's promise is now true: a delivery is handled once or it is
somewhere an operator can see. A poison delivery cannot hold a partition, a
transient failure is retried without an operator, and a crash mid-handler is
followed by a real redelivery. Both subscriber sets share one policy module, so the
worker and the read side cannot drift; the classification, the backoff and the
move-aside order are unit-tested without a broker.

**Harder.** There is a new operational surface: `plantkeeper.dlq.v1` accumulates
messages that somebody has to look at, and nothing pages anyone when it does — the
`list` command is pull-only and there is no depth metric. A replay appends the
message where it is handled last, so per-aggregate ordering is not restored by it:
an operator replaying a `PlantMoved` after a later `PlantRemoved` is reordering the
stream on purpose. `NACK_ON_ERROR` means one commit per handled delivery rather than
the client's batched timer, and it means a subscriber that raises *outside* the
policy — a broken container graph, say — redelivers in a tight loop rather than
losing the message; that is the trade this decision makes deliberately. Finally, the
Django side's copy-then-claim order can produce a duplicate copy if the process dies
between the two, which a replay absorbs but an operator counting messages will
notice.

**Follow-ups this creates.** Retention on the dead-letter topic is unbounded and
undecided. Nothing watches it: a `dlq` depth metric and an alert would turn "an
operator eventually looks" into "somebody is told". A parked *saga* is reset through
`reset_for_retry` on the storage ([`docs/sagas.md`](../sagas.md) records that there
is no CLI for it); a parked *delivery* is this document's replay command, and the
two operator stories are close enough that they will likely want one home.

## References

- [`docs/events.md`](../events.md) — the envelope, the failure-handling section and
  the replay commands
- [ADR 0003](0003-write-side-outbox.md) — the outbox, the relay and the first
  dead-letter path
- [ADR 0008](0008-outbox-pattern.md) — the consumer half of delivery
- [ADR 0010](0010-message-provenance.md) — the headers a moved-aside copy keeps
- [ADR 0005](0005-orchestration-vs-choreography.md) — the saga whose recorded
  failure is left to its own budget rather than dead-lettered
- [`docs/sagas.md`](../sagas.md) — that budget, the park state and
  `reset_for_retry`
- `plantkeeper.application.delivery` — the policy
- `plantkeeper.infrastructure.messaging.failures` — the settings, the copy and the
  acknowledgement policy
- `tools/dlq.py` — the operator's list and replay commands
