"""Recorded cross-context commands.

A process manager coordinates several contexts, but it must not write a context it
does not own: ``AGENTS.md`` and ``event-transport`` both require one writer per
table, and a synchronous call between contexts is forbidden. The pattern that
satisfies both is the transactional outbox applied *inside* the saga:

1. the step that needs another context to do something **records a command** —
   saga id, step number, command name and payload — in the very transaction that
   checkpoints the process;
2. the command dispatcher later claims that row, resolves the command through the
   owning context's own handler and unit of work, and marks it executed in the
   same transaction as the handler's write.

Dispatch is therefore a property of the transaction rather than of a crash-free
run: a process that stops between recording and executing simply leaves a pending
row, and exactly one effect appears when the dispatcher runs it. See
``docs/adr/0012-saga-command-dispatch.md``.

The port is expressed in those terms — record a command, claim the pending ones,
mark the outcome — so a step never learns how intents are stored, and the
dispatcher never learns which saga recorded one.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Protocol, runtime_checkable
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, JsonValue


class IntentStatus(StrEnum):
    """Where a recorded command is in its life."""

    PENDING = "pending"
    """Recorded, not yet executed. The dispatcher's whole job."""

    EXECUTED = "executed"
    """The owning context's handler committed its effect."""

    FAILED = "failed"
    """The handler raised. Retried until the budget runs out, then parked."""

    PARKED = "parked"
    """Past the retry budget. Nothing runs it again but an operator."""

    CANCELLED = "cancelled"
    """Withdrawn by the process that recorded it, before it ever ran.

    A compensation that reaches a step whose command has not been dispatched yet
    prefers this to executing and then undoing: the effect never happens, so there
    is nothing to undo. It is terminal, like ``parked``.
    """


class SagaIntent(BaseModel):
    """One recorded command, as the dispatcher sees it."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    id: int
    saga_id: UUID
    step_no: int
    command_name: str
    payload: dict[str, JsonValue]
    status: IntentStatus
    idempotency_key: str
    """What the owning context's handler deduplicates on.

    Derived from the intent's own identity rather than a client header: the same
    recorded command must produce one effect however often the dispatcher runs it,
    and the handler's idempotency table is what makes that true.
    """

    attempts: int
    """Failed executions so far, out of the dispatcher's budget.

    Exposed for inspection: an operator watching a command retry needs the count,
    and it is the difference between "still trying" and "about to be parked".
    """

    last_error: str | None = None
    """Why the last attempt failed, or ``None`` when it never did.

    Carried on the read model because a parked intent is an operator's problem and
    the reason is the first thing they need; the dispatcher-facing claim omits it,
    since a command about to run has no error yet.
    """


class IntentClaim(BaseModel):
    """A claimed intent that still has to be executed, with how to execute it."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    id: int
    saga_id: UUID
    command_name: str
    payload: dict[str, JsonValue]
    attempts: int
    idempotency_key: str
    recorded_at: AwareDatetime
    """When the step recorded the command.

    Carried because it is the instant the step's *decision* was taken, which is
    what a command deriving a due date from "now" has to use: an intent dispatched
    a moment later must produce the instant the saga intended, and one dispatched
    after a delay must not produce a different one.
    """


class RecordedIntent(BaseModel):
    """What a step hands the repository when it decides to delegate."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    saga_id: UUID
    step_no: int
    command_name: str
    payload: dict[str, JsonValue]
    idempotency_key: str


@runtime_checkable
class SagaIntentRepository(Protocol):
    """Read and write access to the recorded-command table."""

    async def record(self, intent: RecordedIntent) -> None:
        """Stage one recorded command in the caller's transaction.

        Staging rather than committing is the point: the intent becomes durable
        with the process's own checkpoint, so "the process decided to delegate"
        and "the record of that decision exists" cannot come apart.
        """
        ...

    async def claim_pending(self, limit: int, *, lease_seconds: int) -> list[IntentClaim]:
        """Claim up to ``limit`` pending intents, oldest first.

        A lease, like the outbox relay's: an intent claimed by a dispatcher that
        then died becomes claimable again once the lease expires, so no operator
        has to intervene.
        """
        ...

    async def mark_executed(self, intent_id: int) -> None:
        """Record that the owning context's handler committed its effect."""
        ...

    async def mark_failed(self, intent_id: int, error: str, *, max_attempts: int) -> IntentStatus:
        """Count one failed execution and park the intent if that was the last.

        Returns the status the intent ended in, so a caller can log or alert on a
        parked command without re-reading the row.
        """
        ...

    async def release_claim(self, intent_id: int) -> None:
        """Give the lease back after the outcome was recorded."""
        ...

    async def cancel_pending(self, idempotency_key: str) -> bool:
        """Cancel a recorded command that has not run yet.

        Answers ``True`` when the intent was still pending and is now cancelled, so
        a compensating step knows whether the effect it is undoing can still be
        prevented. ``False`` means the command already executed and the undo has to
        be a command of its own.
        """
        ...

    async def park(self, intent_id: int, error: str) -> None:
        """Park an intent an operator has to look at."""
        ...

    async def pending_for_saga(self, saga_id: UUID) -> list[SagaIntent]:
        """Return every intent of one saga, oldest first, for inspection."""
        ...


def intent_key(saga_id: UUID, step_no: int) -> str:
    """Derive the idempotency key for a recorded command.

    ``(saga_id, step_no)`` is unique by construction — a saga runs a step once —
    so it is also the natural identity of the effect the step delegates. Deriving
    the key rather than storing a random one means a retried execution and a
    duplicated delivery agree on what they are deduplicating.
    """
    return f"saga-intent:{saga_id}:{step_no}"


def recorded_intent(
    *,
    saga_id: UUID,
    step_no: int,
    command_name: str,
    payload: dict[str, JsonValue],
) -> RecordedIntent:
    """Build a :class:`RecordedIntent` with its derived key."""
    return RecordedIntent(
        saga_id=saga_id,
        step_no=step_no,
        command_name=command_name,
        payload=payload,
        idempotency_key=intent_key(saga_id, step_no),
    )
