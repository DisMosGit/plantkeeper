"""The orchestration saga base.

``python-cqrs`` models a saga as an ordered list of step handlers plus a typed
context, executed through ``SagaTransaction``: a failed step compensates the
completed ones in reverse order, and a saga that already finished is a no-op when
its context is dispatched again. This module adds the three things the library
deliberately leaves to the application:

* **an event trigger** — a saga in this project starts from a domain event, so
  :meth:`Saga.handle_event` maps one to a context and dispatches it;
* **a deterministic identity** — :meth:`Saga.saga_id_for` derives the saga id from
  the aggregate it concerns, so the same trigger always addresses the same saga: a
  redelivery that reaches the dispatch resumes it instead of starting a second one,
  and a finished saga is a no-op;
* **lifecycle events** — ``SagaStarted``/``SagaCompleted``/``SagaFailed``/
  ``SagaCompensated`` are appended to the outbox so a saga is observable in the
  event stream and not only in ``write_shared.saga_state``.

The engine's storage commits saga state in its own session while the steps commit
their business writes through the request's unit of work. That is deliberate: a
saga is a sequence of durable steps, and each step's effect must survive the
failure of the next one — otherwise there would be nothing to compensate. See
``docs/adr/0005-orchestration-vs-choreography.md``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import ClassVar, TypeVar
from uuid import NAMESPACE_URL, UUID, uuid5

from cqrs.dispatcher.saga import SagaDispatcher
from cqrs.saga.models import SagaContext
from cqrs.saga.saga import Saga as CqrsSaga
from cqrs.saga.storage.enums import SagaStatus
from cqrs.saga.storage.protocol import ISagaStorage

from plantkeeper.application.errors import UnhandledSagaTriggerError
from plantkeeper.application.ports.unit_of_work import UnitOfWork
from plantkeeper.domain.base import DomainEvent
from plantkeeper.domain.saga.events import (
    SagaCompensated,
    SagaCompleted,
    SagaFailed,
    SagaStarted,
)

SagaContextT = TypeVar("SagaContextT", bound=SagaContext)
"""Only the ``require_context`` narrowing helper is generic; the saga itself is
not, because the cqrs base it extends is untyped."""


class Saga(CqrsSaga, ABC):
    """A process manager that reacts to an event and runs a list of steps.

    The cqrs base is left unparameterised on purpose. ``Saga`` is generic over its
    context, and a class inherits ``__orig_bases__`` from its parent, so extending
    ``Saga[SagaContext]`` would make the library's validator read *our* base's
    context (the generic ``SagaContext``) for every concrete saga and reject its
    steps. Leaving it raw means the validator skips the context comparison and
    still checks the part it can check — that every step is a ``SagaStepHandler``.
    The context type is declared explicitly in :attr:`context_type`, which is what
    the saga map binds against.
    """

    trigger_events: ClassVar[tuple[type[DomainEvent], ...]]
    """The events that start this saga."""

    context_type: ClassVar[type[SagaContext]]
    """The context this saga is parameterised with.

    Declared explicitly rather than derived from ``context_from_event``'s
    annotation: the saga map binds a *context type* to a saga, and an explicit
    attribute keeps that binding visible next to the step list.
    """

    def __init__(self, saga_storage: ISagaStorage) -> None:
        self._storage = saga_storage

    @property
    def saga_name(self) -> str:
        """The name persisted with the execution, and used as its correlation."""
        return type(self).__name__

    def handles(self, event: DomainEvent) -> bool:
        """Whether this saga starts from ``event``'s type."""
        return type(event) in self.trigger_events

    @abstractmethod
    def context_from_event(self, event: DomainEvent) -> SagaContext:
        """Build the initial context from the triggering event."""
        ...

    @abstractmethod
    def correlation_id(self, context: SagaContext) -> str:
        """Return the identifier the saga is about (a plant, a trigger event)."""
        ...

    def require_context(self, context: SagaContext, expected: type[SagaContextT]) -> SagaContextT:
        """Narrow ``context`` to ``expected``, refusing anything else.

        ``handle_event`` already filtered by :attr:`trigger_events`, so a mismatch
        is a programming error in a subclass; ``UnhandledSagaTriggerError`` makes
        that explicit where an ``assert`` would only be a comment.
        """
        if not isinstance(context, expected):
            raise UnhandledSagaTriggerError(
                f"{self.saga_name} expected a {expected.__name__}, got {type(context).__name__}"
            )
        return context

    def saga_id_for(self, context: SagaContext) -> UUID:
        """Derive a stable saga id from the correlation.

        Deterministic rather than random: the same trigger always addresses the
        same saga, so a redelivery that reaches the dispatch resumes it (skipping
        the steps the log already records) and a replay of a finished one does
        nothing. Once a step has committed, the delivery's ``processed_events``
        claim was committed with it and stops the redelivery there; what resumes
        such a saga is ``SagaRecoveryJob``, not the redelivery.
        """
        return uuid5(NAMESPACE_URL, f"{self.saga_name}:{self.correlation_id(context)}")

    async def handle_event(
        self,
        event: DomainEvent,
        *,
        dispatcher: SagaDispatcher,
        unit_of_work: UnitOfWork,
    ) -> bool:
        """Start (or resume) the saga named by ``event`` and record its progress.

        Returns ``False`` when the event is not a trigger or the saga already
        completed; otherwise dispatches the steps and appends a lifecycle event.
        A failure compensates inside the engine and is recorded as ``SagaFailed``
        (plus ``SagaCompensated`` when a step was rolled back) before being
        re-raised. The redelivery that re-raise provokes stops at the delivery's
        claim — the recording commit took the claim along — so a recorded failure
        is final: the saga stays ``failed`` for an operator.
        """
        if not self.handles(event):
            return False
        context = self.context_from_event(event)
        saga_id = self.saga_id_for(context)
        status = await self._existing_status(saga_id)
        if status is SagaStatus.COMPLETED:
            return False
        if status is None:
            await unit_of_work.outbox.append(SagaStarted(saga_id=saga_id, saga_name=self.saga_name))
        try:
            async for _ in dispatcher.dispatch(context, saga_id=saga_id):
                pass
        except Exception as error:
            await self._record_failure(saga_id, error, unit_of_work)
            raise
        await unit_of_work.outbox.append(SagaCompleted(saga_id=saga_id, saga_name=self.saga_name))
        # Committed here rather than left to the caller, so completion is durable
        # even if the caller only commits for the claim (which is what
        # ``Consumer.consume`` does): this commit carries the claim as well.
        # Symmetric with the failure path below.
        await unit_of_work.commit()
        return True

    async def _existing_status(self, saga_id: UUID) -> SagaStatus | None:
        """Return the stored status, or ``None`` when the saga has not run yet."""
        try:
            status, _, _ = await self._storage.load_saga_state(saga_id)
        except ValueError:
            return None
        return status

    async def _record_failure(
        self, saga_id: UUID, error: Exception, unit_of_work: UnitOfWork
    ) -> None:
        """Append the failure (and the compensation, if one ran) and commit it.

        The engine finishes compensating before it re-raises, so the step history
        already shows whether anything was rolled back. The events are committed
        here rather than left to the caller, whose transaction would be rolled
        back with the exception. That commit also makes the delivery's
        ``processed_events`` claim durable, so the redelivery stops at the claim:
        a recorded failure is final, and neither redelivery nor
        ``SagaRecoveryJob`` (which only picks up ``running``/``compensating``)
        re-runs the ``failed`` saga.
        """
        await unit_of_work.outbox.append(
            SagaFailed(saga_id=saga_id, saga_name=self.saga_name, error=str(error))
        )
        history = await self._storage.get_step_history(saga_id)
        compensated = any(
            entry.action == "compensate" and entry.status.value == "completed" for entry in history
        )
        if compensated:
            await unit_of_work.outbox.append(
                SagaCompensated(saga_id=saga_id, saga_name=self.saga_name)
            )
        await unit_of_work.commit()
