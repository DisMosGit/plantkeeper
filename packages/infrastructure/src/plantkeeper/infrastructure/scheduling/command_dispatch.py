"""The command dispatcher: the other half of a recorded cross-context command.

A process manager's step does not write another context's tables; it records a
command (:mod:`plantkeeper.application.ports.saga_intents`). This job is what makes
that command happen: it claims the pending intents, resolves each one through the
request map to the handler that owns it, runs that handler in a request scope of its
own, and marks the intent executed in the *same transaction* as the handler's write.

That last detail is the whole guarantee. If the effect and the record of it could
commit separately, a crash between them would either lose the effect (the intent
looks done) or apply it twice (the intent looks pending). One commit makes the
answer unambiguous, and the intent's derived idempotency key makes a re-execution
after an *earlier* crash a no-op inside the handler.

The job polls rather than consuming a topic on purpose: dispatch is a property of
the transaction that recorded the command, not a message anyone publishes, and a
command is a context's private vocabulary rather than a public contract
(``docs/adr/0012-saga-command-dispatch.md``).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import cast

from cqrs.requests.map import RequestMap
from cqrs.requests.request import PydanticRequest
from cqrs.requests.request_handler import RequestHandler
from dishka import AsyncContainer

from plantkeeper.application.ports.saga_intents import (
    IntentClaim,
    IntentStatus,
    SagaIntentRepository,
)
from plantkeeper.application.ports.unit_of_work import UnitOfWork
from plantkeeper.infrastructure.config import Settings

logger = logging.getLogger(__name__)


class CommandDispatcher:
    """Runs the recorded commands of every saga, until stopped."""

    def __init__(
        self,
        *,
        container: AsyncContainer,
        settings: Settings,
        interval_seconds: float | None = None,
    ) -> None:
        self._container = container
        self._settings = settings
        self._interval_seconds = (
            interval_seconds
            if interval_seconds is not None
            else settings.intent_dispatch_interval_seconds
        )
        self._stopped = asyncio.Event()

    def stop(self) -> None:
        """Ask the loop to stop after the tick it is in."""
        self._stopped.set()

    async def run(self) -> None:
        """Tick every interval until stopped; a failed tick is logged, not fatal."""
        while not self._stopped.is_set():
            try:
                executed = await self.run_once()
                if executed:
                    logger.info("command dispatcher executed %d recorded command(s)", executed)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("command dispatch tick failed; retrying next interval")
            await self._wait()

    async def run_once(self) -> int:
        """Claim and execute one batch, returning how many commands ran.

        Public so a test can drive one batch deterministically instead of waiting
        for the interval, and so the recovery path can be exercised the same way
        the live process exercises it.
        """
        executed = 0
        async with self._container() as request_container:
            repository = await request_container.get(SagaIntentRepository)
            claims = await repository.claim_pending(
                self._settings.intent_dispatch_batch_size,
                lease_seconds=self._settings.intent_claim_lease_seconds,
            )
            # The claim is committed before anything runs, so a crash mid-batch
            # leaves the remaining intents leased rather than immediately claimable
            # by a second dispatcher.
            await _commit(request_container)

            for claim in claims:
                if await self._execute(request_container, repository, claim):
                    executed += 1
        return executed

    async def _execute(
        self,
        request_container: AsyncContainer,
        repository: SagaIntentRepository,
        claim: IntentClaim,
    ) -> bool:
        """Execute one claimed command and record its outcome. ``False`` means failed.

        The handler commits the effect *and* the intent's new status together: the
        unit of work they share is the same object, so marking the intent after the
        handler returns is part of that transaction rather than a second one.
        """
        request_map = await request_container.get(RequestMap)
        try:
            handler_type = _handler_for(request_map, claim.command_name)
            handler = await request_container.get(handler_type)
            command = _command_for(handler, claim)
            await handler.handle(command)
            await repository.mark_executed(claim.id)
            await _commit(request_container)
        except Exception as exc:
            await _rollback(request_container)
            await self._record_failure(request_container, repository, claim, exc)
            return False
        return True

    async def _record_failure(
        self,
        request_container: AsyncContainer,
        repository: SagaIntentRepository,
        claim: IntentClaim,
        exc: Exception,
    ) -> None:
        """Count a failure in its own transaction, parking the intent at the budget.

        Its own transaction because the handler's work was rolled back: without the
        rollback the failure count would travel with the work it is counting.
        """
        error = f"{type(exc).__name__}: {exc}"
        status = await repository.mark_failed(
            claim.id, error, max_attempts=self._settings.intent_max_attempts
        )
        await _commit(request_container)
        if status is IntentStatus.PARKED:
            logger.error(
                "recorded command %s (saga %s) parked after %d attempts: %s",
                claim.command_name,
                claim.saga_id,
                claim.attempts + 1,
                error,
            )
        else:
            logger.warning(
                "recorded command %s (saga %s) failed attempt %d: %s",
                claim.command_name,
                claim.saga_id,
                claim.attempts + 1,
                error,
            )

    async def _wait(self) -> None:
        """Sleep until the interval elapses or :meth:`stop` is called."""
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stopped.wait(), timeout=self._interval_seconds)


def _handler_for(
    request_map: RequestMap, command_name: str
) -> type[RequestHandler[PydanticRequest, object]]:
    """Resolve a recorded command's name to the handler that owns it.

    An unknown name is a programming error: the intent was recorded by this
    codebase, so a name the map does not know means the recording step and the
    registry have drifted. Raising rather than skipping is deliberate — a skipped
    intent would be marked executed and its effect silently lost.
    """
    for request_type, handler_type in request_map.items():
        if request_type.__name__ == command_name:
            return cast("type[RequestHandler[PydanticRequest, object]]", handler_type)
    raise UnknownCommandError(f"no handler is registered for the recorded command {command_name!r}")


def _command_for(
    handler: type[RequestHandler[PydanticRequest, object]], claim: IntentClaim
) -> PydanticRequest:
    """Rebuild the command from the payload the recording step stored.

    The command class comes from the handler's own declaration rather than from the
    stored name, so the payload is validated against the type that will actually
    handle it — and a payload that no longer fits fails loudly here instead of
    producing a half-built command.

    The intent's idempotency key is injected so the handler's own deduplication
    speaks about the recorded command: running the same intent twice is the same
    request, whatever the payload says.
    """
    command_type = _command_type_of(handler)
    payload = dict(claim.payload)
    if "idempotency_key" in command_type.model_fields:
        payload["idempotency_key"] = claim.idempotency_key
    return command_type.model_validate(payload)


def _command_type_of(
    handler: type[RequestHandler[PydanticRequest, object]],
) -> type[PydanticRequest]:
    """Return the request type a handler is parameterised with.

    ``python-cqrs`` exposes no attribute for this, so it is read off the class's
    ``__orig_bases__``: ``CommandHandler[CreateXCommand, SomeView]`` is the shape
    every handler in this project declares, and a handler that does not declare one
    is a programming error.
    """
    for base in getattr(handler, "__orig_bases__", ()):
        arguments = getattr(base, "__args__", ())
        if arguments and isinstance(arguments[0], type):
            candidate = cast("type[PydanticRequest]", arguments[0])
            if issubclass(candidate, PydanticRequest):
                return candidate
    raise UnknownCommandError(f"{handler.__name__} does not declare the command it handles")


class UnknownCommandError(Exception):
    """A recorded command names something this process cannot execute.

    Raised on the recording side's mistake rather than the dispatcher's: the step
    that recorded the intent and the registry that resolves handlers disagree.
    """


async def _commit(request_container: AsyncContainer) -> None:
    """Commit the request scope's unit of work, closing its transaction."""
    unit_of_work = await request_container.get(UnitOfWork)
    await unit_of_work.commit()


async def _rollback(request_container: AsyncContainer) -> None:
    """Discard the request scope's transaction, so a failure leaves no trace."""
    unit_of_work = await request_container.get(UnitOfWork)
    await unit_of_work.rollback()
