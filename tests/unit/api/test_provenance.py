"""The API edge binds one conversation per request, and clears it afterwards.

Task 1.3 names the API command as the first of the three edges that have to
produce a stamped outbox row. The binding is what the outbox adapter reads, so
this pins the binding itself — the values, the tolerance for a client that sends
nonsense, and the reset — while ``tests/e2e/test_provenance.py`` shows the row
that comes out the other end.
"""

from __future__ import annotations

from uuid import UUID, uuid4

from starlette.types import Message, Receive, Scope, Send

from plantkeeper.api.main import (
    API_RAISER,
    CORRELATION_HEADER,
    TRACEPARENT_HEADER,
    ProvenanceMiddleware,
)
from plantkeeper.application.provenance import MessageContext, current_provenance
from plantkeeper.infrastructure.messaging.topics import (
    CORRELATION_HTTP_HEADER,
    PROVENANCE_HEADERS,
    TRACEPARENT_HTTP_HEADER,
)

SCOPE: Scope = {"type": "http", "method": "POST", "path": "/api/v1/plants", "headers": []}


def a_scope(*headers: tuple[bytes, bytes]) -> Scope:
    """An HTTP scope carrying ``headers``."""
    return {**SCOPE, "headers": list(headers)}


async def run_middleware(scope: Scope) -> MessageContext | None:
    """Run ``scope`` through the middleware and return what it bound.

    The inner app reports the ambient context it can see, which is what a command
    handler behind the router would see. Whatever the middleware does after the
    app returns happens once this function has already captured the value, so the
    reset is asserted separately by :func:`test_the_binding_is_cleared`.
    """
    seen: MessageContext | None = None

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        nonlocal seen
        seen = current_provenance()

    async def receive() -> Message:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: Message) -> None:
        """Discard the response."""

    await ProvenanceMiddleware(app)(scope, receive, send)
    return seen


async def test_a_command_is_attributed_to_the_api() -> None:
    """There is no authentication, so the API names itself (ADR 0010)."""
    context = await run_middleware(a_scope())
    assert context is not None
    assert context.raised_by == API_RAISER


async def test_a_client_conversation_is_adopted() -> None:
    """``x-correlation-id`` lets a client join a conversation it already has."""
    conversation = uuid4()
    context = await run_middleware(
        a_scope((CORRELATION_HEADER.encode(), str(conversation).encode()))
    )
    assert context is not None
    assert context.correlation_id == conversation


async def test_a_request_without_a_conversation_starts_one() -> None:
    """Every request gets a conversation, so no chain of reactions is headless."""
    context = await run_middleware(a_scope())
    assert context is not None
    assert isinstance(context.correlation_id, UUID)


async def test_two_requests_do_not_share_a_conversation() -> None:
    first = await run_middleware(a_scope())
    second = await run_middleware(a_scope())
    assert first is not None and second is not None
    assert first.correlation_id != second.correlation_id


async def test_a_malformed_conversation_is_ignored_rather_than_refused() -> None:
    """The header is a courtesy, not a contract: nonsense starts a new conversation."""
    context = await run_middleware(a_scope((CORRELATION_HEADER.encode(), b"not-a-uuid")))
    assert context is not None
    assert context.correlation_id is not None


async def test_the_trace_context_is_carried_through_opaquely() -> None:
    """The platform runs no tracer; it forwards the value for whoever does."""
    traceparent = b"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    context = await run_middleware(a_scope((TRACEPARENT_HEADER.encode(), traceparent)))
    assert context is not None
    assert context.traceparent == traceparent.decode()


async def test_the_binding_is_cleared_when_the_request_ends() -> None:
    """A request must not leak its conversation into the next task on the loop."""
    await run_middleware(a_scope())
    assert current_provenance() is None


async def test_a_non_http_scope_is_passed_through_unbound() -> None:
    """Lifespan and websocket scopes have no request headers to read."""
    context = await run_middleware({"type": "lifespan"})
    assert context is None


def test_the_http_headers_are_spelled_the_same_on_both_sides() -> None:
    """The API repeats the two names as literals; this is what pins them together.

    ``plantkeeper.api.main`` deliberately does not import the messaging layer for
    two strings, so the spelling is asserted here instead of shared.
    """
    assert CORRELATION_HEADER == CORRELATION_HTTP_HEADER
    assert TRACEPARENT_HEADER == TRACEPARENT_HTTP_HEADER
    assert TRACEPARENT_HEADER in PROVENANCE_HEADERS
