"""The FastAPI application of the write side.

Nothing here talks to Kafka: the API's job ends when the aggregate and its outbox
row are committed. The relay in ``plantkeeper.workers`` publishes them, which is
what keeps a slow broker from slowing an HTTP request down.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Final
from uuid import UUID

from dishka import AsyncContainer, Provider, make_async_container
from dishka.integrations.fastapi import setup_dishka
from fastapi import FastAPI, Request
from starlette.types import ASGIApp, Receive, Scope, Send

from plantkeeper.api.errors import register_exception_handlers
from plantkeeper.api.rest.routers import build_api_router
from plantkeeper.application.provenance import bind_provenance, request_context, reset_provenance
from plantkeeper.infrastructure.di.providers import api_providers

VERSION = "0.1.0"
"""The API's contract version, stamped into the served OpenAPI document.

One constant for the served document and the exported artefact
(``plantkeeper.api.openapi``), so the two cannot disagree; ``tools/contracts.py``
checks it against the workspace's ``pyproject.toml``.
"""

API_RAISER: Final = "service:api"
"""Who an event raised by an HTTP request is attributed to.

There is no authentication in this project, so the API names *itself* rather than
inventing a user: ``raised_by`` records which component raised the event, and the
absence of a verified identity is the accepted risk ADR 0010 documents.
"""

CORRELATION_HEADER: Final = "x-correlation-id"
TRACEPARENT_HEADER: Final = "traceparent"
"""The headers a client may send to join a conversation or a trace.

Named in the infrastructure layer's ``topics`` module for the Kafka side; repeated
here as plain strings so the API does not import the messaging layer for two
constants, with ``tests/unit/api/test_provenance.py`` pinning the spelling on both
sides.
"""


class ProvenanceMiddleware:
    """Bind one conversation per HTTP request, for the whole request's lifetime.

    ASGI middleware rather than a route dependency because provenance is not a
    route parameter: the router's handler, the mediator it dispatches through and
    the command handler behind that must all observe the same binding, and a
    dependency's scope ends before the response is returned.

    A client may name the conversation it is joining with ``x-correlation-id``;
    without one, the request starts a new conversation. ``traceparent`` is carried
    through opaquely.
    """

    def __init__(self, app: ASGIApp) -> None:
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return
        request = Request(scope, receive)
        context = request_context(
            raised_by=API_RAISER,
            correlation_id=_as_uuid(request.headers.get(CORRELATION_HEADER)),
            traceparent=request.headers.get(TRACEPARENT_HEADER),
        )
        token = bind_provenance(context)
        try:
            await self._app(scope, receive, send)
        finally:
            reset_provenance(token)


def _as_uuid(raw: str | None) -> UUID | None:
    """Parse the client's conversation header, ignoring a malformed one.

    A client that sends nonsense is starting a new conversation, not making the
    request fail: the header is a courtesy, not a contract.
    """
    if raw is None:
        return None
    try:
        return UUID(raw)
    except ValueError:
        return None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Close the container when the application stops.

    The engine's pool lives in the container, so it has to be released on the
    same event loop that created it — which is why this is a lifespan and not an
    ``atexit`` hook.
    """
    try:
        yield
    finally:
        container: AsyncContainer = app.state.dishka_container
        await container.close()


def create_app(*, providers: Sequence[Provider] | None = None) -> FastAPI:
    """Build the application.

    ``providers`` exists for tests: an end-to-end test points the settings and the
    clock at its own fixtures without touching the environment.
    """
    app = FastAPI(
        title="PlantKeeper API",
        version=VERSION,
        summary="Write side of the PlantKeeper plant care platform.",
        lifespan=lifespan,
    )
    container = make_async_container(*(providers if providers is not None else api_providers()))
    setup_dishka(container, app)
    app.add_middleware(ProvenanceMiddleware)
    register_exception_handlers(app)
    app.include_router(build_api_router())
    return app


app = create_app()
"""The ASGI application uvicorn is pointed at."""
