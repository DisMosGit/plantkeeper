"""The FastAPI application of the write side.

Nothing here talks to Kafka: the API's job ends when the aggregate and its outbox
row are committed. The relay in ``plantkeeper.workers`` publishes them, which is
what keeps a slow broker from slowing an HTTP request down.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager

from dishka import AsyncContainer, Provider, make_async_container
from dishka.integrations.fastapi import setup_dishka
from fastapi import FastAPI

from plantkeeper.api.errors import register_exception_handlers
from plantkeeper.api.rest.routers import build_api_router
from plantkeeper.infrastructure.di.providers import api_providers

VERSION = "0.1.0"
"""The API's contract version, stamped into the served OpenAPI document.

One constant for the served document and the exported artefact
(``plantkeeper.api.openapi``), so the two cannot disagree; ``tools/contracts.py``
checks it against the workspace's ``pyproject.toml``.
"""


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
    register_exception_handlers(app)
    app.include_router(build_api_router())
    return app


app = create_app()
"""The ASGI application uvicorn is pointed at."""
