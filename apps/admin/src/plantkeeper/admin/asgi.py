"""The ASGI application of the read side.

Django Admin is mounted under Starlette rather than served by ``runserver`` for
two reasons. Django's own ASGI application has no lifespan, and this process needs
one to start and stop the Kafka broker its projections consume from. It also
gives later phases (the notification long poll) a place to add routes next to
Django instead of inside it.

There is no module-level ``application``: settings have to be read from the
environment after the process has started, and a test has to be able to point
them at its own containers. Point uvicorn at the factory instead::

    uvicorn --factory plantkeeper.admin.asgi:create_admin_application
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from django.conf import settings as django_settings
from django.core.asgi import get_asgi_application
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import PlainTextResponse
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles

from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.messaging.broker import build_broker


async def healthz(request: Request) -> PlainTextResponse:
    """Answer a liveness probe without touching Django or the database."""
    return PlainTextResponse("ok")


def create_admin_application(*, settings: Settings | None = None) -> Starlette:
    """Build the read-side ASGI application.

    ``settings`` exists for tests: an end-to-end test points the read side at the
    same containers as the API and the relay without touching the environment.
    """
    runtime = settings if settings is not None else Settings()
    django_application = get_asgi_application()
    # Imported here rather than at module level: the projections are Django
    # models, and importing them before ``django.setup()`` — which the line above
    # performs — raises ``AppRegistryNotReady``.
    from plantkeeper.admin.projections.subscriber import register_projections

    broker = build_broker(runtime)
    register_projections(broker, prefix=runtime.read_side_consumer_group_prefix)

    @asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        """Own the broker's connection for the process's lifetime."""
        await broker.start()
        try:
            yield
        finally:
            await broker.stop()

    return Starlette(
        routes=[
            Route("/healthz", healthz),
            # Mounted separately because it is served by Starlette, not Django:
            # Django Admin's assets are collected into ``STATIC_ROOT`` by
            # ``manage.py collectstatic``.
            Mount(
                "/static",
                app=StaticFiles(directory=str(django_settings.STATIC_ROOT), check_dir=False),
            ),
            # The catch-all goes last, so the two routes above win.
            Mount("/", app=django_application),
        ],
        lifespan=lifespan,
    )
