"""Shared plumbing of the gRPC servicers.

A servicer is a thin adapter: it opens one request scope per RPC, dispatches
through the same mediator the REST routers use, and translates any failure into a
gRPC status. Everything else — the transaction, the aggregates, the outbox — is
the application layer's business.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import grpc
from cqrs.mediator import RequestMediator
from cqrs.requests.map import RequestMap
from dishka import AsyncContainer

from plantkeeper.api.grpc.errors import status_code_for
from plantkeeper.api.mediator import build_mediator


class BaseServicer:
    """One request scope and one error mapping, shared by every servicer."""

    def __init__(self, *, container: AsyncContainer, request_map: RequestMap) -> None:
        self._container = container
        self._request_map = request_map

    @asynccontextmanager
    async def rpc(self, context: grpc.aio.ServicerContext) -> AsyncIterator[RequestMediator]:
        """Yield a mediator over a fresh request scope, mapping failures to statuses.

        The scope has to be opened per RPC, exactly as a FastAPI request opens
        one: a handler that got a different session than its repositories wrote to
        would commit a different transaction. ``context.abort`` raises, so a
        caller never continues after a failure was translated.
        """
        try:
            async with self._container() as request_container:
                yield build_mediator(request_container, self._request_map)
        except Exception as exc:
            await context.abort(status_code_for(exc), str(exc))
