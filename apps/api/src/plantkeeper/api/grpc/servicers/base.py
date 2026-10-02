"""Shared plumbing of the gRPC servicers.

A servicer is a thin adapter: it opens one request scope per RPC, dispatches
through the same mediator the REST routers use, and translates any failure into a
gRPC status. Everything else — the transaction, the aggregates, the outbox — is
the application layer's business.

Provenance is bound here for the same reason the REST side binds it in middleware:
an RPC is an edge, and the events its command raises must name where they came
from. gRPC metadata carries the client's conversation and trace context as its own
headers.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Final
from uuid import UUID

import grpc
from cqrs.mediator import RequestMediator
from cqrs.requests.map import RequestMap
from dishka import AsyncContainer

from plantkeeper.api.grpc.errors import status_code_for
from plantkeeper.api.mediator import build_mediator
from plantkeeper.application.provenance import (
    MessageContext,
    async_provenance_scope,
    request_context,
)

GRPC_RAISER: Final = "service:grpc"
"""Who an event raised by an RPC is attributed to; see the API's ``API_RAISER``."""

METADATA_TRACEPARENT: Final = "traceparent"
METADATA_CORRELATION_ID: Final = "x-correlation-id"


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
            async with (
                async_provenance_scope(_provenance_of(context)),
                self._container() as request_container,
            ):
                yield build_mediator(request_container, self._request_map)
        except Exception as exc:
            await context.abort(status_code_for(exc), str(exc))


def _provenance_of(context: grpc.aio.ServicerContext) -> MessageContext:
    """Build the conversation for one RPC, joining the client's if it sent one."""
    metadata = dict(context.invocation_metadata() or ())
    return request_context(
        raised_by=GRPC_RAISER,
        correlation_id=_as_uuid(metadata.get(METADATA_CORRELATION_ID)),
        traceparent=metadata.get(METADATA_TRACEPARENT),
    )


def _as_uuid(raw: str | None) -> UUID | None:
    """Parse a client-supplied conversation identifier, ignoring a malformed one."""
    if raw is None:
        return None
    try:
        return UUID(raw)
    except ValueError:
        return None
