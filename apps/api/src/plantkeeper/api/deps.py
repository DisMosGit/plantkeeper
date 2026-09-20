"""FastAPI dependencies that bridge HTTP to the mediator.

The mediator itself is built in :mod:`plantkeeper.api.mediator`, which the gRPC
servicers use too: both protocols must dispatch through the same application
layer. Dishka's middleware puts the request container on ``request.state``, so
that is what the shared builder is handed.
"""

from __future__ import annotations

from typing import Annotated

from cqrs.mediator import RequestMediator
from cqrs.requests.map import RequestMap
from dishka import AsyncContainer, FromDishka
from dishka.integrations.fastapi import inject
from fastapi import Depends, Request

from plantkeeper.api.mediator import build_mediator


@inject
async def get_mediator(
    request: Request,
    request_map: FromDishka[RequestMap],
) -> RequestMediator:
    """Build the mediator for this request, over the request's own container."""
    container: AsyncContainer = request.state.dishka_container
    return build_mediator(container, request_map)


Mediator = Annotated[RequestMediator, Depends(get_mediator)]
"""The mediator, as a route parameter type."""
