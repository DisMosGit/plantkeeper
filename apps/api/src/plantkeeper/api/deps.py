"""FastAPI dependencies that bridge HTTP to the mediator.

python-cqrs's mediator needs a container that can resolve handler classes, and
the handlers must live in the *request's* scope — otherwise a handler would get a
different session than the request it serves. Dishka's middleware puts the
request container on ``request.state``, so that is what is handed over.
"""

from __future__ import annotations

from typing import Annotated

from cqrs.events import EventMap
from cqrs.mediator import RequestMediator
from cqrs.requests.map import RequestMap
from dishka import AsyncContainer, FromDishka
from dishka.integrations.fastapi import inject
from fastapi import Depends, Request

from plantkeeper.infrastructure.di.cqrs import DishkaCQRSContainer


@inject
async def get_mediator(
    request: Request,
    request_map: FromDishka[RequestMap],
) -> RequestMediator:
    """Build the mediator for this request, over the request's own container.

    A mediator per request is cheap; a mediator per process would not be, because
    it would have to resolve handlers from a container that outlives them.
    """
    container: AsyncContainer = request.state.dishka_container
    return RequestMediator(
        request_map=request_map,
        container=DishkaCQRSContainer(container),
        event_map=EventMap(),
        # No handler publishes follow-up events yet; turn this on when one does.
        concurrent_event_handle_enable=False,
    )


Mediator = Annotated[RequestMediator, Depends(get_mediator)]
"""The mediator, as a route parameter type."""


class UnexpectedResponseError(RuntimeError):
    """A handler answered with something other than the view it promises.

    Only reachable through a programming error, which is why it is not an
    ``ApplicationError``: it would be a 500, not a 4xx.
    """


def view_of[ViewT](response: object, view_type: type[ViewT]) -> ViewT:
    """Narrow a mediator result to the view the handler promised.

    python-cqrs ships no ``py.typed`` marker, so ``RequestMediator.send`` is
    untyped and its result is unknowable to mypy. Rather than cast blindly at
    every call site, the promise is checked once, here — and a mismatch is a bug
    worth failing loudly on.
    """
    if not isinstance(response, view_type):
        raise UnexpectedResponseError(
            f"handler answered with {type(response).__name__}, expected {view_type.__name__}"
        )
    return response
