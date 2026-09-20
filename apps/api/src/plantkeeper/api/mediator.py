"""The bridge from the API processes to the application layer's mediator.

Both wire protocols the API speaks — REST and gRPC — dispatch through the same
``python-cqrs`` mediator over the *same* application layer: the mediator resolves
a handler class from Dishka, and the handler, its session and its transaction live
in one request scope. Keeping the construction here means the two protocols cannot
drift into two behaviours, and :func:`view_of` checks a handler's promise in one
place instead of at every call site.
"""

from __future__ import annotations

from cqrs.events import EventMap
from cqrs.mediator import RequestMediator
from cqrs.requests.map import RequestMap
from dishka import AsyncContainer

from plantkeeper.infrastructure.di.cqrs import DishkaCQRSContainer


def build_mediator(container: AsyncContainer, request_map: RequestMap) -> RequestMediator:
    """Build a mediator over one request-scoped container.

    A mediator per request is cheap; a mediator per process would not be, because
    it would have to resolve handlers from a container that outlives them.
    """
    return RequestMediator(
        request_map=request_map,
        container=DishkaCQRSContainer(container),
        event_map=EventMap(),
        # No handler publishes follow-up events yet; turn this on when one does.
        concurrent_event_handle_enable=False,
    )


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
