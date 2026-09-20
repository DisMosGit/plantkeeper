"""The bridge between python-cqrs' mediator and Dishka.

python-cqrs resolves a handler by asking a container for the handler *class*.
Its own ``DIContainer`` adapter wraps the ``di`` package and opens a fresh
request scope per resolution — which would give a handler a different session
than the request it serves. This adapter instead hands the resolution to the
request's own Dishka container, so a handler and everything it depends on live
in one scope, one session and one transaction.
"""

from __future__ import annotations

from dishka import AsyncContainer


class DishkaCQRSContainer:
    """A ``cqrs.container.protocol.Container`` backed by Dishka.

    The three members are exactly the protocol python-cqrs defines. It is
    deliberately not a subclass: the protocol lives in an untyped package, so
    implementing it structurally is both simpler and honestly typed.
    """

    def __init__(self, container: AsyncContainer) -> None:
        self._container = container

    @property
    def external_container(self) -> AsyncContainer:
        """Return the Dishka container this adapter resolves from."""
        return self._container

    def attach_external_container(self, container: AsyncContainer) -> None:
        """Replace the Dishka container, as the protocol allows."""
        self._container = container

    async def resolve[ResolvedT](self, type_: type[ResolvedT]) -> ResolvedT:
        """Resolve ``type_`` from the current scope."""
        return await self._container.get(type_)
