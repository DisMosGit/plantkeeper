"""Base classes for commands and their handlers.

A command is a frozen Pydantic model that names an intention ("add this plant");
it is not a DTO the API can pass straight through, because the route decides
which identifiers and value objects the command carries.

``CommandHandler`` exists so that every handler in the project has the same
shape: a concrete class parameterised by the command it accepts and the view it
answers with. It carries no behaviour — the dispatching, the dependency
resolution and the transaction all happen elsewhere (see ``registry`` and the
Dishka providers).
"""

from __future__ import annotations

from typing import TypeVar

from cqrs.requests.request import PydanticRequest
from cqrs.requests.request_handler import RequestHandler
from cqrs.response import PydanticResponse
from pydantic import ConfigDict, Field

COMMAND_CONFIG = ConfigDict(frozen=True, extra="forbid")


class Command(PydanticRequest):
    """A request that changes state."""

    model_config = COMMAND_CONFIG


class IdempotentCommand(Command):
    """A create command a client may retry.

    ``idempotency_key`` mirrors the HTTP ``Idempotency-Key`` header. When it is
    present the handler stores its response in the same transaction as the write,
    so a replay returns the original answer instead of creating a second
    aggregate (``docs/adr/0003-write-side-outbox.md``).
    """

    idempotency_key: str | None = Field(default=None, max_length=255)


CommandT = TypeVar("CommandT", bound=Command)
CommandResultT = TypeVar("CommandResultT", bound=PydanticResponse)


class CommandHandler(RequestHandler[CommandT, CommandResultT]):
    """Base class for every command handler."""
