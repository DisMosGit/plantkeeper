"""Base classes for queries and their handlers.

A query never changes state, so a query handler is not given a unit of work: it
reads through the request-scoped session the repositories already hold.
"""

from __future__ import annotations

from typing import TypeVar

from cqrs.requests.request import PydanticRequest
from cqrs.requests.request_handler import RequestHandler
from cqrs.response import PydanticResponse
from pydantic import ConfigDict

QUERY_CONFIG = ConfigDict(frozen=True, extra="forbid")


class Query(PydanticRequest):
    """A request that only reads."""

    model_config = QUERY_CONFIG


QueryT = TypeVar("QueryT", bound=Query)
QueryResultT = TypeVar("QueryResultT", bound=PydanticResponse)


class QueryHandler(RequestHandler[QueryT, QueryResultT]):
    """Base class for every query handler."""
