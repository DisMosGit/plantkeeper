"""Mapping from domain and application failures onto HTTP responses.

Every failure the write path can produce is listed here, so a client never has to
guess: an invariant that was broken is a 409, something that does not exist is a
404, a request body that cannot be turned into a value object is a 422. A 500
means a bug, and nothing else does.
"""

from __future__ import annotations

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from plantkeeper.application.errors import (
    ConcurrentWriteError,
    IdempotencyKeyConflictError,
    NotFoundError,
)
from plantkeeper.domain.base import DomainError


def _problem(*, status_code: int, exc: Exception) -> JSONResponse:
    """Return a small, uniform error body."""
    return JSONResponse(
        status_code=status_code,
        content={"detail": str(exc), "error": type(exc).__name__},
    )


async def domain_error_handler(request: Request, exc: Exception) -> JSONResponse:
    """A broken invariant is a conflict, not a bug."""
    return _problem(status_code=status.HTTP_409_CONFLICT, exc=exc)


async def not_found_handler(request: Request, exc: Exception) -> JSONResponse:
    """A missing aggregate is a 404."""
    return _problem(status_code=status.HTTP_404_NOT_FOUND, exc=exc)


async def idempotency_conflict_handler(request: Request, exc: Exception) -> JSONResponse:
    """Reusing a key for a different request body is a conflict."""
    return _problem(status_code=status.HTTP_409_CONFLICT, exc=exc)


async def concurrent_write_handler(request: Request, exc: Exception) -> JSONResponse:
    """Losing a race for a unique key is a conflict, not a bug.

    The write side translates its own optimistic-lock failures — an idempotency key
    claimed twice, an event-store version appended twice — into
    :class:`ConcurrentWriteError`, and this is where that promise becomes a status a
    client can act on instead of a 500.
    """
    return _problem(status_code=status.HTTP_409_CONFLICT, exc=exc)


async def value_object_error_handler(request: Request, exc: Exception) -> JSONResponse:
    """A body that cannot become a value object is unprocessable."""
    assert isinstance(exc, ValidationError)
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        content={"detail": exc.errors(include_url=False), "error": "ValidationError"},
    )


def register_exception_handlers(app: FastAPI) -> None:
    """Attach the handler for every failure the write side can raise.

    Order does not matter: each class gets its own registration, and FastAPI
    looks handlers up by the exception's own class hierarchy.
    """
    app.add_exception_handler(DomainError, domain_error_handler)
    app.add_exception_handler(NotFoundError, not_found_handler)
    app.add_exception_handler(IdempotencyKeyConflictError, idempotency_conflict_handler)
    app.add_exception_handler(ConcurrentWriteError, concurrent_write_handler)
    app.add_exception_handler(ValidationError, value_object_error_handler)
