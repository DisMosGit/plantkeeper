"""Mapping from domain and application failures onto gRPC status codes.

The REST API answers with HTTP statuses (``plantkeeper.api.errors``); gRPC has its
own vocabulary for the same failures, and a client of one protocol should not
have to guess what the other calls them. The two tables live side by side on
purpose: each translates the same application-layer errors.
"""

from __future__ import annotations

import grpc
from pydantic import ValidationError

from plantkeeper.application.errors import (
    ConcurrentWriteError,
    IdempotencyKeyConflictError,
    NotFoundError,
)
from plantkeeper.domain.base import DomainError


def status_code_for(exc: BaseException) -> grpc.StatusCode:
    """Translate an application or domain failure into a gRPC status.

    The order matters: ``EventStoreConcurrencyError`` is a
    ``ConcurrentWriteError``, and every application error is a different
    hierarchy from ``DomainError``. A broken invariant is ``FAILED_PRECONDITION``
    — the request was well formed, the state refuses it — while a malformed
    client value is ``INVALID_ARGUMENT``. Anything unrecognised is a bug on this
    side of the wire, which is exactly what ``INTERNAL`` means.
    """
    if isinstance(exc, NotFoundError):
        return grpc.StatusCode.NOT_FOUND
    if isinstance(exc, IdempotencyKeyConflictError):
        return grpc.StatusCode.ALREADY_EXISTS
    if isinstance(exc, ConcurrentWriteError):
        return grpc.StatusCode.ABORTED
    if isinstance(exc, DomainError):
        return grpc.StatusCode.FAILED_PRECONDITION
    if isinstance(exc, ValidationError):
        return grpc.StatusCode.INVALID_ARGUMENT
    return grpc.StatusCode.INTERNAL
