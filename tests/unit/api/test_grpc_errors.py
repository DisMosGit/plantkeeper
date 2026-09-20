"""The gRPC error table: which application failure becomes which status."""

from __future__ import annotations

import grpc
import pytest
from pydantic import ValidationError

from plantkeeper.api.grpc.errors import status_code_for
from plantkeeper.application.errors import (
    ConcurrentWriteError,
    EventStoreConcurrencyError,
    IdempotencyKeyConflictError,
    NotFoundError,
)
from plantkeeper.domain.base import DomainError
from plantkeeper.domain.identifiers import PlantId


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (NotFoundError("no such plant"), grpc.StatusCode.NOT_FOUND),
        (IdempotencyKeyConflictError("key reused"), grpc.StatusCode.ALREADY_EXISTS),
        (ConcurrentWriteError("lost the race"), grpc.StatusCode.ABORTED),
        (EventStoreConcurrencyError("stale version"), grpc.StatusCode.ABORTED),
        (DomainError("broken invariant"), grpc.StatusCode.FAILED_PRECONDITION),
        (RuntimeError("a bug on this side"), grpc.StatusCode.INTERNAL),
    ],
)
def test_every_failure_becomes_its_status(exc: Exception, expected: grpc.StatusCode) -> None:
    """The subclass cases matter: a lost event-store race is still a lost race."""
    assert status_code_for(exc) is expected


def test_a_malformed_client_value_is_invalid_argument() -> None:
    """A UUID that cannot be parsed is the client's mistake, not a server bug."""
    with pytest.raises(ValidationError) as caught:
        PlantId.model_validate("not-a-uuid")
    assert status_code_for(caught.value) is grpc.StatusCode.INVALID_ARGUMENT
