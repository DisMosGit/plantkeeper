"""The idempotency port.

An HTTP client may retry a create request (a timeout is not proof that the
request failed). The write side stores the response of an ``Idempotency-Key``
in the same transaction as the aggregate, so a replay returns the original
answer instead of creating a second plant. See ``docs/adr/0003-write-side-outbox.md``.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue


class IdempotencyRecord(BaseModel):
    """A stored response, keyed by the client's idempotency key.

    ``request_hash`` is what makes a replay safe: the same key with the same
    body returns this record, and the same key with a different body is a
    client error rather than a silent mix-up of two requests.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    key: str = Field(min_length=1, max_length=255)
    request_hash: str = Field(min_length=1, max_length=64)
    status_code: int
    response: dict[str, JsonValue]
    created_at: AwareDatetime


@runtime_checkable
class IdempotencyRepository(Protocol):
    """Read and write access to the idempotency table."""

    async def get(self, key: str) -> IdempotencyRecord | None:
        """Return the response stored for ``key``, if any."""
        ...

    async def remember(self, record: IdempotencyRecord) -> None:
        """Stage a record in the current transaction.

        The key is the primary key, so a concurrent duplicate makes the
        surrounding transaction fail instead of overwriting the winner's answer.
        """
        ...
