"""The Trefle HTTP client: pagination, rate limiting, retry and the breaker.

Three concerns meet in this one class, in a fixed order that matters:

* the **limiter** holds every request under the configured requests-per-minute;
* **tenacity** retries a transient failure a few times inside one logical call, so
  a blip does not count against the breaker;
* the **breaker** therefore sees one outcome per logical call, and opens after a
  run of real failures.

``fetch_all`` never returns a partial snapshot: a species whose *detail* record is
missing or unparsable is skipped with a warning, but anything that looks like the
service being down aborts the whole fetch. A half-fetched catalogue would produce
a diff that silently drops species, which is worse than doing nothing.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Final

import httpx
from aiolimiter import AsyncLimiter
from pydantic import ValidationError
from tenacity import AsyncRetrying, retry_if_exception, stop_after_attempt, wait_exponential

from plantkeeper.application.ports.catalog import SpeciesRecord
from plantkeeper.domain.identifiers import SpeciesId
from plantkeeper.infrastructure.external.circuit_breaker import (
    AsyncCircuitBreaker,
    CircuitOpenError,
)
from plantkeeper.infrastructure.external.trefle.mapping import record_from_detail
from plantkeeper.infrastructure.external.trefle.models import (
    TrefleDetailResponse,
    TrefleListResponse,
    TrefleSpeciesListItem,
)

logger = logging.getLogger(__name__)

RETRY_WAIT_MULTIPLIER: Final = 0.5
RETRY_WAIT_MAX_SECONDS: Final = 8.0
AUTH_STATUS_CODES: Final = frozenset({401, 403})
MISSING_RECORD_STATUS_CODES: Final = frozenset({404, 422})
RETRYABLE_STATUS_CODES: Final = frozenset({429})


class TrefleClientError(Exception):
    """Base class of everything the Trefle client raises.

    ``CircuitOpenError`` is deliberately *not* a subclass: it means the client did
    not talk to Trefle at all, which callers treat as their fallback signal.
    """


class TrefleAuthError(TrefleClientError):
    """Trefle rejected the token; retrying cannot help."""


class TrefleRecordMissingError(TrefleClientError):
    """Trefle has no such record (or rejected the slug)."""


class TrefleUnavailableError(TrefleClientError):
    """Trefle did not answer, or answered with a server error."""


def _is_transient(exc: BaseException) -> bool:
    """Whether tenacity should retry the exception.

    Transport failures, 429 and 5xx are the transient ones. A 4xx is a decision
    by the server — a bad token, a bad page — and retrying it only delays the
    failure.
    """
    if isinstance(exc, httpx.TransportError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        return status in RETRYABLE_STATUS_CODES or status >= 500
    return False


class TrefleClient:
    """Fetch the upstream catalogue, one page and one detail at a time."""

    def __init__(
        self,
        *,
        client: httpx.AsyncClient,
        limiter: AsyncLimiter,
        breaker: AsyncCircuitBreaker,
        token: str,
        species_limit: int,
        max_attempts: int,
    ) -> None:
        """Take the collaborators the DI provider built for this process."""
        self._client = client
        self._limiter = limiter
        self._breaker = breaker
        self._token = token
        self._species_limit = species_limit
        self._max_attempts = max_attempts

    @property
    def configured(self) -> bool:
        """Whether a token is present; without one the client fetches nothing."""
        return bool(self._token)

    async def fetch_all(self) -> list[SpeciesRecord]:
        """Return up to ``species_limit`` upstream entries with their care data.

        Without a token this returns an empty list: an unconfigured upstream is a
        synchronisation that finds nothing, not a synchronisation that fails.
        """
        if not self.configured:
            logger.warning("TREFLE_TOKEN is not set; the catalogue synchronisation is a no-op")
            return []
        records: dict[SpeciesId, SpeciesRecord] = {}
        page = 1
        while len(records) < self._species_limit:
            payload = await self._list_page(page)
            if not payload.data:
                break
            for item in payload.data:
                if len(records) >= self._species_limit:
                    break
                record = await self._record_for(item)
                if record is not None:
                    records.setdefault(record.species_id, record)
            next_page = payload.links.next_page if payload.links is not None else None
            if not next_page:
                break
            page += 1
        logger.info("Trefle returned %d species", len(records))
        return list(records.values())

    async def _list_page(self, page: int) -> TrefleListResponse:
        """Fetch and parse one page of the species collection."""
        response = await self._get("species", {"page": str(page)})
        return TrefleListResponse.model_validate(response.json())

    async def _record_for(self, item: TrefleSpeciesListItem) -> SpeciesRecord | None:
        """Fetch one species' detail and map it, or skip it with a warning."""
        try:
            response = await self._get(f"species/{item.slug}")
        except TrefleRecordMissingError:
            logger.warning("Trefle has no detail record for %s; skipping", item.slug)
            return None
        try:
            detail = TrefleDetailResponse.model_validate(response.json()).data
        except ValidationError:
            logger.warning("Trefle detail for %s does not validate; skipping", item.slug)
            return None
        record = record_from_detail(detail)
        if record is None:
            logger.warning("Trefle record %s has no scientific name; skipping", item.slug)
        return record

    async def _get(self, path: str, params: dict[str, str] | None = None) -> httpx.Response:
        """One logical GET: retried inside the breaker, errors translated."""

        async def operation() -> httpx.Response:
            return await self._with_retry(lambda: self._request(path, params))

        try:
            return await self._breaker.call(operation)
        except CircuitOpenError:
            raise
        except httpx.HTTPStatusError as exc:
            raise _translate_status(exc) from exc
        except httpx.TransportError as exc:
            raise TrefleUnavailableError(f"Trefle is unreachable for {path}") from exc

    async def _with_retry(
        self, operation: Callable[[], Awaitable[httpx.Response]]
    ) -> httpx.Response:
        """Retry a transient failure up to ``max_attempts`` times."""
        retrying = AsyncRetrying(
            stop=stop_after_attempt(self._max_attempts),
            wait=wait_exponential(multiplier=RETRY_WAIT_MULTIPLIER, max=RETRY_WAIT_MAX_SECONDS),
            retry=retry_if_exception(_is_transient),
            reraise=True,
        )
        async for attempt in retrying:
            with attempt:
                return await operation()
        raise TrefleUnavailableError("the retry loop ended without an answer")

    async def _request(self, path: str, params: dict[str, str] | None) -> httpx.Response:
        """One HTTP request, under the rate limiter, failing on a bad status."""
        query = {"token": self._token, **(params or {})}
        async with self._limiter:
            response = await self._client.get(path, params=query)
        response.raise_for_status()
        return response


def _translate_status(exc: httpx.HTTPStatusError) -> TrefleClientError:
    """Turn an HTTP error into the client's own vocabulary."""
    status = exc.response.status_code
    if status in AUTH_STATUS_CODES:
        return TrefleAuthError(f"Trefle refused the token (HTTP {status})")
    if status in MISSING_RECORD_STATUS_CODES:
        return TrefleRecordMissingError(f"Trefle has no such record (HTTP {status})")
    if status in RETRYABLE_STATUS_CODES or status >= 500:
        return TrefleUnavailableError(f"Trefle failed with HTTP {status}")
    return TrefleClientError(f"Trefle rejected the request with HTTP {status}")
