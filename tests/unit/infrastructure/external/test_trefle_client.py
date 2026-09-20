"""Behaviour tests for the Trefle HTTP client.

Everything runs over ``httpx.MockTransport``: the suite never touches the
network, and each test supplies exactly the upstream it needs — a healthy
catalogue, a flapping one, a rejecting one, a dead one.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from aiolimiter import AsyncLimiter
from pydantic import ValidationError

from plantkeeper.domain.catalog.values import LightRequirement
from plantkeeper.infrastructure.external.circuit_breaker import (
    AsyncCircuitBreaker,
    CircuitOpenError,
)
from plantkeeper.infrastructure.external.trefle.client import (
    TrefleAuthError,
    TrefleClient,
    TrefleUnavailableError,
)

BASE_URL = "https://trefle.test/api/v1/"
NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)

Handler = Callable[[httpx.Request], httpx.Response]


class FixedClock:
    """A clock that never moves."""

    def now(self) -> datetime:
        """Return the fixed instant."""
        return NOW


class Recorder:
    """Remembers the requests a handler served, including retries."""

    def __init__(self) -> None:
        self.paths: list[str] = []
        self.params: list[dict[str, str]] = []

    def record(self, request: httpx.Request) -> None:
        """Note one request."""
        self.paths.append(request.url.path)
        self.params.append(dict(request.url.params))


def a_list_item(slug: str, scientific_name: str, common_name: str | None) -> dict[str, object]:
    """One species entry of a list payload."""
    return {
        "id": len(slug),
        "slug": slug,
        "scientific_name": scientific_name,
        "common_name": common_name,
    }


PAGE_ONE_ITEMS: list[dict[str, object]] = [
    a_list_item("monstera-deliciosa", "Monstera deliciosa", "Swiss cheese plant"),
    a_list_item("aloe-vera", "Aloe vera", "Aloe"),
]

PAGE_TWO_ITEMS: list[dict[str, object]] = [
    a_list_item("nephrolepis-exaltata", "Nephrolepis exaltata", None),
]

ALL_ITEMS: list[dict[str, object]] = [*PAGE_ONE_ITEMS, *PAGE_TWO_ITEMS]

PAGE_ONE: dict[str, object] = {
    "data": PAGE_ONE_ITEMS,
    "links": {"self": "/api/v1/species?page=1", "next": "/api/v1/species?page=2"},
    "meta": {"total": 3},
}

PAGE_TWO: dict[str, object] = {
    "data": PAGE_TWO_ITEMS,
    "links": {"self": "/api/v1/species?page=2", "next": None},
    "meta": {"total": 3},
}

GROWTH: dict[str, dict[str, int] | None] = {
    "monstera-deliciosa": {"light": 5, "soil_humidity": 7},
    "aloe-vera": {"light": 9, "soil_humidity": 2},
    "nephrolepis-exaltata": None,
}


def a_detail(slug: str) -> dict[str, object]:
    """The detail response for one slug."""
    item = next(item for item in ALL_ITEMS if item["slug"] == slug)
    return {"data": {**item, "growth": GROWTH[slug]}}


def catalogue_handler(recorder: Recorder) -> Handler:
    """A healthy upstream: two pages and a detail per species."""

    def handle(request: httpx.Request) -> httpx.Response:
        recorder.record(request)
        if request.url.path.endswith("/species"):
            page = request.url.params.get("page")
            return httpx.Response(200, json=PAGE_ONE if page == "1" else PAGE_TWO)
        slug = request.url.path.rsplit("/", 1)[-1]
        return httpx.Response(200, json=a_detail(slug))

    return handle


@asynccontextmanager
async def a_client(
    handler: Handler,
    *,
    token: str = "test-token",
    species_limit: int = 30,
    max_attempts: int = 3,
    failure_threshold: int = 5,
) -> AsyncIterator[TrefleClient]:
    """Build a client over an in-process transport."""
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url=BASE_URL) as http:
        yield TrefleClient(
            client=http,
            limiter=AsyncLimiter(55, 60),
            breaker=AsyncCircuitBreaker(
                name="trefle",
                failure_threshold=failure_threshold,
                reset_timeout=timedelta(seconds=60),
                clock=FixedClock(),
            ),
            token=token,
            species_limit=species_limit,
            max_attempts=max_attempts,
        )


async def test_the_catalogue_is_paged_and_every_species_is_detailed() -> None:
    recorder = Recorder()
    async with a_client(catalogue_handler(recorder)) as client:
        records = await client.fetch_all()

    assert [record.common_name for record in records] == [
        "Swiss cheese plant",
        "Aloe",
        "Nephrolepis exaltata",
    ]
    by_name = {record.common_name: record for record in records}
    assert by_name["Swiss cheese plant"].light_requirement is LightRequirement.MEDIUM
    assert by_name["Swiss cheese plant"].watering_interval.value == timedelta(days=10)
    assert by_name["Aloe"].light_requirement is LightRequirement.HIGH
    assert by_name["Aloe"].watering_interval.value == timedelta(days=3)
    # A species whose growth Trefle does not know keeps the weekly default.
    assert by_name["Nephrolepis exaltata"].watering_interval.value == timedelta(days=7)
    assert by_name["Nephrolepis exaltata"].light_requirement is LightRequirement.MEDIUM
    # Two list pages and one detail request per species.
    assert len(recorder.paths) == 5


async def test_every_request_carries_the_token() -> None:
    recorder = Recorder()
    async with a_client(catalogue_handler(recorder)) as client:
        await client.fetch_all()

    assert recorder.params
    assert {params.get("token") for params in recorder.params} == {"test-token"}


async def test_the_species_limit_stops_the_fetch_early() -> None:
    recorder = Recorder()
    async with a_client(catalogue_handler(recorder), species_limit=1) as client:
        records = await client.fetch_all()

    assert len(records) == 1
    # One list page and one detail: the limit ends the loop before page two.
    assert len(recorder.paths) == 2


async def test_an_unconfigured_client_fetches_nothing() -> None:
    recorder = Recorder()
    async with a_client(catalogue_handler(recorder), token="") as client:
        assert client.configured is False
        assert await client.fetch_all() == []

    assert recorder.paths == []


async def test_a_transient_failure_is_retried() -> None:
    recorder = Recorder()
    healthy = catalogue_handler(recorder)
    attempts = 0

    def flaky(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        if request.url.path.endswith("/species"):
            attempts += 1
            if attempts == 1:
                return httpx.Response(503, json={"error": "try later"})
        return healthy(request)

    async with a_client(flaky) as client:
        records = await client.fetch_all()

    assert [record.common_name for record in records] == [
        "Swiss cheese plant",
        "Aloe",
        "Nephrolepis exaltata",
    ]
    # Two list pages, and the first page once more after the 503.
    assert attempts == 3


async def test_an_auth_failure_is_not_retried() -> None:
    calls = 0

    def rejecting(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(401, json={"error": "unauthorized"})

    async with a_client(rejecting) as client:
        with pytest.raises(TrefleAuthError):
            await client.fetch_all()

    assert calls == 1


async def test_a_transport_failure_becomes_unavailable() -> None:
    def unreachable(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("no route")

    async with a_client(unreachable, max_attempts=1) as client:
        with pytest.raises(TrefleUnavailableError):
            await client.fetch_all()


async def test_a_missing_detail_record_is_skipped() -> None:
    healthy = catalogue_handler(Recorder())

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/aloe-vera"):
            return httpx.Response(404, json={"error": "not found"})
        return healthy(request)

    async with a_client(handler) as client:
        records = await client.fetch_all()

    assert [record.common_name for record in records] == [
        "Swiss cheese plant",
        "Nephrolepis exaltata",
    ]


async def test_a_run_of_failures_opens_the_breaker() -> None:
    def down(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"error": "down"})

    async with a_client(down, max_attempts=1, failure_threshold=2) as client:
        for _ in range(2):
            with pytest.raises(TrefleUnavailableError):
                await client.fetch_all()
        # The third call is refused without touching the network.
        with pytest.raises(CircuitOpenError):
            await client.fetch_all()


async def test_an_empty_page_ends_the_walk() -> None:
    def empty(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": [], "links": {"next": None}})

    async with a_client(empty) as client:
        assert await client.fetch_all() == []


@pytest.mark.parametrize(
    "payload",
    [
        {"data": "not-a-list"},
        {"data": [{"slug": "missing-fields"}]},
    ],
)
async def test_a_malformed_list_payload_fails_loudly(payload: dict[str, object]) -> None:
    """A changed contract is an operator's problem, not something to paper over."""

    def malformed(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    async with a_client(malformed) as client:
        with pytest.raises(ValidationError):
            await client.fetch_all()
