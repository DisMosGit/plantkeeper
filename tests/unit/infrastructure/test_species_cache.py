"""Unit tests for the Valkey species cache.

The Valkey client is replaced by an in-memory double, so the tests pin the key
layout, the TTL and — the part that matters operationally — that no Valkey
failure ever escapes the cache.
"""

from __future__ import annotations

from datetime import timedelta
from typing import cast

import pytest
from valkey.asyncio import Valkey
from valkey.exceptions import ValkeyError

from plantkeeper.application.views import SpeciesView
from plantkeeper.domain.catalog.values import LightRequirement
from plantkeeper.domain.identifiers import SpeciesId
from plantkeeper.infrastructure.cache.species import ValkeySpeciesCache, cache_key

TTL_SECONDS = 86_400
SPECIES = SpeciesId.new()


class FakeValkey:
    """The three commands the cache uses, in memory."""

    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.expiries: list[int | None] = []
        self.error: Exception | None = None

    async def get(self, key: str) -> str | None:
        """Return the stored value, or fail when the test asks for that."""
        if self.error is not None:
            raise self.error
        return self.store.get(key)

    async def set(self, key: str, value: str, *, ex: int | None = None) -> None:
        """Store a value and remember the TTL it was given."""
        if self.error is not None:
            raise self.error
        self.store[key] = value
        self.expiries.append(ex)

    async def delete(self, *keys: str) -> None:
        """Drop the keys."""
        if self.error is not None:
            raise self.error
        for key in keys:
            self.store.pop(key, None)


@pytest.fixture
def fake() -> FakeValkey:
    return FakeValkey()


@pytest.fixture
def cache(fake: FakeValkey) -> ValkeySpeciesCache:
    return ValkeySpeciesCache(cast("Valkey", fake), ttl_seconds=TTL_SECONDS)


def a_view(*, common_name: str = "Boston fern") -> SpeciesView:
    """One catalogue view."""
    return SpeciesView(
        species_id=SPECIES,
        scientific_name="Nephrolepis exaltata",
        common_name=common_name,
        watering_interval=timedelta(days=7),
        light_requirement=LightRequirement.MEDIUM,
        version=1,
    )


def test_the_key_is_namespaced_by_the_identifier() -> None:
    assert cache_key(SPECIES) == f"species:{SPECIES.value}"


async def test_a_stored_view_round_trips(cache: ValkeySpeciesCache, fake: FakeValkey) -> None:
    await cache.set(a_view())

    assert await cache.get(SPECIES) == a_view()
    assert set(fake.store) == {cache_key(SPECIES)}


async def test_a_stored_view_gets_the_configured_ttl(
    cache: ValkeySpeciesCache, fake: FakeValkey
) -> None:
    await cache.set(a_view())

    assert fake.expiries == [TTL_SECONDS]


async def test_a_miss_answers_none(cache: ValkeySpeciesCache) -> None:
    assert await cache.get(SPECIES) is None


async def test_an_unparsable_entry_is_dropped_and_read_as_a_miss(
    cache: ValkeySpeciesCache, fake: FakeValkey
) -> None:
    fake.store[cache_key(SPECIES)] = "not json"

    assert await cache.get(SPECIES) is None
    assert cache_key(SPECIES) not in fake.store


async def test_invalidate_drops_every_named_entry(
    cache: ValkeySpeciesCache, fake: FakeValkey
) -> None:
    other = SpeciesId.new()
    await cache.set(a_view())
    fake.store[cache_key(other)] = "anything"

    await cache.invalidate([SPECIES, other])

    assert fake.store == {}


async def test_invalidating_nothing_touches_no_key(
    cache: ValkeySpeciesCache, fake: FakeValkey
) -> None:
    await cache.invalidate([])

    assert fake.expiries == []


@pytest.mark.parametrize("operation", ["get", "set", "invalidate"])
async def test_a_valkey_failure_never_escapes(
    cache: ValkeySpeciesCache, fake: FakeValkey, operation: str
) -> None:
    """A cache is an optimisation: its outage must not become the request's."""
    fake.error = ValkeyError("valkey is down")

    if operation == "get":
        assert await cache.get(SPECIES) is None
    elif operation == "set":
        await cache.set(a_view())
    else:
        await cache.invalidate([SPECIES])
