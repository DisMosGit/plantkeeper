"""Tests for the Trefle source's snapshot fallback.

The point of the adapter is what happens on a bad day: Trefle down must mean
"apply yesterday's catalogue", or "change nothing", never "fail the saga". The
client and the snapshot store are both replaced by doubles that can be told to
fail.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta
from typing import cast

import pytest
from valkey.asyncio import Valkey
from valkey.exceptions import ValkeyError

from plantkeeper.application.ports.catalog import SpeciesRecord
from plantkeeper.domain.catalog.values import LightRequirement
from plantkeeper.domain.identifiers import SpeciesId
from plantkeeper.domain.values import WateringInterval
from plantkeeper.infrastructure.external.circuit_breaker import CircuitOpenError
from plantkeeper.infrastructure.external.trefle.client import (
    TrefleAuthError,
    TrefleClient,
    TrefleUnavailableError,
)
from plantkeeper.infrastructure.external.trefle.source import (
    SNAPSHOT_KEY,
    TrefleSpeciesSource,
    ValkeySpeciesSnapshotStore,
)


def a_record(name: str) -> SpeciesRecord:
    """One upstream catalogue entry."""
    return SpeciesRecord(
        species_id=SpeciesId.new(),
        scientific_name=f"{name} scientific",
        common_name=name,
        watering_interval=WateringInterval(value=timedelta(days=7)),
        light_requirement=LightRequirement.MEDIUM,
    )


class StubClient:
    """A ``TrefleClient`` that answers, or fails, as the test asks."""

    def __init__(
        self, *, records: list[SpeciesRecord] | None = None, error: Exception | None = None
    ) -> None:
        self._records = records or []
        self._error = error
        self.calls = 0

    async def fetch_all(self) -> list[SpeciesRecord]:
        """Return the records, or raise the configured error."""
        self.calls += 1
        if self._error is not None:
            raise self._error
        return self._records


class StubSnapshots:
    """A snapshot store held in memory."""

    def __init__(self, cached: list[SpeciesRecord] | None = None) -> None:
        self.cached = cached
        self.saved: list[SpeciesRecord] = []

    async def load(self) -> list[SpeciesRecord] | None:
        """Return whatever the test put there."""
        return self.cached

    async def save(self, records: Sequence[SpeciesRecord]) -> None:
        """Remember what was saved."""
        self.saved = list(records)


@pytest.fixture
def snapshots() -> StubSnapshots:
    return StubSnapshots()


async def test_a_successful_fetch_is_remembered(snapshots: StubSnapshots) -> None:
    records = [a_record("Boston fern")]
    source = TrefleSpeciesSource(
        client=cast("TrefleClient", StubClient(records=records)), snapshots=snapshots
    )

    assert await source.fetch_all() == records
    assert snapshots.saved == records


async def test_an_empty_fetch_replaces_nothing(snapshots: StubSnapshots) -> None:
    source = TrefleSpeciesSource(client=cast("TrefleClient", StubClient()), snapshots=snapshots)

    assert await source.fetch_all() == []
    assert snapshots.saved == []


async def test_an_unavailable_upstream_falls_back_to_the_snapshot() -> None:
    cached = [a_record("Boston fern")]
    snapshots = StubSnapshots(cached)
    source = TrefleSpeciesSource(
        client=cast("TrefleClient", StubClient(error=TrefleUnavailableError("down"))),
        snapshots=snapshots,
    )

    assert await source.fetch_all() == cached


async def test_an_open_breaker_falls_back_to_the_snapshot() -> None:
    cached = [a_record("Boston fern")]
    snapshots = StubSnapshots(cached)
    source = TrefleSpeciesSource(
        client=cast("TrefleClient", StubClient(error=CircuitOpenError("trefle"))),
        snapshots=snapshots,
    )

    assert await source.fetch_all() == cached


async def test_an_unavailable_upstream_without_a_snapshot_changes_nothing() -> None:
    source = TrefleSpeciesSource(
        client=cast("TrefleClient", StubClient(error=TrefleUnavailableError("down"))),
        snapshots=StubSnapshots(),
    )

    assert await source.fetch_all() == []


async def test_an_auth_failure_is_not_papered_over(snapshots: StubSnapshots) -> None:
    """A bad token is the operator's business; stale data would hide it."""
    source = TrefleSpeciesSource(
        client=cast("TrefleClient", StubClient(error=TrefleAuthError("refused"))),
        snapshots=snapshots,
    )

    with pytest.raises(TrefleAuthError):
        await source.fetch_all()


class FakeValkey:
    """The two commands the snapshot store uses."""

    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.error: Exception | None = None

    async def get(self, key: str) -> str | None:
        """Return the stored value."""
        if self.error is not None:
            raise self.error
        return self.store.get(key)

    async def set(self, key: str, value: str, *, ex: int | None = None) -> None:
        """Store a value."""
        if self.error is not None:
            raise self.error
        self.store[key] = value


@pytest.fixture
def fake_valkey() -> FakeValkey:
    return FakeValkey()


@pytest.fixture
def store(fake_valkey: FakeValkey) -> ValkeySpeciesSnapshotStore:
    return ValkeySpeciesSnapshotStore(cast("Valkey", fake_valkey), ttl_seconds=86_400)


async def test_the_snapshot_round_trips_through_valkey(
    store: ValkeySpeciesSnapshotStore, fake_valkey: FakeValkey
) -> None:
    records = [a_record("Boston fern"), a_record("Aloe")]

    await store.save(records)

    assert SNAPSHOT_KEY in fake_valkey.store
    assert await store.load() == records


async def test_no_snapshot_reads_as_none(store: ValkeySpeciesSnapshotStore) -> None:
    assert await store.load() is None


async def test_an_unparsable_snapshot_reads_as_none(
    store: ValkeySpeciesSnapshotStore, fake_valkey: FakeValkey
) -> None:
    fake_valkey.store[SNAPSHOT_KEY] = "not json"

    assert await store.load() is None


async def test_an_empty_snapshot_is_not_stored(
    store: ValkeySpeciesSnapshotStore, fake_valkey: FakeValkey
) -> None:
    await store.save([])

    assert fake_valkey.store == {}


@pytest.mark.parametrize("operation", ["load", "save"])
async def test_a_valkey_failure_never_escapes_the_snapshot_store(
    store: ValkeySpeciesSnapshotStore, fake_valkey: FakeValkey, operation: str
) -> None:
    fake_valkey.error = ValkeyError("valkey is down")

    if operation == "load":
        assert await store.load() is None
    else:
        await store.save([a_record("Boston fern")])
