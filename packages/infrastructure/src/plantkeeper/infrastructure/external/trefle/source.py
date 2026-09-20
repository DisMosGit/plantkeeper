"""The synchronisation saga's upstream, with a cached-snapshot fallback.

``TrefleClient`` answers from the network or not at all. This adapter adds the
part the saga needs for a bad day: when Trefle is unreachable or its breaker is
open, the *last successful snapshot* kept in Valkey is applied instead. The
synchronisation then either applies a whole, slightly stale catalogue or, with no
snapshot at all, finds no changes — it never fails on a network problem, and it
never diffs against half a fetch.

The snapshot is an optimisation with its own TTL: a Valkey failure is logged and
ignored, because losing the fallback must not lose the sync.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Final, Protocol, runtime_checkable

from pydantic import TypeAdapter, ValidationError
from valkey.asyncio import Valkey
from valkey.exceptions import ValkeyError

from plantkeeper.application.ports.catalog import SpeciesRecord
from plantkeeper.infrastructure.external.circuit_breaker import CircuitOpenError
from plantkeeper.infrastructure.external.trefle.client import (
    TrefleClient,
    TrefleUnavailableError,
)

logger = logging.getLogger(__name__)

SNAPSHOT_KEY: Final = "species:upstream:snapshot"
"""Where the last successful upstream fetch is kept, as a JSON list."""

_RECORDS_ADAPTER: Final = TypeAdapter(list[SpeciesRecord])


@runtime_checkable
class SpeciesSnapshotStore(Protocol):
    """Where the last successful upstream snapshot is kept."""

    async def load(self) -> list[SpeciesRecord] | None:
        """Return the snapshot, or ``None`` when there is none."""
        ...

    async def save(self, records: Sequence[SpeciesRecord]) -> None:
        """Replace the snapshot with a fresh fetch."""
        ...


class ValkeySpeciesSnapshotStore:
    """The last successful upstream snapshot, in Valkey."""

    def __init__(self, client: Valkey, *, ttl_seconds: int) -> None:
        """Take the process's Valkey client and how long a snapshot lives."""
        self._client = client
        self._ttl_seconds = ttl_seconds

    async def load(self) -> list[SpeciesRecord] | None:
        """Return the snapshot, or ``None`` when there is none or it is unusable."""
        try:
            raw = await self._client.get(SNAPSHOT_KEY)
        except (ValkeyError, OSError) as exc:
            logger.warning("could not read the upstream snapshot: %s", exc)
            return None
        if raw is None:
            return None
        try:
            return _RECORDS_ADAPTER.validate_json(raw)
        except ValidationError:
            logger.warning("the cached upstream snapshot does not validate; ignoring it")
            return None

    async def save(self, records: Sequence[SpeciesRecord]) -> None:
        """Replace the snapshot with a fresh fetch."""
        if not records:
            return
        try:
            await self._client.set(
                SNAPSHOT_KEY,
                _RECORDS_ADAPTER.dump_json(list(records)),
                ex=self._ttl_seconds,
            )
        except (ValkeyError, OSError) as exc:
            logger.warning("could not store the upstream snapshot: %s", exc)


class TrefleSpeciesSource:
    """The ``SpeciesSource`` port over Trefle, with the snapshot fallback."""

    def __init__(self, *, client: TrefleClient, snapshots: SpeciesSnapshotStore) -> None:
        """Take the HTTP client and where its successful answers are remembered."""
        self._client = client
        self._snapshots = snapshots

    async def fetch_all(self) -> list[SpeciesRecord]:
        """Fetch the upstream catalogue, or fall back to the last snapshot.

        Raises anything that is not a connectivity problem: a bad token or a
        changed contract is an operator's business, and must not be papered over
        with stale data.
        """
        try:
            records = await self._client.fetch_all()
        except (TrefleUnavailableError, CircuitOpenError) as exc:
            logger.warning("Trefle is unavailable (%s); falling back to the snapshot", exc)
            return await self._fallback()
        if records:
            await self._snapshots.save(records)
        return records

    async def _fallback(self) -> list[SpeciesRecord]:
        """Answer with the cached snapshot, or nothing when there is none."""
        cached = await self._snapshots.load()
        if cached is None:
            logger.warning("no upstream snapshot is cached; the synchronisation finds no changes")
            return []
        logger.warning("serving %d species from the cached upstream snapshot", len(cached))
        return cached
