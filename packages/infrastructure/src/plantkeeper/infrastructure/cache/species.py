"""The catalogue's read-through cache, over Valkey.

The cached value is the query layer's own ``SpeciesView``: the read path asks for
exactly the shape it returns, so caching never means rebuilding an aggregate that
is thrown away one line later. Entries are keyed ``species:<uuid>`` and expire
after the configured TTL (24 hours), which is the upper bound on a stale answer
even if an invalidation is lost.

Every Valkey failure is swallowed, as with the notification channel: a cache is
an optimisation, and a request must still be answered from the repository when
Valkey is down. A value that does not deserialise is dropped as well — it cannot
come from this code's own serialiser, so keeping it would only repeat the error.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Final, cast

from pydantic import ValidationError
from valkey.asyncio import Valkey
from valkey.exceptions import ValkeyError

from plantkeeper.application.views import SpeciesView
from plantkeeper.domain.identifiers import SpeciesId

logger = logging.getLogger(__name__)

KEY_PREFIX: Final = "species:"


def cache_key(species_id: SpeciesId) -> str:
    """Return the Valkey key of one catalogue entry."""
    return f"{KEY_PREFIX}{species_id.value}"


class ValkeySpeciesCache:
    """The ``SpeciesCache`` port over the process's Valkey client."""

    def __init__(self, client: Valkey, *, ttl_seconds: int) -> None:
        """Take the client the process owns and how long an entry lives."""
        self._client = client
        self._ttl_seconds = ttl_seconds

    async def get(self, species_id: SpeciesId) -> SpeciesView | None:
        """Return the cached species, or ``None`` on a miss or a broken cache."""
        key = cache_key(species_id)
        try:
            raw = await self._client.get(key)
        except (ValkeyError, OSError) as exc:
            logger.warning("could not read the species cache: %s", exc)
            return None
        if raw is None:
            return None
        try:
            # ``SpeciesView`` subclasses cqrs's untyped ``PydanticResponse``, so a
            # strict build sees its classmethods as ``Any``; the model validates
            # this JSON into a ``SpeciesView`` or raises ``ValidationError``.
            return cast("SpeciesView", SpeciesView.model_validate_json(raw))
        except ValidationError:
            logger.warning("species cache entry %s does not validate; dropping it", key)
            await self._delete_quietly(key)
            return None

    async def set(self, species: SpeciesView) -> None:
        """Cache one species for the configured TTL."""
        try:
            await self._client.set(
                cache_key(species.species_id),
                species.model_dump_json(),
                ex=self._ttl_seconds,
            )
        except (ValkeyError, OSError) as exc:
            logger.warning("could not write the species cache: %s", exc)

    async def invalidate(self, species_ids: Sequence[SpeciesId]) -> None:
        """Drop the cache entries of these species, if they exist."""
        keys = [cache_key(species_id) for species_id in species_ids]
        if not keys:
            return
        try:
            await self._client.delete(*keys)
        except (ValkeyError, OSError) as exc:
            logger.warning("could not invalidate the species cache: %s", exc)

    async def _delete_quietly(self, key: str) -> None:
        """Drop one unusable entry, never failing the reader."""
        try:
            await self._client.delete(key)
        except (ValkeyError, OSError) as exc:
            logger.debug("could not drop the broken species cache entry: %s", exc)
