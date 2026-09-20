"""Catalog ports: how the application reads species, and where they come from.

The Catalog bounded context owns the local catalogue (``write_catalog.species``).
Three seams sit on top of it:

* :class:`SpeciesCatalog` is the anti-corruption layer the onboarding saga reads
  through. It answers from the local table. The synchronous Trefle fallback for a
  local miss is deliberately *not* part of the synchronisation: the catalogue is
  filled by :class:`SpeciesSource` on a schedule, so onboarding never waits on an
  external HTTP call.
* :class:`SpeciesSource` is the upstream the synchronisation saga pulls from; the
  Trefle adapter is bound in the worker's container.
* :class:`SpeciesCache` is the read-through Valkey cache of the catalogue, and
  :class:`SpeciesCacheInvalidator` is the narrower seam the synchronisation saga
  uses to declare entries stale.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict

from plantkeeper.application.views import SpeciesView
from plantkeeper.domain.catalog.species import Species
from plantkeeper.domain.catalog.values import LightRequirement
from plantkeeper.domain.identifiers import SpeciesId
from plantkeeper.domain.values import WateringInterval


class SpeciesRecord(BaseModel):
    """One upstream catalogue entry, before it is compared with the local one."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    species_id: SpeciesId
    scientific_name: str
    common_name: str
    watering_interval: WateringInterval
    light_requirement: LightRequirement


@runtime_checkable
class SpeciesCatalog(Protocol):
    """The catalogue as the rest of the application reads it."""

    async def get(self, species_id: SpeciesId) -> Species | None:
        """Return the species, or ``None`` when the catalogue does not know it."""
        ...


@runtime_checkable
class SpeciesSource(Protocol):
    """Upstream source of catalogue entries (Trefle)."""

    async def fetch_all(self) -> list[SpeciesRecord]:
        """Return every upstream entry available right now.

        An unavailable upstream is not a failure: the adapter falls back to its
        last cached snapshot, and to an empty list when there is none, so the
        synchronisation finds no changes rather than failing.
        """
        ...


@runtime_checkable
class SpeciesCacheInvalidator(Protocol):
    """Declaring cached catalogue entries stale.

    The synchronisation saga's step depends on this narrow contract, not on the
    whole :class:`SpeciesCache`: the step announces staleness through the outbox
    and never reads or fills the cache.
    """

    async def invalidate(self, species_ids: Sequence[SpeciesId]) -> None:
        """Declare the cache entries of these species stale.

        The caller commits the transaction; a failure here fails the saga and
        triggers its compensation.
        """
        ...


@runtime_checkable
class SpeciesCache(SpeciesCacheInvalidator, Protocol):
    """The read-through cache of the catalogue.

    Its value is the query layer's own projection, so the read path never has to
    rebuild an aggregate just to cache it. A cache failure is never a request
    failure: the implementation degrades to a miss, and the repository answers.
    """

    async def get(self, species_id: SpeciesId) -> SpeciesView | None:
        """Return the cached species, or ``None`` on a miss."""
        ...

    async def set(self, species: SpeciesView) -> None:
        """Cache one species for the configured TTL."""
        ...
