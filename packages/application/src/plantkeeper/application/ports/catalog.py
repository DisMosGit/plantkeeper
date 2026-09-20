"""Catalog ports: how the application reads species, and where they come from.

The Catalog bounded context owns the local catalogue (``write_catalog.species``).
Two seams sit on top of it:

* :class:`SpeciesCatalog` is the anti-corruption layer the onboarding saga reads
  through. In Phase 4 it answers from the local table; Phase 9 extends it to fall
  back to Trefle when a species is missing locally.
* :class:`SpeciesSource` is the upstream the synchronisation saga pulls from. The
  Trefle adapter arrives in Phase 9; until then the production binding is a
  placeholder that returns nothing, and tests substitute their own source.
* :class:`SpeciesCache` is the cache invalidation step of the synchronisation.
  Publishing ``SpeciesCacheInvalidated`` is the Phase 4 implementation; the Valkey
  adapter that actually drops keys is Phase 9's.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict

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
    """Upstream source of catalogue entries (Trefle, from Phase 9)."""

    async def fetch_all(self) -> list[SpeciesRecord]:
        """Return every upstream entry available right now."""
        ...


@runtime_checkable
class SpeciesCache(Protocol):
    """Invalidation of the species cache."""

    async def invalidate(self, species_ids: Sequence[SpeciesId]) -> None:
        """Declare the cache entries of these species stale.

        The caller commits the transaction; a failure here fails the saga and
        triggers its compensation.
        """
        ...
