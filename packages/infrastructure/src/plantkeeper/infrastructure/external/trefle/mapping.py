"""Mapping Trefle's vocabulary onto the Catalog context's.

This is the anti-corruption layer proper: Trefle's integer ids, slugs and
Ellenberg indicator classes stop here, and what leaves is a
:class:`~plantkeeper.application.ports.catalog.SpeciesRecord` — a `SpeciesId`,
two names, a `WateringInterval` and a `LightRequirement`. Nothing downstream
knows that Trefle exists.

The two heuristics are deliberately simple and documented:

* **Light** comes from Ellenberg ``L`` (1-9): the lower third is ``LOW``, the
  middle ``MEDIUM`` and the top ``HIGH``.
* **Watering** comes from Ellenberg ``F`` (1-12), the soil-humidity indicator,
  through fixed bands: the drier the habitat a species occupies, the shorter the
  interval. ``1-2`` gives 3 days, ``3-4`` gives 5, ``5-6`` gives 7, ``7-8``
  gives 10, ``9-10`` gives 14 and ``11-12`` gives 21.

Trefle leaves both indicators ``null`` for many species; the documented default
in that case is a weekly watering and ``MEDIUM`` light, which is also the value
the domain already treats as unremarkable.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Final
from uuid import UUID, uuid5

from plantkeeper.application.ports.catalog import SpeciesRecord
from plantkeeper.domain.catalog.values import LightRequirement
from plantkeeper.domain.identifiers import SpeciesId
from plantkeeper.domain.values import WateringInterval
from plantkeeper.infrastructure.external.trefle.models import (
    TrefleGrowth,
    TrefleSpeciesDetail,
)

TREFLE_SPECIES_NAMESPACE: Final = UUID("1f0f4c2e-9a3b-5d6e-8c7f-0a1b2c3d4e5f")
"""The fixed namespace the Trefle slug is hashed into a domain ``SpeciesId``.

Never change it: the id is the join between the local catalogue and the upstream
one, so regenerating the namespace would orphan every previously synced species.
"""

DEFAULT_WATERING_INTERVAL: Final = WateringInterval(value=timedelta(days=7))
"""What a species with no soil-humidity indicator gets."""

DEFAULT_LIGHT_REQUIREMENT: Final = LightRequirement.MEDIUM
"""What a species with no light indicator gets."""

ELLENBERG_LIGHT_MAX: Final = 9
"""``growth.light`` runs 1 (deep shade) … 9 (full sun)."""

ELLENBERG_SOIL_HUMIDITY_MAX: Final = 12
"""``growth.soil_humidity`` runs 1 (very dry) … 12 (submerged) — not 9."""

_WATERING_BANDS: Final = (
    # (highest Ellenberg F in the band, days between waterings)
    (2, 3),
    (4, 5),
    (6, 7),
    (8, 10),
    (10, 14),
    (12, 21),
)


def species_id_for(slug: str) -> SpeciesId:
    """Derive the stable domain identifier of a Trefle species from its slug."""
    return SpeciesId(uuid5(TREFLE_SPECIES_NAMESPACE, slug))


def light_requirement_for(light: int | None) -> LightRequirement:
    """Turn Ellenberg ``L`` into the domain's three-level requirement."""
    if light is None:
        return DEFAULT_LIGHT_REQUIREMENT
    clamped = min(max(light, 1), ELLENBERG_LIGHT_MAX)
    if clamped <= 3:
        return LightRequirement.LOW
    if clamped <= 6:
        return LightRequirement.MEDIUM
    return LightRequirement.HIGH


def watering_interval_for(soil_humidity: int | None) -> WateringInterval:
    """Turn Ellenberg ``F`` into a watering cadence through the documented bands."""
    if soil_humidity is None:
        return DEFAULT_WATERING_INTERVAL
    clamped = min(max(soil_humidity, 1), ELLENBERG_SOIL_HUMIDITY_MAX)
    for highest, days in _WATERING_BANDS:
        if clamped <= highest:
            return WateringInterval(value=timedelta(days=days))
    return DEFAULT_WATERING_INTERVAL


def record_from_detail(detail: TrefleSpeciesDetail) -> SpeciesRecord | None:
    """Map one Trefle detail record, or ``None`` when it cannot be one.

    A record without a scientific name is not a catalogue entry at all: the
    caller logs it with the slug and skips it. A missing common name is not a
    reason to skip — the scientific name stands in for it, because the domain
    requires both names to be non-blank.
    """
    scientific_name = detail.scientific_name.strip()
    if not scientific_name:
        return None
    common_name = (detail.common_name or scientific_name).strip() or scientific_name
    return SpeciesRecord(
        species_id=species_id_for(detail.slug),
        scientific_name=scientific_name,
        common_name=common_name,
        watering_interval=watering_interval_for(_soil_humidity_of(detail.growth)),
        light_requirement=light_requirement_for(_light_of(detail.growth)),
    )


def _soil_humidity_of(growth: TrefleGrowth | None) -> int | None:
    return None if growth is None else growth.soil_humidity


def _light_of(growth: TrefleGrowth | None) -> int | None:
    return None if growth is None else growth.light
