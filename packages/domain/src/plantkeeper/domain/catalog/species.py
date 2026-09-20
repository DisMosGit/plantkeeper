"""The Species aggregate: one catalogue entry, synchronised from Trefle."""

from __future__ import annotations

from datetime import datetime

from plantkeeper.domain.base import AggregateRoot
from plantkeeper.domain.catalog.errors import SpeciesNameEmptyError, SpeciesVersionConflictError
from plantkeeper.domain.catalog.events import SpeciesAdded, SpeciesUpdated
from plantkeeper.domain.catalog.values import LightRequirement
from plantkeeper.domain.identifiers import SpeciesId
from plantkeeper.domain.values import WateringInterval


class Species(AggregateRoot[SpeciesId]):
    """A catalogue species and its ideal care values.

    The aggregate is versioned: Trefle synchronisation reads version N, compares
    it with the local entry, and writes with ``expected_version`` so a concurrent
    update becomes a :class:`SpeciesVersionConflictError` instead of a silent
    overwrite. :meth:`add` records :class:`~plantkeeper.domain.catalog.events.SpeciesAdded`
    for an entry that just entered the catalogue; changing one records
    :class:`~plantkeeper.domain.catalog.events.SpeciesUpdated`.
    """

    def __init__(
        self,
        species_id: SpeciesId,
        *,
        scientific_name: str,
        common_name: str,
        watering_interval: WateringInterval,
        light_requirement: LightRequirement,
        version: int = 1,
    ) -> None:
        """Rebuild a species from its stored state (no events are recorded)."""
        super().__init__(species_id)
        self._scientific_name = _normalize_name(scientific_name, "scientific name")
        self._common_name = _normalize_name(common_name, "common name")
        self._watering_interval = watering_interval
        self._light_requirement = light_requirement
        self._version = version

    @classmethod
    def create(
        cls,
        *,
        scientific_name: str,
        common_name: str,
        watering_interval: WateringInterval,
        light_requirement: LightRequirement,
        species_id: SpeciesId | None = None,
    ) -> Species:
        """Add a species to the local catalogue."""
        return cls(
            species_id or SpeciesId.new(),
            scientific_name=scientific_name,
            common_name=common_name,
            watering_interval=watering_interval,
            light_requirement=light_requirement,
        )

    @classmethod
    def add(
        cls,
        *,
        scientific_name: str,
        common_name: str,
        watering_interval: WateringInterval,
        light_requirement: LightRequirement,
        now: datetime,
        species_id: SpeciesId | None = None,
    ) -> Species:
        """Register a newly synchronised species and record ``SpeciesAdded``.

        Like :meth:`~plantkeeper.domain.garden.plant.Plant.add`, this is the
        factory the write path uses: it records the fact so the read side hears
        about the new catalogue entry. Reconstruction — a row read back, or a test
        that only needs an aggregate — uses :meth:`create` or the constructor,
        neither of which records anything.
        """
        species = cls.create(
            scientific_name=scientific_name,
            common_name=common_name,
            watering_interval=watering_interval,
            light_requirement=light_requirement,
            species_id=species_id,
        )
        species._record(
            SpeciesAdded(
                species_id=species.id,
                scientific_name=species.scientific_name,
                common_name=species.common_name,
                watering_interval=species.watering_interval,
                light_requirement=species.light_requirement,
                version=species.version,
                occurred_at=now,
            )
        )
        return species

    @property
    def scientific_name(self) -> str:
        """The botanical name of the species."""
        return self._scientific_name

    @property
    def common_name(self) -> str:
        """The name the household uses for the species."""
        return self._common_name

    @property
    def watering_interval(self) -> WateringInterval:
        """How often the species should be watered."""
        return self._watering_interval

    @property
    def light_requirement(self) -> LightRequirement:
        """How much light the species needs."""
        return self._light_requirement

    @property
    def version(self) -> int:
        """The optimistic-locking version, incremented by every change."""
        return self._version

    def update(
        self,
        *,
        scientific_name: str,
        common_name: str,
        watering_interval: WateringInterval,
        light_requirement: LightRequirement,
        now: datetime,
        expected_version: int,
    ) -> bool:
        """Apply a Trefle update, returning whether anything changed.

        Returns ``False`` and records nothing when the values are identical, so a
        nightly synchronisation does not emit an event per unchanged species.
        Raises :class:`SpeciesVersionConflictError` when ``expected_version`` is
        stale, and :class:`SpeciesNameEmptyError` when a name is blank.
        """
        if expected_version != self._version:
            raise SpeciesVersionConflictError(
                f"species {self.id} is at version {self._version}, "
                f"the caller expected {expected_version}"
            )
        normalized_scientific = _normalize_name(scientific_name, "scientific name")
        normalized_common = _normalize_name(common_name, "common name")
        changed = (
            normalized_scientific != self._scientific_name
            or normalized_common != self._common_name
            or watering_interval != self._watering_interval
            or light_requirement != self._light_requirement
        )
        if not changed:
            return False
        self._scientific_name = normalized_scientific
        self._common_name = normalized_common
        self._watering_interval = watering_interval
        self._light_requirement = light_requirement
        self._version += 1
        self._record(
            SpeciesUpdated(
                species_id=self.id,
                scientific_name=normalized_scientific,
                common_name=normalized_common,
                watering_interval=watering_interval,
                light_requirement=light_requirement,
                version=self._version,
                occurred_at=now,
            )
        )
        return True


def _normalize_name(raw: str, field: str) -> str:
    normalized = raw.strip()
    if not normalized:
        raise SpeciesNameEmptyError(f"species {field} must not be blank")
    return normalized
