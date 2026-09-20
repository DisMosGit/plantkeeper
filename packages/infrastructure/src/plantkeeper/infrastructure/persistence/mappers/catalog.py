"""Catalog aggregate <-> row mapping."""

from __future__ import annotations

from plantkeeper.domain.catalog.species import Species
from plantkeeper.domain.catalog.values import LightRequirement
from plantkeeper.domain.identifiers import SpeciesId
from plantkeeper.domain.values import WateringInterval
from plantkeeper.infrastructure.persistence.models.catalog import SpeciesModel


def species_to_domain(model: SpeciesModel) -> Species:
    """Rebuild the ``Species`` aggregate from its row."""
    return Species(
        SpeciesId(model.id),
        scientific_name=model.scientific_name,
        common_name=model.common_name,
        watering_interval=WateringInterval(value=model.watering_interval),
        light_requirement=LightRequirement(model.light_requirement),
        version=model.version,
    )


def species_to_model(species: Species) -> SpeciesModel:
    """Build the row that represents ``species``.

    The enum is stored by value: the column stays a plain string even if
    :class:`LightRequirement` gains a level later.
    """
    return SpeciesModel(
        id=species.id.value,
        scientific_name=species.scientific_name,
        common_name=species.common_name,
        watering_interval=species.watering_interval.value,
        light_requirement=species.light_requirement.value,
        version=species.version,
    )
