"""Saga contexts: the state an orchestration saga carries between its steps.

A context is a ``cqrs.saga.models.SagaContext`` dataclass because the saga engine
serialises it into ``write_shared.saga_state`` and rebuilds it on recovery. Its
fields are therefore deliberately JSON scalars — identifiers and timestamps as
strings, durations as seconds — rather than domain value objects: what crosses the
persistence boundary is framework state, not a domain payload, so it does not get
Pydantic or the project's scalar serialisers. The steps convert on the way in and
out.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from cqrs.saga.models import SagaContext


@dataclass
class OnboardPlantContext(SagaContext):
    """What the onboarding saga knows after each of its four steps.

    ``PlantAdded`` already carries the three identifiers, so the context starts
    with them and accumulates what each step produced: the species' cadence
    (step 1), the schedule (step 2) and the first notification (step 3). A
    compensation reads the flags to know what still exists.
    """

    plant_id: str
    household_id: str
    species_id: str
    watering_interval_seconds: float = 0.0
    care_schedule_created: bool = False
    notification_id: str | None = None
    next_watering_at: str | None = None


@dataclass
class SpeciesSnapshot:
    """One catalogue entry, as the synchronisation saga saw it.

    A snapshot rather than a domain object because it is stored twice: as the
    fetched state (step 1) and as the before-image a compensation restores
    (step 2). Keeping it a plain dataclass also lets ``dataclass_wizard``
    round-trip a list of them through the context's JSON column.
    """

    species_id: str
    scientific_name: str
    common_name: str
    watering_interval_seconds: float
    light_requirement: str
    version: int


@dataclass
class SpeciesSyncContext(SagaContext):
    """The catalogue synchronisation's state: what came in, and what changed."""

    requested_at: str
    requested_by: str
    fetched: list[SpeciesSnapshot] = field(default_factory=list)
    updated: list[SpeciesSnapshot] = field(default_factory=list)
    created: list[str] = field(default_factory=list)
