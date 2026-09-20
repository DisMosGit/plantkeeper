"""``SpeciesSyncSaga``: pull the catalogue, diff it, publish what changed.

Orchestration again, because the sync has a sequence with a real rollback: fetch
an upstream snapshot, apply the differences to the local catalogue, and declare
the changed entries stale in the cache. The three steps are separate so the third
can fail *after* the second committed — which is exactly the failure the
compensation exists for.

Compensation restores each changed species to the snapshot taken before its
update and deletes each species the run created. The restore goes through
``Species.update`` like any other change, so it records a ``SpeciesUpdated`` of
its own: a rollback is a catalogue change that consumers (and the read model)
must see, not a silent rewrite.

Creation arrived with the Trefle adapter (Phase 9): an upstream species the local
table does not know is inserted with ``Species.add``, which records
``SpeciesAdded``. Upstream *removal* stays out of scope — Trefle has no "species
gone" signal, so local entries are never deleted by a synchronisation.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import ClassVar
from uuid import UUID

from cqrs.dispatcher.saga import SagaDispatcher
from cqrs.saga.models import SagaContext
from cqrs.saga.step import SagaStepHandler, SagaStepResult

from plantkeeper.application.errors import UnhandledSagaTriggerError
from plantkeeper.application.ports.catalog import SpeciesCacheInvalidator, SpeciesSource
from plantkeeper.application.ports.clock import Clock
from plantkeeper.application.ports.unit_of_work import UnitOfWork
from plantkeeper.application.sagas.base import Saga
from plantkeeper.application.sagas.consumer import Consumer
from plantkeeper.application.sagas.contexts import SpeciesSnapshot, SpeciesSyncContext
from plantkeeper.domain.base import DomainEvent
from plantkeeper.domain.catalog.events import SpeciesSyncRequested
from plantkeeper.domain.catalog.species import Species
from plantkeeper.domain.catalog.values import LightRequirement
from plantkeeper.domain.identifiers import SpeciesId
from plantkeeper.domain.values import WateringInterval

logger = logging.getLogger(__name__)


def _snapshot(species: Species) -> SpeciesSnapshot:
    """Freeze one species' current values, for the compensation to restore."""
    return SpeciesSnapshot(
        species_id=str(species.id),
        scientific_name=species.scientific_name,
        common_name=species.common_name,
        watering_interval_seconds=species.watering_interval.value.total_seconds(),
        light_requirement=species.light_requirement.value,
        version=species.version,
    )


class FetchSpeciesStep(SagaStepHandler[SpeciesSyncContext, None]):
    """Step 1: read the upstream catalogue into the context."""

    def __init__(self, species_source: SpeciesSource) -> None:
        self._species_source = species_source

    async def act(self, context: SpeciesSyncContext) -> SagaStepResult[SpeciesSyncContext, None]:
        """Snapshot every upstream entry."""
        records = await self._species_source.fetch_all()
        context.fetched = [
            SpeciesSnapshot(
                species_id=str(record.species_id),
                scientific_name=record.scientific_name,
                common_name=record.common_name,
                watering_interval_seconds=record.watering_interval.value.total_seconds(),
                light_requirement=record.light_requirement.value,
                version=0,
            )
            for record in records
        ]
        logger.info("catalogue synchronisation fetched %d species", len(context.fetched))
        return self._generate_step_result(None)

    async def compensate(self, context: SpeciesSyncContext) -> None:
        """Nothing was written; nothing to undo."""


class ApplySpeciesUpdatesStep(SagaStepHandler[SpeciesSyncContext, None]):
    """Step 2: write the differences, remembering each before-image.

    A species the local catalogue does not know is *created* here and its
    identifier remembered, so the compensation can remove it again: a species that
    entered the catalogue as part of a run that then failed must not survive the
    rollback. Trefle cannot delete a species, so a catalogue entry that vanishes
    upstream is simply left alone.
    """

    def __init__(self, unit_of_work: UnitOfWork, clock: Clock) -> None:
        self._unit_of_work = unit_of_work
        self._clock = clock

    async def act(self, context: SpeciesSyncContext) -> SagaStepResult[SpeciesSyncContext, None]:
        """Create unknown species, update changed ones, and commit them together."""
        now = self._clock.now()
        for record in context.fetched:
            species = await self._unit_of_work.species.get_for_update(
                SpeciesId(UUID(record.species_id))
            )
            if species is None:
                context.created.append(str(await self._create(record, now=now)))
                continue
            before = _snapshot(species)
            changed = species.update(
                scientific_name=record.scientific_name,
                common_name=record.common_name,
                watering_interval=WateringInterval(
                    value=timedelta(seconds=record.watering_interval_seconds)
                ),
                light_requirement=LightRequirement(record.light_requirement),
                now=now,
                expected_version=species.version,
            )
            if not changed:
                continue
            await self._unit_of_work.species.save(species)
            context.updated.append(before)
        await self._unit_of_work.commit()
        logger.info(
            "catalogue synchronisation created %d and updated %d species",
            len(context.created),
            len(context.updated),
        )
        return self._generate_step_result(None)

    async def _create(self, record: SpeciesSnapshot, *, now: datetime) -> SpeciesId:
        """Insert one upstream species and return the identifier it was given."""
        species_id = SpeciesId(UUID(record.species_id))
        species = Species.add(
            species_id=species_id,
            scientific_name=record.scientific_name,
            common_name=record.common_name,
            watering_interval=WateringInterval(
                value=timedelta(seconds=record.watering_interval_seconds)
            ),
            light_requirement=LightRequirement(record.light_requirement),
            now=now,
        )
        await self._unit_of_work.species.add(species)
        return species_id

    async def compensate(self, context: SpeciesSyncContext) -> None:
        """Delete what this step created and restore every species it changed."""
        now = self._clock.now()
        logger.warning(
            "rolling back %d catalogue creations and %d updates",
            len(context.created),
            len(context.updated),
        )
        for species_id in context.created:
            await self._unit_of_work.species.delete(SpeciesId(UUID(species_id)))
        for before in context.updated:
            species = await self._unit_of_work.species.get_for_update(
                SpeciesId(UUID(before.species_id))
            )
            if species is None:
                continue
            species.update(
                scientific_name=before.scientific_name,
                common_name=before.common_name,
                watering_interval=WateringInterval(
                    value=timedelta(seconds=before.watering_interval_seconds)
                ),
                light_requirement=LightRequirement(before.light_requirement),
                now=now,
                expected_version=species.version,
            )
            await self._unit_of_work.species.save(species)
        await self._unit_of_work.commit()
        context.updated = []
        context.created = []


class InvalidateSpeciesCacheStep(SagaStepHandler[SpeciesSyncContext, None]):
    """Step 3: declare the changed catalogue entries stale."""

    def __init__(self, unit_of_work: UnitOfWork, species_cache: SpeciesCacheInvalidator) -> None:
        self._unit_of_work = unit_of_work
        self._species_cache = species_cache

    async def act(self, context: SpeciesSyncContext) -> SagaStepResult[SpeciesSyncContext, None]:
        """Announce staleness for every species step 2 changed."""
        await self._species_cache.invalidate(
            [SpeciesId(UUID(snapshot.species_id)) for snapshot in context.updated]
        )
        await self._unit_of_work.commit()
        return self._generate_step_result(None)

    async def compensate(self, context: SpeciesSyncContext) -> None:
        """Nothing local was written; the cache is rebuilt by the next sync."""


class SpeciesSyncSaga(Saga):
    """The catalogue synchronisation process manager."""

    trigger_events = (SpeciesSyncRequested,)
    context_type = SpeciesSyncContext
    steps: ClassVar[list[type[SagaStepHandler]]] = [
        FetchSpeciesStep,
        ApplySpeciesUpdatesStep,
        InvalidateSpeciesCacheStep,
    ]

    def context_from_event(self, event: DomainEvent) -> SpeciesSyncContext:
        """Anchor the run on the trigger event, so each request is its own saga."""
        if not isinstance(event, SpeciesSyncRequested):
            raise UnhandledSagaTriggerError(
                f"SpeciesSyncSaga cannot start from {type(event).__name__}"
            )
        return SpeciesSyncContext(
            requested_at=event.occurred_at.isoformat(),
            requested_by=str(event.event_id),
        )

    def correlation_id(self, context: SagaContext) -> str:
        """One saga per request; two requests are two synchronisations."""
        return self.require_context(context, SpeciesSyncContext).requested_by


class SpeciesSyncTrigger(Consumer):
    """The consumer group that starts a synchronisation, and the daily tick.

    ``request_sync`` is the other half of the trigger: the daily scheduler calls
    it, and the manual ``POST /api/v1/catalog/sync`` already appends the event
    through the outbox. Both paths therefore produce the same event and the same
    saga, which is what keeps "cron" and "manual" from being two implementations.
    """

    name = "species-sync"
    handled_types: ClassVar[tuple[type[DomainEvent], ...]] = (SpeciesSyncRequested,)

    def __init__(self, unit_of_work: UnitOfWork, saga: SpeciesSyncSaga) -> None:
        super().__init__(unit_of_work)
        self._saga = saga

    async def handle(self, event: DomainEvent, dispatcher: SagaDispatcher) -> None:
        """Dispatch the synchronisation saga for the request."""
        await self._saga.handle_event(event, dispatcher=dispatcher, unit_of_work=self.unit_of_work)

    async def request_sync(self, *, now: datetime) -> None:
        """Publish the trigger event, exactly as the REST endpoint does."""
        async with self.unit_of_work:
            await self.unit_of_work.outbox.append(SpeciesSyncRequested(occurred_at=now))
            await self.unit_of_work.commit()
