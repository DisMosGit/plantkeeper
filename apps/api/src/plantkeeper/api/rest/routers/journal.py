"""Journal endpoints: the whole timeline, and the journal as of a date.

Both answers come from the event store. That is the point of Phase 6: the journal is
append-only, so asking "what did it look like on any date" is a replay rather than a
second table someone has to keep in step.
"""

from __future__ import annotations

from datetime import date
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query

from plantkeeper.api.deps import Mediator
from plantkeeper.api.mediator import view_of
from plantkeeper.api.rest.schemas.journal import (
    JournalCollectionResponse,
    JournalStateResponse,
)
from plantkeeper.application.queries.journal import (
    GetJournalAtDateQuery,
    GetJournalTimelineQuery,
)
from plantkeeper.application.views import CollectionView, JournalEntryView, JournalStateView
from plantkeeper.domain.identifiers import PlantId

router = APIRouter(prefix="/api/v1/journal", tags=["journal"])


@router.get("/{plant_id}", response_model=JournalCollectionResponse, summary="A plant's journal")
async def get_journal(plant_id: UUID, mediator: Mediator) -> JournalCollectionResponse:
    """Return the plant's entries in chronological order.

    Returns 404 when the plant does not exist: an unknown plant and a plant with an
    empty journal are different answers.
    """
    query = GetJournalTimelineQuery(plant_id=PlantId(plant_id))
    view = view_of(await mediator.send(query), CollectionView[JournalEntryView])
    return JournalCollectionResponse.from_view(view)


@router.get(
    "/{plant_id}/at",
    response_model=JournalStateResponse,
    summary="The journal as of a date",
)
async def get_journal_at_date(
    plant_id: UUID,
    mediator: Mediator,
    date_: Annotated[
        date,
        Query(alias="date", description="Inclusive cut-off on the care moment, in UTC."),
    ],
) -> JournalStateResponse:
    """Return the journal for care that happened on or before ``date``.

    The date is inclusive and read as the last moment of that UTC day, so
    ``2026-03-01`` means everything that happened up to and including March 1st.
    """
    query = GetJournalAtDateQuery(plant_id=PlantId(plant_id), date=date_)
    view = view_of(await mediator.send(query), JournalStateView)
    return JournalStateResponse.from_view(view)
