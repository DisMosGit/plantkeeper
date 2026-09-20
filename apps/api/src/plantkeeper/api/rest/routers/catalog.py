"""Catalog endpoints."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, status

from plantkeeper.api.deps import Mediator, view_of
from plantkeeper.api.rest.schemas.catalog import (
    SpeciesCollectionResponse,
    SpeciesResponse,
    SyncRequestedResponse,
)
from plantkeeper.application.commands.catalog import RequestSpeciesSyncCommand
from plantkeeper.application.queries.catalog import GetSpeciesQuery, ListSpeciesQuery
from plantkeeper.application.views import CollectionView, SpeciesView, SyncRequestedView
from plantkeeper.domain.identifiers import SpeciesId

router = APIRouter(prefix="/api/v1/catalog", tags=["catalog"])


@router.get("/species", response_model=SpeciesCollectionResponse, summary="List the catalogue")
async def list_species(mediator: Mediator) -> SpeciesCollectionResponse:
    """Return every species in the local catalogue."""
    view = view_of(await mediator.send(ListSpeciesQuery()), CollectionView[SpeciesView])
    return SpeciesCollectionResponse.from_view(view)


@router.get("/species/{species_id}", response_model=SpeciesResponse, summary="Read a species")
async def get_species(species_id: UUID, mediator: Mediator) -> SpeciesResponse:
    """Return one catalogue entry."""
    query = GetSpeciesQuery(species_id=SpeciesId(species_id))
    return SpeciesResponse.from_view(view_of(await mediator.send(query), SpeciesView))


@router.post(
    "/sync",
    response_model=SyncRequestedResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Request a Trefle synchronisation",
)
async def request_sync(mediator: Mediator) -> SyncRequestedResponse:
    """Publish ``SpeciesSyncRequested`` and answer 202.

    The synchronisation itself belongs to ``SpeciesSyncSaga`` in another process
    (Phase 4, wired to Trefle in Phase 9), so accepting the request is all this
    endpoint can honestly promise.
    """
    view = view_of(await mediator.send(RequestSpeciesSyncCommand()), SyncRequestedView)
    return SyncRequestedResponse.from_view(view)
