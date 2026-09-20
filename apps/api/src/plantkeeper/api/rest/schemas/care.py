"""HTTP schemas for care schedules."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from plantkeeper.application.views import CareScheduleView, CollectionView


class WateringRequest(BaseModel):
    """An optional body for ``POST /care/{plant_id}/water`` and ``.../skip``.

    Supplying the version the client last saw turns a concurrent change into a
    409 instead of a silent overwrite. Omitting the body is allowed: the schedule
    row is locked for the transaction either way.
    """

    model_config = ConfigDict(extra="forbid")

    expected_version: int | None = Field(default=None, ge=1)


class CareScheduleResponse(BaseModel):
    """One plant's watering schedule."""

    plant_id: UUID
    watering_interval_seconds: int
    next_watering_at: datetime
    version: int

    @classmethod
    def from_view(cls, view: CareScheduleView) -> CareScheduleResponse:
        """Map the view onto the wire format.

        The interval crosses the wire as whole seconds: an ISO-8601 duration
        (``P7D``) is precise but awkward for the clients this API is for.
        """
        return cls(
            plant_id=view.plant_id.value,
            watering_interval_seconds=int(view.watering_interval.total_seconds()),
            next_watering_at=view.next_watering_at,
            version=view.version,
        )


class CareScheduleCollectionResponse(BaseModel):
    """A list of care schedules."""

    items: list[CareScheduleResponse]

    @classmethod
    def from_view(cls, view: CollectionView[CareScheduleView]) -> CareScheduleCollectionResponse:
        """Map each schedule view onto the wire format."""
        return cls(items=[CareScheduleResponse.from_view(item) for item in view.items])
