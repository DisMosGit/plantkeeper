"""HTTP schemas for sensors."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from plantkeeper.application.views import CollectionView, SensorView


class SensorCreate(BaseModel):
    """The body of ``POST /api/v1/sensors``."""

    model_config = ConfigDict(extra="forbid")

    plant_id: UUID


class SensorResponse(BaseModel):
    """One sensor and the plant it watches."""

    sensor_id: UUID
    plant_id: UUID
    added_at: datetime
    last_seen_at: datetime | None

    @classmethod
    def from_view(cls, view: SensorView) -> SensorResponse:
        """Map the view onto the wire format."""
        return cls(
            sensor_id=view.sensor_id.value,
            plant_id=view.plant_id.value,
            added_at=view.added_at,
            last_seen_at=view.last_seen_at,
        )


class SensorCollectionResponse(BaseModel):
    """A list of sensors."""

    items: list[SensorResponse]

    @classmethod
    def from_view(cls, view: CollectionView[SensorView]) -> SensorCollectionResponse:
        """Map each sensor view onto the wire format."""
        return cls(items=[SensorResponse.from_view(item) for item in view.items])
