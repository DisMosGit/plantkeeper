"""Scalar value objects shared by every bounded context.

Every value object is an immutable Pydantic model that validates its own
boundary on construction, so an invalid value cannot exist in the domain.
Boundary violations surface as ``pydantic.ValidationError``; aggregate
invariants raise :class:`~plantkeeper.domain.base.DomainError` subclasses.
"""

from __future__ import annotations

from datetime import timedelta

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator

from plantkeeper.domain.identifiers import SensorId


class Location(BaseModel):
    """A human-readable place inside the household (1-100 characters)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    value: str = Field(min_length=1, max_length=100)

    @field_validator("value", mode="before")
    @classmethod
    def _strip(cls, raw: object) -> object:
        """Strip surrounding whitespace before the length constraints run."""
        return raw.strip() if isinstance(raw, str) else raw


class Moisture(BaseModel):
    """Soil moisture as a percentage (0-100)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    value: float = Field(ge=0, le=100)


class Temperature(BaseModel):
    """Air temperature in degrees Celsius (-50..+60)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    value: float = Field(ge=-50, le=60)


class LightLevel(BaseModel):
    """Illuminance in lux (>= 0)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    value: float = Field(ge=0)


class CareInterval(BaseModel):
    """A strictly positive care cadence.

    The subclasses below carry the meaning of the cadence (watering,
    fertilising, repotting); the base class owns the ``timedelta > 0`` rule.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    value: timedelta = Field(gt=timedelta(0))


class WateringInterval(CareInterval):
    """How often a plant should be watered."""


class FertilizingInterval(CareInterval):
    """How often a plant should be fertilised."""


class RepottingInterval(CareInterval):
    """How often a plant should be repotted."""


class CareRule(BaseModel):
    """The three care cadences that come from a species catalogue entry."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    watering: WateringInterval
    fertilizing: FertilizingInterval
    repotting: RepottingInterval


class SensorReading(BaseModel):
    """One immutable measurement reported by a sensor."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    sensor_id: SensorId
    recorded_at: AwareDatetime
    moisture: Moisture
    temperature: Temperature
    light: LightLevel
