"""Scalar value objects shared by every bounded context.

Every value object is an immutable Pydantic model that validates its own
boundary on construction, so an invalid value cannot exist in the domain.
Boundary violations surface as ``pydantic.ValidationError``; aggregate
invariants raise :class:`~plantkeeper.domain.base.DomainError` subclasses.
"""

from __future__ import annotations

from datetime import timedelta

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_serializer,
    model_validator,
)

from plantkeeper.domain.identifiers import SensorId


class ScalarValue[ValueT](BaseModel):
    """A value object that wraps exactly one scalar.

    The wrappers exist so the domain can tell ``Moisture`` from ``Temperature``
    and so an invalid value cannot be constructed; neither is a concern of the
    JSON an event or an outbox row carries. A scalar value object therefore
    **serialises as the scalar itself** — ``"Shelf"``, not ``{"value": "Shelf"}``
    — which is the shape ``docs/events.md`` promises and the shape
    :class:`~plantkeeper.domain.identifiers.UuidIdentifier` already has, since it
    subclasses ``RootModel``.

    Validation accepts both shapes, so a message written by hand or stored by an
    older build still validates. Subclasses keep the keyword constructor
    (``Location(value="Shelf")``) and the ``.value`` accessor; only
    ``model_dump``/``model_dump_json`` change.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    value: ValueT

    @model_validator(mode="before")
    @classmethod
    def _accept_bare_scalar(cls, raw: object) -> object:
        """Wrap a bare scalar, and pass an instance or a mapping through."""
        if isinstance(raw, (cls, dict)):
            return raw
        return {"value": raw}

    @model_serializer(mode="plain")
    def _serialize(self) -> object:
        """Answer with the wrapped scalar.

        Annotated as ``object`` rather than ``ValueT`` because Pydantic cannot
        resolve a PEP 695 type parameter inside a decorated method's signature.
        The runtime value is the parameter's, so the "any" serializer Pydantic
        picks renders a ``timedelta`` as ``P7D`` and a ``str`` unchanged.
        """
        return self.value


class Location(ScalarValue[str]):
    """A human-readable place inside the household (1-100 characters)."""

    value: str = Field(min_length=1, max_length=100)

    @field_validator("value", mode="before")
    @classmethod
    def _strip(cls, raw: object) -> object:
        """Strip surrounding whitespace before the length constraints run."""
        return raw.strip() if isinstance(raw, str) else raw


class Moisture(ScalarValue[float]):
    """Soil moisture as a percentage (0-100)."""

    value: float = Field(ge=0, le=100)


class Temperature(ScalarValue[float]):
    """Air temperature in degrees Celsius (-50..+60)."""

    value: float = Field(ge=-50, le=60)


class LightLevel(ScalarValue[float]):
    """Illuminance in lux (>= 0)."""

    value: float = Field(ge=0)


class CareInterval(ScalarValue[timedelta]):
    """A strictly positive care cadence.

    The subclasses below carry the meaning of the cadence (watering,
    fertilising, repotting); the base class owns the ``timedelta > 0`` rule.
    """

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
