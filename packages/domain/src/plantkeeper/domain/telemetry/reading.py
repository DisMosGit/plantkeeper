"""The persisted telemetry fact.

:class:`~plantkeeper.domain.values.SensorReading` is what a sensor reports — it
knows the sensor but not the plant behind it. This is the row the telemetry
ingress writes once the sensor-to-plant mapping has been resolved: the same
measurement plus the plant it belongs to, which is what every reader of the
telemetry table actually wants.

It is a value object rather than an aggregate because readings are append-only
facts: there is no invariant that a sequence of them has to satisfy, and the only
rule they carry — one reading per sensor per instant — is the table's own key.
"""

from __future__ import annotations

from pydantic import AwareDatetime, BaseModel, ConfigDict

from plantkeeper.domain.identifiers import PlantId, SensorId
from plantkeeper.domain.values import LightLevel, Moisture, Temperature


class TelemetryReading(BaseModel):
    """One measurement, attributed to the plant its sensor watches."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    sensor_id: SensorId
    plant_id: PlantId
    recorded_at: AwareDatetime
    moisture: Moisture
    temperature: Temperature
    light: LightLevel
