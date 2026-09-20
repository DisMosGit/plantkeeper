"""Telemetry ingress: the raw-topic consumer that turns sensor JSON into events."""

from __future__ import annotations

from plantkeeper.application.telemetry.ingest import (
    RawReading,
    TelemetryIngestConsumer,
    parse_raw_reading,
)

__all__ = ["RawReading", "TelemetryIngestConsumer", "parse_raw_reading"]
