"""Notifications value objects."""

from __future__ import annotations

from enum import StrEnum


class NotificationType(StrEnum):
    """Why a notification was created."""

    WATERING_DUE = "watering_due"
    CARE_MISSED = "care_missed"
    WATERING_RESCHEDULED = "watering_rescheduled"
    SOIL_MOISTURE_LOW = "soil_moisture_low"
    SOIL_MOISTURE_HIGH = "soil_moisture_high"
    TEMPERATURE_ANOMALY = "temperature_anomaly"
    SENSOR_OFFLINE = "sensor_offline"
    PLANT_ONBOARDED = "plant_onboarded"
