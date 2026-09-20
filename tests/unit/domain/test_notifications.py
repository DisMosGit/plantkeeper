"""Tests for the Notification aggregate."""

from __future__ import annotations

from datetime import datetime
from uuid import uuid4

import pytest
from pydantic import JsonValue

from plantkeeper.domain.identifiers import HouseholdId, NotificationId
from plantkeeper.domain.notifications.errors import NotificationAlreadyReadError
from plantkeeper.domain.notifications.events import NotificationCreated, NotificationRead
from plantkeeper.domain.notifications.notification import Notification
from plantkeeper.domain.notifications.values import NotificationType

PAYLOAD: dict[str, JsonValue] = {"plant_id": "8f14e45f", "moisture": 12.5, "urgent": True}


def _create(now: datetime, *, payload: dict[str, JsonValue] | None = None) -> Notification:
    return Notification.create(
        household_id=HouseholdId(uuid4()),
        notification_type=NotificationType.SOIL_MOISTURE_LOW,
        now=now,
        payload=payload,
    )


def test_create_records_notification_created(now: datetime) -> None:
    notification = _create(now, payload=PAYLOAD)

    assert notification.notification_type is NotificationType.SOIL_MOISTURE_LOW
    assert notification.created_at == now
    assert notification.read_at is None
    assert notification.is_read is False
    assert notification.payload == PAYLOAD

    events = notification.collect_events()
    assert len(events) == 1
    created = events[0]
    assert isinstance(created, NotificationCreated)
    assert created.notification_id == notification.id
    assert created.household_id == notification.household_id
    assert created.notification_type is NotificationType.SOIL_MOISTURE_LOW
    assert created.payload == PAYLOAD
    assert created.created_at == now
    assert created.occurred_at == now


def test_create_defaults_the_payload_to_an_empty_mapping(now: datetime) -> None:
    notification = _create(now)

    assert notification.payload == {}
    created = notification.collect_events()[0]
    assert isinstance(created, NotificationCreated)
    assert created.payload == {}


def test_create_copies_the_payload(now: datetime) -> None:
    payload = dict(PAYLOAD)

    notification = _create(now, payload=payload)
    payload["urgent"] = False

    assert notification.payload["urgent"] is True


def test_the_payload_property_is_a_copy(now: datetime) -> None:
    notification = _create(now, payload=PAYLOAD)

    exposed = notification.payload
    exposed["urgent"] = False

    assert notification.payload["urgent"] is True


def test_mark_read_records_notification_read(now: datetime) -> None:
    notification = _create(now)
    notification.collect_events()

    notification.mark_read(now=now)

    assert notification.read_at == now
    assert notification.is_read is True
    events = notification.collect_events()
    assert len(events) == 1
    read = events[0]
    assert isinstance(read, NotificationRead)
    assert read.notification_id == notification.id
    assert read.household_id == notification.household_id
    assert read.read_at == now
    assert read.occurred_at == now


def test_marking_read_twice_is_rejected(now: datetime) -> None:
    notification = _create(now)
    notification.mark_read(now=now)

    with pytest.raises(NotificationAlreadyReadError):
        notification.mark_read(now=now)

    assert notification.read_at == now


def test_create_accepts_an_explicit_identifier(now: datetime) -> None:
    notification_id = NotificationId(uuid4())

    notification = Notification.create(
        household_id=HouseholdId(uuid4()),
        notification_type=NotificationType.WATERING_DUE,
        now=now,
        notification_id=notification_id,
    )

    assert notification.id == notification_id


def test_rebuilding_a_notification_records_no_events(now: datetime) -> None:
    notification = Notification(
        NotificationId(uuid4()),
        household_id=HouseholdId(uuid4()),
        notification_type=NotificationType.PLANT_ONBOARDED,
        created_at=now,
        read_at=now,
    )

    assert notification.is_read is True
    assert notification.collect_events() == []


def test_notification_type_exposes_every_alert_kind() -> None:
    assert {kind.value for kind in NotificationType} == {
        "watering_due",
        "care_missed",
        "watering_rescheduled",
        "soil_moisture_low",
        "soil_moisture_high",
        "temperature_anomaly",
        "sensor_offline",
        "plant_onboarded",
    }
