"""Invariant tests for the CareSchedule aggregate."""

from __future__ import annotations

from datetime import datetime, timedelta
from uuid import uuid4

import pytest

from plantkeeper.domain.care.errors import (
    CareScheduleInvariantError,
    CareScheduleVersionConflictError,
    WateringNotDueError,
)
from plantkeeper.domain.care.events import (
    CareMissed,
    CareScheduleCreated,
    CareSkipped,
    WateringCompleted,
    WateringDue,
    WateringRescheduled,
)
from plantkeeper.domain.care.schedule import CareSchedule
from plantkeeper.domain.identifiers import PlantId
from plantkeeper.domain.values import WateringInterval

WEEK = timedelta(days=7)
INTERVAL = WateringInterval(value=WEEK)


def _create(now: datetime, *, starts_at: datetime | None = None) -> CareSchedule:
    return CareSchedule.create(
        plant_id=PlantId(uuid4()),
        watering_interval=INTERVAL,
        starts_at=now if starts_at is None else starts_at,
        now=now,
    )


def test_create_records_care_schedule_created(now: datetime) -> None:
    schedule = _create(now)

    assert schedule.version == 1
    assert schedule.watering_interval == INTERVAL
    assert schedule.next_watering_at == now
    assert schedule.plant_id == schedule.id

    events = schedule.collect_events()
    assert len(events) == 1
    created = events[0]
    assert isinstance(created, CareScheduleCreated)
    assert created.plant_id == schedule.id
    assert created.watering_interval == INTERVAL
    assert created.next_watering_at == now
    assert created.occurred_at == now


def test_create_accepts_a_future_start(now: datetime) -> None:
    starts_at = now + WEEK

    assert _create(now, starts_at=starts_at).next_watering_at == starts_at


def test_create_rejects_a_start_in_the_past(now: datetime) -> None:
    with pytest.raises(CareScheduleInvariantError):
        _create(now, starts_at=now - timedelta(seconds=1))


def test_rebuilding_a_schedule_records_no_events(now: datetime) -> None:
    schedule = CareSchedule(
        PlantId(uuid4()),
        watering_interval=INTERVAL,
        next_watering_at=now,
        version=3,
    )

    assert schedule.collect_events() == []
    assert schedule.version == 3


def test_mark_due_at_the_boundary_records_watering_due(now: datetime) -> None:
    schedule = _create(now)
    schedule.collect_events()

    schedule.mark_due(now=now)

    events = schedule.collect_events()
    assert len(events) == 1
    due = events[0]
    assert isinstance(due, WateringDue)
    assert due.plant_id == schedule.id
    assert due.due_at == now
    assert due.occurred_at == now
    assert schedule.version == 1


def test_mark_due_after_the_boundary_records_watering_due(now: datetime) -> None:
    schedule = _create(now)
    schedule.collect_events()

    schedule.mark_due(now=now + timedelta(minutes=5))

    assert isinstance(schedule.collect_events()[0], WateringDue)


def test_mark_due_before_the_boundary_is_rejected(now: datetime) -> None:
    schedule = _create(now, starts_at=now + WEEK)
    schedule.collect_events()

    with pytest.raises(WateringNotDueError):
        schedule.mark_due(now=now)

    assert schedule.collect_events() == []


def test_complete_watering_schedules_the_next_interval(now: datetime) -> None:
    schedule = _create(now)
    schedule.collect_events()

    schedule.complete_watering(now=now, expected_version=1)

    assert schedule.version == 2
    assert schedule.next_watering_at == now + WEEK
    events = schedule.collect_events()
    assert len(events) == 1
    completed = events[0]
    assert isinstance(completed, WateringCompleted)
    assert completed.plant_id == schedule.id
    assert completed.completed_at == now
    assert completed.next_watering_at == now + WEEK
    assert completed.occurred_at == now


def test_reschedule_records_the_previous_moment(now: datetime) -> None:
    schedule = _create(now)
    schedule.collect_events()
    moved_to = now + timedelta(days=2)

    schedule.reschedule(
        next_watering_at=moved_to,
        now=now,
        expected_version=1,
        reason="soil moisture low",
    )

    assert schedule.version == 2
    assert schedule.next_watering_at == moved_to
    events = schedule.collect_events()
    assert len(events) == 1
    rescheduled = events[0]
    assert isinstance(rescheduled, WateringRescheduled)
    assert rescheduled.previous_next_watering_at == now
    assert rescheduled.next_watering_at == moved_to
    assert rescheduled.reason == "soil moisture low"
    assert rescheduled.occurred_at == now


def test_reschedule_defaults_the_reason_to_none(now: datetime) -> None:
    schedule = _create(now)
    schedule.collect_events()

    schedule.reschedule(next_watering_at=now + WEEK, now=now, expected_version=1)

    rescheduled = schedule.collect_events()[0]
    assert isinstance(rescheduled, WateringRescheduled)
    assert rescheduled.reason is None


def test_reschedule_rejects_a_moment_in_the_past(now: datetime) -> None:
    schedule = _create(now)
    schedule.collect_events()

    with pytest.raises(CareScheduleInvariantError):
        schedule.reschedule(
            next_watering_at=now - timedelta(seconds=1),
            now=now,
            expected_version=1,
        )

    assert schedule.version == 1
    assert schedule.next_watering_at == now
    assert schedule.collect_events() == []


def test_skip_moves_to_the_following_interval(now: datetime) -> None:
    schedule = _create(now)
    schedule.collect_events()

    schedule.skip(now=now, expected_version=1)

    assert schedule.version == 2
    assert schedule.next_watering_at == now + WEEK
    events = schedule.collect_events()
    assert len(events) == 1
    skipped = events[0]
    assert isinstance(skipped, CareSkipped)
    assert skipped.skipped_at == now
    assert skipped.next_watering_at == now + WEEK


def test_mark_missed_moves_to_the_following_interval(now: datetime) -> None:
    schedule = _create(now)
    schedule.collect_events()

    schedule.mark_missed(now=now, expected_version=1)

    assert schedule.version == 2
    assert schedule.next_watering_at == now + WEEK
    events = schedule.collect_events()
    assert len(events) == 1
    missed = events[0]
    assert isinstance(missed, CareMissed)
    assert missed.next_watering_at == now + WEEK
    assert missed.occurred_at == now


def test_a_stale_version_is_rejected_by_every_mutator(now: datetime) -> None:
    schedule = _create(now)
    schedule.collect_events()
    stale_version = schedule.version + 1

    with pytest.raises(CareScheduleVersionConflictError):
        schedule.complete_watering(now=now, expected_version=stale_version)
    with pytest.raises(CareScheduleVersionConflictError):
        schedule.reschedule(
            next_watering_at=now + WEEK,
            now=now,
            expected_version=stale_version,
        )
    with pytest.raises(CareScheduleVersionConflictError):
        schedule.skip(now=now, expected_version=stale_version)
    with pytest.raises(CareScheduleVersionConflictError):
        schedule.mark_missed(now=now, expected_version=stale_version)

    assert schedule.version == 1
    assert schedule.next_watering_at == now
    assert schedule.collect_events() == []
