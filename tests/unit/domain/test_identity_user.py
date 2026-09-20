"""Tests for the User aggregate."""

from __future__ import annotations

from datetime import datetime
from uuid import uuid4

import pytest

from plantkeeper.domain.identifiers import HouseholdId, UserId
from plantkeeper.domain.identity.errors import UserNameEmptyError
from plantkeeper.domain.identity.user import User


def test_create_binds_the_member_to_one_household(now: datetime) -> None:
    household_id = HouseholdId(uuid4())

    user = User.create(household_id=household_id, display_name="Ada", now=now)

    assert user.household_id == household_id
    assert user.display_name == "Ada"
    assert user.added_at == now
    assert user.collect_events() == []


def test_create_strips_the_display_name(now: datetime) -> None:
    user = User.create(
        household_id=HouseholdId(uuid4()),
        display_name="  Ada  ",
        now=now,
    )

    assert user.display_name == "Ada"


@pytest.mark.parametrize("display_name", ["", "   "])
def test_create_rejects_a_blank_display_name(now: datetime, display_name: str) -> None:
    with pytest.raises(UserNameEmptyError):
        User.create(
            household_id=HouseholdId(uuid4()),
            display_name=display_name,
            now=now,
        )


def test_create_accepts_an_explicit_identifier(now: datetime) -> None:
    user_id = UserId(uuid4())

    user = User.create(
        household_id=HouseholdId(uuid4()),
        display_name="Ada",
        now=now,
        user_id=user_id,
    )

    assert user.id == user_id


def test_rebuilding_a_user_records_no_events(now: datetime) -> None:
    user = User(
        UserId(uuid4()),
        household_id=HouseholdId(uuid4()),
        display_name="  Ada  ",
        added_at=now,
    )

    assert user.display_name == "Ada"
    assert user.collect_events() == []
