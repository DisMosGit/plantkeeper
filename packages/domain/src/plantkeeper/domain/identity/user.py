"""The User aggregate: a member of a household (no authentication)."""

from __future__ import annotations

from datetime import datetime

from plantkeeper.domain.base import AggregateRoot
from plantkeeper.domain.identifiers import HouseholdId, UserId
from plantkeeper.domain.identity.errors import UserNameEmptyError


class User(AggregateRoot[UserId]):
    """A household member.

    There is no authentication in this project, so a user is just a name bound to
    exactly one household: the single ``household_id`` field makes "belongs to
    exactly one household" true by construction. Creating a user records no
    event; the Identity catalogue has no cross-context events.
    """

    def __init__(
        self,
        user_id: UserId,
        *,
        household_id: HouseholdId,
        display_name: str,
        added_at: datetime,
    ) -> None:
        """Rebuild a user from its stored state (no events are recorded)."""
        super().__init__(user_id)
        self._household_id = household_id
        normalized_name = display_name.strip()
        if not normalized_name:
            raise UserNameEmptyError("a user display name must not be blank")
        self._display_name = normalized_name
        self._added_at = added_at

    @classmethod
    def create(
        cls,
        *,
        household_id: HouseholdId,
        display_name: str,
        now: datetime,
        user_id: UserId | None = None,
    ) -> User:
        """Add a member to a household."""
        return cls(
            user_id or UserId.new(),
            household_id=household_id,
            display_name=display_name,
            added_at=now,
        )

    @property
    def household_id(self) -> HouseholdId:
        """The household this member belongs to."""
        return self._household_id

    @property
    def display_name(self) -> str:
        """The name the household sees."""
        return self._display_name

    @property
    def added_at(self) -> datetime:
        """When the member joined the household."""
        return self._added_at
