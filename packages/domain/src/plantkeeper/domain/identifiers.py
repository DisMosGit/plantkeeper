"""Typed UUID identifiers shared by every bounded context.

Identifiers are value objects: immutable Pydantic ``RootModel`` wrappers around
:class:`uuid.UUID`. Wrapping the raw UUID keeps ``PlantId`` and ``SensorId``
distinct for ``mypy --strict`` and keeps them serialisable by Pydantic when they
appear inside domain events.
"""

from __future__ import annotations

from typing import Self
from uuid import UUID, uuid7

from pydantic import ConfigDict, RootModel


class UuidIdentifier(RootModel[UUID]):
    """Immutable, typed wrapper around a UUID.

    Subclasses add no fields: their only purpose is to name the identifier so
    that the type checker and the reader can tell ``PlantId`` from ``SensorId``.
    Identifiers default to UUIDv7, which sorts by creation time.
    """

    model_config = ConfigDict(frozen=True)

    @classmethod
    def new(cls) -> Self:
        """Return a new identifier backed by a fresh UUIDv7."""
        return cls(uuid7())

    @property
    def value(self) -> UUID:
        """The wrapped UUID, for the code that needs the raw value."""
        return self.root

    def __str__(self) -> str:
        return str(self.root)


class HouseholdId(UuidIdentifier):
    """Identifier of a household: the family unit that owns the plants."""


class PlantId(UuidIdentifier):
    """Identifier of a plant in the household."""


class SensorId(UuidIdentifier):
    """Identifier of a telemetry sensor bound to one plant."""


class SpeciesId(UuidIdentifier):
    """Identifier of a catalogue species."""


class JournalEntryId(UuidIdentifier):
    """Identifier of one immutable journal entry."""


class NotificationId(UuidIdentifier):
    """Identifier of one notification addressed to a household."""


class UserId(UuidIdentifier):
    """Identifier of a household member (no authentication in this project)."""
