"""The read side's projections, one per topic that feeds a read model.

Every projection is a class with a name (its consumer-group suffix), the topics
it consumes, and the events it handles. ``ALL_PROJECTIONS`` is the single list
the subscriber walks, and the contract test in
``tests/integration/test_projections.py`` asserts it covers the catalogue.
"""

from __future__ import annotations

from typing import Final

from plantkeeper.admin.projections.base import Projection, apply_event
from plantkeeper.admin.projections.care import CareProjection
from plantkeeper.admin.projections.catalog import SpeciesProjection
from plantkeeper.admin.projections.garden import GardenProjection
from plantkeeper.admin.projections.journal import JournalProjection
from plantkeeper.admin.projections.notifications import NotificationProjection

ALL_PROJECTIONS: Final[tuple[Projection, ...]] = (
    GardenProjection(),
    CareProjection(),
    SpeciesProjection(),
    NotificationProjection(),
    JournalProjection(),
)
"""Every projection, in the order they are subscribed."""

__all__ = ["ALL_PROJECTIONS", "Projection", "apply_event"]
