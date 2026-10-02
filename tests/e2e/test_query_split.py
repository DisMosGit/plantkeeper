"""End-to-end test of the query split: a list answered with the write side down.

The point of moving client queries onto the read models is that the write store is
not on their path. This test makes that literal: the API is pointed at a write
instance that refuses connections, the read instance carries rows written by the
real projections, and every list/report question the change moved still answers.

The last two assertions are what keep the first four honest. With the write side
made unreachable, a single-plant read — which the split deliberately keeps on the
write store — must fail, and a command must fail too. Without them, a query that
quietly still read the write tables would pass by accident.
"""

from __future__ import annotations

import socket
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime, timedelta
from urllib.parse import urlparse

import pytest
from httpx import ASGITransport, AsyncClient

from plantkeeper.admin.projections import ALL_PROJECTIONS
from plantkeeper.admin.projections.base import apply_event
from plantkeeper.api.main import create_app
from plantkeeper.domain.care.events import CareScheduleCreated
from plantkeeper.domain.catalog.events import SpeciesAdded
from plantkeeper.domain.catalog.values import LightRequirement
from plantkeeper.domain.garden.events import PlantAdded
from plantkeeper.domain.identifiers import (
    HouseholdId,
    JournalEntryId,
    PlantId,
    SpeciesId,
)
from plantkeeper.domain.journal.events import JournalEntryAdded
from plantkeeper.domain.journal.values import JournalEntryType
from plantkeeper.domain.values import Location, WateringInterval

pytestmark = pytest.mark.slow

NOW = datetime(2026, 3, 1, 9, 0, tzinfo=UTC)
WEEK = timedelta(days=7)
CONSUMER_GROUP_PREFIX = "e2e-query-split"
"""One prefix for this test's projections; the read tables are emptied per test."""

PLANT_ID = PlantId.new()
HOUSEHOLD_ID = HouseholdId.new()
SPECIES_ID = SpeciesId.new()
ENTRY_ID = JournalEntryId.new()


@contextmanager
def an_unreachable_port() -> Iterator[int]:
    """Yield a local TCP port nothing listens on.

    Bound and held for the test's duration but never put into ``listen()``: a
    connection to it is refused immediately rather than timing out, so the write
    side is *unreachable* without the test having to wait for a timeout.
    """
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        yield int(sock.getsockname()[1])


async def project_the_read_side() -> None:
    """Write the read models through the production projections.

    The same ``apply_event`` the projection consumer awaits, driven directly: this
    test is about where a query reads from, not about Kafka, and the read side's
    own end-to-end test already covers the consumer path.
    """
    events = [
        PlantAdded(
            plant_id=PLANT_ID,
            household_id=HOUSEHOLD_ID,
            species_id=SPECIES_ID,
            name="Monstera",
            location=Location(value="Window"),
            added_at=NOW,
        ),
        CareScheduleCreated(
            plant_id=PLANT_ID,
            watering_interval=WateringInterval(value=WEEK),
            next_watering_at=NOW,
        ),
        SpeciesAdded(
            species_id=SPECIES_ID,
            scientific_name="Monstera deliciosa",
            common_name="Swiss cheese plant",
            watering_interval=WateringInterval(value=WEEK),
            light_requirement=LightRequirement.MEDIUM,
            version=1,
        ),
        JournalEntryAdded(
            entry_id=ENTRY_ID,
            plant_id=PLANT_ID,
            entry_type=JournalEntryType.WATERING,
            note="Watered",
            entry_occurred_at=NOW,
        ),
    ]
    for event in events:
        for projection in ALL_PROJECTIONS:
            await apply_event(
                projection, event, consumer_group=f"{CONSUMER_GROUP_PREFIX}-{projection.name}"
            )


@asynccontextmanager
async def api_without_a_write_database(
    monkeypatch: pytest.MonkeyPatch, read_dsn: str
) -> AsyncIterator[AsyncClient]:
    """Run the real API with an unreachable write instance and a live read one."""
    parsed = urlparse(read_dsn)
    assert parsed.username and parsed.password and parsed.hostname and parsed.port
    with an_unreachable_port() as dead_port:
        for name, value in (
            ("POSTGRES_USER", parsed.username),
            ("POSTGRES_PASSWORD", parsed.password),
            ("POSTGRES_DB", parsed.path.lstrip("/")),
            ("POSTGRES_HOST", parsed.hostname),
            # The write instance is a port nothing answers on.
            ("POSTGRES_PORT", str(dead_port)),
            ("READ_POSTGRES_USER", parsed.username),
            ("READ_POSTGRES_PASSWORD", parsed.password),
            ("READ_POSTGRES_DB", parsed.path.lstrip("/")),
            ("READ_POSTGRES_HOST", parsed.hostname),
            ("READ_POSTGRES_PORT", str(parsed.port)),
        ):
            monkeypatch.setenv(name, value)

        app = create_app()
        async with app.router.lifespan_context(app):
            # ``raise_app_exceptions=False``: an unreachable write database makes
            # the requests that need it fail with the 500 Starlette produces, and
            # the control test below asserts exactly that. With the default, the
            # connection error would be re-raised and no status would be visible.
            transport = ASGITransport(app=app, raise_app_exceptions=False)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                yield client


async def test_every_moved_query_is_answered_with_the_write_database_unreachable(
    read_side_database: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    await project_the_read_side()

    async with api_without_a_write_database(monkeypatch, read_side_database) as client:
        plants = await client.get("/api/v1/plants", params={"household_id": str(HOUSEHOLD_ID)})
        today = await client.get("/api/v1/care/today", params={"household_id": str(HOUSEHOLD_ID)})
        species = await client.get("/api/v1/catalog/species")
        journal = await client.get(f"/api/v1/journal/{PLANT_ID}")

    assert plants.status_code == 200, plants.text
    assert [item["name"] for item in plants.json()["items"]] == ["Monstera"]
    assert plants.json()["items"][0]["plant_id"] == str(PLANT_ID)

    assert today.status_code == 200, today.text
    assert [item["plant_id"] for item in today.json()["items"]] == [str(PLANT_ID)]

    assert species.status_code == 200, species.text
    assert [item["common_name"] for item in species.json()["items"]] == ["Swiss cheese plant"]

    assert journal.status_code == 200, journal.text
    assert [item["entry_id"] for item in journal.json()["items"]] == [str(ENTRY_ID)]
    assert journal.json()["items"][0]["entry_type"] == "watering"


async def test_the_write_side_really_is_unreachable(
    read_side_database: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The control for the test above: what stays on the write side still fails.

    Reading one plant and running a command both need the write store — the first
    because it answers the caller's own write, the second because it is one — so
    with that store unreachable they must not answer ``200``. If they did, the
    previous test's success would prove nothing.
    """
    await project_the_read_side()

    async with api_without_a_write_database(monkeypatch, read_side_database) as client:
        one_plant = await client.get(f"/api/v1/plants/{PLANT_ID}")
        command = await client.post("/api/v1/households", json={"name": "Home"})

    assert one_plant.status_code >= 500, one_plant.text
    assert command.status_code >= 500, command.text


async def test_a_list_of_an_unknown_household_is_empty_not_an_error(
    read_side_database: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A read model has no rows to miss: an empty list is the honest answer."""
    await project_the_read_side()

    async with api_without_a_write_database(monkeypatch, read_side_database) as client:
        listed = await client.get("/api/v1/plants", params={"household_id": str(uuid.uuid4())})

    assert listed.status_code == 200, listed.text
    assert listed.json()["items"] == []
