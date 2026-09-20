"""The read side's AsyncAPI document is a fact about the projections.

The document is generated from the same broker registrations ``make admin`` uses,
so these tests check the half a generated document cannot check for itself: that
every projection reached the schema with a unique channel, that no subscription
silently overwrote another, and that the event catalogue extension is present for
the producers, which are not AsyncAPI routes at all.

Django is configured by ``tests/unit/contracts/conftest.py`` before this module is
imported; importing a projection imports its read model.
"""

from __future__ import annotations

import warnings

from plantkeeper.admin.asyncapi import VERSION, build_document
from plantkeeper.admin.projections import ALL_PROJECTIONS
from plantkeeper.infrastructure.config import Settings

EXPECTED_TOPICS = {topic for projection in ALL_PROJECTIONS for topic in projection.topics}


def build_document_without_warnings(settings: Settings) -> dict[str, object]:
    """Build the document, turning FastStream's channel-collision warning into an error.

    ``get_broker_channels`` warns and overwrites when two subscriptions derive the
    same channel key. That is precisely the failure a document full of untitled
    handlers produces, so the test has to fail on the warning rather than accept
    the truncated result.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        return build_document(settings)


def test_the_document_stamps_the_release_version() -> None:
    """``info.version`` is the contract version, and matches the workspace's."""
    document = build_document_without_warnings(Settings())
    info = document["info"]
    assert isinstance(info, dict)
    assert info["version"] == VERSION


def test_every_projection_topic_has_a_channel() -> None:
    """Each topic a projection consumes appears in the document."""
    document = build_document_without_warnings(Settings())
    channels = document["channels"]
    assert isinstance(channels, dict)
    for topic in EXPECTED_TOPICS:
        assert any(topic in channel for channel in channels), topic


def test_every_projection_has_its_own_channel() -> None:
    """One channel per (projection, topic), not one shared by every handler.

    The set equality is the point: FastStream derives a channel's key from the
    subscription's title, so two projections sharing a handler name without one
    would collapse into a single key and the second would be dropped.
    """
    settings = Settings()
    document = build_document_without_warnings(settings)
    channels = document["channels"]
    assert isinstance(channels, dict)
    expected = {
        f"{topic} to {settings.read_side_consumer_group_prefix}-{projection.name}"
        for projection in ALL_PROJECTIONS
        for topic in projection.topics
    }
    assert set(channels) == expected


def test_every_channel_is_an_operation() -> None:
    """AsyncAPI 3.0 keeps operations beside channels, and both must be complete."""
    document = build_document_without_warnings(Settings())
    channels = document["channels"]
    operations = document["operations"]
    assert isinstance(channels, dict)
    assert isinstance(operations, dict)
    assert set(channels) == set(operations)


def test_the_document_carries_the_event_catalogue() -> None:
    """The producer half — not a FastStream route — travels in the extension."""
    document = build_document_without_warnings(Settings())
    extension = document["x-plantkeeper-event-catalogue"]
    assert isinstance(extension, dict)
    events = extension["events"]
    assert isinstance(events, list)
    assert events
    assert all("topic" in event and "producer" in event for event in events)
