"""The write side's AsyncAPI document is a fact about the worker's subscriptions.

``python -m plantkeeper.workers`` registers every route built from the saga and
consumer registries; this module builds the same broker and checks that the
document FastStream derives from it says what the registries say. The failure this
guards against is a quiet one: FastStream keys a channel by the subscription's
title and otherwise falls back to the handler's function name, so an untitled
subscriber collapses into another one's channel instead of erroring.
"""

from __future__ import annotations

import warnings

from plantkeeper.application.sagas.registry import WORKER_CONSUMER_TYPES
from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.messaging.topics import EVENT_TOPICS
from plantkeeper.workers.asyncapi import VERSION, build_document
from plantkeeper.workers.consumers import topics_for


def build_document_without_warnings(settings: Settings) -> dict[str, object]:
    """Build the document, turning FastStream's channel-collision warning into an error."""
    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        return build_document(settings)


def expected_subscriptions(settings: Settings) -> set[str]:
    """Return the ``(topic, group)`` pairs the worker actually registers."""
    subscriptions: set[str] = set()
    for consumer_type in WORKER_CONSUMER_TYPES:
        group = f"{settings.worker_consumer_group_prefix}-{consumer_type.name}"
        for topic in topics_for(consumer_type.handled_types):
            subscriptions.add(f"{topic} to {group}")
    subscriptions.add(
        f"{settings.telemetry_raw_topic} to {settings.telemetry_ingest_consumer_group}"
    )
    return subscriptions


def test_the_document_stamps_the_release_version() -> None:
    """``info.version`` is the contract version, and matches the workspace's."""
    document = build_document_without_warnings(Settings())
    info = document["info"]
    assert isinstance(info, dict)
    assert info["version"] == VERSION


def test_every_registered_subscription_has_a_channel() -> None:
    """Each consumer group's subscription appears, under its own title."""
    settings = Settings()
    document = build_document_without_warnings(settings)
    channels = document["channels"]
    assert isinstance(channels, dict)
    assert set(channels) == expected_subscriptions(settings)


def test_the_telemetry_ingress_is_in_the_document() -> None:
    """``telemetry.raw`` is not a domain-event topic, and is easy to lose."""
    settings = Settings()
    document = build_document_without_warnings(settings)
    channels = document["channels"]
    assert isinstance(channels, dict)
    assert settings.telemetry_raw_topic in " ".join(channels)


def test_every_channel_is_an_operation() -> None:
    """AsyncAPI 3.0 keeps operations beside channels, and both must be complete."""
    document = build_document_without_warnings(Settings())
    channels = document["channels"]
    operations = document["operations"]
    assert isinstance(channels, dict)
    assert isinstance(operations, dict)
    assert set(channels) == set(operations)


def test_the_document_carries_the_event_catalogue() -> None:
    """The producer half — the outbox relay — is not a FastStream route."""
    document = build_document_without_warnings(Settings())
    extension = document["x-plantkeeper-event-catalogue"]
    assert isinstance(extension, dict)
    events = extension["events"]
    assert isinstance(events, list)
    assert {event["event_name"] for event in events} == {e.__name__ for e in EVENT_TOPICS}
