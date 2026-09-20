"""The write side's AsyncAPI document is a fact about the worker's subscriptions.

``python -m plantkeeper.workers`` registers every route built from the saga and
consumer registries; this module builds the same broker and checks that the
document FastStream derives from it says what the registries say. The failure this
guards against is a quiet one: FastStream keys a channel by the subscription's
title and otherwise falls back to the handler's function name, so an untitled
subscriber collapses into another one's channel instead of erroring.

The event catalogue is the read side's, handed in exactly as ``make contracts``
hands it in: this process may not import Django, so it cannot build the
platform-wide view itself.
"""

from __future__ import annotations

import json
import warnings
from pathlib import Path

from plantkeeper.admin.asyncapi import build_document as build_admin_document
from plantkeeper.application.sagas.registry import WORKER_CONSUMER_TYPES
from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.messaging.topics import EVENT_TOPICS
from plantkeeper.workers.asyncapi import CATALOGUE_KEY, VERSION, build_document, read_catalogue
from plantkeeper.workers.consumers import topics_for


def platform_catalogue(settings: Settings) -> dict[str, object]:
    """Return the catalogue the admin generator produces, as the tool passes it."""
    document = build_admin_document(settings)
    catalogue = document[CATALOGUE_KEY]
    assert isinstance(catalogue, dict)
    return catalogue


def build_document_without_warnings(settings: Settings) -> dict[str, object]:
    """Build the document, turning FastStream's channel-collision warning into an error."""
    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        return build_document(settings, catalogue=platform_catalogue(settings))


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
    extension = document[CATALOGUE_KEY]
    assert isinstance(extension, dict)
    events = extension["events"]
    assert isinstance(events, list)
    assert {event["event_name"] for event in events} == {e.__name__ for e in EVENT_TOPICS}


def test_the_catalogue_the_worker_embeds_names_the_read_side_too() -> None:
    """The worker's document must not report a read-side-only event as unconsumed.

    ``JournalEntryAdded`` is consumed by Django Admin's journal projection and by
    nothing on the write side; a write-side-only catalogue would call it
    unconsumed, which is why ``make contracts`` generates the catalogue in the
    admin process and hands it over.
    """
    document = build_document_without_warnings(Settings())
    extension = document[CATALOGUE_KEY]
    assert isinstance(extension, dict)
    events = {event["event_name"]: event for event in extension["events"]}
    consumers = events["JournalEntryAdded"]["consumers"]
    assert any(consumer["consumer_group"] == "journal" for consumer in consumers)


def test_a_document_built_without_a_catalogue_still_has_one() -> None:
    """The fallback is the write-side view, which is narrower but never empty."""
    document = build_document(Settings())
    extension = document[CATALOGUE_KEY]
    assert isinstance(extension, dict)
    events = extension["events"]
    assert isinstance(events, list)
    assert {event["event_name"] for event in events} == {e.__name__ for e in EVENT_TOPICS}


def test_the_catalogue_can_be_handed_over_as_a_file(tmp_path: Path) -> None:
    """``make contracts`` passes it on disk; the reader must accept that shape.

    The tool cannot import this module's caller either, so the hand-over is a file,
    and the shape of that file — an AsyncAPI document whose extension is the
    catalogue — is part of the contract between the two generators.
    """
    admin_path = tmp_path / "asyncapi-read.json"
    admin_path.write_text(
        json.dumps(build_admin_document(Settings())),
        encoding="utf-8",
    )

    catalogue = read_catalogue(str(admin_path))
    assert catalogue is not None
    events = catalogue["events"]
    assert isinstance(events, list)
    assert {event["event_name"] for event in events} == {e.__name__ for e in EVENT_TOPICS}
