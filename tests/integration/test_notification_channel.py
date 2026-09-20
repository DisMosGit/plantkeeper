"""The Valkey-backed notification channel, against a real Valkey.

The channel is the only piece of Phase 8 that has its own wire to get wrong: a
nudge must reach the subscriber of the household it names, must not reach another
household's, and a wait with no signal must end. A fake client would only test the
fake, so these run against the container the service uses.
"""

from __future__ import annotations

from uuid import UUID

import pytest
from valkey.asyncio import Valkey

from plantkeeper.domain.identifiers import HouseholdId
from plantkeeper.infrastructure.notifications.channel import (
    ValkeyNotificationChannel,
    channel_for,
)

pytestmark = pytest.mark.integration

HOUSEHOLD = HouseholdId(UUID("00000000-0000-0000-0000-0000000000a1"))
OTHER_HOUSEHOLD = HouseholdId(UUID("00000000-0000-0000-0000-0000000000a2"))


def a_channel(valkey_url: str) -> tuple[Valkey, ValkeyNotificationChannel]:
    """A client and the channel over it; the caller closes the client."""
    client = Valkey.from_url(valkey_url, decode_responses=True)
    return client, ValkeyNotificationChannel(client)


def test_the_channel_name_is_the_household_id() -> None:
    assert channel_for(HOUSEHOLD) == f"household:{HOUSEHOLD.value}"


async def test_a_published_nudge_wakes_the_subscriber(valkey_url: str) -> None:
    client, channel = a_channel(valkey_url)
    try:
        async with channel.subscribe(HOUSEHOLD) as subscription:
            await channel.publish(HOUSEHOLD)
            assert await subscription.wait(5.0) is True
    finally:
        await client.aclose()


async def test_a_wait_with_no_signal_ends_at_its_timeout(valkey_url: str) -> None:
    client, channel = a_channel(valkey_url)
    try:
        async with channel.subscribe(HOUSEHOLD) as subscription:
            assert await subscription.wait(0.2) is False
    finally:
        await client.aclose()


async def test_another_household_does_not_wake_the_subscriber(valkey_url: str) -> None:
    """One channel per household is the contract the long poll depends on."""
    client, channel = a_channel(valkey_url)
    try:
        async with channel.subscribe(HOUSEHOLD) as subscription:
            await channel.publish(OTHER_HOUSEHOLD)
            assert await subscription.wait(0.2) is False
    finally:
        await client.aclose()


async def test_a_nudge_published_before_the_wait_is_still_read(valkey_url: str) -> None:
    """A nudge is buffered on the connection, not dropped between calls."""
    client, channel = a_channel(valkey_url)
    try:
        async with channel.subscribe(HOUSEHOLD) as subscription:
            await channel.publish(HOUSEHOLD)
            assert await subscription.wait(5.0) is True
            # The nudge was consumed by the first wait; nothing repeats it.
            assert await subscription.wait(0.2) is False
    finally:
        await client.aclose()
