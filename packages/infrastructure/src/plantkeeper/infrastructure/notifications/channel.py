"""Valkey-backed notification channel (Phase 8).

This is the infrastructure half of
:mod:`plantkeeper.application.ports.notifications`: the domain-neutral
``NotificationChannel`` port over a Valkey pub/sub channel. One channel per
household, ``household:<uuid>``, carrying a nudge rather than a payload — what is
published is the household identifier, and a subscriber answers it by reading its
own database.

Two mappings are deliberate:

* a Valkey error is translated into :class:`NotificationChannelError`, so the
  application layer never has to know which client is behind the port; and
* a subscriber owns a `pubsub` connection of its own, opened when the
  subscription starts and closed when it ends, because a long poll outlives the
  request that started it and must not borrow the shared command connection.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Final

from valkey.asyncio import Valkey
from valkey.asyncio.client import PubSub
from valkey.exceptions import ValkeyError

from plantkeeper.application.ports.notifications import (
    NotificationChannelError,
    NotificationSubscription,
)
from plantkeeper.domain.identifiers import HouseholdId

logger = logging.getLogger(__name__)

CHANNEL_PREFIX: Final = "household:"
"""Every household's channel is ``household:<household_id>``."""


def channel_for(household_id: HouseholdId) -> str:
    """Return the presence channel of one household."""
    return f"{CHANNEL_PREFIX}{household_id.value}"


class ValkeySubscription:
    """One open subscription, reading nudges off its own pub/sub connection."""

    def __init__(self, pubsub: PubSub) -> None:
        self._pubsub = pubsub

    async def wait(self, timeout: float) -> bool:
        """Wait up to ``timeout`` seconds for a nudge.

        A non-positive timeout never touches the network: it reports "nothing
        arrived", which is what a caller that does not want to wait asked for.

        The read loops rather than returning on the first ``None``: the subscribe
        confirmation is filtered out and a health-check reply can arrive at any
        time, and neither is the signal the caller is waiting for.
        """
        if timeout <= 0:
            return False
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while (remaining := deadline - loop.time()) > 0:
            try:
                message = await self._pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=remaining
                )
            except (ValkeyError, OSError) as exc:
                raise NotificationChannelError("the notification channel went away") from exc
            if message is not None:
                return True
        return False


class ValkeyNotificationChannel:
    """The ``NotificationChannel`` port over a shared Valkey client."""

    def __init__(self, client: Valkey) -> None:
        """Hold the client the process owns; subscriptions open their own."""
        self._client = client

    async def publish(self, household_id: HouseholdId) -> None:
        """Publish the household's nudge, translating a client failure."""
        try:
            await self._client.publish(channel_for(household_id), str(household_id.value))
        except (ValkeyError, OSError) as exc:
            raise NotificationChannelError(
                f"could not signal household {household_id.value}"
            ) from exc

    @asynccontextmanager
    async def subscribe(self, household_id: HouseholdId) -> AsyncIterator[NotificationSubscription]:
        """Open a pub/sub connection, subscribed to the household's channel."""
        pubsub = self._client.pubsub()
        try:
            await pubsub.subscribe(channel_for(household_id))
        except (ValkeyError, OSError) as exc:
            await self._close_quietly(pubsub)
            raise NotificationChannelError(
                f"could not subscribe to household {household_id.value}"
            ) from exc
        try:
            yield ValkeySubscription(pubsub)
        finally:
            await self._close_quietly(pubsub)

    async def _close_quietly(self, pubsub: PubSub) -> None:
        """Release the subscription's connection, never failing the caller.

        The work is over by the time this runs — the caller either got its answer
        or is unwinding — so a failure to close is worth a debug line and nothing
        more.
        """
        try:
            # valkey's ``PubSub.aclose`` is declared without a return annotation,
            # so a strict build reports this one library boundary as untyped.
            await pubsub.aclose()  # type: ignore[no-untyped-call]
        except ValkeyError, OSError:
            logger.debug("could not close the notification subscription cleanly")
