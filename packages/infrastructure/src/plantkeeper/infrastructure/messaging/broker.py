"""The Kafka broker factory.

One broker object per process: FastStream's ``KafkaBroker`` owns the aiokafka
producer and the admin client, and those are loop-bound, so the object must be
created in the process that starts it (see ``plantkeeper.workers``).
"""

from __future__ import annotations

from faststream.kafka import KafkaBroker

from plantkeeper.infrastructure.config import Settings


def build_broker(settings: Settings) -> KafkaBroker:
    """Return a broker pointed at the configured bootstrap servers.

    The broker is not connected: the caller starts it, so the connection lives
    inside the application's lifespan and is closed on shutdown.
    """
    return KafkaBroker(settings.kafka_bootstrap_servers)
