"""Runtime settings, read from the environment.

Field names match the variables in ``.env.example`` (``POSTGRES_USER``,
``KAFKA_BOOTSTRAP_SERVERS``, …), so the local ``.env`` and the Docker Compose
file describe the same world without a translation layer.
"""

from __future__ import annotations

from urllib.parse import quote_plus

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Everything the services need to reach their infrastructure."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- Postgres -------------------------------------------------------------
    postgres_user: str = "plantkeeper"
    postgres_password: str = "plantkeeper"
    postgres_db: str = "plantkeeper"
    postgres_host: str = "localhost"
    postgres_port: int = 5432

    # --- Kafka ----------------------------------------------------------------
    kafka_bootstrap_servers: str = "localhost:9092"

    # --- Valkey ---------------------------------------------------------------
    valkey_url: str = "valkey://localhost:6379/0"

    # --- Outbox relay ---------------------------------------------------------
    outbox_batch_size: int = 100
    outbox_poll_interval_seconds: float = 1.0
    outbox_max_attempts: int = 5
    outbox_retry_initial_wait_seconds: float = 0.5
    outbox_retry_max_wait_seconds: float = 10.0

    # --- Read side ------------------------------------------------------------
    # Every projection gets its own consumer group (``<prefix>-garden``, …), so
    # each one has its own offsets and its own idempotency domain. The prefix is
    # configurable because a test that reuses a group would inherit the offsets
    # of an earlier run.
    read_side_consumer_group_prefix: str = "plantkeeper-read"

    @property
    def postgres_dsn(self) -> str:
        """The SQLAlchemy URL of the write database.

        Credentials are percent-encoded: a password may contain characters that
        a URL would otherwise read as separators.
        """
        user = quote_plus(self.postgres_user)
        password = quote_plus(self.postgres_password)
        return (
            f"postgresql+asyncpg://{user}:{password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )
