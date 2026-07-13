from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    anthropic_api_key: SecretStr = SecretStr("")
    # None means "let the SDK decide" (env ANTHROPIC_BASE_URL or its default);
    # set to point the client at an Anthropic-compat endpoint (e.g. Ollama).
    anthropic_base_url: str | None = None
    kafka_brokers: str = "localhost:19092"
    kafka_topic: str = "wiki.edits.raw"
    kafka_retry_topic: str = "wiki.edits.retry"
    kafka_dlq_topic: str = "wiki.edits.dlq"
    consumer_group: str = "reasoning-service"
    retrier_consumer_group: str = "reasoning-service-retrier"
    sweeper_consumer_group: str = "reasoning-service-sweeper"

    postgres_dsn: str = "postgresql://wiki:wiki@localhost:5433/wiki"

    anthropic_model: str = "claude-haiku-4-5"
    # Stronger model for manual DLQ drains; None falls back to anthropic_model.
    sweeper_model: str | None = None

    # Below this confidence, a second-pass prompt with more context is attempted.
    confidence_threshold: float = 0.6

    # Crash after this many consecutive transient-exhausted outcomes; Docker's
    # restart backoff then acts as the half-open probe.
    breaker_threshold: int = 25

    # Retry-topic schedule: passes beyond the worker's first attempt, with
    # exponential delay base * 2**(n-1) capped at max. The cap must stay well
    # under the retrier's max_poll_interval_ms (see retrier.py).
    max_retry_passes: int = 3
    retry_backoff_base_seconds: int = 30
    retry_backoff_max_seconds: int = 120


settings = Settings()
