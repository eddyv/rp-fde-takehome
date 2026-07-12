from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")
    
    anthropic_api_key: SecretStr = SecretStr("")
    kafka_brokers: str = "localhost:19092"
    kafka_topic: str = "wiki.edits.raw"
    consumer_group: str = "reasoning-service"

    postgres_dsn: str = "postgresql://wiki:wiki@localhost:5433/wiki"

    anthropic_model: str = "claude-haiku-4-5"

    # Below this confidence, a second-pass prompt with more context is attempted.
    confidence_threshold: float = 0.6


settings = Settings()
