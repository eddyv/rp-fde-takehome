import os

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:19092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "wiki.edits.raw")
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "reasoning-service")

POSTGRES_DSN = os.environ.get(
    "POSTGRES_DSN", "postgresql://wiki:wiki@localhost:5433/wiki"
)

ANTHROPIC_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-haiku-4-5")

# Below this confidence, a second-pass prompt with more context is attempted.
CONFIDENCE_THRESHOLD = float(os.environ.get("CONFIDENCE_THRESHOLD", "0.6"))
