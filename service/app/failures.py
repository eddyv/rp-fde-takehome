"""Shared failure plumbing for worker, retrier, and sweeper.

Three pieces:

- Envelopes: the JSON shape published to `wiki.edits.retry` / `wiki.edits.dlq`.
  `first_failed_at` is preserved across republishes so the full failure window
  survives; `not_before` exists only on retry-topic envelopes; `raw` carries
  the base64 original bytes only when the message never parsed (`malformed`).
- Producer/publish: publish is synchronously acked (`send().get()`) so an
  envelope is durably owned by its topic BEFORE the caller commits the source
  offset. A broker failure raises, the caller crashes uncommitted, and the
  message is redelivered — duplicates are possible, message loss is not.
- CircuitBreaker: counts consecutive transient-exhausted outcomes; tripping
  tells the caller to crash so Docker's restart backoff becomes the automatic
  half-open probe.
"""

import base64
import json
import logging
from datetime import UTC, datetime, timedelta

from kafka import KafkaProducer
from kafka.serializer import Serializer

from app.config import settings

logger = logging.getLogger(__name__)

# Stamped into every envelope; the retrier and sweeper route any other
# version to their malformed/skip paths, so a future producer-side bump
# cannot be silently misread by an older consumer.
ENVELOPE_SCHEMA = 1

REASON_TRANSIENT_EXHAUSTED = "transient_exhausted"
REASON_PARSE_FAILED = "parse_failed"
REASON_MALFORMED = "malformed"
REASON_RETRIES_EXHAUSTED = "retries_exhausted"


def utcnow() -> datetime:
    return datetime.now(UTC)


def make_envelope(
    *,
    reason: str,
    error: str,
    source: str,
    message,
    edit: dict | None = None,
    raw: bytes | None = None,
    attempts: int = 1,
    first_failed_at: str | None = None,
    not_before: str | None = None,
) -> dict:
    """Build a retry/DLQ envelope; `message` is the consumed Kafka record.

    Pass `first_failed_at` from the previous envelope when republishing;
    `not_before` only for retry-topic destinations.
    """
    now = utcnow().isoformat()
    envelope = {
        "schema": ENVELOPE_SCHEMA,
        "reason": reason,
        "error": error,
        "source": source,
        "attempts": attempts,
        "first_failed_at": first_failed_at or now,
        "last_failed_at": now,
        "kafka": {
            "topic": message.topic,
            "partition": message.partition,
            "offset": message.offset,
        },
    }
    if edit is not None:
        envelope["edit"] = edit
    if raw is not None:
        envelope["raw"] = base64.b64encode(raw).decode("ascii")
    if not_before is not None:
        envelope["not_before"] = not_before
    return envelope


def retry_delay_seconds(attempts: int) -> int:
    """Exponential schedule: base * 2**(attempts-1), capped at the max."""
    return min(
        settings.retry_backoff_base_seconds * 2 ** (attempts - 1),
        settings.retry_backoff_max_seconds,
    )


def next_not_before(attempts: int) -> str:
    return (utcnow() + timedelta(seconds=retry_delay_seconds(attempts))).isoformat()


class JsonSerializer(Serializer):
    def serialize(self, topic, headers, data):
        return json.dumps(data).encode("utf-8")


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=settings.kafka_broker_list,
        acks="all",
        retries=5,
        value_serializer=JsonSerializer(),
    )


def publish(producer: KafkaProducer, topic: str, envelope: dict) -> None:
    """Publish and wait for the broker ack before returning.

    Callers must invoke this BEFORE committing the source offset: if the ack
    never comes, the KafkaError propagates, the process dies uncommitted, and
    the source message is redelivered.
    """
    key = None
    edit = envelope.get("edit")
    if isinstance(edit, dict) and edit.get("id") is not None:
        key = str(edit["id"]).encode("utf-8")
    producer.send(topic, value=envelope, key=key).get(timeout=30)


def park_malformed(producer, consumer, message, error, source: str) -> None:
    """Route an unprocessable message to the DLQ (acked publish), then commit.

    Used for payloads that cannot be decoded into an edit AND for edits whose
    data does not fit the schema — both are deterministic per-message, so
    crashing would only wedge the partition on redelivery. The original bytes
    travel as base64 so the evidence survives.
    """
    envelope = make_envelope(
        reason=REASON_MALFORMED,
        error=str(error),
        source=source,
        message=message,
        raw=message.value,
    )
    publish(producer, settings.kafka_dlq_topic, envelope)
    consumer.commit()


class CircuitBreaker:
    """Trip after `threshold` consecutive failures; any success resets."""

    def __init__(self, threshold: int):
        self.threshold = threshold
        self.consecutive_failures = 0

    def record_failure(self) -> bool:
        """Returns True when the breaker trips (caller should crash)."""
        self.consecutive_failures += 1
        return self.consecutive_failures >= self.threshold

    def record_success(self) -> None:
        self.consecutive_failures = 0
