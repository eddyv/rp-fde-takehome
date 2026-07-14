"""Always-on retry consumer: wiki.edits.retry -> re-classify -> Postgres or DLQ.

Delay model: each envelope carries `not_before`; the retrier sleeps until it
(consumer lag eats most of the delay for free — a message already past its
`not_before` is processed immediately). Per-message time is bounded:
`not_before` delays cap at settings.retry_backoff_max_seconds (120s) and every
model call is bounded by the client's 60s request timeout, both well inside
infra.MAX_POLL_INTERVAL_MS (600s); kafka-python heartbeats from a background
thread, so sleeping in the poll loop is safe. If delays ever needed to
approach the poll interval, the right tool would be consumer.pause() +
periodic poll() instead of sleeping.

Outcomes per envelope (same commit-after-publish invariant as the worker):
- success -> classified row (flips the failed row), commit, breaker resets.
- ModelConfigError -> SystemExit(1), no commit — never swallowed.
- ModelUnavailableError -> attempts+1; refreshed failed row; republish to the
  retry topic with a later not_before, or to the DLQ (`retries_exhausted`)
  once attempts exceed 1 + max_retry_passes; commit; breaker increments.
- ClassificationParseError -> refreshed failed row, DLQ (`parse_failed`),
  commit, breaker resets.
- Undecodable/invalid envelope (our own bug), or an edit whose data does not
  fit the schema -> DLQ as malformed, commit.
"""

import json
import logging
import time
from datetime import UTC, datetime

from kafka import KafkaConsumer

from app import db, failures, infra, routing
from app.classifier import (
    ClassificationParseError,
    ModelConfigError,
    ModelUnavailableError,
    classify,
)
from app.config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SLEEP_CHUNK_SECONDS = 5


def make_consumer(retries: int = 30, delay: float = 2.0) -> KafkaConsumer:
    return infra.make_consumer(
        settings.kafka_retry_topic,
        settings.retrier_consumer_group,
        retries=retries,
        delay=delay,
    )


def _now() -> datetime:
    return datetime.now(UTC)


def _parse_not_before(value) -> datetime | None:
    """None passes through; naive timestamps are assumed UTC; junk raises
    TypeError/ValueError (validated at decode time, tolerated in wait_until)."""
    if value is None:
        return None
    target = datetime.fromisoformat(value)
    if target.tzinfo is None:
        target = target.replace(tzinfo=UTC)
    return target


def wait_until(not_before) -> None:
    """Sleep until the envelope's earliest-retry time; no-op if already past.

    Chunked sleeps keep each pause well under max_poll_interval_ms and make
    the loop responsive to clock re-checks.
    """
    try:
        target = _parse_not_before(not_before)
    except (TypeError, ValueError):
        return  # unparseable timestamp: retry immediately rather than wedge
    if target is None:
        return
    while True:
        remaining = (target - _now()).total_seconds()
        if remaining <= 0:
            return
        time.sleep(min(remaining, SLEEP_CHUNK_SECONDS))


def handle_envelope(client, conn, consumer, producer, breaker, message):
    """Process one retry envelope; returns the (possibly reconnected) conn."""
    try:
        envelope = json.loads(message.value)
        edit = envelope["edit"]
        if not isinstance(edit, dict) or edit.get("id") is None:
            raise TypeError(f"not an edit object: {type(edit).__name__}")
        schema = envelope.get("schema")
        if schema != failures.ENVELOPE_SCHEMA:
            raise ValueError(f"unsupported envelope schema: {schema!r}")
        attempts = int(envelope.get("attempts", 1))
        _parse_not_before(envelope.get("not_before"))  # validate before use
    except (
        json.JSONDecodeError,
        UnicodeDecodeError,
        KeyError,
        TypeError,
        ValueError,
    ) as error:
        # An envelope we can't decode/validate is a bug in this service; park
        # the evidence rather than crash-loop on it.
        logger.error("invalid retry envelope at offset %s -> DLQ", message.offset)
        failures.park_malformed(producer, consumer, message, error, source="retrier")
        return conn

    return routing.guard_schema_error(
        conn,
        consumer,
        producer,
        message,
        edit,
        "retrier",
        lambda: _handle_edit(
            client, conn, consumer, producer, breaker, message, envelope, edit, attempts
        ),
    )


def _handle_edit(
    client, conn, consumer, producer, breaker, message, envelope, edit, attempts
):
    wait_until(envelope.get("not_before"))
    first_failed_at = envelope.get("first_failed_at")

    try:
        result = classify(client, edit)
    except ModelConfigError as error:
        logger.critical(
            "deterministic model failure (bad key/config?), crashing: %s", error
        )
        raise SystemExit(1) from error
    except (ModelUnavailableError, ClassificationParseError) as error:
        if isinstance(error, ModelUnavailableError):
            attempts += 1  # counts the worker's first pass plus each retrier pass
        return routing.handle_classifier_failure(
            error,
            conn=conn,
            consumer=consumer,
            producer=producer,
            breaker=breaker,
            message=message,
            edit=edit,
            source="retrier",
            attempts=attempts,
            first_failed_at=first_failed_at,
        )

    conn = db.write_with_reconnect(conn, lambda c: db.upsert_edit(c, edit, result))
    consumer.commit()
    breaker.record_success()
    logger.info(
        "retried edit %s -> %s (%.2f) after %d attempts",
        edit.get("id"),
        result.label,
        result.confidence,
        attempts,
    )
    return conn


def run(client, conn, consumer, producer, breaker) -> None:
    """Consume until asked to stop, then leave the group cleanly. See
    worker.run() for the full rationale (SIGTERM/SIGINT -> ShutdownRequested
    -> consumer.close() sends LeaveGroup instead of waiting out
    session_timeout_ms)."""
    try:
        for message in consumer:
            conn = handle_envelope(client, conn, consumer, producer, breaker, message)
    except infra.ShutdownRequested:
        logger.info(
            "shutdown requested, leaving consumer group %s",
            settings.retrier_consumer_group,
        )
    finally:
        consumer.close()
        producer.close(timeout=10)
        conn.close()


def main() -> None:
    infra.install_shutdown_handler()
    # See infra.make_classifier_client for the guardrail rationale.
    client = infra.make_classifier_client()
    conn = db.connect()
    consumer = make_consumer()
    producer = failures.make_producer()
    breaker = failures.CircuitBreaker(settings.breaker_threshold)
    logger.info(
        "consuming %s from %s", settings.kafka_retry_topic, settings.kafka_brokers
    )

    run(client, conn, consumer, producer, breaker)


if __name__ == "__main__":
    main()
