"""Single consumer: wiki.edits.raw -> classify with Claude -> UPSERT to Postgres.

At-least-once: offsets are committed only after the row is written (and, for
failures, after the retry/DLQ envelope is broker-acked), so redelivery is the
worst case and loss is impossible. The UPSERT makes redelivery idempotent, and
a status pre-check skips the LLM for an already-classified redelivered id
('failed' and absent rows still classify).

Per-class routing (see classifier.py for the taxonomy):
- ModelConfigError    -> CRITICAL + SystemExit(1), no commit: a visible crash
                         loop instead of silently draining the topic.
- ModelUnavailableError -> failed row + envelope on wiki.edits.retry, commit,
                         circuit breaker increments (trips -> crash).
- ClassificationParseError -> failed row + envelope on wiki.edits.dlq, commit,
                         breaker resets (the API answered; only output was bad).
- Malformed message   -> base64 envelope on wiki.edits.dlq, commit. This also
                         covers decodable-but-unusable edits (no id, values
                         that don't fit the schema): deterministic per-message
                         failures must park, not wedge the partition.
- Success             -> classified row, commit, breaker resets.
"""

import json
import logging

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

# Model calls are bounded (see main: request timeout 60s; classify makes at
# most 6 calls (3 bounded attempts × first + optional second pass) plus
# seconds of backoff), so per-message time stays under infra.MAX_POLL_INTERVAL_MS.
# The kafka-python default (300s) is too tight for a slow multi-pass classify.


def make_consumer(retries: int = 30, delay: float = 2.0) -> KafkaConsumer:
    return infra.make_consumer(
        settings.kafka_topic, settings.consumer_group, retries=retries, delay=delay
    )


def handle_message(client, conn, consumer, producer, breaker, message):
    """Process one record; returns the (possibly reconnected) Postgres conn.

    Ordering invariant on every failure path: DB write, then broker-acked
    publish, then commit, then breaker bookkeeping. A crash between publish
    and commit yields a duplicate envelope (harmless: idempotent UPSERT and
    guarded failed-row writes), never a lost message.
    """
    try:
        edit = json.loads(message.value)
        if not isinstance(edit, dict) or edit.get("id") is None:
            # A JSON scalar/array, or an object without an id, is not an
            # edit; park it rather than crash-loop on a poison message.
            raise TypeError(f"not an edit object: {type(edit).__name__}")
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError) as error:
        logger.warning("malformed message at offset %s -> DLQ", message.offset)
        failures.park_malformed(producer, consumer, message, error, source="worker")
        return conn

    return routing.guard_schema_error(
        conn,
        consumer,
        producer,
        message,
        edit,
        "worker",
        lambda: _handle_edit(client, conn, consumer, producer, breaker, message, edit),
    )


def _handle_edit(client, conn, consumer, producer, breaker, message, edit):
    # Redelivery pre-check: an already-classified row means the verdict is
    # durable, so re-running classify() would only re-burn its model calls.
    # A 'failed' row must NOT skip — a redelivery is its retry — and
    # an absent row is first delivery. A Postgres outage here propagates as
    # OperationalError (crash uncommitted), same as the write paths.
    conn, status = db.read_with_reconnect(conn, lambda c: db.fetch_edit_status(c, edit))
    if status == "classified":
        logger.info("edit %s already classified, skipping redelivery", edit.get("id"))
        consumer.commit()
        return conn

    try:
        result = classify(client, edit)
    except ModelConfigError as error:
        logger.critical(
            "deterministic model failure (bad key/config?), crashing: %s", error
        )
        raise SystemExit(1) from error
    except (ModelUnavailableError, ClassificationParseError) as error:
        return routing.handle_classifier_failure(
            error,
            conn=conn,
            consumer=consumer,
            producer=producer,
            breaker=breaker,
            message=message,
            edit=edit,
            source="worker",
            attempts=1,
        )

    conn = db.write_with_reconnect(conn, lambda c: db.upsert_edit(c, edit, result))
    consumer.commit()
    breaker.record_success()
    logger.info(
        "edit %s %r -> %s (%.2f)",
        edit.get("id"),
        edit.get("title"),
        result.label,
        result.confidence,
    )
    return conn


def main() -> None:
    # See infra.make_classifier_client for the guardrail rationale.
    client = infra.make_classifier_client()
    conn = db.connect()
    consumer = make_consumer()
    producer = failures.make_producer()
    breaker = failures.CircuitBreaker(settings.breaker_threshold)
    logger.info("consuming %s from %s", settings.kafka_topic, settings.kafka_brokers)

    for message in consumer:
        conn = handle_message(client, conn, consumer, producer, breaker, message)


if __name__ == "__main__":
    main()
