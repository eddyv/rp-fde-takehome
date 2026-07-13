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
import time

import anthropic
import psycopg
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from app import db, failures
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
# most 9 calls plus seconds of backoff), so per-message time stays under this.
# The kafka-python default (300s) is too tight for a slow multi-pass classify.
MAX_POLL_INTERVAL_MS = 600_000


def make_consumer(retries: int = 30, delay: float = 2.0) -> KafkaConsumer:
    for attempt in range(retries):
        try:
            return KafkaConsumer(
                settings.kafka_topic,
                bootstrap_servers=settings.kafka_brokers.split(","),
                group_id=settings.consumer_group,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                max_poll_interval_ms=MAX_POLL_INTERVAL_MS,
            )
        except KafkaError as error:  # broker not up yet at stack boot
            if attempt == retries - 1:
                raise
            logger.info("kafka not ready (%s), retrying...", type(error).__name__)
            time.sleep(delay)
    raise RuntimeError("unreachable")


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

    try:
        return _handle_edit(client, conn, consumer, producer, breaker, message, edit)
    except psycopg.OperationalError:
        raise  # connection-level failure even after reconnect: crash, redeliver
    except psycopg.Error as error:
        # Data-shaped failure (e.g. byte_delta that isn't an int): retrying
        # the same message can never succeed, so park it and move on.
        logger.warning(
            "edit %s does not fit the schema -> DLQ: %s", edit.get("id"), error
        )
        failures.park_malformed(producer, consumer, message, error, source="worker")
        return conn


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
    except ModelUnavailableError as error:
        error_text = str(error)  # `error` is unbound once the except block exits
        logger.warning(
            "transient failure exhausted for edit %s: %s", edit.get("id"), error_text
        )
        conn = db.write_with_reconnect(
            conn,
            lambda c: db.upsert_failed_edit(
                c, edit, failures.REASON_TRANSIENT_EXHAUSTED, error_text
            ),
        )
        envelope = failures.make_envelope(
            reason=failures.REASON_TRANSIENT_EXHAUSTED,
            error=error_text,
            source="worker",
            message=message,
            edit=edit,
            attempts=1,
            not_before=failures.next_not_before(1),
        )
        failures.publish(producer, settings.kafka_retry_topic, envelope)
        consumer.commit()
        if breaker.record_failure():
            logger.critical(
                "circuit breaker tripped after %d consecutive transient failures; "
                "crashing so restart backoff becomes the half-open probe",
                breaker.threshold,
            )
            raise SystemExit(1)
        return conn
    except ClassificationParseError as error:
        error_text = str(error)
        logger.warning("unusable model output for edit %s -> DLQ", edit.get("id"))
        conn = db.write_with_reconnect(
            conn,
            lambda c: db.upsert_failed_edit(
                c, edit, failures.REASON_PARSE_FAILED, error_text
            ),
        )
        envelope = failures.make_envelope(
            reason=failures.REASON_PARSE_FAILED,
            error=error_text,
            source="worker",
            message=message,
            edit=edit,
        )
        failures.publish(producer, settings.kafka_dlq_topic, envelope)
        consumer.commit()
        breaker.record_success()  # the API is reachable; only the output was bad
        return conn

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
    # SDK retries are disabled: this service owns retry/backoff (classifier.py).
    # The explicit request timeout (SDK default is 600s) keeps one hung call
    # from blowing past max_poll_interval_ms and evicting us from the group.
    client = anthropic.Anthropic(
        api_key=settings.anthropic_api_key.get_secret_value(),
        base_url=settings.anthropic_base_url,
        max_retries=0,
        timeout=60.0,
    )
    conn = db.connect()
    consumer = make_consumer()
    producer = failures.make_producer()
    breaker = failures.CircuitBreaker(settings.breaker_threshold)
    logger.info("consuming %s from %s", settings.kafka_topic, settings.kafka_brokers)

    for message in consumer:
        conn = handle_message(client, conn, consumer, producer, breaker, message)


if __name__ == "__main__":
    main()
