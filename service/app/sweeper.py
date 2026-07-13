"""Manual DLQ drain: re-attempt terminal failures, optionally with a stronger
model.

    python -m app.sweeper [--model MODEL] [--limit N]

Not a compose service — run on demand, e.g.:

    docker compose run --rm worker python -m app.sweeper --model claude-sonnet-4-5

End offsets are snapshotted at start and only messages below the snapshot are
processed: a failed sweep republishes to the DLQ *tail*, so without the
snapshot a persistent failure would loop forever within one run. Requeued
envelopes are simply picked up by the next sweep. `consumer_timeout_ms` ends
the run once the snapshot range is drained. Offsets are committed per message
(explicit offset, not position) so paused-at-boundary partitions and the
requeued tail stay uncommitted for the next run.
"""

import argparse
import base64
import json
import logging

import anthropic
import psycopg
from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata

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

CONSUMER_TIMEOUT_MS = 10_000


def make_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        settings.kafka_dlq_topic,
        bootstrap_servers=settings.kafka_brokers.split(","),
        group_id=settings.sweeper_consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        consumer_timeout_ms=CONSUMER_TIMEOUT_MS,  # exit the loop when drained
    )


def _commit(consumer: KafkaConsumer, message) -> None:
    tp = TopicPartition(message.topic, message.partition)
    consumer.commit({tp: OffsetAndMetadata(message.offset + 1, "", -1)})


def main() -> None:
    parser = argparse.ArgumentParser(description="Drain wiki.edits.dlq once.")
    parser.add_argument("--model", default=None, help="override the classifier model")
    parser.add_argument("--limit", type=int, default=None, help="max envelopes")
    args = parser.parse_args()
    model = args.model or settings.sweeper_model or settings.anthropic_model

    # SDK retries disabled (classifier.py owns them); the explicit request
    # timeout keeps a hung call from stalling the drain for 10 minutes.
    client = anthropic.Anthropic(
        api_key=settings.anthropic_api_key.get_secret_value(),
        base_url=settings.anthropic_base_url,
        max_retries=0,
        timeout=60.0,
    )
    conn = db.connect()
    consumer = make_consumer()
    producer = failures.make_producer()

    partitions = consumer.partitions_for_topic(settings.kafka_dlq_topic) or set()
    tps = [TopicPartition(settings.kafka_dlq_topic, p) for p in sorted(partitions)]
    end_offsets = consumer.end_offsets(tps)
    logger.info(
        "sweeping %s with model %s up to %s",
        settings.kafka_dlq_topic,
        model,
        end_offsets,
    )

    counts = {"reclassified": 0, "requeued": 0, "skipped": 0}
    processed = 0

    for message in consumer:
        tp = TopicPartition(message.topic, message.partition)
        if message.offset >= end_offsets.get(tp, 0):
            # Reached this run's snapshot boundary; leave the tail (including
            # anything we requeued below) uncommitted for the next sweep.
            consumer.pause(tp)
            continue
        if args.limit is not None and processed >= args.limit:
            break
        processed += 1

        try:
            envelope = json.loads(message.value)
            schema = envelope.get("schema")
            if schema != failures.ENVELOPE_SCHEMA:
                raise ValueError(f"unsupported envelope schema: {schema!r}")
            reason = envelope.get("reason")
            attempts = int(envelope.get("attempts", 1))
        except (
            json.JSONDecodeError,
            UnicodeDecodeError,
            AttributeError,
            TypeError,
            ValueError,
        ):
            logger.error(
                "undecodable DLQ record at offset %s, skipping", message.offset
            )
            _commit(consumer, message)
            counts["skipped"] += 1
            continue

        edit = envelope.get("edit")
        if (
            reason == failures.REASON_MALFORMED
            or not isinstance(edit, dict)
            or edit.get("id") is None
        ):
            # Nothing classifiable; surface the original bytes for a human.
            # A bad `raw` field must not abort the run — the same record
            # would kill every future sweep before its commit.
            try:
                raw = base64.b64decode(envelope.get("raw") or "")
            except (TypeError, ValueError):
                raw = b"<undecodable raw field>"
            logger.warning(
                "skipping %s envelope at offset %s, raw=%r", reason, message.offset, raw
            )
            _commit(consumer, message)
            counts["skipped"] += 1
            continue

        try:
            result = classify(client, edit, model=model)
        except ModelConfigError as error:
            logger.critical(
                "deterministic model failure (bad key/config?), aborting: %s", error
            )
            raise SystemExit(1) from error
        except (ModelUnavailableError, ClassificationParseError) as error:
            failed_reason = (
                failures.REASON_PARSE_FAILED
                if isinstance(error, ClassificationParseError)
                else failures.REASON_TRANSIENT_EXHAUSTED
            )
            logger.warning(
                "edit %s still failing (%s), requeueing", edit.get("id"), failed_reason
            )
            out = failures.make_envelope(
                reason=failed_reason,
                error=str(error),
                source="sweeper",
                message=message,
                edit=edit,
                attempts=attempts + 1,
                first_failed_at=envelope.get("first_failed_at"),
            )
            failures.publish(producer, settings.kafka_dlq_topic, out)
            _commit(consumer, message)
            counts["requeued"] += 1
            continue

        try:
            conn = db.write_with_reconnect(
                conn, lambda c, e=edit, r=result: db.upsert_edit(c, e, r)
            )
        except psycopg.OperationalError:
            raise  # connection-level failure even after reconnect: abort
        except psycopg.Error as error:
            # Data-shaped failure: this record can never be persisted; skip
            # it so the DLQ stays drainable.
            logger.error(
                "edit %s does not fit the schema, skipping: %s", edit.get("id"), error
            )
            _commit(consumer, message)
            counts["skipped"] += 1
            continue
        _commit(consumer, message)
        counts["reclassified"] += 1
        logger.info(
            "swept edit %s -> %s (%.2f)",
            edit.get("id"),
            result.label,
            result.confidence,
        )

    logger.info(
        "sweep done: %d reclassified, %d requeued, %d skipped",
        counts["reclassified"],
        counts["requeued"],
        counts["skipped"],
    )


if __name__ == "__main__":
    main()
