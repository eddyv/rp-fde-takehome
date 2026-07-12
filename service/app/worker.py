"""Single consumer: wiki.edits.raw -> classify with Claude -> UPSERT to Postgres.

At-least-once: offsets are committed only after the row is written, and the
UPSERT makes redelivery idempotent.
"""

import json
import logging
import time

import anthropic
import psycopg
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from app import db
from app.classifier import classify
from app.config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def make_consumer(retries: int = 30, delay: float = 2.0) -> KafkaConsumer:
    for attempt in range(retries):
        try:
            return KafkaConsumer(
                settings.kafka_topic,
                bootstrap_servers=settings.kafka_brokers.split(","),
                group_id=settings.consumer_group,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
            )
        except KafkaError as error:  # broker not up yet at stack boot
            if attempt == retries - 1:
                raise
            logger.info("kafka not ready (%s), retrying...", type(error).__name__)
            time.sleep(delay)
    raise RuntimeError("unreachable")


def main() -> None:
    # SDK retries are disabled: this service owns retry/backoff (classifier.py).
    client = anthropic.Anthropic(
        api_key=settings.anthropic_api_key.get_secret_value(), max_retries=0
    )
    conn = db.connect()
    consumer = make_consumer()
    logger.info("consuming %s from %s", settings.kafka_topic, settings.kafka_brokers)

    for message in consumer:
        try:
            edit = json.loads(message.value)
        except (json.JSONDecodeError, UnicodeDecodeError):
            logger.warning("skipping malformed message at offset %s", message.offset)
            consumer.commit()
            continue

        result = classify(client, edit)  # never raises; falls back to unclear
        try:
            db.upsert_edit(conn, edit, result)
        except psycopg.OperationalError:
            logger.warning("postgres connection lost, reconnecting")
            conn = db.connect()
            db.upsert_edit(conn, edit, result)

        consumer.commit()
        logger.info(
            "edit %s %r -> %s (%.2f)",
            edit.get("id"),
            edit.get("title"),
            result.label,
            result.confidence,
        )


if __name__ == "__main__":
    main()
