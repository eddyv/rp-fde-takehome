"""Shared process-startup factories for worker, retrier, and sweeper."""

import logging
import time

from anthropic import Anthropic  # module-local name: tests patch infra.Anthropic
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from app.config import settings

logger = logging.getLogger(__name__)

# One definition (was duplicated in worker.py/retrier.py); config.py's
# retry_backoff_max_seconds documents an invariant against this value.
MAX_POLL_INTERVAL_MS = 600_000


def make_consumer(
    topic: str, group_id: str, retries: int = 30, delay: float = 2.0
) -> KafkaConsumer:
    for attempt in range(retries):
        try:
            return KafkaConsumer(
                topic,
                bootstrap_servers=settings.kafka_broker_list,
                group_id=group_id,
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


def make_classifier_client(timeout: float = 60.0) -> Anthropic:
    # SDK retries are disabled: this service owns retry/backoff (classifier.py).
    # The explicit request timeout (SDK default is 600s) keeps one hung call
    # from blowing past max_poll_interval_ms and evicting us from the group.
    return Anthropic(
        api_key=settings.anthropic_api_key.get_secret_value(),
        base_url=settings.anthropic_base_url,
        max_retries=0,
        timeout=timeout,
    )
