"""Shared classifier-failure routing used by worker._handle_edit and
retrier._handle_edit.

Both callers share one taxonomy for `ModelUnavailableError` and
`ClassificationParseError` (see worker.py's module docstring for the full
per-class routing table). `ModelConfigError` stays inline in each caller: it's
three lines, and moving `raise SystemExit(1) from error` into a helper would
change the traceback shape reviewers rely on.

Ordering invariant, unchanged from the pre-extraction code: DB write ->
broker-acked publish -> commit -> breaker bookkeeping. A crash between
publish and commit yields a duplicate envelope (harmless: idempotent UPSERT
and guarded failed-row writes), never a lost message.
"""

import logging

import sqlalchemy.exc

from app import db, failures
from app.classifier import ClassificationParseError, ModelUnavailableError
from app.config import settings

logger = logging.getLogger(__name__)


def transient_destination(attempts: int) -> tuple[str, str, str | None]:
    """(topic, reason, not_before_iso|None) for a transient-exhausted edit.

    `attempts` counts the worker's first pass plus each retrier pass. The
    worker's fixed one-shot behavior is the `attempts=1` case of this same
    rule: 1 < 1 + max_retry_passes for any max_retry_passes >= 1, so it always
    resolves to the retry topic with `next_not_before(1)`.
    """
    if attempts >= 1 + settings.max_retry_passes:
        return settings.kafka_dlq_topic, failures.REASON_RETRIES_EXHAUSTED, None
    return (
        settings.kafka_retry_topic,
        failures.REASON_TRANSIENT_EXHAUSTED,
        failures.next_not_before(attempts),
    )


def handle_classifier_failure(
    error,
    *,
    conn,
    consumer,
    producer,
    breaker,
    message,
    edit,
    source: str,
    attempts: int,
    first_failed_at: str | None = None,
):
    """Route a caught ModelUnavailableError/ClassificationParseError; returns
    the (possibly reconnected) conn.

    Callers must handle ModelConfigError inline before reaching here (crash
    path, no commit). `attempts`/`first_failed_at` are the values the caller
    already computed: the worker passes `attempts=1, first_failed_at=None`
    (a no-op against make_envelope's own defaults); the retrier passes its
    incremented `attempts` and the envelope's carried `first_failed_at`.
    """
    error_text = str(error)  # `error` is unbound once its except block exits

    if isinstance(error, ModelUnavailableError):
        if source == "worker":
            logger.warning(
                "transient failure exhausted for edit %s: %s",
                edit.get("id"),
                error_text,
            )
        conn = db.write_with_reconnect(
            conn,
            lambda c: db.upsert_failed_edit(
                c, edit, failures.REASON_TRANSIENT_EXHAUSTED, error_text
            ),
        )
        topic, reason, not_before = transient_destination(attempts)
        if source == "retrier" and reason == failures.REASON_RETRIES_EXHAUSTED:
            logger.warning(
                "edit %s exhausted %d attempts -> DLQ", edit.get("id"), attempts
            )
        envelope = failures.make_envelope(
            reason=reason,
            error=error_text,
            source=source,
            message=message,
            edit=edit,
            attempts=attempts,
            first_failed_at=first_failed_at,
            not_before=not_before,
        )
        failures.publish(producer, topic, envelope)
        consumer.commit()
        if breaker.record_failure():
            logger.critical(
                "circuit breaker tripped after %d consecutive transient failures; "
                "crashing so restart backoff becomes the half-open probe",
                breaker.threshold,
            )
            raise SystemExit(1)
        return conn

    if isinstance(error, ClassificationParseError):
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
            source=source,
            message=message,
            edit=edit,
            attempts=attempts,
            first_failed_at=first_failed_at,
        )
        failures.publish(producer, settings.kafka_dlq_topic, envelope)
        consumer.commit()
        breaker.record_success()  # the API is reachable; only the output was bad
        return conn

    raise TypeError(f"unhandled classifier failure type: {type(error)!r}")


def guard_schema_error(conn, consumer, producer, message, edit, source: str, run):
    """Run `run()`; a data-shaped SQLAlchemyError parks the message instead of
    crashing (identical in worker._handle_edit and retrier._handle_edit).

    OperationalError (connection-level) always re-raises so the caller crashes
    and Kafka redelivers, same as every other write path in this service.
    """
    try:
        return run()
    except db.CONNECTION_ERRORS:
        raise
    except sqlalchemy.exc.SQLAlchemyError as error:
        logger.warning(
            "edit %s does not fit the schema -> DLQ: %s", edit.get("id"), error
        )
        failures.park_malformed(producer, consumer, message, error, source=source)
        return conn
