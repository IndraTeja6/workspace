"""Kafka I/O wrappers for the Nova fleet tracking pipeline.

Every interaction with ``confluent-kafka`` is isolated behind this module so
the rest of Nova never imports librdkafka directly. The module imports
cleanly even when ``confluent-kafka`` (or its native librdkafka dependency)
is unavailable: the client symbols are guarded and any attempt to actually
build a client raises :class:`KafkaIOError` with an actionable message.

Delivery semantics
------------------
The consumer is created with ``enable.auto.commit=False`` and
``auto.offset.reset=earliest``. :func:`consume_loop` commits an offset only
once the caller asks for the *next* message, which yields at-least-once
processing: a crash between handling a message and requesting the next one
causes that message to be redelivered.
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, Iterator, Optional, Tuple

from nova.config import AppConfig
from nova.logging_setup import get_logger, log_extra

logger = get_logger(__name__)

try:  # pragma: no cover - exercised only when confluent-kafka is installed
    from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

    CONFLUENT_KAFKA_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised only without librdkafka
    Consumer = None  # type: ignore[assignment,misc]
    Producer = None  # type: ignore[assignment,misc]
    KafkaError = None  # type: ignore[assignment,misc]
    KafkaException = None  # type: ignore[assignment,misc]
    CONFLUENT_KAFKA_AVAILABLE = False

__all__ = [
    "CONFLUENT_KAFKA_AVAILABLE",
    "KafkaIOError",
    "make_consumer",
    "make_producer",
    "produce_json",
    "send_to_dlq",
    "consume_loop",
    "commit_message",
    "flush_producer",
    "close_consumer",
]

DEFAULT_POLL_TIMEOUT = 1.0
DEFAULT_PRODUCE_ATTEMPTS = 5
DEFAULT_PRODUCE_BACKOFF = 0.1
DEFAULT_PRODUCE_BACKOFF_MAX = 2.0
DEFAULT_FLUSH_TIMEOUT = 10.0


class KafkaIOError(RuntimeError):
    """Raised when a Kafka operation cannot be completed."""


def _require_confluent() -> None:
    """Raise a clear error when confluent-kafka is not importable."""
    if not CONFLUENT_KAFKA_AVAILABLE:
        raise KafkaIOError(
            "confluent-kafka is not available. Install it with "
            "'pip install confluent-kafka' (requires librdkafka)."
        )


def _security_settings(config: AppConfig) -> Dict[str, Any]:
    """Return librdkafka security settings for the configured broker.

    Security is applied only when it is actually configured. The base
    configuration ships SASL placeholders (protocol/mechanism) with no
    credentials; forwarding those to a plaintext broker would break local
    development, so the block is skipped unless a username, password, or CA
    file is present.
    """
    security = config.kafka.security
    if not (security.username or security.password or security.ca_file):
        return {}

    settings: Dict[str, Any] = {}
    if security.protocol:
        settings["security.protocol"] = security.protocol
    if security.mechanism:
        settings["sasl.mechanism"] = security.mechanism
    if security.username:
        settings["sasl.username"] = security.username
    if security.password:
        settings["sasl.password"] = security.password
    if security.ca_file:
        settings["ssl.ca.location"] = security.ca_file
    return settings


def _delivery_report(err: Any, msg: Any) -> None:
    """Log the outcome of a produce request (librdkafka callback)."""
    if err is not None:
        logger.error(
            "kafka delivery failed",
            extra=log_extra(topic=msg.topic(), error=str(err)),
        )
        return
    logger.debug(
        "kafka delivery succeeded",
        extra=log_extra(
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
        ),
    )


def make_consumer(config: AppConfig) -> "Consumer":
    """Create a consumer subscribed to the raw telemetry topic.

    Auto-commit is disabled and offsets reset to ``earliest`` so the
    processor controls exactly when an offset is committed.
    """
    _require_confluent()

    settings: Dict[str, Any] = {
        "bootstrap.servers": config.kafka.bootstrap_servers,
        "group.id": config.kafka.group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }
    settings.update(_security_settings(config))

    consumer = Consumer(settings)
    consumer.subscribe([config.kafka.topics.raw])
    logger.info(
        "kafka consumer created",
        extra=log_extra(
            bootstrap_servers=config.kafka.bootstrap_servers,
            group_id=config.kafka.group_id,
            topic=config.kafka.topics.raw,
        ),
    )
    return consumer


def make_producer(config: AppConfig) -> "Producer":
    """Create a producer for the Nova pipeline.

    Delivery reports are logged by :func:`_delivery_report`, which is
    attached to every :func:`produce_json` call.
    """
    _require_confluent()

    settings: Dict[str, Any] = {
        "bootstrap.servers": config.kafka.bootstrap_servers,
        "client.id": "nova-producer",
        "acks": "all",
        "enable.idempotence": True,
    }
    settings.update(_security_settings(config))

    producer = Producer(settings)
    logger.info(
        "kafka producer created",
        extra=log_extra(bootstrap_servers=config.kafka.bootstrap_servers),
    )
    return producer


def produce_json(
    producer: "Producer",
    topic: str,
    key: Optional[str],
    payload: bytes,
    *,
    attempts: int = DEFAULT_PRODUCE_ATTEMPTS,
    backoff: float = DEFAULT_PRODUCE_BACKOFF,
    backoff_max: float = DEFAULT_PRODUCE_BACKOFF_MAX,
) -> None:
    """Produce ``payload`` to ``topic`` with bounded retry on backpressure.

    Retries with exponential backoff when librdkafka's local queue is full
    (:class:`BufferError`). Raises :class:`KafkaIOError` once every attempt
    has been exhausted.
    """
    if not isinstance(payload, (bytes, bytearray)):
        raise TypeError("payload must be bytes")
    if attempts < 1:
        raise ValueError("attempts must be >= 1")

    key_bytes = key.encode("utf-8") if isinstance(key, str) else key
    delay = backoff
    last_error: Optional[BaseException] = None

    for attempt in range(1, attempts + 1):
        try:
            producer.produce(
                topic,
                value=bytes(payload),
                key=key_bytes,
                callback=_delivery_report,
            )
            producer.poll(0)
            return
        except BufferError as exc:
            last_error = exc
            logger.warning(
                "kafka produce buffer full; retrying",
                extra=log_extra(
                    topic=topic,
                    attempt=attempt,
                    max_attempts=attempts,
                    backoff_s=round(delay, 3),
                ),
            )
            producer.poll(delay)
            time.sleep(delay)
            delay = min(delay * 2, backoff_max)

    raise KafkaIOError(
        f"failed to produce to '{topic}' after {attempts} attempts: {last_error}"
    )


def _decode_for_dlq(raw_bytes: bytes) -> Any:
    """Decode a rejected payload for embedding in a DLQ envelope."""
    try:
        return json.loads(raw_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return raw_bytes.decode("utf-8", errors="replace")


def send_to_dlq(
    producer: "Producer",
    config: AppConfig,
    raw_bytes: bytes,
    reason: str,
) -> bool:
    """Best-effort publish of a rejected payload to the dead-letter topic.

    Never raises: any failure is logged and reported as ``False`` so the
    caller can keep the pipeline moving.
    """
    try:
        envelope = {
            "reason": reason,
            "source_topic": config.kafka.topics.raw,
            "payload": _decode_for_dlq(raw_bytes),
        }
        payload = json.dumps(
            envelope, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
        produce_json(producer, config.kafka.topics.dlq, key=None, payload=payload)
        logger.info(
            "message routed to dlq",
            extra=log_extra(topic=config.kafka.topics.dlq, reason=reason),
        )
        return True
    except Exception as exc:  # noqa: BLE001 - the DLQ path must never raise
        logger.error(
            "failed to route message to dlq",
            extra=log_extra(topic=config.kafka.topics.dlq, error=str(exc)),
        )
        return False


def _is_stopped(stop_event: Optional[Any]) -> bool:
    return stop_event is not None and stop_event.is_set()


def _handle_consume_error(msg: Any, error: Any) -> None:
    if KafkaError is not None and error.code() == KafkaError._PARTITION_EOF:
        logger.debug("reached end of partition", extra=log_extra(topic=msg.topic()))
        return
    logger.error(
        "kafka consume error",
        extra=log_extra(topic=msg.topic(), error=str(error)),
    )


def consume_loop(
    consumer: "Consumer",
    *,
    poll_timeout: float = DEFAULT_POLL_TIMEOUT,
    stop_event: Optional[Any] = None,
) -> Iterator[Tuple[Any, bytes]]:
    """Yield ``(message, raw_bytes)`` pairs from the subscribed topic.

    The offset of a message is committed only when the caller requests the
    *next* message, giving at-least-once processing. Iterate with a ``for``
    loop and let the body complete normally to commit; break out (or raise)
    to leave the current offset uncommitted for redelivery.
    """
    _require_confluent()

    while not _is_stopped(stop_event):
        msg = consumer.poll(poll_timeout)
        if msg is None:
            continue

        error = msg.error()
        if error is not None:
            _handle_consume_error(msg, error)
            continue

        raw = msg.value() or b""
        yield msg, raw
        # Resumed: the caller has finished with the previous message and is
        # asking for the next one, so it is safe to commit (at-least-once).
        commit_message(consumer, msg)


def commit_message(consumer: "Consumer", msg: Any) -> None:
    """Synchronously commit the offset of ``msg`` (i.e. offset + 1).

    Failures are logged rather than raised: an uncommitted offset simply
    means the message is redelivered, which preserves at-least-once.
    """
    try:
        consumer.commit(message=msg, asynchronous=False)
    except Exception as exc:  # noqa: BLE001 - commit failures must not crash the loop
        logger.error("kafka commit failed", extra=log_extra(error=str(exc)))


def flush_producer(producer: "Producer", timeout: float = DEFAULT_FLUSH_TIMEOUT) -> int:
    """Flush pending produce requests, returning the number still queued."""
    try:
        remaining = producer.flush(timeout)
    except Exception as exc:  # noqa: BLE001 - best-effort cleanup
        logger.error("kafka flush failed", extra=log_extra(error=str(exc)))
        return -1
    if remaining:
        logger.warning("kafka flush incomplete", extra=log_extra(remaining=remaining))
    return remaining


def close_consumer(consumer: "Consumer") -> None:
    """Close the consumer, releasing its group membership."""
    try:
        consumer.close()
    except Exception as exc:  # noqa: BLE001 - best-effort cleanup
        logger.error("kafka consumer close failed", extra=log_extra(error=str(exc)))
