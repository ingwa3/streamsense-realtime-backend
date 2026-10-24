from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
import signal
import time
from datetime import datetime, timezone
from threading import Event
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer, TopicPartition
from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError as JsonSchemaValidationError
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from pydantic import ValidationError

from streamsense.common.logging import configure_logging
from streamsense.common.models import SensorReading
from streamsense.common.settings import PersisterSettings
from streamsense.persister.repository import (
    PersistableReading,
    RejectedEvent,
    persist_batch,
)


CONSUMED = Counter("streamsense_persister_consumed_total", "Kafka messages consumed")
PERSISTED = Counter("streamsense_persister_persisted_total", "Rows accepted by PostgreSQL")
DUPLICATES = Counter("streamsense_persister_duplicates_total", "Idempotently ignored rows")
DLQ_EVENTS = Counter("streamsense_persister_dlq_total", "Invalid events sent to the DLQ")
BATCH_FAILURES = Counter("streamsense_persister_batch_failures_total", "Failed persistence batches")
BATCH_DURATION = Histogram("streamsense_persister_batch_seconds", "Batch processing duration")
LAST_OFFSET = Gauge(
    "streamsense_persister_last_offset",
    "Last consumed offset",
    labelnames=("partition",),
)
CONSUMER_LAG = Gauge(
    "streamsense_persister_consumer_lag",
    "Approximate Kafka lag in messages",
    labelnames=("partition",),
)


def _dlq_payload(message: Message, reason: str) -> bytes:
    raw = message.value()
    try:
        decoded: Any = json.loads(raw.decode("utf-8")) if raw else None
    except Exception:
        decoded = raw.decode("utf-8", errors="replace") if raw else None
    return json.dumps(
        {
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "reason": reason,
            "source": {
                "topic": message.topic(),
                "partition": message.partition(),
                "offset": message.offset(),
            },
            "original_event": decoded,
        },
        ensure_ascii=False,
    ).encode("utf-8")


def main() -> None:
    settings = PersisterSettings()
    configure_logging("persister", settings.log_level)
    logger = logging.getLogger(__name__)
    stop = Event()

    def request_stop(signum: int, _frame: object) -> None:
        logger.info("shutdown requested", extra={"signal": signum})
        stop.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    start_http_server(settings.metrics_port)

    schema = json.loads(Path(settings.schema_path).read_text(encoding="utf-8"))
    schema_validator = Draft202012Validator(schema, format_checker=FormatChecker())

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.consumer_group,
            "client.id": f"{settings.consumer_group}-worker",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed",
            "max.poll.interval.ms": 300_000,
            "session.timeout.ms": 45_000,
        }
    )
    dlq_producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "client.id": "streamsense-dlq-producer",
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    consumer.subscribe([settings.input_topic])

    valid: list[PersistableReading] = []
    invalid: list[tuple[Message, str]] = []
    batch_messages: list[Message] = []
    last_flush = time.monotonic()

    def flush_batch() -> None:
        nonlocal last_flush
        if not batch_messages:
            return
        started = time.monotonic()
        try:
            for message, reason in invalid:
                dlq_producer.produce(
                    settings.dlq_topic,
                    key=f"{message.topic()}:{message.partition()}:{message.offset()}".encode(),
                    value=_dlq_payload(message, reason),
                )
            if invalid:
                undelivered = dlq_producer.flush(15)
                if undelivered:
                    raise RuntimeError(f"{undelivered} DLQ messages were not delivered")

            rejected_records = [
                RejectedEvent(
                    topic=message.topic(),
                    partition=message.partition(),
                    offset=message.offset(),
                    failed_at=datetime.now(timezone.utc),
                    reason=reason,
                    payload_sha256=hashlib.sha256(message.value() or b"").hexdigest(),
                )
                for message, reason in invalid
            ]
            result = persist_batch(settings.database_url, valid, rejected_records)
            duplicate_count = max(len(valid) - result.accepted_rows, 0)
            PERSISTED.inc(result.accepted_rows)
            DUPLICATES.inc(duplicate_count)
            DLQ_EVENTS.inc(len(invalid))
            consumer.commit(asynchronous=False)
            logger.info(
                "batch committed",
                extra={
                    "messages": len(batch_messages),
                    "valid": len(valid),
                    "inserted": result.accepted_rows,
                    "rejections_recorded": result.rejection_rows,
                    "duplicates": duplicate_count,
                    "dlq": len(invalid),
                    "duration_seconds": round(time.monotonic() - started, 4),
                },
            )
            valid.clear()
            invalid.clear()
            batch_messages.clear()
            last_flush = time.monotonic()
        except Exception:
            BATCH_FAILURES.inc()
            logger.exception("batch failed; offsets remain uncommitted")
            time.sleep(2)
        finally:
            BATCH_DURATION.observe(time.monotonic() - started)

    logger.info(
        "persister started",
        extra={"topic": settings.input_topic, "group": settings.consumer_group},
    )
    try:
        while not stop.is_set():
            message = consumer.poll(0.5)
            now = time.monotonic()
            if message is None:
                if batch_messages and now - last_flush >= settings.persist_flush_seconds:
                    flush_batch()
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())

            CONSUMED.inc()
            partition_label = str(message.partition())
            LAST_OFFSET.labels(partition=partition_label).set(message.offset())
            try:
                _low, high = consumer.get_watermark_offsets(
                    TopicPartition(message.topic(), message.partition()),
                    timeout=1.0,
                    cached=True,
                )
                if high >= 0:
                    CONSUMER_LAG.labels(partition=partition_label).set(
                        max(high - message.offset() - 1, 0)
                    )
            except KafkaException:
                pass
            batch_messages.append(message)
            try:
                decoded = json.loads(message.value().decode("utf-8"))
                schema_validator.validate(decoded)
                reading = SensorReading.model_validate(decoded)
                flags = reading.quality_flags(
                    temperature_min=settings.comfort_temperature_min_c,
                    temperature_max=settings.comfort_temperature_max_c,
                    humidity_min=settings.comfort_humidity_min_pct,
                    humidity_max=settings.comfort_humidity_max_pct,
                    co2_max=settings.co2_warning_ppm,
                )
                valid.append(
                    PersistableReading(
                        reading=reading,
                        quality_flags=flags,
                        topic=message.topic(),
                        partition=message.partition(),
                        offset=message.offset(),
                    )
                )
            except (
                JsonSchemaValidationError,
                ValidationError,
                json.JSONDecodeError,
                UnicodeDecodeError,
                ValueError,
                TypeError,
            ) as exc:
                invalid.append((message, str(exc)))

            if (
                len(batch_messages) >= settings.persist_batch_size
                or now - last_flush >= settings.persist_flush_seconds
            ):
                flush_batch()
    finally:
        flush_batch()
        consumer.close()
        dlq_producer.flush(5)
        logger.info("persister stopped")


if __name__ == "__main__":
    main()
