from __future__ import annotations

import logging
import signal
import time
from datetime import datetime, timedelta, timezone
from threading import Event

from confluent_kafka import KafkaError, Producer
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from streamsense.common.logging import configure_logging
from streamsense.common.settings import ProducerSettings
from streamsense.producer.generator import SensorGenerator


EVENTS_SENT = Counter("streamsense_producer_events_sent_total", "Successfully delivered events")
DELIVERY_ERRORS = Counter("streamsense_producer_delivery_errors_total", "Kafka delivery errors")
PRODUCE_LATENCY = Histogram("streamsense_producer_produce_seconds", "Time spent enqueueing events")
LAST_EVENT_TIME = Gauge("streamsense_producer_last_event_unixtime", "Event-time of latest event")
EVENT_TIME_DELAY = Histogram(
    "streamsense_producer_event_time_delay_seconds",
    "Difference between emission time and event time",
    buckets=(0.1, 1, 5, 15, 30, 60, 120, 300, 900, 3600),
)


def main() -> None:
    settings = ProducerSettings()
    configure_logging("producer", settings.log_level)
    logger = logging.getLogger(__name__)
    stop = Event()

    def request_stop(signum: int, _frame: object) -> None:
        logger.info("shutdown requested", extra={"signal": signum})
        stop.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    start_http_server(settings.metrics_port)

    if settings.burst_mode and settings.total_events > 0:
        simulated_span = settings.total_events / settings.events_per_second
        start_time = datetime.now(timezone.utc) - timedelta(seconds=simulated_span)
    else:
        start_time = datetime.now(timezone.utc)

    generator = SensorGenerator(
        sensor_count=settings.sensor_count,
        room_count=settings.room_count,
        events_per_second=settings.events_per_second,
        producer_instance=settings.producer_instance,
        seed=settings.random_seed,
        start_time=start_time,
        late_event_probability=settings.late_event_probability,
        late_event_max_seconds=settings.late_event_max_seconds,
    )

    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "client.id": settings.producer_instance,
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "lz4",
            "linger.ms": 20,
            "batch.num.messages": 10_000,
            "message.timeout.ms": 30_000,
        }
    )

    delivery_failures = 0

    def delivered(error: KafkaError | None, _message: object) -> None:
        nonlocal delivery_failures
        if error is not None:
            delivery_failures += 1
            DELIVERY_ERRORS.inc()
            logger.error("event delivery failed", extra={"error": str(error)})
        else:
            EVENTS_SENT.inc()

    logger.info(
        "producer started",
        extra={
            "topic": settings.input_topic,
            "events_per_second": settings.events_per_second,
            "total_events": settings.total_events,
            "burst_mode": settings.burst_mode,
            "sensor_count": settings.sensor_count,
            "late_event_probability": settings.late_event_probability,
            "late_event_max_seconds": settings.late_event_max_seconds,
        },
    )

    next_deadline = time.monotonic()
    produced = 0
    while not stop.is_set() and (settings.total_events == 0 or produced < settings.total_events):
        reading = generator.next_reading()
        payload = reading.model_dump_json()
        with PRODUCE_LATENCY.time():
            while not stop.is_set():
                try:
                    producer.produce(
                        settings.input_topic,
                        key=reading.room_id.encode("utf-8"),
                        value=payload.encode("utf-8"),
                        on_delivery=delivered,
                    )
                    break
                except BufferError:
                    producer.poll(0.1)
        producer.poll(0)
        LAST_EVENT_TIME.set(reading.event_time.timestamp())
        EVENT_TIME_DELAY.observe(max((reading.emitted_at - reading.event_time).total_seconds(), 0.0))
        produced += 1

        if produced % 10_000 == 0:
            logger.info("producer progress", extra={"events_enqueued": produced})

        if not settings.burst_mode:
            next_deadline += 1.0 / settings.events_per_second
            remaining = next_deadline - time.monotonic()
            if remaining > 0:
                stop.wait(remaining)
            elif remaining < -1.0:
                next_deadline = time.monotonic()

    remaining = producer.flush(30)
    logger.info(
        "producer stopped",
        extra={
            "events_enqueued": produced,
            "undelivered_events": remaining,
            "delivery_failures": delivery_failures,
        },
    )
    if remaining or delivery_failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
