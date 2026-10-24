from __future__ import annotations

import logging
import signal
import time
from datetime import datetime, timedelta, timezone
from threading import Event

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from streamsense.aggregator.repository import apply_retention, refresh_aggregates
from streamsense.common.logging import configure_logging
from streamsense.common.settings import AggregatorSettings
from streamsense.common.windowing import floor_to_minute


RUNS = Counter("streamsense_aggregator_runs_total", "Aggregation runs", ["status"])
ROWS = Counter("streamsense_aggregator_rows_total", "Rows refreshed", ["window"])
RUN_DURATION = Histogram("streamsense_aggregator_run_seconds", "Aggregation run duration")
WATERMARK = Gauge("streamsense_aggregator_watermark_unixtime", "Current event-time watermark")
LAST_SUCCESS = Gauge("streamsense_aggregator_last_success_unixtime", "Last successful run")


def main() -> None:
    settings = AggregatorSettings()
    configure_logging("aggregator", settings.log_level)
    logger = logging.getLogger(__name__)
    stop = Event()

    def request_stop(signum: int, _frame: object) -> None:
        logger.info("shutdown requested", extra={"signal": signum})
        stop.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    start_http_server(settings.metrics_port)

    next_retention = 0.0
    logger.info(
        "aggregator started",
        extra={
            "interval_seconds": settings.aggregation_interval_seconds,
            "allowed_lateness_seconds": settings.allowed_lateness_seconds,
            "lookback_minutes": settings.aggregation_lookback_minutes,
        },
    )

    while not stop.is_set():
        started = time.monotonic()
        try:
            watermark = floor_to_minute(
                datetime.now(timezone.utc)
                - timedelta(seconds=settings.allowed_lateness_seconds)
            )
            start = watermark - timedelta(minutes=settings.aggregation_lookback_minutes)
            WATERMARK.set(watermark.timestamp())
            result = refresh_aggregates(
                settings.database_url,
                start,
                watermark,
                settings.comfort_temperature_min_c,
                settings.comfort_temperature_max_c,
                settings.comfort_humidity_min_pct,
                settings.comfort_humidity_max_pct,
                settings.co2_warning_ppm,
            )
            if result is None:
                RUNS.labels(status="standby").inc()
                logger.info("another aggregator instance holds the advisory lock")
            else:
                RUNS.labels(status="success").inc()
                ROWS.labels(window="1m").inc(result.one_minute_rows)
                ROWS.labels(window="5m").inc(result.five_minute_rows)
                LAST_SUCCESS.set(time.time())
                logger.info(
                    "aggregates refreshed",
                    extra={
                        "from": start.isoformat(),
                        "to": watermark.isoformat(),
                        "one_minute_rows": result.one_minute_rows,
                        "five_minute_rows": result.five_minute_rows,
                        "alert_rows": result.alert_rows,
                    },
                )

            if time.monotonic() >= next_retention:
                deleted = apply_retention(
                    settings.database_url,
                    settings.raw_retention_days,
                    settings.aggregate_retention_days,
                )
                logger.info(
                    "retention applied",
                    extra={
                        "raw_deleted": deleted[0],
                        "one_minute_deleted": deleted[1],
                        "five_minute_deleted": deleted[2],
                    },
                )
                next_retention = time.monotonic() + 3600
        except Exception:
            RUNS.labels(status="failure").inc()
            logger.exception("aggregation run failed")
        finally:
            duration = time.monotonic() - started
            RUN_DURATION.observe(duration)
            stop.wait(max(0.0, settings.aggregation_interval_seconds - duration))

    logger.info("aggregator stopped")


if __name__ == "__main__":
    main()
