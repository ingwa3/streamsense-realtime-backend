from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import psycopg


ADVISORY_LOCK_ID = 8_214_006_337


@dataclass(frozen=True)
class RefreshResult:
    one_minute_rows: int
    five_minute_rows: int
    alert_rows: int


def refresh_aggregates(
    database_url: str,
    start: datetime,
    end: datetime,
    temperature_min: float,
    temperature_max: float,
    humidity_min: float,
    humidity_max: float,
    co2_max: int,
) -> RefreshResult | None:
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_try_advisory_lock(%s)", (ADVISORY_LOCK_ID,))
            locked = bool(cursor.fetchone()[0])
            if not locked:
                return None
            cursor.execute(
                "SELECT one_minute_rows, five_minute_rows, alert_rows "
                "FROM refresh_room_metrics(%s, %s, %s, %s, %s, %s, %s)",
                (
                    start,
                    end,
                    temperature_min,
                    temperature_max,
                    humidity_min,
                    humidity_max,
                    co2_max,
                ),
            )
            row = cursor.fetchone()
            connection.commit()
            return RefreshResult(int(row[0]), int(row[1]), int(row[2]))


def apply_retention(
    database_url: str,
    raw_retention_days: int,
    aggregate_retention_days: int,
) -> tuple[int, int, int]:
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM sensor_readings "
                "WHERE event_time < now() - make_interval(days => %s)",
                (raw_retention_days,),
            )
            raw_deleted = max(cursor.rowcount, 0)
            cursor.execute(
                "DELETE FROM room_metrics_1m "
                "WHERE window_start < now() - make_interval(days => %s)",
                (aggregate_retention_days,),
            )
            one_deleted = max(cursor.rowcount, 0)
            cursor.execute(
                "DELETE FROM room_metrics_5m "
                "WHERE window_start < now() - make_interval(days => %s)",
                (aggregate_retention_days,),
            )
            five_deleted = max(cursor.rowcount, 0)
        connection.commit()
    return raw_deleted, one_deleted, five_deleted
