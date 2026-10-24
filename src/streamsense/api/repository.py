from __future__ import annotations

from typing import Any, Literal

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool


class ReportingRepository:
    def __init__(self, pool: ConnectionPool) -> None:
        self.pool = pool

    def health(self) -> bool:
        with self.pool.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                return cursor.fetchone()[0] == 1

    def overview(self, window: Literal["1m", "5m"]) -> dict[str, Any]:
        table = "room_metrics_1m" if window == "1m" else "room_metrics_5m"
        sql = f"""
        WITH latest AS (SELECT max(window_end) AS window_end FROM {table})
        SELECT
            latest.window_end,
            count(metrics.room_id)::int AS room_count,
            coalesce(sum(metrics.sample_count), 0)::bigint AS sample_count,
            avg(metrics.avg_temperature_c)::double precision AS mean_temperature_c,
            avg(metrics.avg_humidity_pct)::double precision AS mean_humidity_pct,
            avg(metrics.avg_co2_ppm)::double precision AS mean_co2_ppm,
            count(*) FILTER (WHERE metrics.warning_count > 0)::int AS rooms_with_warnings
        FROM latest
        LEFT JOIN {table} AS metrics ON metrics.window_end = latest.window_end
        GROUP BY latest.window_end
        """
        with self.pool.connection() as connection:
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(sql)
                return dict(cursor.fetchone())

    def latest_rooms(self, limit: int = 200) -> list[dict[str, Any]]:
        sql = """
        SELECT DISTINCT ON (room_id)
            window_start, window_end, room_id, sample_count,
            avg_temperature_c, min_temperature_c, max_temperature_c,
            avg_humidity_pct, avg_co2_ppm, max_co2_ppm,
            warning_count, low_battery_count
        FROM room_metrics_1m
        ORDER BY room_id, window_end DESC
        LIMIT %s
        """
        with self.pool.connection() as connection:
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(sql, (limit,))
                return [dict(row) for row in cursor.fetchall()]

    def room_history(
        self,
        room_id: str,
        minutes: int,
        window: Literal["1m", "5m"],
    ) -> list[dict[str, Any]]:
        table = "room_metrics_1m" if window == "1m" else "room_metrics_5m"
        sql = f"""
        SELECT
            window_start, window_end, room_id, sample_count,
            avg_temperature_c, min_temperature_c, max_temperature_c,
            avg_humidity_pct, avg_co2_ppm, max_co2_ppm,
            warning_count, low_battery_count
        FROM {table}
        WHERE room_id = %s
          AND window_end >= now() - make_interval(mins => %s)
        ORDER BY window_end ASC
        """
        with self.pool.connection() as connection:
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(sql, (room_id, minutes))
                return [dict(row) for row in cursor.fetchall()]

    def alerts(self, limit: int, severity: str | None) -> list[dict[str, Any]]:
        sql = """
        SELECT window_start, window_end, room_id, alert_type, severity,
               observed_value, threshold_value, message, updated_at
        FROM room_alerts
        WHERE (%s IS NULL OR severity = %s)
        ORDER BY window_end DESC, room_id
        LIMIT %s
        """
        with self.pool.connection() as connection:
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(sql, (severity, severity, limit))
                return [dict(row) for row in cursor.fetchall()]

    def data_quality(self, minutes: int) -> dict[str, Any]:
        sql = """
        WITH accepted AS (
            SELECT
                count(*)::bigint AS accepted_rows,
                count(*) FILTER (WHERE quality_status = 'valid')::bigint AS valid_rows,
                count(*) FILTER (WHERE quality_status = 'warning')::bigint AS warning_rows
            FROM sensor_readings
            WHERE received_at >= now() - make_interval(mins => %s)
        ), rejected AS (
            SELECT count(*)::bigint AS rejected_rows
            FROM ingestion_rejections
            WHERE failed_at >= now() - make_interval(mins => %s)
        )
        SELECT accepted.accepted_rows, accepted.valid_rows, accepted.warning_rows,
               rejected.rejected_rows
        FROM accepted CROSS JOIN rejected
        """
        with self.pool.connection() as connection:
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(sql, (minutes, minutes))
                row = dict(cursor.fetchone())
        accepted = int(row["accepted_rows"])
        rejected = int(row["rejected_rows"])
        total = accepted + rejected
        row["period_minutes"] = minutes
        row["total_events"] = total
        row["warning_ratio"] = (int(row["warning_rows"]) / accepted) if accepted else 0.0
        row["rejection_ratio"] = (rejected / total) if total else 0.0
        return row
