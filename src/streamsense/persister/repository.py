from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

import psycopg

from streamsense.common.models import SensorReading


INSERT_READING_SQL = """
INSERT INTO sensor_readings (
    event_id, schema_version, event_time, emitted_at, device_id, room_id,
    sequence_no, temperature_c, humidity_pct, co2_ppm, battery_pct, status,
    quality_status, quality_flags, kafka_topic, kafka_partition, kafka_offset
) VALUES (
    %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s,
    %s, %s::jsonb, %s, %s, %s
)
ON CONFLICT DO NOTHING
"""

INSERT_REJECTION_SQL = """
INSERT INTO ingestion_rejections (
    kafka_topic, kafka_partition, kafka_offset, failed_at, reason, payload_sha256
) VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO UPDATE SET
    failed_at = EXCLUDED.failed_at,
    reason = EXCLUDED.reason,
    payload_sha256 = EXCLUDED.payload_sha256
"""


@dataclass(frozen=True)
class PersistableReading:
    reading: SensorReading
    quality_flags: list[str]
    topic: str
    partition: int
    offset: int

    def as_db_tuple(self) -> tuple[object, ...]:
        r = self.reading
        quality_status = "warning" if self.quality_flags else "valid"
        return (
            r.event_id,
            r.schema_version,
            r.event_time,
            r.emitted_at,
            r.device_id,
            r.room_id,
            r.sequence_no,
            r.temperature_c,
            r.humidity_pct,
            r.co2_ppm,
            r.battery_pct,
            r.status,
            quality_status,
            json.dumps(self.quality_flags),
            self.topic,
            self.partition,
            self.offset,
        )


@dataclass(frozen=True)
class RejectedEvent:
    topic: str
    partition: int
    offset: int
    failed_at: datetime
    reason: str
    payload_sha256: str

    def as_db_tuple(self) -> tuple[object, ...]:
        return (
            self.topic,
            self.partition,
            self.offset,
            self.failed_at,
            self.reason[:4000],
            self.payload_sha256,
        )


@dataclass(frozen=True)
class PersistResult:
    accepted_rows: int
    rejection_rows: int


def persist_batch(
    database_url: str,
    readings: Sequence[PersistableReading],
    rejections: Sequence[RejectedEvent] = (),
) -> PersistResult:
    if not readings and not rejections:
        return PersistResult(0, 0)
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            accepted_rows = 0
            rejection_rows = 0
            if readings:
                cursor.executemany(
                    INSERT_READING_SQL,
                    [item.as_db_tuple() for item in readings],
                )
                accepted_rows = max(cursor.rowcount, 0)
            if rejections:
                cursor.executemany(
                    INSERT_REJECTION_SQL,
                    [item.as_db_tuple() for item in rejections],
                )
                rejection_rows = max(cursor.rowcount, 0)
        connection.commit()
    return PersistResult(accepted_rows, rejection_rows)
