from __future__ import annotations

import math
import random
from datetime import datetime, timedelta, timezone
from uuid import NAMESPACE_URL, uuid5

from streamsense.common.models import SensorReading


class SensorGenerator:
    """Deterministic event generator with realistic, bounded variations."""

    def __init__(
        self,
        sensor_count: int,
        room_count: int,
        events_per_second: int,
        producer_instance: str,
        seed: int = 42,
        start_time: datetime | None = None,
        late_event_probability: float = 0.0,
        late_event_max_seconds: int = 0,
    ) -> None:
        if sensor_count < 1 or room_count < 1 or events_per_second < 1:
            raise ValueError("counts and event rate must be positive")
        if not 0.0 <= late_event_probability <= 1.0:
            raise ValueError("late_event_probability must be between 0 and 1")
        if late_event_max_seconds < 0:
            raise ValueError("late_event_max_seconds must not be negative")
        self.sensor_count = sensor_count
        self.room_count = room_count
        self.events_per_second = events_per_second
        self.producer_instance = producer_instance
        self.random = random.Random(seed)
        # Separate RNG: simulated lateness does not change the sensor values.
        self.late_random = random.Random(seed + 10_000_019)
        self.late_event_probability = late_event_probability
        self.late_event_max_seconds = late_event_max_seconds
        self.start_time = start_time or datetime.now(timezone.utc)
        if self.start_time.tzinfo is None:
            raise ValueError("start_time must be timezone-aware")
        self.total_generated = 0
        self._sequence = [0] * sensor_count
        self._room_temp = [20.0 + self.random.random() * 4.0 for _ in range(room_count)]
        self._room_humidity = [38.0 + self.random.random() * 18.0 for _ in range(room_count)]
        self._room_co2 = [520 + self.random.randint(0, 350) for _ in range(room_count)]

    def next_reading(self) -> SensorReading:
        sensor_index = self.total_generated % self.sensor_count
        room_index = sensor_index % self.room_count
        sequence_no = self._sequence[sensor_index]
        self._sequence[sensor_index] += 1

        event_time = self.start_time + timedelta(
            seconds=self.total_generated / self.events_per_second
        )
        if (
            self.late_event_max_seconds > 0
            and self.late_random.random() < self.late_event_probability
        ):
            event_time -= timedelta(
                seconds=self.late_random.randint(1, self.late_event_max_seconds)
            )
        phase = self.total_generated / max(self.events_per_second, 1)
        daily_wave = math.sin(phase / 180.0)
        occupancy_wave = max(0.0, math.sin(phase / 45.0))

        temperature = self._room_temp[room_index] + 1.8 * daily_wave + self.random.gauss(0, 0.18)
        humidity = self._room_humidity[room_index] - 2.5 * daily_wave + self.random.gauss(0, 0.6)
        co2 = self._room_co2[room_index] + int(550 * occupancy_wave) + self.random.randint(-35, 35)

        # Rare deterministic-looking anomalies make the alert path observable.
        if self.total_generated > 0 and self.total_generated % 50_000 == 0:
            co2 += 900
        if self.total_generated > 0 and self.total_generated % 75_000 == 0:
            temperature += 8.0

        battery = max(2.0, 100.0 - (sequence_no / 80_000.0) + self.random.gauss(0, 0.05))
        status = "maintenance" if sequence_no > 0 and sequence_no % 120_000 == 0 else "ok"

        device_id = f"sensor-{sensor_index + 1:04d}"
        room_id = f"room-{room_index + 1:03d}"
        identity = (
            f"{self.producer_instance}|{device_id}|{sequence_no}|{event_time.isoformat()}"
        )

        reading = SensorReading(
            event_id=uuid5(NAMESPACE_URL, identity),
            schema_version=1,
            event_time=event_time,
            emitted_at=datetime.now(timezone.utc),
            device_id=device_id,
            room_id=room_id,
            sequence_no=sequence_no,
            temperature_c=round(min(79.0, max(-39.0, temperature)), 2),
            humidity_pct=round(min(99.9, max(0.1, humidity)), 2),
            co2_ppm=max(250, min(9_999, co2)),
            battery_pct=round(min(100.0, battery), 2),
            status=status,
        )
        self.total_generated += 1
        return reading
