from datetime import datetime, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError

from streamsense.common.models import SensorReading


def valid_reading() -> dict[str, object]:
    now = datetime.now(timezone.utc)
    return {
        "event_id": uuid4(),
        "schema_version": 1,
        "event_time": now,
        "emitted_at": now,
        "device_id": "sensor-0001",
        "room_id": "room-001",
        "sequence_no": 1,
        "temperature_c": 22.5,
        "humidity_pct": 45.0,
        "co2_ppm": 700,
        "battery_pct": 98.0,
        "status": "ok",
    }


def test_valid_reading_has_no_quality_flags() -> None:
    reading = SensorReading.model_validate(valid_reading())
    assert reading.quality_flags() == []


def test_warning_flags_are_deterministic() -> None:
    payload = valid_reading()
    payload.update({"temperature_c": 31.0, "co2_ppm": 1500, "battery_pct": 10.0})
    reading = SensorReading.model_validate(payload)
    assert reading.quality_flags() == ["temperature_high", "co2_high", "battery_low"]


def test_naive_timestamp_is_rejected() -> None:
    payload = valid_reading()
    payload["event_time"] = datetime.now()
    with pytest.raises(ValidationError):
        SensorReading.model_validate(payload)
