from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


class SensorReading(BaseModel):
    """Canonical v1 event used by the producer and persister."""

    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    schema_version: Literal[1] = 1
    event_time: datetime
    emitted_at: datetime
    device_id: str = Field(pattern=r"^sensor-[0-9]{4}$")
    room_id: str = Field(pattern=r"^room-[0-9]{3}$")
    sequence_no: int = Field(ge=0)
    temperature_c: float = Field(ge=-40, le=80)
    humidity_pct: float = Field(ge=0, le=100)
    co2_ppm: int = Field(ge=250, le=10_000)
    battery_pct: float = Field(ge=0, le=100)
    status: Literal["ok", "maintenance", "calibration"] = "ok"

    @field_validator("event_time", "emitted_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must include a timezone")
        return value

    def quality_flags(
        self,
        temperature_min: float = 18.0,
        temperature_max: float = 27.0,
        humidity_min: float = 30.0,
        humidity_max: float = 70.0,
        co2_max: int = 1200,
    ) -> list[str]:
        flags: list[str] = []
        if self.temperature_c < temperature_min:
            flags.append("temperature_low")
        elif self.temperature_c > temperature_max:
            flags.append("temperature_high")
        if self.humidity_pct < humidity_min:
            flags.append("humidity_low")
        elif self.humidity_pct > humidity_max:
            flags.append("humidity_high")
        if self.co2_ppm > co2_max:
            flags.append("co2_high")
        if self.battery_pct < 15:
            flags.append("battery_low")
        if self.status != "ok":
            flags.append(f"sensor_status_{self.status}")
        return flags
