from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel


class RoomMetric(BaseModel):
    window_start: datetime
    window_end: datetime
    room_id: str
    sample_count: int
    avg_temperature_c: float
    min_temperature_c: float
    max_temperature_c: float
    avg_humidity_pct: float
    avg_co2_ppm: float
    max_co2_ppm: int
    warning_count: int
    low_battery_count: int


class Alert(BaseModel):
    window_start: datetime
    window_end: datetime
    room_id: str
    alert_type: str
    severity: Literal["warning", "critical"]
    observed_value: float
    threshold_value: float
    message: str
    updated_at: datetime


class Overview(BaseModel):
    window_end: datetime | None
    room_count: int
    sample_count: int
    mean_temperature_c: float | None
    mean_humidity_pct: float | None
    mean_co2_ppm: float | None
    rooms_with_warnings: int


class DataQualitySummary(BaseModel):
    period_minutes: int
    accepted_rows: int
    valid_rows: int
    warning_rows: int
    rejected_rows: int
    total_events: int
    warning_ratio: float
    rejection_ratio: float
