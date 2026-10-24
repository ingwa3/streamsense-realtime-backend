from __future__ import annotations

from datetime import datetime, timedelta, timezone


def floor_to_minute(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("value must be timezone-aware")
    return value.astimezone(timezone.utc).replace(second=0, microsecond=0)


def tumbling_minute_bounds(value: datetime) -> tuple[datetime, datetime]:
    start = floor_to_minute(value)
    return start, start + timedelta(minutes=1)


def sliding_five_minute_bounds(window_end: datetime) -> tuple[datetime, datetime]:
    end = floor_to_minute(window_end)
    return end - timedelta(minutes=5), end
