from datetime import datetime, timezone

import pytest

from streamsense.common.windowing import (
    floor_to_minute,
    sliding_five_minute_bounds,
    tumbling_minute_bounds,
)


def test_tumbling_minute_bounds() -> None:
    value = datetime(2026, 5, 1, 12, 34, 56, 123456, tzinfo=timezone.utc)
    start, end = tumbling_minute_bounds(value)
    assert start.isoformat() == "2026-05-01T12:34:00+00:00"
    assert end.isoformat() == "2026-05-01T12:35:00+00:00"


def test_sliding_five_minute_bounds() -> None:
    value = datetime(2026, 5, 1, 12, 35, tzinfo=timezone.utc)
    start, end = sliding_five_minute_bounds(value)
    assert start.isoformat() == "2026-05-01T12:30:00+00:00"
    assert end == value


def test_naive_datetime_is_rejected() -> None:
    with pytest.raises(ValueError):
        floor_to_minute(datetime(2026, 1, 1))
