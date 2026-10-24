from datetime import datetime, timezone

from streamsense.producer.generator import SensorGenerator


def test_generator_produces_valid_ordered_events() -> None:
    generator = SensorGenerator(
        sensor_count=10,
        room_count=2,
        events_per_second=100,
        producer_instance="test",
        seed=7,
        start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    first = generator.next_reading()
    second = generator.next_reading()
    assert first.device_id == "sensor-0001"
    assert second.device_id == "sensor-0002"
    assert second.event_time > first.event_time
    assert first.room_id == "room-001"
    assert second.room_id == "room-002"


def test_event_ids_are_reproducible() -> None:
    kwargs = dict(
        sensor_count=2,
        room_count=1,
        events_per_second=10,
        producer_instance="same",
        seed=1,
        start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    assert SensorGenerator(**kwargs).next_reading().event_id == SensorGenerator(**kwargs).next_reading().event_id


def test_generator_can_simulate_late_events_without_changing_sensor_values() -> None:
    kwargs = dict(
        sensor_count=2,
        room_count=1,
        events_per_second=10,
        producer_instance="late-test",
        seed=5,
        start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    regular = SensorGenerator(**kwargs).next_reading()
    late = SensorGenerator(
        **kwargs,
        late_event_probability=1.0,
        late_event_max_seconds=30,
    ).next_reading()
    assert late.event_time < regular.event_time
    assert late.temperature_c == regular.temperature_c
    assert late.humidity_pct == regular.humidity_pct
    assert late.co2_ppm == regular.co2_ppm
