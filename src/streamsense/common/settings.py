from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SettingsBase(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=None,
        case_sensitive=False,
        extra="ignore",
    )

    log_level: str = "INFO"


class ProducerSettings(SettingsBase):
    kafka_bootstrap_servers: str = "broker:19092"
    input_topic: str = "sensor-readings.v1"
    producer_instance: str = "simulator-01"
    sensor_count: int = Field(default=500, ge=1, le=9_999)
    room_count: int = Field(default=50, ge=1, le=999)
    events_per_second: int = Field(default=250, ge=1, le=1_000_000)
    total_events: int = Field(default=0, ge=0)
    burst_mode: bool = False
    random_seed: int = 42
    late_event_probability: float = Field(default=0.02, ge=0.0, le=1.0)
    late_event_max_seconds: int = Field(default=90, ge=0, le=3600)
    metrics_port: int = 9101


class PersisterSettings(SettingsBase):
    kafka_bootstrap_servers: str = "broker:19092"
    input_topic: str = "sensor-readings.v1"
    dlq_topic: str = "sensor-readings.dlq.v1"
    consumer_group: str = "streamsense-persister-v2"
    database_url: str
    schema_path: str = "/app/schemas/sensor-reading-v1.json"
    persist_batch_size: int = Field(default=1000, ge=1, le=10_000)
    persist_flush_seconds: float = Field(default=1.0, gt=0, le=60)
    metrics_port: int = 9102
    comfort_temperature_min_c: float = 18.0
    comfort_temperature_max_c: float = 27.0
    comfort_humidity_min_pct: float = 30.0
    comfort_humidity_max_pct: float = 70.0
    co2_warning_ppm: int = 1200


class AggregatorSettings(SettingsBase):
    database_url: str
    aggregation_interval_seconds: int = Field(default=5, ge=1, le=300)
    allowed_lateness_seconds: int = Field(default=15, ge=0, le=3600)
    aggregation_lookback_minutes: int = Field(default=10, ge=5, le=1440)
    raw_retention_days: int = Field(default=30, ge=1)
    aggregate_retention_days: int = Field(default=365, ge=1)
    comfort_temperature_min_c: float = 18.0
    comfort_temperature_max_c: float = 27.0
    comfort_humidity_min_pct: float = 30.0
    comfort_humidity_max_pct: float = 70.0
    co2_warning_ppm: int = 1200
    metrics_port: int = 9103


class ApiSettings(SettingsBase):
    database_url: str
    reporting_api_key: str = Field(min_length=12)
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    db_pool_min_size: int = 1
    db_pool_max_size: int = 10
