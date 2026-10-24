BEGIN;

CREATE TABLE IF NOT EXISTS schema_versions (
    schema_name text NOT NULL,
    schema_version integer NOT NULL,
    status text NOT NULL CHECK (status IN ('active', 'deprecated')),
    description text NOT NULL,
    registered_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (schema_name, schema_version)
);

INSERT INTO schema_versions (schema_name, schema_version, status, description)
VALUES (
    'sensor-reading', 1, 'active',
    'Timestamped indoor-air sensor event validated against sensor-reading-v1.json'
)
ON CONFLICT (schema_name, schema_version) DO NOTHING;

CREATE TABLE IF NOT EXISTS sensor_readings (
    event_id uuid PRIMARY KEY,
    schema_version smallint NOT NULL,
    event_time timestamptz NOT NULL,
    emitted_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    device_id text NOT NULL,
    room_id text NOT NULL,
    sequence_no bigint NOT NULL CHECK (sequence_no >= 0),
    temperature_c double precision NOT NULL,
    humidity_pct double precision NOT NULL,
    co2_ppm integer NOT NULL,
    battery_pct double precision NOT NULL,
    status text NOT NULL,
    quality_status text NOT NULL CHECK (quality_status IN ('valid', 'warning')),
    quality_flags jsonb NOT NULL DEFAULT '[]'::jsonb,
    kafka_topic text NOT NULL,
    kafka_partition integer NOT NULL,
    kafka_offset bigint NOT NULL,
    UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS idx_sensor_readings_room_time
    ON sensor_readings (room_id, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_sensor_readings_event_time_brin
    ON sensor_readings USING brin (event_time) WITH (pages_per_range = 64);
CREATE INDEX IF NOT EXISTS idx_sensor_readings_quality
    ON sensor_readings (quality_status, event_time DESC);

CREATE TABLE IF NOT EXISTS ingestion_rejections (
    kafka_topic text NOT NULL,
    kafka_partition integer NOT NULL,
    kafka_offset bigint NOT NULL,
    failed_at timestamptz NOT NULL,
    reason text NOT NULL,
    payload_sha256 char(64) NOT NULL,
    PRIMARY KEY (kafka_topic, kafka_partition, kafka_offset)
);
CREATE INDEX IF NOT EXISTS idx_ingestion_rejections_failed_at
    ON ingestion_rejections (failed_at DESC);

CREATE TABLE IF NOT EXISTS room_metrics_1m (
    window_start timestamptz NOT NULL,
    window_end timestamptz NOT NULL,
    room_id text NOT NULL,
    sample_count bigint NOT NULL,
    avg_temperature_c double precision NOT NULL,
    min_temperature_c double precision NOT NULL,
    max_temperature_c double precision NOT NULL,
    avg_humidity_pct double precision NOT NULL,
    min_humidity_pct double precision NOT NULL,
    max_humidity_pct double precision NOT NULL,
    avg_co2_ppm double precision NOT NULL,
    max_co2_ppm integer NOT NULL,
    low_battery_count bigint NOT NULL,
    warning_count bigint NOT NULL,
    refreshed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, room_id)
);
CREATE INDEX IF NOT EXISTS idx_room_metrics_1m_end
    ON room_metrics_1m (window_end DESC, room_id);

CREATE TABLE IF NOT EXISTS room_metrics_5m (
    window_start timestamptz NOT NULL,
    window_end timestamptz NOT NULL,
    room_id text NOT NULL,
    sample_count bigint NOT NULL,
    avg_temperature_c double precision NOT NULL,
    min_temperature_c double precision NOT NULL,
    max_temperature_c double precision NOT NULL,
    avg_humidity_pct double precision NOT NULL,
    min_humidity_pct double precision NOT NULL,
    max_humidity_pct double precision NOT NULL,
    avg_co2_ppm double precision NOT NULL,
    max_co2_ppm integer NOT NULL,
    low_battery_count bigint NOT NULL,
    warning_count bigint NOT NULL,
    refreshed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (window_end, room_id)
);
CREATE INDEX IF NOT EXISTS idx_room_metrics_5m_end
    ON room_metrics_5m (window_end DESC, room_id);

CREATE TABLE IF NOT EXISTS room_alerts (
    alert_key text PRIMARY KEY,
    window_start timestamptz NOT NULL,
    window_end timestamptz NOT NULL,
    room_id text NOT NULL,
    alert_type text NOT NULL,
    severity text NOT NULL CHECK (severity IN ('warning', 'critical')),
    observed_value double precision NOT NULL,
    threshold_value double precision NOT NULL,
    message text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_room_alerts_latest
    ON room_alerts (window_end DESC, severity, room_id);

CREATE TABLE IF NOT EXISTS pipeline_checkpoints (
    component text PRIMARY KEY,
    watermark timestamptz,
    last_success_at timestamptz,
    last_status text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE OR REPLACE FUNCTION refresh_room_metrics(
    p_from timestamptz,
    p_to timestamptz,
    p_temperature_min double precision,
    p_temperature_max double precision,
    p_humidity_min double precision,
    p_humidity_max double precision,
    p_co2_max integer
)
RETURNS TABLE(one_minute_rows bigint, five_minute_rows bigint, alert_rows bigint)
LANGUAGE plpgsql
AS $$
DECLARE
    v_one bigint := 0;
    v_five bigint := 0;
    v_alerts bigint := 0;
BEGIN
    IF p_from >= p_to THEN
        RAISE EXCEPTION 'p_from must be earlier than p_to';
    END IF;

    INSERT INTO room_metrics_1m (
        window_start, window_end, room_id, sample_count,
        avg_temperature_c, min_temperature_c, max_temperature_c,
        avg_humidity_pct, min_humidity_pct, max_humidity_pct,
        avg_co2_ppm, max_co2_ppm, low_battery_count, warning_count, refreshed_at
    )
    SELECT
        date_trunc('minute', event_time) AS window_start,
        date_trunc('minute', event_time) + interval '1 minute' AS window_end,
        room_id,
        count(*) AS sample_count,
        avg(temperature_c), min(temperature_c), max(temperature_c),
        avg(humidity_pct), min(humidity_pct), max(humidity_pct),
        avg(co2_ppm), max(co2_ppm),
        count(*) FILTER (WHERE battery_pct < 15),
        count(*) FILTER (WHERE quality_status = 'warning'),
        now()
    FROM sensor_readings
    WHERE event_time >= p_from
      AND event_time < p_to
    GROUP BY date_trunc('minute', event_time), room_id
    ON CONFLICT (window_start, room_id) DO UPDATE SET
        window_end = EXCLUDED.window_end,
        sample_count = EXCLUDED.sample_count,
        avg_temperature_c = EXCLUDED.avg_temperature_c,
        min_temperature_c = EXCLUDED.min_temperature_c,
        max_temperature_c = EXCLUDED.max_temperature_c,
        avg_humidity_pct = EXCLUDED.avg_humidity_pct,
        min_humidity_pct = EXCLUDED.min_humidity_pct,
        max_humidity_pct = EXCLUDED.max_humidity_pct,
        avg_co2_ppm = EXCLUDED.avg_co2_ppm,
        max_co2_ppm = EXCLUDED.max_co2_ppm,
        low_battery_count = EXCLUDED.low_battery_count,
        warning_count = EXCLUDED.warning_count,
        refreshed_at = now();
    GET DIAGNOSTICS v_one = ROW_COUNT;

    WITH window_ends AS (
        SELECT DISTINCT window_end
        FROM room_metrics_1m
        WHERE window_end > date_trunc('minute', p_from)
          AND window_end <= date_trunc('minute', p_to)
    ), calculated AS (
        SELECT
            e.window_end - interval '5 minutes' AS window_start,
            e.window_end,
            m.room_id,
            sum(m.sample_count)::bigint AS sample_count,
            sum(m.avg_temperature_c * m.sample_count) / nullif(sum(m.sample_count), 0) AS avg_temperature_c,
            min(m.min_temperature_c) AS min_temperature_c,
            max(m.max_temperature_c) AS max_temperature_c,
            sum(m.avg_humidity_pct * m.sample_count) / nullif(sum(m.sample_count), 0) AS avg_humidity_pct,
            min(m.min_humidity_pct) AS min_humidity_pct,
            max(m.max_humidity_pct) AS max_humidity_pct,
            sum(m.avg_co2_ppm * m.sample_count) / nullif(sum(m.sample_count), 0) AS avg_co2_ppm,
            max(m.max_co2_ppm) AS max_co2_ppm,
            sum(m.low_battery_count)::bigint AS low_battery_count,
            sum(m.warning_count)::bigint AS warning_count
        FROM window_ends e
        JOIN room_metrics_1m m
          ON m.window_start >= e.window_end - interval '5 minutes'
         AND m.window_end <= e.window_end
        GROUP BY e.window_end, m.room_id
    )
    INSERT INTO room_metrics_5m (
        window_start, window_end, room_id, sample_count,
        avg_temperature_c, min_temperature_c, max_temperature_c,
        avg_humidity_pct, min_humidity_pct, max_humidity_pct,
        avg_co2_ppm, max_co2_ppm, low_battery_count, warning_count, refreshed_at
    )
    SELECT
        window_start, window_end, room_id, sample_count,
        avg_temperature_c, min_temperature_c, max_temperature_c,
        avg_humidity_pct, min_humidity_pct, max_humidity_pct,
        avg_co2_ppm, max_co2_ppm, low_battery_count, warning_count, now()
    FROM calculated
    ON CONFLICT (window_end, room_id) DO UPDATE SET
        window_start = EXCLUDED.window_start,
        sample_count = EXCLUDED.sample_count,
        avg_temperature_c = EXCLUDED.avg_temperature_c,
        min_temperature_c = EXCLUDED.min_temperature_c,
        max_temperature_c = EXCLUDED.max_temperature_c,
        avg_humidity_pct = EXCLUDED.avg_humidity_pct,
        min_humidity_pct = EXCLUDED.min_humidity_pct,
        max_humidity_pct = EXCLUDED.max_humidity_pct,
        avg_co2_ppm = EXCLUDED.avg_co2_ppm,
        max_co2_ppm = EXCLUDED.max_co2_ppm,
        low_battery_count = EXCLUDED.low_battery_count,
        warning_count = EXCLUDED.warning_count,
        refreshed_at = now();
    GET DIAGNOSTICS v_five = ROW_COUNT;

    DELETE FROM room_alerts
    WHERE window_start >= date_trunc('minute', p_from)
      AND window_start < date_trunc('minute', p_to);

    WITH candidates AS (
        SELECT window_start, window_end, room_id,
               'co2_high'::text AS alert_type,
               CASE WHEN avg_co2_ppm >= p_co2_max * 1.5 THEN 'critical' ELSE 'warning' END AS severity,
               avg_co2_ppm AS observed_value,
               p_co2_max::double precision AS threshold_value,
               format('Average CO2 concentration is %s ppm', round(avg_co2_ppm)::text) AS message
        FROM room_metrics_1m
        WHERE window_start >= date_trunc('minute', p_from)
          AND window_start < date_trunc('minute', p_to)
          AND avg_co2_ppm > p_co2_max
        UNION ALL
        SELECT window_start, window_end, room_id,
               'temperature_high',
               CASE WHEN avg_temperature_c >= p_temperature_max + 5 THEN 'critical' ELSE 'warning' END,
               avg_temperature_c, p_temperature_max,
               format('Average temperature is %s C', round(avg_temperature_c::numeric, 1)::text)
        FROM room_metrics_1m
        WHERE window_start >= date_trunc('minute', p_from)
          AND window_start < date_trunc('minute', p_to)
          AND avg_temperature_c > p_temperature_max
        UNION ALL
        SELECT window_start, window_end, room_id,
               'temperature_low',
               CASE WHEN avg_temperature_c <= p_temperature_min - 5 THEN 'critical' ELSE 'warning' END,
               avg_temperature_c, p_temperature_min,
               format('Average temperature is %s C', round(avg_temperature_c::numeric, 1)::text)
        FROM room_metrics_1m
        WHERE window_start >= date_trunc('minute', p_from)
          AND window_start < date_trunc('minute', p_to)
          AND avg_temperature_c < p_temperature_min
        UNION ALL
        SELECT window_start, window_end, room_id,
               'humidity_high', 'warning', avg_humidity_pct, p_humidity_max,
               format('Average humidity is %s percent', round(avg_humidity_pct::numeric, 1)::text)
        FROM room_metrics_1m
        WHERE window_start >= date_trunc('minute', p_from)
          AND window_start < date_trunc('minute', p_to)
          AND avg_humidity_pct > p_humidity_max
        UNION ALL
        SELECT window_start, window_end, room_id,
               'humidity_low', 'warning', avg_humidity_pct, p_humidity_min,
               format('Average humidity is %s percent', round(avg_humidity_pct::numeric, 1)::text)
        FROM room_metrics_1m
        WHERE window_start >= date_trunc('minute', p_from)
          AND window_start < date_trunc('minute', p_to)
          AND avg_humidity_pct < p_humidity_min
    )
    INSERT INTO room_alerts (
        alert_key, window_start, window_end, room_id, alert_type, severity,
        observed_value, threshold_value, message, created_at, updated_at
    )
    SELECT
        room_id || '|' || window_start::text || '|' || alert_type,
        window_start, window_end, room_id, alert_type, severity,
        observed_value, threshold_value, message, now(), now()
    FROM candidates
    ON CONFLICT (alert_key) DO UPDATE SET
        severity = EXCLUDED.severity,
        observed_value = EXCLUDED.observed_value,
        threshold_value = EXCLUDED.threshold_value,
        message = EXCLUDED.message,
        updated_at = now();
    GET DIAGNOSTICS v_alerts = ROW_COUNT;

    INSERT INTO pipeline_checkpoints (component, watermark, last_success_at, last_status, metadata)
    VALUES (
        'window-aggregator', p_to, now(), 'success',
        jsonb_build_object(
            'from', p_from,
            'one_minute_rows', v_one,
            'five_minute_rows', v_five,
            'alert_rows', v_alerts
        )
    )
    ON CONFLICT (component) DO UPDATE SET
        watermark = EXCLUDED.watermark,
        last_success_at = EXCLUDED.last_success_at,
        last_status = EXCLUDED.last_status,
        metadata = EXCLUDED.metadata;

    RETURN QUERY SELECT v_one, v_five, v_alerts;
END;
$$;

COMMIT;
