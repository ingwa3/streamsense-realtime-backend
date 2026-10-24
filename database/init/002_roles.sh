#!/bin/sh
set -eu

: "${POSTGRES_DB:?POSTGRES_DB is required}"
: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${STREAMSENSE_WRITER_PASSWORD:?STREAMSENSE_WRITER_PASSWORD is required}"
: "${STREAMSENSE_AGGREGATOR_PASSWORD:?STREAMSENSE_AGGREGATOR_PASSWORD is required}"
: "${STREAMSENSE_READER_PASSWORD:?STREAMSENSE_READER_PASSWORD is required}"

psql \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  --set=ON_ERROR_STOP=1 \
  --set=db_name="$POSTGRES_DB" \
  --set=writer_password="$STREAMSENSE_WRITER_PASSWORD" \
  --set=aggregator_password="$STREAMSENSE_AGGREGATOR_PASSWORD" \
  --set=reader_password="$STREAMSENSE_READER_PASSWORD" <<'SQL'
DO $roles$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'streamsense_writer') THEN
        CREATE ROLE streamsense_writer LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'streamsense_aggregator') THEN
        CREATE ROLE streamsense_aggregator LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'streamsense_reader') THEN
        CREATE ROLE streamsense_reader LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE;
    END IF;
END
$roles$;

ALTER ROLE streamsense_writer PASSWORD :'writer_password';
ALTER ROLE streamsense_aggregator PASSWORD :'aggregator_password';
ALTER ROLE streamsense_reader PASSWORD :'reader_password';
ALTER ROLE streamsense_reader SET default_transaction_read_only = on;

REVOKE ALL ON DATABASE :"db_name" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"db_name" TO streamsense_writer, streamsense_aggregator, streamsense_reader;
GRANT USAGE ON SCHEMA public TO streamsense_writer, streamsense_aggregator, streamsense_reader;

GRANT SELECT, INSERT ON sensor_readings TO streamsense_writer;
GRANT SELECT, INSERT, UPDATE ON ingestion_rejections TO streamsense_writer;

GRANT SELECT, DELETE ON sensor_readings TO streamsense_aggregator;
GRANT SELECT, INSERT, UPDATE, DELETE ON
    room_metrics_1m,
    room_metrics_5m,
    room_alerts,
    pipeline_checkpoints
TO streamsense_aggregator;

REVOKE ALL ON FUNCTION refresh_room_metrics(
    timestamptz, timestamptz, double precision, double precision,
    double precision, double precision, integer
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION refresh_room_metrics(
    timestamptz, timestamptz, double precision, double precision,
    double precision, double precision, integer
) TO streamsense_aggregator;

GRANT SELECT ON
    schema_versions,
    sensor_readings,
    ingestion_rejections,
    room_metrics_1m,
    room_metrics_5m,
    room_alerts,
    pipeline_checkpoints
TO streamsense_reader;
SQL
