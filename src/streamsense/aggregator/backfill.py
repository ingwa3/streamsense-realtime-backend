from __future__ import annotations

import argparse
from datetime import datetime

from streamsense.aggregator.repository import refresh_aggregates
from streamsense.common.settings import AggregatorSettings


def parse_timestamp(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("timestamp must include a timezone, for example Z")
    return parsed


def main() -> None:
    parser = argparse.ArgumentParser(description="Idempotently rebuild aggregate windows")
    parser.add_argument("--from", dest="start", required=True, type=parse_timestamp)
    parser.add_argument("--to", dest="end", required=True, type=parse_timestamp)
    args = parser.parse_args()
    if args.start >= args.end:
        parser.error("--from must be earlier than --to")

    settings = AggregatorSettings()
    result = refresh_aggregates(
        settings.database_url,
        args.start,
        args.end,
        settings.comfort_temperature_min_c,
        settings.comfort_temperature_max_c,
        settings.comfort_humidity_min_pct,
        settings.comfort_humidity_max_pct,
        settings.co2_warning_ppm,
    )
    if result is None:
        raise SystemExit("another aggregation process currently holds the advisory lock")
    print(
        f"Backfill complete: 1m={result.one_minute_rows}, "
        f"5m={result.five_minute_rows}, alerts={result.alert_rows}"
    )


if __name__ == "__main__":
    main()
