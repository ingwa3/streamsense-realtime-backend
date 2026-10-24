from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shutil
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
ENV_FILE = ROOT / ".env"
ENV_EXAMPLE = ROOT / ".env.example"
REPORT_MD = DOCS / "TESTERGEBNISSE_AUTOMATISCH.md"
REPORT_JSON = DOCS / "TESTERGEBNISSE_AUTOMATISCH.json"
ERROR_LOG = DOCS / "TESTFEHLER.log"
COMPOSE = ["docker", "compose"]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def run(
    command: list[str],
    *,
    check: bool = True,
    capture: bool = True,
    timeout: int | None = None,
) -> subprocess.CompletedProcess[str]:
    print("+", " ".join(command), flush=True)
    result = subprocess.run(
        command,
        cwd=ROOT,
        text=True,
        capture_output=capture,
        timeout=timeout,
        encoding="utf-8",
        errors="replace",
    )
    if check and result.returncode != 0:
        detail = (result.stderr or result.stdout or "unbekannter Fehler").strip()
        raise RuntimeError(f"Befehl fehlgeschlagen ({result.returncode}): {' '.join(command)}\n{detail}")
    return result


def load_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def ensure_environment() -> dict[str, str]:
    if not ENV_FILE.exists():
        shutil.copy2(ENV_EXAMPLE, ENV_FILE)
        print(".env wurde aus .env.example angelegt.")
    return load_env(ENV_FILE)


def require_docker() -> None:
    if shutil.which("docker") is None:
        raise RuntimeError("Docker wurde nicht gefunden. Bitte Docker Desktop installieren und starten.")
    run(["docker", "--version"])
    run(COMPOSE + ["version"])
    run(["docker", "info"], timeout=30)


def request_json(url: str, api_key: str | None = None, timeout: float = 10.0) -> Any:
    headers = {"X-API-Key": api_key} if api_key else {}
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def wait_for_api(deadline_seconds: int, api_key: str) -> dict[str, Any]:
    deadline = time.monotonic() + deadline_seconds
    last_error = ""
    while time.monotonic() < deadline:
        try:
            health = request_json("http://localhost:8000/health")
            request_json("http://localhost:8000/v1/data-quality?minutes=10080", api_key)
            return health
        except Exception as exc:  # noqa: BLE001 - diagnostics are written below
            last_error = str(exc)
            time.sleep(3)
    raise RuntimeError(f"API wurde innerhalb von {deadline_seconds} Sekunden nicht bereit: {last_error}")


def sql_value(sql: str, env: dict[str, str]) -> str:
    user = env.get("POSTGRES_USER", "streamsense_admin")
    database = env.get("POSTGRES_DB", "streamsense")
    result = run(
        COMPOSE
        + [
            "exec",
            "-T",
            "postgres",
            "psql",
            "-U",
            user,
            "-d",
            database,
            "-tA",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            sql,
        ],
        timeout=60,
    )
    return result.stdout.strip()


def sql_int(sql: str, env: dict[str, str]) -> int:
    value = sql_value(sql, env)
    if not value:
        return 0
    return int(value.splitlines()[-1].strip())


def wait_for_rows(target: int, env: dict[str, str], timeout_minutes: int) -> tuple[int, float]:
    started = time.monotonic()
    deadline = started + timeout_minutes * 60
    last = 0
    while time.monotonic() < deadline:
        last = sql_int("SELECT count(*) FROM sensor_readings;", env)
        print(f"Gespeicherte Ereignisse: {last:,} / {target:,}", flush=True)
        if last >= target:
            return last, time.monotonic() - started
        time.sleep(5)
    return last, time.monotonic() - started


def consumer_lag(group: str) -> tuple[int | None, str]:
    result = run(
        COMPOSE
        + [
            "exec",
            "-T",
            "broker",
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server",
            "broker:19092",
            "--describe",
            "--group",
            group,
        ],
        check=False,
        timeout=60,
    )
    output = (result.stdout + "\n" + result.stderr).strip()
    lags: list[int] = []
    for line in output.splitlines():
        parts = re.split(r"\s+", line.strip())
        if len(parts) >= 6 and parts[0] == group:
            try:
                lags.append(int(parts[5]))
            except ValueError:
                pass
    return (max(lags) if lags else None), output


def api_latency(api_key: str, requests_count: int = 25) -> tuple[float | None, float | None, list[float]]:
    values: list[float] = []
    for _ in range(requests_count):
        started = time.perf_counter()
        request_json("http://localhost:8000/v1/overview?window=1m", api_key)
        values.append((time.perf_counter() - started) * 1000.0)
    if not values:
        return None, None, values
    ordered = sorted(values)
    p50 = statistics.median(ordered)
    index95 = max(0, min(len(ordered) - 1, int(round(0.95 * len(ordered) + 0.5)) - 1))
    return p50, ordered[index95], values


def collect_counts(env: dict[str, str]) -> dict[str, Any]:
    return {
        "stored_events": sql_int("SELECT count(*) FROM sensor_readings;", env),
        "valid_events": sql_int("SELECT count(*) FROM sensor_readings WHERE quality_status = 'valid';", env),
        "warning_events": sql_int("SELECT count(*) FROM sensor_readings WHERE quality_status = 'warning';", env),
        "rejected_events": sql_int("SELECT count(*) FROM ingestion_rejections;", env),
        "windows_1m": sql_int("SELECT count(*) FROM room_metrics_1m;", env),
        "windows_5m": sql_int("SELECT count(*) FROM room_metrics_5m;", env),
        "alerts": sql_int("SELECT count(*) FROM room_alerts;", env),
        "event_time_min": sql_value("SELECT coalesce(min(event_time)::text, '') FROM sensor_readings;", env),
        "event_time_max": sql_value("SELECT coalesce(max(event_time)::text, '') FROM sensor_readings;", env),
        "last_aggregation": sql_value(
            "SELECT coalesce(last_success_at::text, '') FROM pipeline_checkpoints WHERE component = 'window-aggregator';",
            env,
        ),
    }


def write_reports(payload: dict[str, Any]) -> None:
    DOCS.mkdir(parents=True, exist_ok=True)
    REPORT_JSON.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    counts = payload.get("counts", {})
    p50 = payload.get("api_latency_ms", {}).get("p50")
    p95 = payload.get("api_latency_ms", {}).get("p95")
    lines = [
        "# Automatisches Abschluss-Testprotokoll - StreamSense",
        "",
        f"- **Status:** {payload.get('status', 'UNBEKANNT')}",
        f"- **Start (UTC):** {payload.get('started_at', '')}",
        f"- **Ende (UTC):** {payload.get('finished_at', '')}",
        f"- **Betriebssystem:** {payload.get('system', {}).get('platform', '')}",
        f"- **CPU-Kerne:** {payload.get('system', {}).get('cpu_count', '')}",
        f"- **Geplante Last:** {payload.get('requested_events', 0):,} Ereignisse",
        f"- **Producer-Laufzeit:** {payload.get('producer_seconds', 0):.2f} Sekunden",
        f"- **Zeit bis vollständiger Speicherung:** {payload.get('end_to_end_seconds', 0):.2f} Sekunden",
        f"- **Ende-zu-Ende-Durchsatz:** {payload.get('throughput_events_per_second', 0):.2f} Ereignisse/Sekunde",
        f"- **Gespeicherte Ereignisse:** {counts.get('stored_events', 0):,}",
        f"- **Davon ohne Warnflag:** {counts.get('valid_events', 0):,}",
        f"- **Davon mit Warnflag:** {counts.get('warning_events', 0):,}",
        f"- **Abgelehnte Ereignisse:** {counts.get('rejected_events', 0):,}",
        f"- **Ein-Minuten-Fenster:** {counts.get('windows_1m', 0):,}",
        f"- **Fünf-Minuten-Fenster:** {counts.get('windows_5m', 0):,}",
        f"- **Warnungen/Alarme:** {counts.get('alerts', 0):,}",
        f"- **Maximaler Kafka-Consumer-Lag am Testende:** {payload.get('max_consumer_lag')}",
        f"- **API-Antwortzeit p50:** {p50:.2f} ms" if isinstance(p50, (int, float)) else "- **API-Antwortzeit p50:** nicht ermittelt",
        f"- **API-Antwortzeit p95:** {p95:.2f} ms" if isinstance(p95, (int, float)) else "- **API-Antwortzeit p95:** nicht ermittelt",
        "",
        "## API-Stichprobe",
        "",
        "```json",
        json.dumps(payload.get("api_sample", {}), indent=2, ensure_ascii=False),
        "```",
        "",
        "## Hinweise",
        "",
        "Die Messwerte gelten nur für den oben genannten lokalen Rechner. Der Test startet mit leeren Docker-Volumes, sofern nicht `--keep-data` verwendet wurde. Die vollständigen Rohdaten stehen zusätzlich in `TESTERGEBNISSE_AUTOMATISCH.json`.",
    ]
    if payload.get("errors"):
        lines += ["", "## Fehler/Hinweise", ""] + [f"- {item}" for item in payload["errors"]]
    REPORT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Startet StreamSense, erzeugt eine Million Ereignisse und schreibt ein Testprotokoll."
    )
    parser.add_argument("--events", type=int, default=1_000_000)
    parser.add_argument("--timeout-minutes", type=int, default=30)
    parser.add_argument(
        "--keep-data",
        action="store_true",
        help="Vorhandene Docker-Volumes nicht löschen. Für einen sauberen Prüfungsnachweis nicht empfohlen.",
    )
    args = parser.parse_args()
    if args.events < 1:
        parser.error("--events muss mindestens 1 sein")

    started_wall = now_iso()
    started = time.monotonic()
    payload: dict[str, Any] = {
        "status": "GESTARTET",
        "started_at": started_wall,
        "requested_events": args.events,
        "system": {
            "platform": platform.platform(),
            "python": sys.version.split()[0],
            "cpu_count": os.cpu_count(),
        },
        "errors": [],
    }

    try:
        env = ensure_environment()
        require_docker()
        payload["docker_version"] = run(["docker", "--version"]).stdout.strip()
        payload["compose_version"] = run(COMPOSE + ["version"]).stdout.strip()

        if not args.keep_data:
            print("Vorhandene StreamSense-Demo-Volumes werden für einen sauberen Test entfernt.")
            run(COMPOSE + ["down", "-v", "--remove-orphans"], check=False, capture=False)

        # Start without the endless default producer, so the test count remains unambiguous.
        run(
            COMPOSE
            + [
                "up",
                "-d",
                "--build",
                "broker",
                "postgres",
                "topic-init",
                "persister",
                "aggregator",
                "api",
            ],
            capture=False,
            timeout=1200,
        )

        api_key = env.get("REPORTING_API_KEY", "CHANGE_ME_REPORTING_API_KEY")
        payload["api_health_before_load"] = wait_for_api(240, api_key)
        baseline = sql_int("SELECT count(*) FROM sensor_readings;", env)
        payload["baseline_events"] = baseline
        target = baseline + args.events

        producer_started = time.monotonic()
        run(
            COMPOSE
            + [
                "run",
                "--rm",
                "--build",
                "-e",
                "PRODUCER_INSTANCE=abschluss-test-1m",
                "-e",
                f"TOTAL_EVENTS={args.events}",
                "-e",
                "BURST_MODE=true",
                "-e",
                "EVENTS_PER_SECOND=10000",
                "-e",
                "LATE_EVENT_PROBABILITY=0.02",
                "-e",
                "LATE_EVENT_MAX_SECONDS=90",
                "producer",
            ],
            capture=False,
            timeout=max(1800, args.timeout_minutes * 60),
        )
        payload["producer_seconds"] = time.monotonic() - producer_started

        stored, persistence_wait = wait_for_rows(target, env, args.timeout_minutes)
        payload["persistence_wait_seconds_after_producer"] = persistence_wait
        payload["end_to_end_seconds"] = time.monotonic() - producer_started
        payload["throughput_events_per_second"] = (
            args.events / payload["end_to_end_seconds"] if payload["end_to_end_seconds"] else 0.0
        )

        # Give the watermark and periodic aggregator enough time to close recent windows.
        print("Warte 25 Sekunden auf Watermark und Aggregation ...", flush=True)
        time.sleep(25)
        payload["counts"] = collect_counts(env)
        payload["api_sample"] = {
            "health": request_json("http://localhost:8000/health"),
            "overview_1m": request_json("http://localhost:8000/v1/overview?window=1m", api_key),
            "data_quality": request_json("http://localhost:8000/v1/data-quality?minutes=10080", api_key),
        }
        p50, p95, raw = api_latency(api_key)
        payload["api_latency_ms"] = {"p50": p50, "p95": p95, "samples": raw}
        lag, lag_output = consumer_lag(env.get("CONSUMER_GROUP", "streamsense-persister-v2"))
        payload["max_consumer_lag"] = lag
        payload["consumer_group_output"] = lag_output
        payload["compose_ps"] = run(COMPOSE + ["ps", "--all"]).stdout

        counts = payload["counts"]
        passed = (
            stored >= target
            and counts["stored_events"] >= target
            and counts["windows_1m"] > 0
            and payload["api_sample"].get("health", {}).get("status") == "ok"
        )
        if stored < target:
            payload["errors"].append(
                f"Innerhalb des Zeitlimits wurden nur {stored:,} von erwarteten {target:,} Ereignissen gespeichert."
            )
        if counts["windows_1m"] == 0:
            payload["errors"].append("Es wurden keine Ein-Minuten-Fenster gefunden.")
        payload["status"] = "BESTANDEN" if passed else "TEILWEISE BESTANDEN"
    except Exception as exc:  # noqa: BLE001 - a diagnostic report is the purpose of this script
        payload["status"] = "FEHLGESCHLAGEN"
        payload["errors"].append(str(exc))
        try:
            logs = run(COMPOSE + ["logs", "--tail=300"], check=False).stdout
            ERROR_LOG.write_text(logs, encoding="utf-8")
        except Exception as log_exc:  # noqa: BLE001
            payload["errors"].append(f"Logs konnten nicht gelesen werden: {log_exc}")
    finally:
        payload["finished_at"] = now_iso()
        payload["total_script_seconds"] = time.monotonic() - started
        write_reports(payload)
        print("\nErgebnis:", payload["status"])
        print("Testprotokoll:", REPORT_MD)
        if payload["errors"]:
            for item in payload["errors"]:
                print("-", item)

    return 0 if payload["status"] == "BESTANDEN" else 1


if __name__ == "__main__":
    raise SystemExit(main())
