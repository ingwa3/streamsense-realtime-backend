from __future__ import annotations

import json
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    services = compose["services"]
    assert services["broker"]["image"] == "apache/kafka:4.3.1"
    assert set(("producer", "persister", "aggregator", "api")).issubset(services)
    assert "env_file" not in services["producer"]
    assert services["persister"]["environment"]["DATABASE_URL"] == "${PERSISTER_DATABASE_URL}"
    assert services["aggregator"]["environment"]["DATABASE_URL"] == "${AGGREGATOR_DATABASE_URL}"
    assert services["api"]["environment"]["DATABASE_URL"] == "${API_DATABASE_URL}"

    schema = json.loads((ROOT / "schemas/sensor-reading-v1.json").read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)

    database_schema = (ROOT / "database/init/001_schema.sql").read_text(encoding="utf-8")
    roles_script = (ROOT / "database/init/002_roles.sh").read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS ingestion_rejections" in database_schema
    assert "streamsense_writer" in roles_script
    assert "streamsense_aggregator" in roles_script
    assert "streamsense_reader" in roles_script
    assert "SELECT, INSERT, UPDATE ON ingestion_rejections" in roles_script

    print("Compose, JSON schema, database schema and role setup are structurally valid")


if __name__ == "__main__":
    main()
