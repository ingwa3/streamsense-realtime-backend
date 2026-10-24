import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

from streamsense.producer.generator import SensorGenerator


def test_generated_event_matches_json_schema() -> None:
    root = Path(__file__).resolve().parents[1]
    schema = json.loads((root / "schemas/sensor-reading-v1.json").read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    reading = SensorGenerator(2, 1, 10, "schema-test").next_reading()
    validator.validate(json.loads(reading.model_dump_json()))
