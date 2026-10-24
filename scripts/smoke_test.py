from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request


BASE_URL = os.getenv("STREAMSENSE_API_URL", "http://localhost:8000")


def load_api_key() -> str:
    explicit = os.getenv("REPORTING_API_KEY")
    if explicit:
        return explicit
    env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
    try:
        with open(env_path, encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("REPORTING_API_KEY="):
                    return line.split("=", 1)[1].strip()
    except FileNotFoundError:
        pass
    raise RuntimeError("REPORTING_API_KEY fehlt. Bitte Umgebungsvariable oder lokale .env konfigurieren.")


API_KEY = load_api_key()


def request(path: str, authenticated: bool = False) -> object:
    headers = {"X-API-Key": API_KEY} if authenticated else {}
    req = urllib.request.Request(BASE_URL + path, headers=headers)
    with urllib.request.urlopen(req, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


def main() -> None:
    deadline = time.time() + 90
    while True:
        try:
            health = request("/health")
            break
        except (urllib.error.URLError, TimeoutError):
            if time.time() >= deadline:
                raise SystemExit("API did not become healthy within 90 seconds")
            time.sleep(2)

    overview = request("/v1/overview?window=1m", authenticated=True)
    latest = request("/v1/rooms/latest?limit=3", authenticated=True)
    quality = request("/v1/data-quality?minutes=60", authenticated=True)
    print(json.dumps({"health": health, "overview": overview, "sample_rooms": latest, "quality": quality}, indent=2))


if __name__ == "__main__":
    main()
