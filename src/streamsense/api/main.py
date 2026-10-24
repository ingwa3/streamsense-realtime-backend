from __future__ import annotations

import logging
import secrets
from contextlib import asynccontextmanager
from typing import Annotated, Literal

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.security import APIKeyHeader
from prometheus_client import Counter, Histogram, make_asgi_app
from psycopg_pool import ConnectionPool

from streamsense.api.repository import ReportingRepository
from streamsense.api.schemas import Alert, DataQualitySummary, Overview, RoomMetric
from streamsense.common.logging import configure_logging
from streamsense.common.settings import ApiSettings


settings = ApiSettings()
configure_logging("api", settings.log_level)
logger = logging.getLogger(__name__)

pool = ConnectionPool(
    conninfo=settings.database_url,
    min_size=settings.db_pool_min_size,
    max_size=settings.db_pool_max_size,
    open=False,
)
repository = ReportingRepository(pool)
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
REQUESTS = Counter("streamsense_api_requests_total", "API requests", ["endpoint"])
REQUEST_DURATION = Histogram("streamsense_api_request_seconds", "API request duration", ["endpoint"])


@asynccontextmanager
async def lifespan(_app: FastAPI):
    pool.open(wait=True)
    logger.info("database pool opened")
    try:
        yield
    finally:
        pool.close()
        logger.info("database pool closed")


app = FastAPI(
    title="StreamSense Reporting API",
    version="2.0.0",
    description="Read-only API for current and historical indoor-air metrics.",
    lifespan=lifespan,
)
app.mount("/metrics", make_asgi_app())


def require_api_key(key: Annotated[str | None, Depends(api_key_header)]) -> None:
    if key is None or not secrets.compare_digest(key, settings.reporting_api_key):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing or invalid API key",
        )


@app.get("/health", tags=["operations"])
def health() -> dict[str, str]:
    try:
        if repository.health():
            return {"status": "ok"}
    except Exception as exc:
        logger.warning("health check failed", extra={"error": str(exc)})
    raise HTTPException(status_code=503, detail="database unavailable")


@app.get(
    "/v1/overview",
    response_model=Overview,
    dependencies=[Depends(require_api_key)],
    tags=["reporting"],
)
def overview(window: Literal["1m", "5m"] = "1m") -> Overview:
    endpoint = "overview"
    REQUESTS.labels(endpoint=endpoint).inc()
    with REQUEST_DURATION.labels(endpoint=endpoint).time():
        return Overview.model_validate(repository.overview(window))


@app.get(
    "/v1/rooms/latest",
    response_model=list[RoomMetric],
    dependencies=[Depends(require_api_key)],
    tags=["reporting"],
)
def latest_rooms(limit: int = Query(default=200, ge=1, le=1000)) -> list[RoomMetric]:
    endpoint = "latest_rooms"
    REQUESTS.labels(endpoint=endpoint).inc()
    with REQUEST_DURATION.labels(endpoint=endpoint).time():
        return [RoomMetric.model_validate(row) for row in repository.latest_rooms(limit)]


@app.get(
    "/v1/rooms/{room_id}/history",
    response_model=list[RoomMetric],
    dependencies=[Depends(require_api_key)],
    tags=["reporting"],
)
def room_history(
    room_id: str,
    minutes: int = Query(default=60, ge=1, le=10_080),
    window: Literal["1m", "5m"] = "1m",
) -> list[RoomMetric]:
    endpoint = "room_history"
    REQUESTS.labels(endpoint=endpoint).inc()
    with REQUEST_DURATION.labels(endpoint=endpoint).time():
        rows = repository.room_history(room_id, minutes, window)
        return [RoomMetric.model_validate(row) for row in rows]


@app.get(
    "/v1/alerts",
    response_model=list[Alert],
    dependencies=[Depends(require_api_key)],
    tags=["reporting"],
)
def alerts(
    limit: int = Query(default=100, ge=1, le=1000),
    severity: Literal["warning", "critical"] | None = None,
) -> list[Alert]:
    endpoint = "alerts"
    REQUESTS.labels(endpoint=endpoint).inc()
    with REQUEST_DURATION.labels(endpoint=endpoint).time():
        return [Alert.model_validate(row) for row in repository.alerts(limit, severity)]


@app.get(
    "/v1/data-quality",
    response_model=DataQualitySummary,
    dependencies=[Depends(require_api_key)],
    tags=["governance"],
)
def data_quality(
    minutes: int = Query(default=60, ge=1, le=10_080),
) -> DataQualitySummary:
    endpoint = "data_quality"
    REQUESTS.labels(endpoint=endpoint).inc()
    with REQUEST_DURATION.labels(endpoint=endpoint).time():
        return DataQualitySummary.model_validate(repository.data_quality(minutes))


def main() -> None:
    uvicorn.run(app, host=settings.api_host, port=settings.api_port, log_config=None)


if __name__ == "__main__":
    main()
