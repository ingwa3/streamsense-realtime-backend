SHELL := /bin/sh

.PHONY: init build up down reset logs ps test smoke load-1m late-demo scale-persister backfill

init:
	@test -f .env || cp .env.example .env
	@echo ".env is ready. Review the demo credentials before starting."

build: init
	docker compose build

up: init
	docker compose up -d --build

ps:
	docker compose ps

logs:
	docker compose logs -f --tail=200 producer persister aggregator api

down:
	docker compose down

reset:
	docker compose down -v --remove-orphans

# Local unit tests. Install requirements/dev.txt in a virtual environment first.
test:
	python -m pytest

smoke:
	python scripts/smoke_test.py

# Generates exactly 1,000,000 timestamped events in accelerated mode.
# The simulated event-time span is 100 seconds, so the normal aggregator lookback covers the run.
load-1m: init
	docker compose run --rm --build \
		-e PRODUCER_INSTANCE=loadtest-1m \
		-e TOTAL_EVENTS=1000000 \
		-e BURST_MODE=true \
		-e EVENTS_PER_SECOND=10000 \
		producer

# Generates a smaller run in which every event is deliberately late.
late-demo: init
	docker compose run --rm --build \
		-e PRODUCER_INSTANCE=late-data-demo \
		-e TOTAL_EVENTS=25000 \
		-e BURST_MODE=true \
		-e EVENTS_PER_SECOND=1000 \
		-e LATE_EVENT_PROBABILITY=1.0 \
		-e LATE_EVENT_MAX_SECONDS=120 \
		producer

# Six Kafka partitions allow up to six active consumers for this topic.
scale-persister:
	docker compose up -d --scale persister=3

# Usage: make backfill FROM=2026-01-01T00:00:00Z TO=2026-01-02T00:00:00Z
backfill:
	docker compose run --rm --build aggregator python -m streamsense.aggregator.backfill \
		--from "$(FROM)" --to "$(TO)"
