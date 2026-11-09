.PHONY: help env setup up down ps logs topics peek dlq once test lint format reset clean

help:            ## Show available commands
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-10s %s\n", $$1, $$2}'

env:             ## Create .env from the example if missing
	@test -f .env || cp .env.example .env

setup: env       ## Create local virtual env with dev tools
	python3.12 -m venv .venv
	.venv/bin/pip install -r requirements-dev.txt
	.venv/bin/pre-commit install

up: env          ## Build and start Redpanda, Console and the producer
	docker compose up -d --build

down:            ## Stop all services (data is kept)
	docker compose down

ps:              ## Show running services
	docker compose ps

logs:            ## Follow the producer's per-cycle stats
	docker compose logs -f producer

topics:          ## List topics with partitions and message counts
	docker compose exec redpanda rpk topic list --detailed

peek:            ## Show the 5 newest alerts (decoded from Avro)
	docker compose exec redpanda rpk topic consume gov.nws.alerts.v1 --offset -5 --num 5 --use-schema-registry=value --format '%k  %v\n'

dlq:             ## Show records rejected to the dead letter queue
	docker compose exec redpanda rpk topic consume gov.ingest.dlq.v1 --offset start --num 10 --format '%k  %v\n'

test:            ## Run unit and Kafka integration tests
	.venv/bin/pytest -v

lint:            ## Check code style
	.venv/bin/ruff check . && .venv/bin/ruff format --check .

format:          ## Auto-format code
	.venv/bin/ruff format . && .venv/bin/ruff check --fix .

reset:           ## Stop everything AND delete all data
	docker compose down -v

clean:           ## Remove caches
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	rm -rf .pytest_cache .ruff_cache
