.PHONY: help env setup up down ps logs plogs topics peek dlq sql incidents dashboard test test-db lint format reset clean

help:            ## Show available commands
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-10s %s\n", $$1, $$2}'

env:             ## Create .env from the example if missing
	@test -f .env || cp .env.example .env

setup: env       ## Create local virtual env with dev tools
	python3.12 -m venv .venv
	.venv/bin/pip install -r requirements-dev.txt
	.venv/bin/pre-commit install

up: env          ## Build and start the whole stack
	docker compose up -d --build

down:            ## Stop all services (data is kept)
	docker compose down

ps:              ## Show running services
	docker compose ps

logs:            ## Follow the producer's per-cycle stats
	docker compose logs -f producer

plogs:           ## Follow the stream processor's logs
	docker compose logs -f processor

topics:          ## List topics with partitions and message counts
	docker compose exec redpanda rpk topic list --detailed

peek:            ## Show the 5 newest alerts (decoded from Avro)
	docker compose exec redpanda rpk topic consume gov.nws.alerts.v1 --offset -5 --num 5 --use-schema-registry=value --format '%k  %v\n'

dlq:             ## Show records rejected to the dead letter queue
	docker compose exec redpanda rpk topic consume gov.ingest.dlq.v1 --offset start --num 10 --format '%k  %v\n'

sql:             ## Open a SQL prompt on the live database
	docker compose exec postgres psql -U app -d public_safety

incidents:       ## Active hazards affecting the most people
	docker compose exec postgres psql -U app -d public_safety -c "select event, severity, left(area_desc, 50) as area, population, expires from v_active_incidents order by population desc limit 15"

dashboard:       ## Open the Grafana dashboard
	open http://localhost:3000

test:            ## Unit and Kafka tests (database tests skip without TEST_DATABASE_URL)
	.venv/bin/pytest -v

test-db:         ## All tests, using the running stack's PostgreSQL for database tests
	TEST_DATABASE_URL=postgresql://app:$${POSTGRES_PASSWORD:-app}@localhost:15432/public_safety .venv/bin/pytest -v

lint:            ## Check code style
	.venv/bin/ruff check . && .venv/bin/ruff format --check .

format:          ## Auto-format code
	.venv/bin/ruff format . && .venv/bin/ruff check --fix .

reset:           ## Stop everything AND delete all data
	docker compose down -v

clean:           ## Remove caches
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	rm -rf .pytest_cache .ruff_cache
