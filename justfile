# Webhook receiver commands.

default:
    @just --list

# Install dependencies and the git pre-commit hook.
install:
    uv sync
    git config core.hooksPath .githooks

# Run the API with a local SQLite file.
run:
    mkdir -p data
    DB_PATH=./data/webhooks.db uv run uvicorn app.main:create_app --factory --port 8000

# Run the test suite.
test:
    uv run pytest -q

# Ruff lint and format check.
lint:
    uv run ruff check . && uv run ruff format --check .

# Static security scan and known-CVE check of runtime dependencies.
security:
    uv run bandit -r app -q
    uv export --frozen --no-dev --no-emit-project --format requirements-txt -o requirements.audit.txt -q
    uv run pip-audit -r requirements.audit.txt --disable-pip --no-deps --strict --progress-spinner off
    rm requirements.audit.txt

# Everything CI checks, in one command.
check: lint test security

# Build the service image.
docker:
    docker build -t webhook-receiver .

# Start the service in Compose.
up:
    docker compose up --build

# Start the service plus Prometheus and Grafana.
monitoring:
    docker compose --profile monitoring up --build

# Run the service in Docker and check it end to end (kills it once on purpose).
e2e *args:
    scripts/e2e.sh {{args}}

# Send 1000 webhooks/min for 60 s. The service must already be running.
bench:
    uv run python bench/load.py --rate 1000 --duration 60
