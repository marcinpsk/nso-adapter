# SPDX-License-Identifier: Apache-2.0
# NSO Adapter — development workflow
# Usage: make <target>
#   make dev       bring up dev container (hot-reload)
#   make down      stop containers
#   make rebuild   force full image rebuild then start
#   make logs      tail container logs
#   make shell     exec into running container
#   make test      run pytest
#   make test PYTEST_WORKERS=1  run pytest serially
#   make lint      ruff check
#   make validate  run validate_pipe.py (NSO + Vault smoke test)

COMPOSE     = docker compose -f docker-compose.dev.yml
SERVICE     = nso-adapter
PYTHON      = uv run --native-tls --
PYTEST_WORKERS ?= 8

.PHONY: dev up down build rebuild restart logs shell ps \
        test lint format validate migrate clean

# ── Container lifecycle ──────────────────────────────────────────────────────

dev: build
	$(COMPOSE) up

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

build:
	mkdir -p certs
	cp /etc/ssl/certs/ca-certificates.crt certs/ca-certificates.crt
	$(COMPOSE) build

rebuild:
	mkdir -p certs
	cp /etc/ssl/certs/ca-certificates.crt certs/ca-certificates.crt
	$(COMPOSE) build --no-cache
	$(COMPOSE) up -d

restart:
	$(COMPOSE) restart $(SERVICE)

logs:
	$(COMPOSE) logs -f $(SERVICE)

shell:
	$(COMPOSE) exec $(SERVICE) /bin/bash

ps:
	$(COMPOSE) ps

# ── Local dev (without Docker) ───────────────────────────────────────────────

test:
	$(PYTHON) pytest tests/ -q -n $(PYTEST_WORKERS) --maxschedchunk=1

lint:
	$(PYTHON) ruff check nso_adapter/ tests/

format:
	$(PYTHON) ruff format nso_adapter/ tests/
	$(PYTHON) ruff check --select I --fix nso_adapter/ tests/

validate:
	$(PYTHON) python scripts/validate_pipe.py

migrate:
	$(PYTHON) alembic upgrade head

# ── Housekeeping ─────────────────────────────────────────────────────────────

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .ruff_cache .pytest_cache .mypy_cache
