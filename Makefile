COMPOSE := docker compose
N ?= 100
Q ?= 조선 시대 과거 제도
DUMP ?= https://dumps.wikimedia.org/other/enterprise_html/runs/20250301/kowiki-NS0-20250301-ENTERPRISE-HTML.json.tar.gz
LIMIT ?= 10000
TOPIC ?= doc.events

.PHONY: up down logs seed seed-top backfill search dlq test e2e lint

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f --tail=50 live parser chunker embedder indexer

seed:
	$(COMPOSE) run --rm cli seed --count $(N)

seed-top:
	$(COMPOSE) run --rm cli seed --count $(N) --top

backfill:
	$(COMPOSE) run --rm cli backfill $(DUMP) --limit $(LIMIT)

search:
	@curl -sG localhost:8000/search --data-urlencode "q=$(Q)" | python3 -m json.tool

dlq:
	$(COMPOSE) run --rm cli replay $(TOPIC) --dry-run

test:
	uv run pytest

e2e:
	$(COMPOSE) up -d kafka qdrant
	uv run pytest -m e2e

lint:
	uv run ruff check .
	uv run ruff format --check .
