COMPOSE := docker compose
N ?= 100
Q ?= 조선 시대 과거 제도
DUMP ?= https://dumps.wikimedia.org/other/enterprise_html/runs/20250301/kowiki-NS0-20250301-ENTERPRISE-HTML.json.tar.gz
LIMIT ?= 10000
TOPIC ?= doc.events

CLUSTER := kowiki
NS := kowiki
IMAGE := kowiki-live-index:dev

.PHONY: up down logs seed seed-top backfill search dlq test e2e lint \
	kind-up kind-down k8s-deps k8s-deploy k8s-monitoring

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

kind-up:
	kind create cluster --name $(CLUSTER) --config deploy/kind/cluster.yaml

kind-down:
	kind delete cluster --name $(CLUSTER)

k8s-deps:
	helm upgrade --install strimzi oci://quay.io/strimzi-helm/strimzi-kafka-operator \
		--version 1.2.0 -n $(NS) --create-namespace --wait
	helm upgrade --install keda keda --repo https://kedacore.github.io/charts \
		--version 2.21.0 -n keda --create-namespace --wait
	helm upgrade --install qdrant qdrant --repo https://qdrant.github.io/qdrant-helm \
		--version 1.19.1 -n $(NS) -f deploy/qdrant-values.yaml --wait
	kubectl apply -n $(NS) -f deploy/kafka/
	kubectl wait kafka/kowiki -n $(NS) --for=condition=Ready --timeout=600s

k8s-deploy:
	docker build -t $(IMAGE) .
	kind load docker-image $(IMAGE) --name $(CLUSTER)
	helm upgrade --install kowiki deploy/helm/kowiki -n $(NS) --timeout 15m

k8s-monitoring:
	helm upgrade --install monitoring kube-prometheus-stack \
		--repo https://prometheus-community.github.io/helm-charts \
		-n monitoring --create-namespace --set grafana.adminPassword=admin --wait
	helm upgrade kowiki deploy/helm/kowiki -n $(NS) --reuse-values --set podMonitor.enabled=true
