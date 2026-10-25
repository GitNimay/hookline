# Hookline task runner. Run `make help` for targets.
SHELL := bash
GO_VERSION   := 1.27.3
BUF          := github.com/bufbuild/buf/cmd/buf@v1.75.0
GOLANGCI     := github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0
SQLC_IMAGE   := sqlc/sqlc:1.31.1
SERVICES     := ingest control dispatcher worker scheduler chaos-customer hookctl
VERSION      ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT       ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
LDFLAGS      := -s -w -X github.com/GitNimay/hookline/internal/telemetry.Version=$(VERSION) -X github.com/GitNimay/hookline/internal/telemetry.Commit=$(COMMIT)
COMPOSE      := docker compose --env-file .env -f deploy/compose/docker-compose.yml
GOBIN        := $(shell go env GOPATH)/bin
export PATH  := $(GOBIN):$(PATH)
# -race needs cgo; native Windows has no C toolchain, so CI (Linux) and WSL2 run it.
RACE         ?= $(if $(filter Windows_NT,$(OS)),,-race)
PYTHON       ?= $(if $(filter Windows_NT,$(OS)),python,python3)

.PHONY: help tools gen lint test test-integration test-e2e cover build images compose-up compose-up-tracing compose-down compose-logs \
        seed demo demo-full secrets-local cluster-up cluster-down cluster-reset deploy-local deploy-homeserver loadtest chaos chaos-stop

help: ## Show targets
	@grep -hE '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-18s %s\n", $$1, $$2}'

tools: ## Install pinned code generation and lint tools
	go install $(BUF)
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
	go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@v2.32.0
	go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@v2.32.0
	go install $(GOLANGCI)
	go install golang.org/x/vuln/cmd/govulncheck@v1.9.0

gen: ## Generate protobuf/gRPC/gateway/OpenAPI code and sqlc queries
	buf lint
	buf generate
	MSYS_NO_PATHCONV=1 docker run --rm -v "$(CURDIR):/src" -w /src $(SQLC_IMAGE) generate
	$(PYTHON) dashboards/generate.py
	mkdir -p deploy/helm/hookline/files/dashboards
	cp alerts/hookline.prom.yaml deploy/helm/hookline/files/alerts.yaml
	cp dashboards/*.json deploy/helm/hookline/files/dashboards/

lint: ## Run golangci-lint and buf lint
	golangci-lint run ./...
	buf lint

test: ## Unit tests with the race detector
	go test $(RACE) -count=1 ./internal/... ./cmd/...

test-integration: ## Integration tests against real Postgres/NATS/Redis (testcontainers; needs Docker)
	go test $(RACE) -count=1 -tags=integration -timeout=15m ./...

test-e2e: ## End-to-end scenarios against the running stack (Compose or k3d); E2E_DESTRUCTIVE=1 adds restarts
	. scripts/target-env.sh && go test -count=1 -tags=e2e -timeout=45m -v ./e2e/...

cover: ## Coverage for the core packages
	go test -coverprofile=coverage.out ./internal/delivery ./internal/breaker ./internal/ratelimit ./internal/signing ./internal/ssrf
	go tool cover -func=coverage.out | tail -1

build: ## Build all binaries into bin/
	@mkdir -p bin
	@for s in $(SERVICES); do CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/$$s ./cmd/$$s || exit 1; done
	@echo built: $(SERVICES)

images: ## Build all container images
	@for s in $(SERVICES); do docker build -q --build-arg SERVICE=$$s --build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) -t hookline/$$s:$(VERSION) . || exit 1; done

.env:
	@$(MAKE) --no-print-directory secrets-local

secrets-local: ## Create .env with random local secrets (keeps an existing one)
	@test -f .env && echo ".env exists, leaving it" || { \
	  cp .env.example .env; \
	  mk=$$(head -c 32 /dev/urandom | base64); \
	  ak=hk_live_$$(head -c 32 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 40); \
	  pw=$$(head -c 16 /dev/urandom | base64 | tr -dc 'A-Za-z0-9'); \
	  sed -i "s#^HOOKLINE_MASTER_KEY=.*#HOOKLINE_MASTER_KEY=$$mk#; s#^HOOKLINE_ADMIN_KEY=.*#HOOKLINE_ADMIN_KEY=$$ak#; s#^POSTGRES_PASSWORD=.*#POSTGRES_PASSWORD=$$pw#; s#hookline:hookline@#hookline:$$pw@#" .env; \
	  echo "wrote .env with random secrets"; }

compose-up: .env ## Start the full stack in Docker Compose
	@mkdir -p .hookline && echo compose > .hookline/target
	VERSION=$(VERSION) COMMIT=$(COMMIT) $(COMPOSE) up -d --build --wait
	@echo "ingest http://localhost:8080  control http://localhost:8082  chaos http://localhost:9000  grafana http://localhost:3000"

compose-up-tracing: .env ## Start the stack plus Tempo, Loki and Alloy, with tracing on
	VERSION=$(VERSION) COMMIT=$(COMMIT) OTEL_EXPORTER_OTLP_ENDPOINT=http://alloy:4317 $(COMPOSE) --profile tracing up -d --build --wait

compose-down: ## Stop the Compose stack and delete its volumes
	$(COMPOSE) --profile tracing down -v

compose-logs: ## Follow Compose logs
	$(COMPOSE) logs -f --tail=100

seed: ## Create demo tenants, keys, event types and chaos endpoints (Compose or k3d)
	bash scripts/hookctl.sh seed

demo: ## Send demo traffic for 60 s across three tenants
	bash scripts/hookctl.sh send --tenants acme,globex,initech --rate 100 --duration 60s
	@if [ "$$(cat .hookline/target 2>/dev/null)" = k8s ]; then echo "Grafana: http://grafana.localhost:8080 (folder Hookline)"; else echo "Grafana: http://localhost:3000 (folder Hookline)"; fi

demo-full: ## The 5-minute demo script (docs/demo.md)
	bash scripts/demo-full.sh

HELMFILE := helmfile -f deploy/helmfile.yaml.gotmpl

cluster-up: ## Create the local k3d cluster, secrets and all third-party charts
	k3d cluster list hookline >/dev/null 2>&1 || k3d cluster create --config deploy/k3d/local.yaml
	bash scripts/k8s-secrets.sh
	$(HELMFILE) -e local sync --selector name!=hookline
	@mkdir -p .hookline && echo k8s > .hookline/target

cluster-down: ## Delete the local k3d cluster
	k3d cluster delete hookline
	@rm -f .hookline/target .hookline/k8s.env .hookline/seed.json

cluster-reset: cluster-down cluster-up ## Recreate the cluster from scratch

deploy-local: ## Build images, import them into k3d and install the Hookline chart
	$(MAKE) images VERSION=local
	k3d image import -c hookline $(addprefix hookline/,$(addsuffix :local,$(SERVICES)))
	$(HELMFILE) -e local sync --selector name=hookline --skip-needs

deploy-homeserver: ## Deploy everything to the home server cluster (current kube context)
	bash scripts/k8s-secrets.sh
	$(HELMFILE) -e homeserver sync

loadtest: ## Run the k6 steady-load test (K6_SCRIPT=loadtest/steady.js)
	k6 run $${K6_SCRIPT:-loadtest/steady.js}

chaos: ## Install Chaos Mesh (on demand) and apply the experiments to the local cluster
	$(HELMFILE) -e local --state-values-set chaosMesh=true sync --selector name=chaos-mesh
	kubectl apply -f deploy/chaos/

chaos-stop: ## Remove the Chaos Mesh experiments
	kubectl delete -f deploy/chaos/ --ignore-not-found
