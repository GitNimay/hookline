# Hookline task runner. Run `make help` for targets.
SHELL := bash
GO_VERSION   := 1.27.1
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

.PHONY: help tools gen lint test test-integration test-e2e cover build images compose-up compose-down compose-logs \
        seed demo demo-full secrets-local cluster-up cluster-down cluster-reset deploy-local deploy-homeserver loadtest chaos

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
	docker run --rm -v "$(CURDIR):/src" -w /src $(SQLC_IMAGE) generate

lint: ## Run golangci-lint and buf lint
	golangci-lint run ./...
	buf lint

test: ## Unit tests with the race detector
	go test $(RACE) -count=1 ./internal/... ./cmd/...

test-integration: ## Integration tests against real Postgres/NATS/Redis (testcontainers; needs Docker)
	go test $(RACE) -count=1 -tags=integration -timeout=15m ./...

test-e2e: ## End-to-end scenarios against the running Compose stack
	go test -count=1 -tags=e2e -timeout=30m -v ./e2e/...

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
	VERSION=$(VERSION) COMMIT=$(COMMIT) $(COMPOSE) up -d --build --wait
	@echo "ingest http://localhost:8080  control http://localhost:8082  chaos http://localhost:9000  grafana http://localhost:3000"

compose-down: ## Stop the Compose stack and delete its volumes
	$(COMPOSE) --profile tracing down -v

compose-logs: ## Follow Compose logs
	$(COMPOSE) logs -f --tail=100

seed: .env ## Create demo tenants, keys, event types and chaos endpoints
	set -a; . ./.env; set +a; go run ./cmd/hookctl seed

demo: ## Send demo traffic for 60 s across three tenants
	set -a; . ./.env; set +a; go run ./cmd/hookctl send --tenants acme,globex,initech --rate 100 --duration 60s
	@echo "Open Grafana: http://localhost:3000 (dashboard: Hookline overview)"

demo-full: ## The 5-minute demo script (docs/demo.md)
	bash scripts/demo-full.sh

cluster-up: ## Create the local k3d cluster and install third-party charts
	k3d cluster create --config deploy/k3d/local.yaml
	helmfile -f deploy/helmfile.yaml -e local apply

cluster-down: ## Delete the local k3d cluster
	k3d cluster delete hookline

cluster-reset: cluster-down cluster-up ## Recreate the cluster from scratch

deploy-local: ## Build images, import them into k3d and install the Hookline chart
	$(MAKE) images VERSION=local
	k3d image import -c hookline $(addprefix hookline/,$(addsuffix :local,$(SERVICES)))
	bash scripts/k8s-secrets.sh hookline
	helm upgrade --install hookline deploy/helm/hookline -n hookline --create-namespace -f deploy/values/local.yaml --wait --timeout 5m

deploy-homeserver: ## Deploy to the home server cluster (current kube context)
	helmfile -f deploy/helmfile.yaml -e homeserver apply

loadtest: ## Run the k6 steady-load test (K6_SCRIPT=loadtest/steady.js)
	k6 run $${K6_SCRIPT:-loadtest/steady.js}

chaos: ## Apply the Chaos Mesh experiments to the local cluster
	kubectl apply -f deploy/chaos/
