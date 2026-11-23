# EventFlow developer tasks. Run `make help` for a list.

SHELL := /bin/bash
GO ?= go
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
COMPOSE ?= docker compose
K6_IMAGE ?= grafana/k6:0.54.0
BASE_URL ?= http://localhost:8080
# Reads the first API key from .env for local load tests (never printed).
API_KEY ?= $(shell grep -E '^EVENTFLOW_API_KEYS=' .env 2>/dev/null | cut -d= -f2 | cut -d, -f1)

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z0-9_.-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-22s\033[0m %s\n",$$1,$$2}'

## ---- Build & quality ----
.PHONY: build
build: ## Build all binaries into ./bin
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/ ./cmd/...

.PHONY: fmt
fmt: ## Format Go code
	gofmt -s -w .

.PHONY: fmt-check
fmt-check: ## Fail if any Go file is not gofmt-formatted
	@out=$$(gofmt -s -l .); if [ -n "$$out" ]; then echo "unformatted files:"; echo "$$out"; exit 1; fi

.PHONY: vet
vet: ## Run go vet (including integration-tagged files)
	$(GO) vet ./...
	$(GO) vet -tags integration ./tests/...

.PHONY: lint
lint: fmt-check vet ## Run gofmt check, go vet and golangci-lint
	golangci-lint run ./...

.PHONY: vuln
vuln: ## Scan dependencies for known vulnerabilities
	$(GO) run golang.org/x/vuln/cmd/govulncheck@v1.1.3 ./...

## ---- Tests ----
.PHONY: test
test: ## Run unit tests with the race detector
	$(GO) test -race -count=1 ./...

.PHONY: test-integration
test-integration: ## Run integration & failure tests (needs Docker)
	$(GO) test -tags integration -race -count=1 -timeout 15m ./tests/integration/...

.PHONY: test-all
test-all: test test-integration ## Run every test suite

.PHONY: cover
cover: ## Unit + integration coverage report (coverage.html)
	$(GO) test -tags integration -count=1 -timeout 15m -coverpkg=./internal/... -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out | tail -1
	$(GO) tool cover -html=coverage.out -o coverage.html

## ---- Local stack ----
.PHONY: env
env: ## Create .env with freshly generated random secrets (refuses to overwrite)
	@test ! -f .env || { echo ".env already exists"; exit 1; }
	@sed -e "s|CHANGE_ME_postgres_owner_password|$$(openssl rand -hex 24)|" \
	     -e "s|CHANGE_ME_app_role_password|$$(openssl rand -hex 24)|" \
	     -e "s|CHANGE_ME_redis_password|$$(openssl rand -hex 24)|" \
	     -e "s|CHANGE_ME_generate_a_long_random_api_key|ef_$$(openssl rand -hex 24)|" \
	     -e "s|CHANGE_ME_grafana_admin_password|$$(openssl rand -hex 12)|" .env.example > .env
	@chmod 600 .env
	@echo "wrote .env with random secrets"

.PHONY: up
up: ## Build and start the full stack
	$(COMPOSE) up -d --build

.PHONY: down
down: ## Stop the stack (keeps volumes)
	$(COMPOSE) down

.PHONY: clean-stack
clean-stack: ## Stop the stack and DELETE its volumes
	$(COMPOSE) down -v

.PHONY: logs
logs: ## Tail api and worker logs
	$(COMPOSE) logs -f api worker

.PHONY: ps
ps: ## Show stack status
	$(COMPOSE) ps

.PHONY: smoke
smoke: ## Submit jobs to the running stack and wait for their outcomes
	@BASE_URL=$(BASE_URL) API_KEY=$(API_KEY) ./scripts/smoke.sh

.PHONY: docker-build
docker-build: ## Build the api, worker and migrate images
	for t in api worker migrate; do docker build -f deploy/docker/Dockerfile --target $$t --build-arg VERSION=$(VERSION) -t eventflow-$$t:$(VERSION) . || exit 1; done

.PHONY: migrate-up
migrate-up: ## Apply migrations (needs EVENTFLOW_DATABASE_URL)
	$(GO) run ./cmd/migrate up

.PHONY: migrate-down
migrate-down: ## Roll back the last migration (needs EVENTFLOW_DATABASE_URL)
	$(GO) run ./cmd/migrate down 1

.PHONY: dashboard
dashboard: ## Regenerate the Grafana dashboard JSON
	python3 scripts/gen_dashboard.py

## ---- Load tests (k6 in Docker against BASE_URL) ----
# Tune scenarios with K6_ARGS, e.g. make loadtest-submit K6_ARGS="-e RATE=800 -e DURATION=2m"
K6_ARGS ?=
K6 = docker run --rm -i --network host -u "$$(id -u):$$(id -g)" -v "$(CURDIR)/loadtest:/scripts" \
	-e BASE_URL=$(BASE_URL) -e API_KEY=$(API_KEY) $(K6_IMAGE) run --quiet $(K6_ARGS)

.PHONY: loadtest-submit loadtest-concurrent loadtest-read loadtest-processing loadtest-sustained
loadtest-submit: ## k6: job submission throughput
	$(K6) /scripts/submit.js
loadtest-concurrent: ## k6: concurrent mixed API requests
	$(K6) /scripts/concurrent.js
loadtest-read: ## k6: read API performance
	$(K6) /scripts/read.js
loadtest-processing: ## k6: end-to-end queue processing throughput
	$(K6) /scripts/processing.js
loadtest-sustained: ## k6: sustained load (DURATION=10m by default)
	$(K6) /scripts/sustained.js

.PHONY: clean
clean: ## Remove build artifacts
	rm -rf bin dist coverage.out coverage.html
