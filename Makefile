# SPDX-License-Identifier: Apache-2.0

SHELL       := /usr/bin/env bash
.SHELLFLAGS := -euo pipefail -c
.DEFAULT_GOAL := help
.DELETE_ON_ERROR:
MAKEFLAGS   += --warn-undefined-variables --no-builtin-rules

MODULE  := github.com/a-holm/messq
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse --short=12 HEAD 2>/dev/null || echo unknown)
SOURCE_DATE_EPOCH ?= $(shell git log -1 --format=%ct 2>/dev/null || date -u +%s)
DATE    := $(shell date -u -d @$(SOURCE_DATE_EPOCH) +%Y-%m-%dT%H:%M:%SZ)

# Pinned tools. Run through `go run tool@version` so they never enter go.mod and never spend
# the dependency budget of PLAN.md section 13. Both fetch on first use.
GOFUMPT  := go run mvdan.cc/gofumpt@v0.11.0
GOLANGCI := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.1

LDFLAGS := -s -w \
  -X '$(MODULE)/internal/buildinfo.version=$(VERSION)' \
  -X '$(MODULE)/internal/buildinfo.commit=$(COMMIT)' \
  -X '$(MODULE)/internal/buildinfo.date=$(DATE)'
GOBUILD := CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)"

.PHONY: help build build-all test race cover lint fmt fmt-check vet tidy-check dep-budget \
        layers repro hooks ci clean

help: ## Show this help.
	@echo "messq $(VERSION)"
	@echo
	@awk 'BEGIN {FS = ":.*## "} /^[a-z][a-z-]*:.*## / {printf "  %-12s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build a static host binary into dist/messq.
	@mkdir -p dist
	$(GOBUILD) -o dist/messq ./cmd/messq

build-all: ## Build static linux/amd64 and linux/arm64 binaries into dist/.
	@mkdir -p dist
	GOOS=linux GOARCH=amd64 $(GOBUILD) -o dist/messq-linux-amd64 ./cmd/messq
	GOOS=linux GOARCH=arm64 $(GOBUILD) -o dist/messq-linux-arm64 ./cmd/messq

test: ## Run the test suite.
	go test ./...

race: ## Run the test suite under the race detector.
	go test -race ./...

cover: ## Run tests with coverage and print a per-function report.
	go test -coverprofile=cover.out ./...
	go tool cover -func=cover.out

lint: ## Run the pinned golangci-lint. Fetches the tool on first use.
	$(GOLANGCI) run

fmt: ## Format every Go file with the pinned gofumpt.
	$(GOFUMPT) -l -w .

fmt-check: ## Fail when a Go file is not gofumpt-clean.
	@unformatted="$$($(GOFUMPT) -l .)"; \
	if [[ -n "$$unformatted" ]]; then \
		echo "fmt-check: not gofumpt-clean, run 'make fmt':" >&2; \
		echo "$$unformatted" >&2; \
		exit 1; \
	fi; \
	echo "fmt-check: gofumpt-clean"

vet: ## Run go vet over every package.
	go vet ./...

tidy-check: ## Fail when go mod tidy would change go.mod or go.sum.
	go mod tidy -diff

dep-budget: ## Fail when a direct dependency is outside the PLAN.md section 13 allow-list.
	scripts/dep-budget.sh

layers: ## Fail when a package imports across a forbidden layer boundary.
	scripts/layers.sh

repro: ## Build twice from a clean tree and compare sha256.
	@if [[ -n "$$(git status --porcelain)" ]]; then \
		echo "repro: worktree is dirty; commit or stash before checking reproducibility" >&2; \
		exit 1; \
	fi
	@mkdir -p dist
	GOOS=linux GOARCH=amd64 $(GOBUILD) -o dist/repro-a ./cmd/messq
	go clean -cache
	GOOS=linux GOARCH=amd64 $(GOBUILD) -o dist/repro-b ./cmd/messq
	@a="$$(sha256sum dist/repro-a | cut -d' ' -f1)"; \
	b="$$(sha256sum dist/repro-b | cut -d' ' -f1)"; \
	echo "repro-a $$a"; \
	echo "repro-b $$b"; \
	if [[ "$$a" != "$$b" ]]; then echo "repro: builds differ" >&2; exit 1; fi; \
	echo "repro: byte-identical"

hooks: ## Route git at the repository hooks in .githooks.
	git config core.hooksPath .githooks
	@echo "hooks: pre-commit runs fmt-check and vet, pre-push runs make ci"

ci: fmt-check vet tidy-check dep-budget layers test build-all ## Run the whole gate. GitHub Actions runs exactly this.

clean: ## Remove build and coverage artifacts.
	rm -rf dist cover.out
