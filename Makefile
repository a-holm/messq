# SPDX-License-Identifier: Apache-2.0

SHELL       := /usr/bin/env bash
.SHELLFLAGS := -euo pipefail -c
.DEFAULT_GOAL := help
.DELETE_ON_ERROR:
MAKEFLAGS   += --warn-undefined-variables --no-builtin-rules

MODULE  := github.com/a-holm/messq
# VERSION carries no dirty marker: DIRTY is the single source for that, so `messq version`
# reports a modified worktree once rather than in two fields.
VERSION ?= $(shell git describe --tags --always 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse --short=12 HEAD 2>/dev/null || echo unknown)
DIRTY   ?= $(shell if git rev-parse --verify HEAD >/dev/null 2>&1; then git diff-index --quiet HEAD -- && echo false || echo true; else echo false; fi)
SOURCE_DATE_EPOCH ?= $(shell git log -1 --format=%ct 2>/dev/null || date -u +%s)
DATE    := $(shell date -u -d @$(SOURCE_DATE_EPOCH) +%Y-%m-%dT%H:%M:%SZ)

# Pinned tools. Each lives in its own module file under tools/, so it never enters go.mod and
# never spends the dependency budget of PLAN.md section 13, and its whole dependency graph is
# hash-pinned in the matching .sum file.
#
# `go tool -modfile=...` names no version at the call site, so there is no deprecation lookup
# against the module proxy on every invocation. That is what makes `make ci` work offline on a
# warm cache without setting GONOPROXY, which would have forced a direct VCS fetch and broken
# proxy-only environments. Update a pin with:
#
#     go get -tool -modfile=tools/gofumpt.mod mvdan.cc/gofumpt@<version>
#
# not with `go mod tidy`: -modfile keeps the module root here, so tidy would try to resolve this
# repository's own packages as dependencies of the tool module.
GOFUMPT  := go tool -modfile=tools/gofumpt.mod gofumpt
GOLANGCI := go tool -modfile=tools/golangci-lint.mod golangci-lint

LDFLAGS := -s -w \
  -X '$(MODULE)/internal/buildinfo.version=$(VERSION)' \
  -X '$(MODULE)/internal/buildinfo.commit=$(COMMIT)' \
  -X '$(MODULE)/internal/buildinfo.date=$(DATE)' \
  -X '$(MODULE)/internal/buildinfo.dirty=$(DIRTY)'
GOBUILD := CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)"

# Directory that fmt-list inspects. The pre-commit hook overrides it with a mirror of the index.
DIR ?= .

.PHONY: help build build-all test race cover lint fmt fmt-check fmt-list vet tidy-check \
        dep-budget layers static-check repro hooks ci clean

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

fmt-list: ## Print the gofumpt-unclean files under DIR. The pre-commit hook points DIR at staged content.
	@$(GOFUMPT) -l "$(DIR)"

vet: ## Run go vet over every package.
	go vet ./...

tidy-check: ## Fail when go mod tidy would change go.mod or go.sum.
	go mod tidy -diff

dep-budget: ## Fail when a direct dependency is outside the PLAN.md section 13 allow-list.
	scripts/dep-budget.sh

layers: ## Fail when a package imports across a forbidden layer boundary.
	scripts/layers.sh

# Ordered, not just listed: under `make -j` an unordered prerequisite would run the assertion
# before build-all has produced the binaries.
static-check: build-all ## Assert the cross-compiled binaries are static, trimmed and cgo-free.
	scripts/assert-static.sh dist/messq-linux-amd64
	scripts/assert-static.sh dist/messq-linux-arm64

repro: ## Build twice with cold, isolated caches and compare sha256.
	@if [[ -n "$$(git status --porcelain)" ]]; then \
		echo "repro: worktree is dirty; commit or stash before checking reproducibility" >&2; \
		exit 1; \
	fi
	@mkdir -p dist
	@cache_a="$$(mktemp -d)"; cache_b="$$(mktemp -d)"; \
	trap 'rm -rf "$$cache_a" "$$cache_b"' EXIT; \
	GOCACHE="$$cache_a" GOOS=linux GOARCH=amd64 $(GOBUILD) -o dist/repro-a ./cmd/messq; \
	GOCACHE="$$cache_b" GOOS=linux GOARCH=amd64 $(GOBUILD) -o dist/repro-b ./cmd/messq; \
	a="$$(sha256sum dist/repro-a | cut -d' ' -f1)"; \
	b="$$(sha256sum dist/repro-b | cut -d' ' -f1)"; \
	echo "repro-a $$a"; \
	echo "repro-b $$b"; \
	if [[ "$$a" != "$$b" ]]; then echo "repro: builds differ" >&2; exit 1; fi; \
	echo "repro: byte-identical"

hooks: ## Route git at the repository hooks in .githooks.
	git config core.hooksPath .githooks
	@echo "hooks: pre-commit checks staged formatting and vets the worktree, pre-push runs make ci"

ci: fmt-check vet tidy-check dep-budget layers test build-all static-check ## Run the whole gate. GitHub Actions runs exactly this.

clean: ## Remove build and coverage artifacts.
	rm -rf dist cover.out
