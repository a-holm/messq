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
GOFUMPT     := go tool -modfile=tools/gofumpt.mod gofumpt
GOLANGCI    := go tool -modfile=tools/golangci-lint.mod golangci-lint
ACTIONLINT  := go tool -modfile=tools/actionlint.mod actionlint
GOVULNCHECK := go tool -modfile=tools/govulncheck.mod govulncheck
VULNGATE    := go run ./internal/tools/vulngate -allow .govulncheck-allow

LDFLAGS := -s -w \
  -X '$(MODULE)/internal/buildinfo.version=$(VERSION)' \
  -X '$(MODULE)/internal/buildinfo.commit=$(COMMIT)' \
  -X '$(MODULE)/internal/buildinfo.date=$(DATE)' \
  -X '$(MODULE)/internal/buildinfo.dirty=$(DIRTY)'
GOBUILD := CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)"

# Directory that fmt-list inspects. The pre-commit hook overrides it with a mirror of the index.
DIR ?= .

.PHONY: help build build-all test cover cover-html cover-ratchet cover-ratchet-check lint \
        vuln vuln-strict fmt fmt-check fmt-list vet tidy-check dep-budget layers \
        spdx static-check repro hooks ci clean

help: ## Show this help.
	@echo "messq $(VERSION)"
	@echo
	@awk 'BEGIN {FS = ":.*## "} /^[a-z][a-z-]*:.*## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build a static host binary into dist/messq.
	@mkdir -p dist
	$(GOBUILD) -o dist/messq ./cmd/messq

build-all: ## Build static linux/amd64 and linux/arm64 binaries into dist/.
	@mkdir -p dist
	GOOS=linux GOARCH=amd64 $(GOBUILD) -o dist/messq-linux-amd64 ./cmd/messq
	GOOS=linux GOARCH=arm64 $(GOBUILD) -o dist/messq-linux-arm64 ./cmd/messq

# The race detector needs cgo, while the shipped binary must be built without it (GOBUILD
# above pins CGO_ENABLED=0). Both facts are true at once and neither may be dropped: an
# unraced suite hides the bugs this project exists to avoid, and a cgo-linked release binary
# breaks the static-binary promise. -count=1 defeats the test cache, without which an
# `ok (cached)` line makes the gate pass vacuously after an unrelated change. -shuffle=on
# catches order-dependent tests while there are still few of them.
test: ## Run the test suite under the race detector.
	CGO_ENABLED=1 go test -race -count=1 -shuffle=on -timeout=5m ./...

# -covermode=atomic is mandatory under -race. -coverpkg spans the whole tree because
# internal/queue is exercised by the reference model in internal/model and internal/store
# through the API and the crash harness; a per-package profile would undercount both and push
# tests into the wrong package to satisfy a number.
cover: ## Measure coverage and enforce the floors in coverage.floors.
	CGO_ENABLED=1 go test -race -count=1 -covermode=atomic \
		-coverpkg=./internal/...,./pkg/... -coverprofile=cover.out ./...
	go run ./internal/tools/covergate -profile cover.out -floors coverage.floors

cover-html: ## Open the coverage profile produced by `make cover` as HTML.
	go tool cover -html=cover.out

# Run by a human, committed, reviewed. Never by CI: a bot that edits the gate is not a gate.
cover-ratchet: cover ## Raise the floors that measured coverage clears by a whole point.
	go run ./internal/tools/covergate -profile cover.out -floors coverage.floors -ratchet

# Compares against the merge base rather than against origin/main's tip, so a floor raised on
# main after this branch started does not read as a lowering here. Silent when there is no
# merge base to compare against, which is the case in a fresh shallow clone.
cover-ratchet-check: ## Fail when this branch lowers a coverage floor without saying why.
	@base="$$(git merge-base HEAD origin/main 2>/dev/null)" || { \
		echo "cover-ratchet-check: no merge base with origin/main, nothing to compare"; \
		exit 0; \
	}; \
	baseline="$$(mktemp)"; \
	trap 'rm -f "$$baseline"' EXIT; \
	git show "$$base:coverage.floors" >"$$baseline" 2>/dev/null || { \
		echo "cover-ratchet-check: the merge base has no coverage.floors, nothing to compare"; \
		exit 0; \
	}; \
	allow=(); \
	if git log -1 --format=%B | grep -q 'coverage-floor-lowered:'; then allow=(-allow-lower); fi; \
	go run ./internal/tools/covergate -floors coverage.floors -compare-floors "$$baseline" $${allow[@]+"$${allow[@]}"}

# config verify runs first so a typo in .golangci.yml is a schema error rather than half the
# linter set silently switching itself off. It validates against a schema embedded in the
# pinned binary, so it needs no network.
lint: ## Verify .golangci.yml, run the pinned golangci-lint, and lint the workflows.
	$(GOLANGCI) config verify
	$(GOLANGCI) run
	$(ACTIONLINT) .github/workflows/*.yml

# Source mode reports the vulnerabilities reachable from messq's own code, which is the right
# signal-to-noise for a gate. With -format sarif govulncheck always exits 0, so vulngate makes
# the decision. The expiry check runs first: it needs no network and it is what makes a stale
# suppression fail before the scan is even attempted.
#
# Unlike every other target, this one needs network access on every run: a CVE published today
# is not in yesterday's database.
vuln: ## Fail on a reachable vulnerability or an expired suppression.
	$(VULNGATE) -check-expiry
	$(GOVULNCHECK) -format sarif ./... | $(VULNGATE)

vuln-strict: ## Same as vuln, and also fail on a suppression that no longer matches anything.
	$(VULNGATE) -check-expiry
	$(GOVULNCHECK) -format sarif ./... | $(VULNGATE) -strict

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

spdx: ## Fail when a source file is missing its SPDX licence header.
	scripts/spdx.sh

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

ci: fmt-check vet tidy-check dep-budget layers spdx lint test cover cover-ratchet-check vuln build-all static-check ## Run the whole gate. GitHub Actions runs exactly this.

clean: ## Remove build and coverage artifacts.
	rm -rf dist cover.out
