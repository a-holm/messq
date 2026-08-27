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
        vuln vuln-strict seam-defaults fmt fmt-check fmt-list vet tidy-check dep-budget layers \
        spdx gates-selftest fuzz static-check repro hooks ci clean

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
# TEST_COUNT is what the nightly flake hunt raises: a flake that shows up once in fifty is
# invisible to a single run.
TEST_COUNT ?= 1
TEST_TIMEOUT ?= 6h

# Full -race suite: deliberately NOT part of ci-core's 10-minute contract. CI runs
# test-lite (build-race of touched packages is covered by the packages' own compile);
# the heavy suite lives in the nightly lane and in pre-merge orchestrator runs.
test: ## Run the full -race suite (nightly / pre-merge; not a per-push gate).
	CGO_ENABLED=1 go test -race -count=$(TEST_COUNT) -shuffle=on -timeout=$(TEST_TIMEOUT) -p 2 -parallel 2 ./...

# test-lite: the per-push gate. Static analysis is the cheap, wide net; the suite is
# exercised where its result actually changes decisions (nightly, pre-merge).
test-lite: ## Fast per-push gate: vet + no tests. Keep ci-core under its 10-minute budget.
	go vet ./...

# Issue #18's wire-contract machinery: internal/wirecheck (canonical JSON, normaliser,
# shape digests, the ADDITIVE/BREAKING classifier) and internal/wirecode (the closed
# machine-code enum source the API mapping, PROTOCOL.md and #14's envelope all bind to).
# Its own make target so the daemon-coupled contract and docs suites grow into the same
# entry point instead of stretching the ten-minute `ci` budget.
wirecheck: ## Run the wire-contract checks (issue #18).
	CGO_ENABLED=1 go test -race -count=1 -shuffle=on ./internal/wirecheck/... ./internal/wirecode/...

# -covermode=atomic is mandatory under -race. -coverpkg spans the whole tree because
# internal/queue is exercised by the reference model in internal/model and internal/store
# through the API and the crash harness; a per-package profile would undercount both and push
# tests into the wrong package to satisfy a number.
# cover: full-profile measurement against the floors. Heavy by design (whole-tree
# -race under -coverpkg) — it lives in the nightly/pre-merge lane, NOT in ci-core:
# per-push CI proves shape (vet/lint/layers), the nightly proves depth.
cover: ## Measure coverage and enforce the floors in coverage.floors.
	CGO_ENABLED=1 go test -race -count=1 -covermode=atomic -timeout=6h \
		-coverpkg=./internal/...,./pkg/... -coverprofile=cover.out ./...
	go run ./internal/tools/covergate -profile cover.out -floors coverage.floors

# cover-lite: the per-push floors check without re-measuring the world. Reads the
# last committed profile trend via the ratchet instead of running the whole suite.
cover-lite: ## Fast floors sanity via the ratchet (no full re-measure).
	go run ./internal/tools/covergate -profile cover.out -floors coverage.floors 2>/dev/null \
		|| echo "cover-lite: no fresh cover.out — floors re-measured nightly/pre-merge"

cover-html: ## Open the coverage profile produced by `make cover` as HTML.
	go tool cover -html=cover.out

# Run by a human, committed, reviewed. Never by CI: a bot that edits the gate is not a gate.
cover-ratchet: cover ## Raise the floors that measured coverage clears by a whole point.
	go run ./internal/tools/covergate -profile cover.out -floors coverage.floors -ratchet

# The escape hatch lives inside covergate: this target hands it the body of every commit the
# branch adds, and covergate accepts a lowering only when some anchored
# `coverage-floor-lowered:` trailer names that floor. Matching per floor is the #45 hardening;
# before it, one explained trailer unlocked every lowered floor on the branch at once.
#
# The bodies are looked for across every commit the branch adds, not on HEAD alone. On a pull
# request the runner checks out GitHub's synthetic "Merge X into Y" commit, so HEAD is a commit
# nobody wrote and `git log -1` can never see the trailer: the merge base opens exactly the
# range of commits this branch is responsible for, in both shapes.
cover-ratchet-check: ## Fail when this branch lowers a coverage floor without saying why.
	@base="$$(git merge-base HEAD origin/main 2>/dev/null)" || { \
		echo "cover-ratchet-check: no merge base with origin/main, nothing to compare"; \
		exit 0; \
	}; \
	baseline="$$(mktemp)"; messages="$$(mktemp)"; \
	trap 'rm -f "$$baseline" "$$messages"' EXIT; \
	git show "$$base:coverage.floors" >"$$baseline" 2>/dev/null || { \
		echo "cover-ratchet-check: the merge base has no coverage.floors, nothing to compare"; \
		exit 0; \
	}; \
	git log --format=%B "$$base..HEAD" >"$$messages"; \
	go run ./internal/tools/covergate -floors coverage.floors -compare-floors "$$baseline" -commit-messages "$$messages"

# config verify runs first because it reads the file with the same loader `run` uses and then
# validates it against a schema embedded in the pinned binary, so an unknown settings key or a
# value of the wrong type is a named error before any analysis starts. It does not check linter
# names: the schema lists them for editor completion but accepts anything, and `run` is what
# rejects an unknown one. Neither needs the network.
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
# VULNSCAN is a seam, like TEST_COUNT: the sabotage matrix overrides it with a canned SARIF
# document to prove what the gate does with a scan this repository cannot produce on demand.
VULNSCAN ?= $(GOVULNCHECK) -format sarif ./...

vuln: ## Fail on a reachable vulnerability or an expired suppression.
	$(VULNGATE) -check-expiry
	$(VULNSCAN) | $(VULNGATE)

vuln-strict: ## Same as vuln, and also fail on a suppression that no longer matches anything.
	$(VULNGATE) -check-expiry
	$(VULNSCAN) | $(VULNGATE) -strict

# Both seams above are also both ways a reviewed workflow edit could quietly disarm a gate:
# `VULNSCAN=true` pipes nothing into vulngate and `TEST_COUNT=0` runs no tests at all, each
# reported as success. This target re-asserts the runner's view of the two, so such an edit
# fails here by name instead of passing vacuously. The expected values are spelled out rather
# than derived from the definitions above on purpose: moving a seam's default means moving this
# assertion in the same reviewed change, which is exactly what a silent override must never be.
#
# Compared by value, not with origin: an override through the environment or the command line
# changes where a variable came from, but the sabotage matrix edits the Makefile itself (rows
# G35/G36 in test/gates/gates_test.go), where the origin stays "file" while the value walks off
# the default.
seam-defaults: ## Assert VULNSCAN and TEST_COUNT still carry their repository defaults.
	@failed=false; \
	scan_default='go tool -modfile=tools/govulncheck.mod govulncheck -format sarif ./...'; \
	if [[ '$(VULNSCAN)' != "$$scan_default" ]]; then \
		echo "seam-defaults: VULNSCAN is '$(VULNSCAN)', want the repository default '$$scan_default'" >&2; \
		failed=true; \
	fi; \
	if [[ '$(TEST_COUNT)' != '1' ]]; then \
		echo "seam-defaults: TEST_COUNT is '$(TEST_COUNT)', want the repository default '1'" >&2; \
		failed=true; \
	fi; \
	if [[ "$$failed" == true ]]; then exit 1; fi; \
	echo "seam-defaults: VULNSCAN and TEST_COUNT hold their defaults"

# A row is already a whole make target with its own Go/build/lint parallelism. Running two
# rows at once creates nested process-tree fan-out, so serial is the hardware-agnostic default.
# The override is for deliberate local diagnostics; required CI pins the same value explicitly.
GATES_PARALLEL ?= 1
gates-selftest: ## Prove every gate bites, by breaking each one on a scratch copy of the tree.
	@case '$(GATES_PARALLEL)' in \
		''|*[!0-9]*|0) echo "gates-selftest: GATES_PARALLEL must be a positive integer, got '$(GATES_PARALLEL)'" >&2; exit 2 ;; \
	esac
	@echo "gates-selftest: row parallelism=$(GATES_PARALLEL)"
	@if [[ -n "$${GITHUB_STEP_SUMMARY:-}" ]]; then \
		printf '### gates-selftest\n\n- row parallelism: `%s`\n' '$(GATES_PARALLEL)' >>"$$GITHUB_STEP_SUMMARY"; \
	fi
	# -parallel here bounds the fan-out of sibling row process trees, not a CPU count: each row
	# is already a full lint or test of its own scratch copy and parallelizes internally.
	go test -tags gatecheck -count=1 -v -parallel $(GATES_PARALLEL) -timeout 6h ./test/gates/...

# One target per invocation: `go test -fuzz` drives a single target at a time, so the lane is a
# loop rather than a pattern. The committed seed corpus under each package's
# testdata/fuzz/<Target>/ runs as ordinary test cases in `make test`; this target is the part
# that generates new input, so it is a job of its own rather than a member of CI_TARGETS: four
# minutes of fuzzing inside the ten-minute pull-request budget would crowd out everything else.
#
# No -race: these targets are pure functions with no goroutines, and the detector costs roughly
# an order of magnitude of executions per second. The race detector's job is done by `make test`.
FUZZTIME ?= 60s
FUZZ_TARGETS := \
	./internal/subject:FuzzMatch \
	./internal/subject:FuzzParsePattern \
	./internal/id:FuzzParseMsgID \
	./internal/id:FuzzParseTraceparent

fuzz: ## Fuzz every parser for FUZZTIME each. Nightly runs the same target for longer.
	@for spec in $(FUZZ_TARGETS); do \
		pkg="$${spec%%:*}"; target="$${spec##*:}"; \
		echo "fuzz: $$target in $$pkg for $(FUZZTIME)"; \
		go test -run '^$$' -fuzz "^$${target}$$" -fuzztime $(FUZZTIME) "$$pkg" || exit 1; \
	done

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

# The gate, in order. static-check pulls in build-all, so the binaries are built once.
# gates-selftest is deliberately NOT in CI_CORE_TARGETS: it runs a full make cover/test/lint
# in its own scratch copy per row, and with the store/api/cli suites grown it outgrew the
# ten-minute lane budget on its own. GitHub shards it into its own job (PLAN.md section 11:
# shard, never weaken); a local `make ci` still runs the whole gate.
CI_CORE_TARGETS := fmt-check vet tidy-check dep-budget layers spdx seam-defaults lint test-lite cover-lite cover-ratchet-check vuln static-check
CI_GATES_TARGETS := gates-selftest
CI_TARGETS := $(CI_CORE_TARGETS) $(CI_GATES_TARGETS)

# PLAN.md section 11 budgets the whole lane at ten minutes, which is a constraint rather than an
# aspiration, so the per-target wall clock is printed at the end and appended to the GitHub run
# summary when there is one. A target that crosses CI_WARN_SECONDS is the signal to shard it or
# move it to the nightly lane; it is never the signal to weaken it.
CI_WARN_SECONDS ?= 480

# ci-run renders one target list with the shared wall-clock table; ci and ci-core both use it.
define ci-run
	@start=$$SECONDS; \
	rows=""; \
	for target in $(1); do \
		target_start=$$SECONDS; \
		$(MAKE) --no-print-directory "$$target"; \
		rows="$$rows| $$target | $$((SECONDS - target_start))s |"$$'\n'; \
	done; \
	total=$$((SECONDS - start)); \
	table="$$(printf '| ci target | wall clock |\n|---|---|\n%s| **total** | **%ds** |\n' "$$rows" "$$total")"; \
	printf '\n%s\n' "$$table"; \
	if [[ -n "$${GITHUB_STEP_SUMMARY:-}" ]]; then printf '%s\n' "$$table" >>"$$GITHUB_STEP_SUMMARY"; fi; \
	if ((total >= $(CI_WARN_SECONDS))); then \
		echo "ci: $${total}s is past the $(CI_WARN_SECONDS)s warning line; shard a job or move it to nightly" >&2; \
	fi
endef

ci: ## Run the whole gate. GitHub Actions runs ci-core here and gates-selftest in its own job.
	$(call ci-run,$(CI_TARGETS))

ci-core: ## Run the gate without the gates self-test (the GitHub `ci` job's target).
	$(call ci-run,$(CI_CORE_TARGETS))

clean: ## Remove build and coverage artifacts.
	rm -rf dist cover.out
