# messq — Implementation Backlog (human-readable)

Derived from `docs/PLAN.md`; machine form in `docs/planning/issues.json`. One developer,
start **2026-08-24**, sequential with light (half-day) overlap; `seq` will equal the GitHub
issue number, and every issue's blockers have lower seq numbers. 42 issues, 73.5 estimated
dev-days; **v1.0.0 tags 2026-11-20**, phase 2 completes 2026-12-03.

## Milestones

| Milestone | Due | Theme |
|---|---|---|
| M1: Foundations | 2026-08-28 | Repo, CI gates, primitives, normative spec |
| M2: Durable store & publish | 2026-09-10 | SQLite store, group commit, publish, crash harness (D1 gate) |
| M3: Delivery engine | 2026-09-24 | Consumers, ack family, sweeper, DLQ, property suite |
| M4: API & daemon | 2026-10-06 | HTTP surface, auth, lifecycle, contract tests |
| M5: Observability | 2026-10-14 | Events, slog, trace, metrics |
| M6: CLI & client | 2026-10-26 | Client pkg, cobra tree, `--exec`, quickstart |
| M7: Operations | 2026-11-06 | Retention, seek/replay, DLQ ops, backup, doctor, bench |
| M8: Hardening & v1.0 | 2026-11-20 | Fault matrix, fuzz/soak, upgrades, packaging, docs, tag |
| M9: Phase 2 | 2026-12-03 | Delayed delivery, ordering, rate limit, TLS, workers, export |

## Backlog

| # | Title | Milestone | Prio | Size | Est (d) | Start | Target | Blocked by |
|---:|---|---|---|---|---:|---|---|---|
| 1 | Repo bootstrap: module, layout, Makefile, licence, CI skeleton | M1 | P0 | S | 1 | 2026-08-24 | 2026-08-24 | — |
| 2 | CI quality gates: lint config, race, coverage floors, govulncheck | M1 | P0 | S | 1 | 2026-08-25 | 2026-08-25 | 1 |
| 3 | Core primitives: ULID ids, subject matcher (+fuzz), Clock seam, sentinel errors | M1 | P0 | M | 1.5 | 2026-08-26 | 2026-08-27 | 1 |
| 4 | Normative spec: SEMANTICS.md (state machine, transitions, invariants) + ADRs | M1 | P0 | M | 1.5 | 2026-08-27 | 2026-08-28 | 1 |
| 5 | SQLite store foundation: schema v1, migrations, pragma enforcement, recovery | M2 | P0 | L | 2.5 | 2026-08-31 | 2026-09-02 | 2, 3, 4 |
| 6 | Single-writer group-commit engine + durability modes | M2 | P0 | M | 2 | 2026-09-02 | 2026-09-04 | 5 |
| 7 | Streams CRUD + publish path + publish dedup + peek reads | M2 | P0 | M | 2 | 2026-09-04 | 2026-09-08 | 5, 6 |
| 8 | Crash harness v1: SIGKILL loop, three-valued ledger oracle, `messq verify` | M2 | P0 | L | 2.5 | 2026-09-08 | 2026-09-10 | 6, 7 |
| 9 | Consumers: CRUD, cursor, claim/top-up, flow control | M3 | P0 | L | 2.5 | 2026-09-11 | 2026-09-15 | 4, 7 |
| 10 | Ack/nak/term/extend with fenced tokens and stale-ack detection | M3 | P0 | M | 2 | 2026-09-15 | 2026-09-17 | 9 |
| 11 | Timeout sweeper: ack_wait expiry, backoff schedule, max_deliver | M3 | P0 | M | 1.5 | 2026-09-17 | 2026-09-18 | 10 |
| 12 | DLQ as a stream: atomic dead-lettering with provenance | M3 | P0 | M | 1.5 | 2026-09-21 | 2026-09-22 | 10, 11 |
| 13 | Reference model + rapid property/invariant suite (incl. restart) | M3 | P0 | L | 2.5 | 2026-09-22 | 2026-09-24 | 9, 10, 11, 12 |
| 14 | HTTP data plane: publish, long-poll fetch, settle endpoints, error envelope | M4 | P0 | L | 2.5 | 2026-09-25 | 2026-09-29 | 7, 9, 10 |
| 15 | HTTP admin & inspection endpoints | M4 | P1 | M | 1.5 | 2026-09-29 | 2026-09-30 | 14 |
| 16 | AuthN/Z: bearer token file, roles, socket permissions, listener policy | M4 | P1 | M | 1.5 | 2026-10-01 | 2026-10-02 | 14 |
| 17 | Daemon lifecycle: signals, graceful drain, SIGHUP reload, systemd unit | M4 | P1 | M | 1.5 | 2026-10-02 | 2026-10-05 | 14 |
| 18 | API contract tests + executable curl golden tests | M4 | P1 | S | 1 | 2026-10-06 | 2026-10-06 | 14, 15, 16 |
| 19 | Event pipeline: vocabulary, in-tx events table, slog handlers, sampling | M5 | P0 | L | 3 | 2026-10-07 | 2026-10-09 | 5, 11 |
| 20 | `messq trace` + events query/follow API | M5 | P0 | M | 1.5 | 2026-10-12 | 2026-10-13 | 19 |
| 21 | Prometheus metrics, shipped alert rules, Grafana dashboard | M5 | P1 | M | 1.5 | 2026-10-13 | 2026-10-14 | 14, 19 |
| 22 | Go client package (`pkg/client`) + Worker helper | M6 | P1 | M | 1.5 | 2026-10-15 | 2026-10-16 | 14, 15 |
| 23 | CLI framework: cobra root, output contract, exit codes | M6 | P1 | M | 1.5 | 2026-10-16 | 2026-10-19 | 22 |
| 24 | CLI core commands: stream/consumer/pub/sub/peek/pending/lag/ack-family | M6 | P1 | M | 2 | 2026-10-20 | 2026-10-21 | 15, 23 |
| 25 | `messq sub --exec`: shell-script workers with the sysexits contract | M6 | P1 | M | 1.5 | 2026-10-22 | 2026-10-23 | 24 |
| 26 | Quickstart, shell completions, help topics, testscript golden suite | M6 | P1 | M | 1.5 | 2026-10-23 | 2026-10-26 | 24, 25 |
| 27 | Retention (limits/workqueue), housekeeping, disk safety | M7 | P0 | L | 2.5 | 2026-10-27 | 2026-10-29 | 9, 19 |
| 28 | Seek + replay + destructive-action discipline | M7 | P1 | M | 2 | 2026-10-29 | 2026-11-02 | 9, 24 |
| 29 | DLQ operations: ls/show/redrive with guard rails | M7 | P1 | M | 1.5 | 2026-11-02 | 2026-11-03 | 12, 24, 28 |
| 30 | Backup/restore (VACUUM INTO) + `messq doctor` | M7 | P2 | M | 1.5 | 2026-11-04 | 2026-11-05 | 5, 24 |
| 31 | `messq bench` + honest performance methodology and numbers | M7 | P2 | M | 1.5 | 2026-11-05 | 2026-11-06 | 14, 24 |
| 32 | Fault-injection points + crash matrix + ENOSPC/EIO/clock scenarios | M8 | P0 | L | 2.5 | 2026-11-09 | 2026-11-11 | 8, 13, 27 |
| 33 | Fuzzing corpus + nightly jobs + soak test | M8 | P1 | M | 1.5 | 2026-11-11 | 2026-11-12 | 13, 31 |
| 34 | Upgrade fixtures + schema-version gate tests | M8 | P1 | S | 1 | 2026-11-13 | 2026-11-13 | 5 |
| 35 | 1.0 documentation set: README, guarantees, operations, runbooks, comparisons | M8 | P1 | L | 2.5 | 2026-11-16 | 2026-11-18 | 4, 26, 31 |
| 36 | Packaging & release engineering + **v1.0.0 tag** | M8 | P1 | M | 2 | 2026-11-18 | 2026-11-20 | 32, 33, 34, 35 |
| 37 | Phase 2: delayed delivery (`Messq-Deliver-At`) | M9 | P2 | M | 1.5 | 2026-11-20 | 2026-11-23 | 11, 36 |
| 38 | Phase 2: ordered-by-subject consumers | M9 | P2 | M | 2 | 2026-11-24 | 2026-11-25 | 9, 13, 36 |
| 39 | Phase 2: per-consumer delivery rate limiting | M9 | P2 | S | 1 | 2026-11-26 | 2026-11-26 | 9, 36 |
| 40 | Phase 2: native TLS listener with hot certificate reload | M9 | P2 | M | 1.5 | 2026-11-27 | 2026-11-30 | 16, 36 |
| 41 | Phase 2: worker attribution & per-worker caps (consumer groups, lite) | M9 | P3 | M | 2 | 2026-11-30 | 2026-12-02 | 10, 36 |
| 42 | Phase 2: audit export (events NDJSON export & sink) | M9 | P3 | S | 1 | 2026-12-02 | 2026-12-03 | 20, 36 |

### Reading notes

- **Priorities:** P0 = correctness-critical spine (storage, delivery semantics, crash/property
  harnesses, data-plane API, event pipeline, retention safety); P1 = required for a credible
  release; P2/P3 = important or deferrable.
- **The D1 gate** lives in issue 8: if the measured durable-throughput target fails there, the
  storage escape hatch (payload segment files behind the store package) is invoked *then*, not
  at M8.
- Testing infrastructure is deliberately interleaved, not deferred: the crash harness (8) lands
  before the delivery engine, the property suite (13) closes M3, and the docs-cannot-rot
  harness (18) precedes all user-facing documentation.
