# messq — The Definitive Project Plan

**Status:** adopted. This is the project's master plan; conflicting design options are adjudicated in §2 with reasons. The implementation backlog derived from this plan lives in the issue tracker and the GitHub project (issues #1-#42, dependency-ordered).

> **One-line thesis:** messq is a single-binary, single-node queue daemon for Linux with
> at-least-once delivery and JetStream-grade ack semantics, whose differentiator is
> *answerability*: every state transition of every message is durably recorded in the same
> transaction as the state change, and `messq trace <id>` replays the whole story.
> Kafka-minimum semantics, beanstalkd-grade operational simplicity, understandable in an evening.

---

## 1. Vision & positioning

### 1.1 The gap

Small teams (3–15 engineers, internal services, no dedicated SRE) that outgrow "cron + a jobs
table" or "a Redis list" are told the next step is Kafka, RabbitMQ, or NATS JetStream — each of
which costs a cluster, a mental model, and an operator. Their actual pain is not throughput.
It is: *"a webhook didn't get processed last Tuesday and we cannot prove what happened."*

messq fills that gap: **a durable, ack-based queue whose entire operational story fits in one
binary, one SQLite file, and one CLI — and which answers forensic questions natively.**

### 1.2 What messq is

- A **single-node, single-binary** queue daemon for Linux (`messq serve`); the same binary is
  the CLI client.
- **At-least-once, always.** Never claims more. Explicit ack/nak/term/extend, server-enforced
  ack timeout, bounded redelivery with backoff, max-deliver cap, dead-letter stream.
- **Replayable.** Streams are append-only sequences; consumers are durable cursors. Seek and
  replay are first-class.
- **Observable by construction.** Every state transition writes a durable event row *inside the
  same SQLite transaction as the state change*, is emitted as a structured log line, and updates
  a metric — one vocabulary across all three surfaces. The audit trail cannot disagree with
  reality.
- **CLI-first.** Every daemon capability is a subcommand. `messq sub jobs worker --exec ./h.sh`
  turns any shell script into a durable, retrying, dead-lettering worker with zero client code.
- **Honest.** Durability modes are named and printed at startup; guarantees are stated precisely
  and each is backed by a named test; the README says when *not* to use messq.

### 1.3 Vocabulary

NATS JetStream's vocabulary is adopted verbatim — **stream, subject, consumer, ack, nak, term,
ack_wait, max_deliver, backoff, max_ack_pending** — because it is already correct and already
known; zero teaching cost, and "graduating to JetStream" becomes a config change, not a rewrite
(we will ship that guide). Only two terms are ours: **events** (the durable transition journal)
and **trace** (the verb that reads it). Subject syntax is NATS-style: dot-separated tokens,
`*` matches one token, `>` matches one-or-more trailing tokens.

### 1.4 What messq is NOT (permanent non-goals, printed in the README)

No clustering, no replication, no consensus, no partitions, no exactly-once, no plugin system,
no web UI in core, no stream processing, no schema registry, no AMQP/Kafka wire compatibility.
If you need >50k msg/s sustained, multi-node HA, or terabyte retention: use Kafka or JetStream —
our docs say so and link the migration guide. Publishing the anti-pitch is a trust tactic, not
modesty.

### 1.5 Success criteria for v1.0

| Criterion | Target |
|---|---|
| Time from download to first acked message | < 60 s, no config file (`messq quickstart`) |
| Runtime dependencies | ≤ 8 direct Go modules, `CGO_ENABLED=0`, one static binary |
| Durable throughput (1 KiB, NVMe, `--durability=full`) | ≥ 5 000 msg/s publish+ack round trip |
| Crash test (SIGKILL at any point) | 0 acknowledged publishes lost, 0 checkpointed acks redelivered |
| "What happened to message X" | `messq trace <id>` — one command, full timeline |
| Codebase | small enough to read the core in an evening; every guarantee has a named test |

---

## 2. Adjudicated decisions

Each row is a conflict that split the eleven plans. The decision is final; revisit triggers are
noted where they exist. Persona credits use plan numbers (01 pragmatic-shipper, 02
correctness-purist, 03 sre-operator, 04 cli-craftsman, 05 storage-engineer, 06
performance-engineer, 07 protocol-designer, 08 security-engineer, 09 quality-advocate, 10
go-purist, 11 product-strategist).

### D1 — Storage engine: **SQLite via `modernc.org/sqlite`** (single DB file, WAL)

- **Contenders:** SQLite (01, 04, 07, 08, 09, 11) · custom segmented log + bbolt/checkpoints
  (02, 03, 05, 06) · bbolt-only (10).
- **Winner:** SQLite. Reasons, in order of weight:
  1. **One transactional boundary collapses the crash matrix** (09's decisive argument): a
     state change, its delivery bookkeeping, its DLQ copy, and its audit event commit
     atomically. A log+KV split creates two durability domains and a cross-domain
     crash-consistency protocol a small team cannot exhaustively test.
  2. **Inspectability is the product** (04, 07, 08, 11): `trace`, `pending`, `dlq ls`, `lag`
     are indexed SQL queries; `sqlite3 messq.db` works for forensics even when the daemon is
     down. With bbolt or a custom log, every one of those becomes a hand-maintained index —
     the differentiator becomes the expensive part.
  3. **Zero bespoke recovery code.** SQLite's WAL replay is among the most-tested code in
     existence; plan 02/05's hand-rolled recovery would need the entire crash-point-enumerator
     apparatus just to reach parity.
  4. The ICP's throughput (≤ 5k msg/s sustained) is comfortably within SQLite + group commit.
- **Rejected arguments answered:** 02's "durability lives in a DSN string" → we verify pragmas
  on every pooled connection via a connection hook, read them back, and refuse to start on
  mismatch (04's fix). 05/06's delete-amplification and checkpoint stalls → deletes are batched
  in the retention sweep, WAL checkpointing is on our schedule (`wal_autocheckpoint` tuned +
  explicit `TRUNCATE` checkpoints off-peak), `auto_vacuum=INCREMENTAL`.
- **Escape hatches, wired from day one:** a `//go:build cgosqlite` file swaps in
  `mattn/go-sqlite3` (build-tag change, exercised weekly in CI); the store speaks plain
  `database/sql` with vanilla SQL. **Revisit trigger** (07's contained escape): if the M2
  measured gate (≥ 5k durable 1 KiB msg/s on NVMe) fails, payload BLOBs move to append-only
  segment files while SQLite keeps all metadata — a change behind the store package that does
  not touch the wire.

### D2 — Protocol: **HTTP/1.1 + JSON on stdlib `net/http`**, Unix socket default, long-poll pull

- **Contenders:** HTTP/JSON (01, 04, 08, 10, 11) · ConnectRPC (02, 05, 07, 09) · gRPC (03) ·
  custom binary framing (06).
- **Winner:** HTTP/1.1 + JSON, Go 1.22+ `ServeMux` method+pattern routing, no router
  dependency, no codegen, no protoc. Reasons:
  1. **`curl` is the universal client and the 3 a.m. debugger** — the one point *every* plan
     agreed on; even the Connect advocates chose Connect *because* its unary path is curl-able.
     Plain HTTP gets there with zero toolchain.
  2. D1 sets the throughput ceiling well below where HTTP framing overhead matters (04's
     argument); gRPC/Connect would buy wire efficiency we cannot use at the cost of a codegen
     step, a dependency tree parsing hostile bytes (08), and an h2c/bidi-streaming
     compatibility story (02's own R5).
  3. Pull + long-poll makes backpressure structural and deletes the credit/streaming state
     machine entirely (09: "flow control is an argument to a request").
  4. A five-line shell worker (fetch → work → ack via curl) is a README feature.
- **Kept doors open:** stable JSON shapes + `Accept: application/x-ndjson` content negotiation
  mean a binary or gRPC gateway can be added post-1.0 without a v2 API (06's gateway idea,
  deferred until three real users ask — 11).

### D3 — DLQ: **a real stream** (`<stream>.dlq`), not an in-place terminal state

- **Contenders:** DLQ-as-stream (01, 03, 04-redrive, 05, 06, 09, 10, 11) vs. in-place terminal
  state + index (02).
- **Winner:** DLQ-as-stream. 02's objection (atomicity across two journals) evaporates under
  D1: the copy into `<stream>.dlq`, the deletion of the delivery row, and the `msg.dead` event
  are **one SQLite transaction**. One mechanism then buys four features: inspection = `peek`,
  redrive = consume+republish, alerting = depth metric, retention = stream retention (09's
  point: DLQs nobody watches are the documented failure mode; a first-class stream gets lag
  metrics and alerts for free). Provenance headers (`Messq-Origin-*`, attempts, cause, last
  error) and the **original `msg_id`/`trace_id` preserved** so `trace` shows one continuous
  story (03, 10).

### D4 — fsync policy: **two named modes, group commit, acks durable before response**

- **Winner:** `--durability=full` (default; `synchronous=FULL`, publish/ack response only after
  the group commit's fsync returns) and `--durability=relaxed` (`synchronous=NORMAL`; survives
  process crash, may lose the last commits on power loss, **never corrupts**; loud startup
  banner, flagged by `doctor`). Group commit window: 2 ms / 256 commands, whichever first —
  this is what makes `full` affordable (all plans agree on group commit; the window numbers
  are 01/07/11's consensus).
- **Rejected:** a third `volatile/none` mode (11, 06) — two options an operator can hold in
  their head beat three they will misconfigure (01). A separate `strict` per-call mode (04) —
  `full` already means "fsynced before response"; batching is an implementation detail.
- **Ack-durability asymmetry rejected as machinery, adopted as insight:** plans 02/03/05/06
  built 200 ms lazy ack-flush windows because their log engines pay per-fsync. Under D1, acks
  ride the same group commit as publishes at ~zero marginal fsync cost, so **an ack response is
  a durability promise too** (07's "a Settle you waited for is a promise") with no extra
  machinery and no documented duplicate window from ack loss.
- **fsync failure is fatal** (the fsyncgate rule, 03/05): on `EIO`/`ENOSPC` from commit, latch
  the stream/process read-only, log `storage.fatal` at ERROR, refuse writes, exit non-zero
  after a short drain. Never retry an fsync. systemd restarts into recovery, which re-derives
  truth from disk.

### D5 — Delivery state representation: **durable rows, ack = DELETE** (01's model)

Delivery state lives in a `deliveries(stream, consumer, seq)` row with
`state ∈ {READY, INFLIGHT}` and a `visible_at` timestamp; ack **deletes** the row (terminal
states are row-deletion or DLQ migration). The pending set *is* the table, so it stays small by
construction and cannot become a Redis-PEL-style leak. Timeouts are one indexed `UPDATE` in a
250 ms sweeper tick, not per-message timers (01, vs. 06's timing wheel — rejected as premature
at our throughput). In-memory state is limited to the long-poll waiter registry.

### D6 — Attempt counting: **increment at claim, durable before the fetch response** (10's G6)

`attempts` increments exactly once per delivery, in the same transaction that claims the row,
committed before the fetch response is sent. Therefore `max_deliver = 5` means exactly five
handler invocations, counters survive crashes, a crash-looping broker cannot loop a poison
message forever, and restart-recovery does **not** re-increment (the increment already
happened). Recovered in-flight rows become immediately redeliverable with
`reason=broker_restart` recorded, with startup jitter to avoid a redelivery stampede (01, 03).
**Rejected:** 02's dual counters (`dispatch_count` vs `fail_count`) — a second durable counter
and a subtler rule on the one state machine everyone must understand; its spirit survives as
the `reason`/`cause` recorded on every redelivery (09: *every duplicate has a named reason, or
it is a bug*).

### D7 — Ack fencing: **plain fenced tokens, no HMAC** (10/09 over 07/08)

The ack token is `"<stream>/<consumer>/<seq>/<attempt>/<generation>"` — human-readable on
purpose (an operator can read attempt numbers out of a log line). It is validated against the
live delivery row: wrong attempt → `409 stale_ack` + WARN event + metric (the "my worker acked
but it was processed twice" mystery becomes an alertable line); wrong generation (bumped by
seek/purge) → rejected; already-resolved → **idempotent success** flagged `stale`. Rejected:
07's HMAC-signed tokens and 08's random single-use leases — key management and crypto for a
forgery threat that authorization already covers on a single-node broker. Cost of our scheme:
one integer comparison.

### D8 — Configuration: **flags + `MESSQ_*` env only for v1** (01, 04, 11)

No config file until the flag surface stabilises (optional TOML post-1.0 if demanded). No
viper, ever (unanimous). Runtime-mutable settings (log level, auth tokens) reload on SIGHUP.
This kills a parser, a precedence bug class, and a "which setting won?" support category.

### D9 — CLI framework: **spf13/cobra** (9 of 11; 10 dissented)

Cobra earns its place with `RunE` error propagation, persistent flags, generated shell
completions with **dynamic completion of live stream/consumer names** (03: fewer typos at 3 a.m.
is a reliability feature), and doc generation from one source of truth. 10's hand-rolled
dispatcher saves a dependency but forfeits dynamic completion and man pages. No viper, no
cobra-cli scaffolding.

### D10 — IDs: **ULID** (`oklog/ulid/v2`)

26-char Crockford base32: time-sortable (logs sort, incident IDs cluster), no ambiguous
characters (survives being read off a screenshot), embedded timestamp (date a message from its
ID alone). Rejected: UUIDv7 (05 — longer, hex, case-sensitive), `stream:seq` as the only id
(06 — seq is not stable across DLQ copies and replays; we expose both).

### D11 — Observability: **events table in-transaction is the source of truth; logs and metrics are projections** (01, 04, 09, 11 — unanimous among SQLite plans)

Three surfaces, one closed event vocabulary. Hot-path events (`publish`, `deliver`, `ack`) log
at DEBUG and are sampleable **per message** (whole lifecycle or nothing — 07/10's sticky-trace
insight); problem events (`nak`, `timeout`, `dead`, `stale_ack`, `retention.blocked`,
`storage.fatal`) log at WARN/ERROR and are **never sampleable** (enforced by an allow-list).
The events table is never sampled, so `messq trace` is always complete. Metrics: prometheus
client_golang on a custom registry; labels are only `stream` and `consumer` — never `subject`,
never ids (unanimous cardinality rule). 10's hand-rolled exposition rejected: client_golang is
the one place buying the standard is obviously right.

### D12 — Security model: **secure-by-default minimalism** (08's defaults, 01's scope)

Unix socket (0660) with filesystem permissions is the default and the whole story for local
use. TCP requires a bearer token file (SHA-256-hashed secrets, constant-time compare, never
logged — `slog.LogValuer` redaction) or refuses to bind non-loopback. Three roles
(`publish`/`consume`/`admin`) scoped per stream — ~100 lines, no RBAC engine. Hardened systemd
unit; `govulncheck` in CI; SECURITY.md. **Rejected for v1:** 08's hash-chained audit trail,
Ed25519 checkpoints, grant-coverage decision procedure, SLSA ceremony — real designs, wrong
tax bracket for the ICP; the in-transaction events table is the audit trail. Native TLS is
phase 2; until then, document reverse-proxy/WireGuard termination into the Unix socket.

### D13 — Testing: **the quality-advocate's discipline at the pragmatic-shipper's scale**

Adopted from 09 (with 01/02/05 reinforcement): pure-function state machine, independent
reference model driven by `rapid` with invariants checked after every action (including a
`restart` action), SIGKILL crash harness with a **three-valued ledger oracle**
(OK/UNKNOWN/FAILED — the only correct oracle for at-least-once under crashes),
`testing/synctest` for all timing (no `time.Sleep` in tests, lint-enforced), testscript CLI
goldens, executable documentation, fuzzing on every parser, `-race` everywhere, golden
log/metric schema tests. **Rejected:** mutation testing, porcupine linearizability, LazyFS as
1.0 *gates* (nightly/nice-to-have, not the critical path), gosim, dm-log-writes as a merge
gate. Nine fault families reduced to the six that matter for D1 (kill, torn tail via SQLite,
ENOSPC, EIO, clock jump, client disconnect/slow consumer).

### D14 — Client libraries: **one thin Go package, used by the CLI itself** (04/11 over 01's "none")

The CLI consumes `pkg/client`, which guarantees the client is real without maintaining an SDK
surface; it ships a `Worker` helper (auto-extend heartbeats — the #1 at-least-once footgun
solved in the library). Python/TypeScript: copy-pasteable snippets in docs, tested in CI; no
maintained SDKs at 1.0.

### D15 — Scheduling/consumer-group/priority features: **phase 2, strictly demand-ordered**

Delayed delivery (nearly free on the `visible_at` machinery), ordered-by-subject consumers
(with head-of-line blocking displayed, not hidden), per-consumer rate limiting, worker
attribution ("groups-lite" — multiple workers per consumer already works because acks are
fenced; the feature is visibility and per-worker caps), audit export, native TLS. **Replication
is not on the roadmap** — a design doc gated on real demand is the ceiling (unanimous).

---

## 3. Architecture

### 3.1 Process model

Exactly one process: `messq serve`. One binary contains daemon and CLI. The CLI talks to the
daemon over HTTP (Unix socket by default). The data directory is held under `flock` so a second
instance cannot corrupt it. No sidecars, no helper daemons.

```
                        ┌──────────────────────── messq serve ─────────────────────────┐
 publishers ──HTTP────▶ │ net/http server (unix:///run/messq/messq.sock, opt. TCP)     │
 consumers  ──HTTP────▶ │   handlers: parse → auth → build Cmd → send on cmdCh → wait  │
 CLI ── unix socket ──▶ │        │                                                     │
                        │        ▼                                                     │
                        │  ┌──────────────┐  cmdCh (bounded)   ┌────────────────────┐  │
                        │  │ writer       │◀───────────────────│ sweeper (250 ms)   │  │
                        │  │ goroutine    │◀───────────────────│ janitor (60 s)     │  │
                        │  │ (exactly 1,  │                    └────────────────────┘  │
                        │  │ group commit)│                                            │
                        │  └──────┬───────┘                                            │
                        │         ▼                                                    │
                        │  ┌──────────────┐   read-only pool (WAL: readers never       │
                        │  │ SQLite (WAL) │◀── block the writer): peek/trace/list/lag  │
                        │  │ messq.db     │                                            │
                        │  └──────────────┘                                            │
                        │  waiters registry → wakes parked long-poll fetches           │
                        │  event fan-out → slog · metrics · /v1/events followers       │
                        └──────────────────────────────────────────────────────────────┘
```

### 3.2 Goroutine census (complete)

| Goroutine | Count | Job |
|---|---|---|
| `http.Server` handlers | 1 + per-conn (stdlib) | Parse, authorize, marshal. Never write SQLite directly. |
| **writer** | **exactly 1** | Owns the sole read-write connection. Drains `cmdCh`, groups commands into one transaction per commit window (2 ms / 256 cmds), commits (one fsync), replies to waiters, publishes events. |
| sweeper | 1 | Every 250 ms: expire ack timeouts → redeliver-or-dead; wake waiters. Runs as commands through the writer. |
| janitor | 1 | Every 60 s: retention, dedup-window trim, event-table trim, WAL checkpoint when oversized, incremental vacuum, gauge refresh. |
| event fan-out | 1 | Committed events → slog, Prometheus counters, `/v1/events` followers (bounded ring per follower; overflow drops loudly). |
| signal/lifecycle | 1 | SIGTERM → graceful drain; SIGHUP → reload log level + tokens; sd_notify. |

No goroutine per message, per delivery, or per timer. Every channel is bounded; a full channel
is backpressure, propagated to the socket. The whole concurrency model fits on an index card
(10), and there is no mutex on queue state — the writer serializes everything.

### 3.3 Package layout

```
cmd/messq/           ~30 lines: run() pattern
internal/queue/      pure state machine: Apply(state, cmd, now) → (mutations, events, error)
internal/store/      SQLite schema (embedded migrations), writer, group commit, recovery, queries
internal/api/        net/http handlers, error envelope, long-poll waiters, auth
internal/obs/        event vocabulary, slog handlers (json + human), metrics registry
internal/cli/        cobra commands, output rendering, --exec worker
internal/model/      independent reference implementation (test oracle)
internal/testutil/   fake clock seam, crash harness, ledger oracle, load generator
pkg/client/          public Go client (used by the CLI), Worker helper
docs/                SEMANTICS.md (normative), OPERATIONS.md, PROTOCOL.md, runbooks, ADRs
packaging/           systemd unit, nfpm.yaml, Dockerfile
```

The state machine in `internal/queue` is a pure function — no I/O, no `time.Now()` (a `Clock`
seam), no map-iteration-order dependence. This one choice makes property testing, the reference
model, and deterministic timing tests all cheap (09's load-bearing decision, adopted whole).

---

## 4. Storage & durability design (concrete)

### 4.1 Engine and connections

One SQLite database file `<data-dir>/messq.db` (mode 0600, dir 0700), driver
`modernc.org/sqlite`. Two `database/sql` handles:

```
rw: file:messq.db?_journal=WAL&_synchronous=FULL&_txlock=immediate&_timeout=5000&_foreign_keys=1
    SetMaxOpenConns(1)                      — owned exclusively by the writer goroutine
ro: same + &mode=ro&_pragma=query_only(1)
    SetMaxOpenConns(NumCPU)                 — peek/trace/list/lag/metrics
```

Plus on open: `wal_autocheckpoint=4000`, `cache_size=-65536` (64 MiB), `temp_store=MEMORY`,
`auto_vacuum=INCREMENTAL` (set at creation). `_txlock=immediate` eliminates the
deferred-upgrade `SQLITE_BUSY` class. A **connection hook re-asserts and reads back the
durability pragmas on every pooled connection**; if `synchronous` is not what the configured
durability mode demands, the daemon refuses to start, and `messq doctor` prints the live values
(the answer to "durability in a DSN is a trap").

### 4.2 Schema (v1)

All tables `STRICT`. Timestamps are Unix milliseconds (`INTEGER`). Migrations are numbered,
embedded via `go:embed`, forward-only, applied in one transaction, tracked in `meta`.

```sql
CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT NOT NULL) STRICT;
-- schema_version, node_id (ULID), created_at, clean_shutdown

CREATE TABLE streams (
  name            TEXT PRIMARY KEY,
  subjects        TEXT    NOT NULL,               -- JSON array of accepted publish patterns
  retention       TEXT    NOT NULL DEFAULT 'limits',  -- 'limits' | 'workqueue'
  max_msgs        INTEGER NOT NULL DEFAULT 0,     -- 0 = unlimited
  max_bytes       INTEGER NOT NULL DEFAULT 0,
  max_age_ms      INTEGER NOT NULL DEFAULT 604800000,  -- 7 d
  max_msg_size    INTEGER NOT NULL DEFAULT 1048576,    -- 1 MiB
  discard         TEXT    NOT NULL DEFAULT 'old',      -- 'old' | 'new'
  dedup_window_ms INTEGER NOT NULL DEFAULT 120000,
  created_at      INTEGER NOT NULL
) STRICT;

CREATE TABLE stream_seq (                          -- monotonic even across purge
  stream TEXT PRIMARY KEY REFERENCES streams(name) ON DELETE CASCADE,
  next   INTEGER NOT NULL
) STRICT;

CREATE TABLE messages (
  stream       TEXT    NOT NULL,
  seq          INTEGER NOT NULL,                   -- per-stream, monotonic
  id           TEXT    NOT NULL,                   -- ULID, stable across DLQ/replay lineage
  subject      TEXT    NOT NULL,
  hdr          TEXT,                               -- JSON object, user headers, ≤ 4 KiB
  body         BLOB    NOT NULL,
  size         INTEGER NOT NULL,
  published_at INTEGER NOT NULL,
  trace_id     TEXT    NOT NULL,
  dedup_key    TEXT,                               -- from Messq-Msg-Id; NULLed after window
  PRIMARY KEY (stream, seq)
) STRICT;
CREATE UNIQUE INDEX messages_id    ON messages(id);
CREATE UNIQUE INDEX messages_dedup ON messages(stream, dedup_key) WHERE dedup_key IS NOT NULL;
CREATE INDEX        messages_subj  ON messages(stream, subject, seq);
CREATE INDEX        messages_age   ON messages(stream, published_at);

CREATE TABLE consumers (
  stream          TEXT    NOT NULL,
  name            TEXT    NOT NULL,
  filters         TEXT    NOT NULL DEFAULT '[">"]',
  ack_wait_ms     INTEGER NOT NULL DEFAULT 30000,
  max_deliver     INTEGER NOT NULL DEFAULT 5,      -- 0 = unlimited (warned)
  max_ack_pending INTEGER NOT NULL DEFAULT 1000,
  backoff_ms      TEXT    NOT NULL DEFAULT '[1000,5000,30000,120000,600000]',
  ordered         INTEGER NOT NULL DEFAULT 0,      -- phase 2: per-subject serial delivery
  dead_policy     TEXT    NOT NULL DEFAULT 'dlq',  -- 'dlq' | 'drop'
  cursor_seq      INTEGER NOT NULL DEFAULT 1,      -- next stream seq to consider
  generation      INTEGER NOT NULL DEFAULT 1,      -- bumped by seek/purge; fences tokens
  paused          INTEGER NOT NULL DEFAULT 0,
  created_at      INTEGER NOT NULL,
  PRIMARY KEY (stream, name)
) STRICT;

CREATE TABLE deliveries (                          -- the pending set. Ack = DELETE.
  stream       TEXT    NOT NULL,
  consumer     TEXT    NOT NULL,
  seq          INTEGER NOT NULL,
  subject      TEXT    NOT NULL,                   -- denormalized for ordered mode (phase 2)
  state        INTEGER NOT NULL,                   -- 0 = READY, 1 = INFLIGHT
  attempts     INTEGER NOT NULL DEFAULT 0,
  visible_at   INTEGER NOT NULL DEFAULT 0,         -- READY when now >= visible_at
  generation   INTEGER NOT NULL,
  delivered_at INTEGER,
  last_reason  TEXT,                               -- last nak/timeout reason
  PRIMARY KEY (stream, consumer, seq)
) STRICT;
CREATE INDEX deliveries_ready ON deliveries(stream, consumer, state, visible_at, seq);
CREATE INDEX deliveries_seq   ON deliveries(stream, seq);          -- workqueue reaping

CREATE TABLE events (                              -- THE audit trail; same tx as the change
  id       INTEGER PRIMARY KEY,
  ts       INTEGER NOT NULL,
  event    TEXT    NOT NULL,                       -- closed vocabulary, §8.2
  stream   TEXT, consumer TEXT, subject TEXT,
  msg_id   TEXT, seq INTEGER, attempt INTEGER,
  trace_id TEXT,
  actor    TEXT,                                   -- token id / uid for admin actions
  detail   TEXT                                    -- small JSON: reasons, delays, counts
) STRICT;
CREATE INDEX events_msg   ON events(msg_id, id);
CREATE INDEX events_trace ON events(trace_id, id);
CREATE INDEX events_ts    ON events(ts);
```

Design notes that matter:
- **An ack is a `DELETE`.** `deliveries` only holds unfinished work; every "pending" query is
  fast regardless of stream size; there is no PEL to leak.
- **Payloads are inline BLOBs, capped at 1 MiB default (8 MiB hard ceiling).** No blob sidecar.
  Larger payloads belong in object storage with a pointer in the message (the claim-check
  pattern, documented).
- **The event row commits with the state change.** Same transaction ⇒ zero extra fsyncs and an
  audit trail that structurally cannot lie.

### 4.3 Durability modes and group commit

| `--durability` | SQLite | Meaning |
|---|---|---|
| `full` (default) | WAL + `synchronous=FULL` | A 2xx on publish **or ack** means it survived a power cut. |
| `relaxed` | WAL + `synchronous=NORMAL` | Survives SIGKILL; the last commits may be lost on power loss/kernel panic. Never corrupts. Startup WARN banner; `doctor` flags it. |

The writer accumulates commands for up to `--commit-window` (2 ms) or `--commit-max-batch`
(256), commits once: N concurrent operations share one fsync. `messq_commit_batch_size` and
`messq_commit_duration_seconds` histograms make the batching observable. fsync/EIO failure:
latch read-only (`storage.fatal`, `/readyz` 503 for writes), exit after drain — never retry
(fsyncgate).

### 4.4 Crash recovery (no bespoke replay code)

On `messq serve` start, before listeners open:

1. Open DB; SQLite replays/rolls back the WAL. If `meta.clean_shutdown` is absent, log
   `recovery.unclean` WARN and run `PRAGMA quick_check` (full `integrity_check` under
   `--fsck`).
2. Apply migrations; **refuse to start** if the schema is newer than the binary.
3. **Reclaim leases:** one indexed UPDATE flips every `INFLIGHT` row to `READY` with
   `visible_at = now + jitter(0..1s)` (thundering-herd protection). `attempts` is **not**
   changed — the in-flight delivery already counted (D6). Emit `recovery.reclaimed count=N`.
4. Trim expired dedup keys and stale events; write `clean_shutdown=0`.
5. Emit `server.start` with version, durability mode, DB size, per-stream counts; sd_notify
   READY.

Graceful shutdown (SIGTERM): stop accepting → release long-polls → drain handlers (10 s) →
final commit → `wal_checkpoint(TRUNCATE)` → set `clean_shutdown` → exit 0. Drain is an
optimization, never a correctness requirement — `kill -9` is always safe, and the crash suite
asserts it.

### 4.5 Retention, housekeeping, disk safety

- `retention=limits` (default): age/count/bytes limits, oldest-first deletion. A message with
  an outstanding delivery row is **not** deleted; the sweep emits `retention.blocked` (metric +
  shipped alert — a stuck consumer is about to be a disk problem). `discard=new` rejects
  publishes at the limit with a typed error instead.
- `retention=workqueue`: delete once every consumer has moved past a message and no delivery
  rows remain — the job-queue mode that keeps the file small. Implemented in the 60 s sweep.
- Events trim: `--event-retention` (default 72 h) and `--event-max-rows`.
- WAL checkpoint (`TRUNCATE`) when WAL > `--wal-max-bytes` (256 MiB); `incremental_vacuum` when
  the freelist grows.
- Disk safety: `--min-free-bytes` (default 256 MiB); below it, publishes are rejected
  (`507 disk_full`) while **acks, naks and DLQ writes continue** — a full disk must never wedge
  the ack path, because a wedged ack path becomes mass redelivery. `/readyz` is not tied to
  disk pressure (the RabbitMQ-on-Kubernetes lesson: don't let the orchestrator kill the node
  that is draining itself).
- Backup: `messq backup out.db` runs `VACUUM INTO` on the read pool — consistent online
  snapshot; restore is stop + copy + start. That is the whole story, in one sentence.

---

## 5. Message lifecycle state machine

State is per **(consumer, seq)** — "the message is acked" is a meaningless sentence; each
consumer has an independent lifecycle (unanimous across plans). States: implicit `UNSEEN`
(seq ≥ cursor, no row), `READY`, `INFLIGHT`; terminal outcomes are row deletion (ACKED/TERMED)
or DLQ migration (DEAD).

```
                     top-up from messages (seq ≥ cursor_seq, filter matches)
                                     │
                                     ▼
                              ┌────────────┐
                  ┌──────────▶│   READY    │◀───────────────┐
                  │           │ visible_at │                │
                  │           └──────┬─────┘                │
                  │                  │ fetch claim:         │
                  │                  │  now ≥ visible_at    │
                  │                  │  pending < max_ack_pending
                  │                  │  attempts++ (durable before response)
                  │                  ▼
                  │           ┌────────────┐   ack(token)  ──▶ ● ACKED (row DELETEd)
                  │           │  INFLIGHT  │   term(token) ──▶ ◆ DEAD  (→ <stream>.dlq)
                  │           │ deadline = │   extend(token) ─▶ visible_at += ack_wait (capped)
                  │           │ visible_at │
                  │           └─────┬──────┘
                  │   nak(delay) /  │ sweeper: now > visible_at (TIMEOUT)
                  │                 ▼
                  │        attempts ≥ max_deliver ?
                  │            no  │        │ yes
                  └────────────────┘        ▼
                   visible_at =          ◆ DEAD
                   now + backoff[n]±20%   dead_policy=dlq  → copy to <stream>.dlq (one tx)
                                          dead_policy=drop → delete + event only
```

### 5.1 Transition rules (normative — the conformance tests mirror this table 1:1)

| # | From | Trigger | Guard | Effect | Event |
|---|---|---|---|---|---|
| T1 | UNSEEN | fetch top-up | filter matches, seq ≥ cursor | insert READY row; advance `cursor_seq` past scanned seqs (including non-matches, so narrow filters are amortized O(1)/msg) | — |
| T2 | READY | fetch claim | `now ≥ visible_at` ∧ pending < `max_ack_pending` | `attempts++`, state=INFLIGHT, `visible_at = now + ack_wait`, mint token; **committed before response** | `msg.deliver` |
| T3 | INFLIGHT | `ack(token)` | token attempt = row attempt ∧ generation matches | DELETE row | `msg.ack` |
| T3a | any | `ack(token)` on missing row | generation matches | idempotent success, flagged stale | `msg.ack_dup` (DEBUG) |
| T3b | INFLIGHT | `ack(token)` wrong attempt | — | reject 409; row untouched | `msg.ack_stale` (WARN) |
| T4 | INFLIGHT | `nak(token, delay?, reason?)` | fenced as T3 ∧ attempts < max_deliver | state=READY, `visible_at = now + (delay ?? backoff[attempts-1])±20%`, store reason | `msg.nak` |
| T5 | INFLIGHT | `nak`/timeout at `attempts ≥ max_deliver` | — | → DEAD | `msg.dead` |
| T6 | INFLIGHT | `term(token, reason)` | fenced | → DEAD immediately (permanent error; skips remaining attempts) | `msg.term`, `msg.dead` |
| T7 | INFLIGHT | `extend(token)` | fenced ∧ total extension ≤ `--max-ack-wait` (1 h) | `visible_at += ack_wait`; attempts unchanged | `msg.extend` (DEBUG) |
| T8 | INFLIGHT | sweeper: `now > visible_at` | attempts < max_deliver | state=READY, `visible_at = now + backoff[attempts-1]±20%` | `msg.timeout` (WARN) |
| T9 | INFLIGHT | broker restart | — | state=READY, `visible_at = now + jitter`; attempts unchanged | `recovery.reclaimed` |
| T10 | any | `seek` | operator, confirmed | `generation++`, cursor reset, delivery rows dropped (counted) | `consumer.seek` (WARN) |
| T11 | DEAD in DLQ | `dlq redrive` | operator, rate-limited | republish to origin stream (new seq, `Messq-Redrive-Of` header, trace preserved) | `dlq.redrive` |

**Backoff:** per-consumer array, default `[1s, 5s, 30s, 2m, 10m]`, indexed by attempt, last
value repeats, **±20 % jitter always applied and not configurable off** (a synchronized retry
wave from a recovering downstream is a self-inflicted second outage — 03). An explicit
`nak --delay` overrides the schedule for that attempt (the handler may know better).

**Dead-lettering (one transaction):** publish the payload into auto-created `<stream>.dlq`
under the original subject with headers `Messq-Origin-Stream/-Seq/-Consumer`, `Messq-Attempts`,
`Messq-Cause` (`max_deliver`|`terminated`), `Messq-Last-Reason`, `Messq-Dead-At`; the original
`id` and `trace_id` are preserved; delete the delivery row; write `msg.dead`. The `Messq-`
header prefix is reserved (user headers with it are rejected).

**Publish dedup:** `Messq-Msg-Id: <key>` → `INSERT … ON CONFLICT DO NOTHING`; on conflict the
original `{seq, id}` returns with `duplicate: true` + `msg.dup` event. Keys are cleared by the
janitor after `dedup_window_ms` (default 2 min) so the unique index stays bounded. This makes
publisher retries safe — the other half of at-least-once most small brokers omit.

### 5.2 Invariants (each has a stable ID, a checker in `internal/model`, and a named test)

| ID | Invariant |
|---|---|
| I1 | Every publish that returned 2xx under `full` durability is present after any crash (no acknowledged loss). |
| I2 | Every `(consumer, seq)` in `[first_seq, cursor)` is in exactly one of: resolved (below-floor/absent), READY, INFLIGHT. |
| I3 | An acked-and-committed `(consumer, seq)` is never redelivered except via explicit seek/replay/redrive. |
| I4 | `attempts ≤ max_deliver` for every non-terminal row; delivery stops at the bound, across restarts. |
| I5 | `count(deliveries WHERE consumer=c) ≤ max_ack_pending(c)` always. |
| I6 | `cursor_seq` is monotone non-decreasing within a generation. |
| I7 | No stale-fenced ack/nak/term ever mutates a live row. |
| I8 | Each `(consumer, seq)` enters DEAD at most once, and each DEAD has exactly one DLQ message (when `dead_policy=dlq`). |
| I9 | In a run with no faults where every delivery is acked within `ack_wait`, every message is delivered exactly once per consumer; any `attempts > 1` is preceded in the event log by a `msg.nak`, `msg.timeout`, or `recovery.reclaimed` for that pair (**every duplicate has a named cause**). |
| I10 | Folding the events table from the beginning reproduces the persisted state (log ≡ state; checked by `messq verify --deep`). |
| I11 | No unbounded queue or collection exists anywhere; every bound is config-derived. |

---

## 6. Delivery semantics & guarantees (README verbatim)

> messq delivers each message to each consumer **at least once**. Duplicates are possible
> whenever a worker completes work but its ack does not reach the server: after an ack timeout,
> a nak, a network failure, or a broker restart. **Consumers must be idempotent.** messq helps:
> `Messq-Msg-Id` deduplicates publishes within a window; every delivery carries its attempt
> number; a stale ack is rejected and reported rather than silently accepted; and every
> redelivery records its cause, so `messq trace` explains every duplicate. messq does not offer
> exactly-once and will not pretend to.

Ordering: within a consumer, READY messages are claimed in ascending `seq`; with concurrent
in-flight messages, *processing* order is not guaranteed. `ordered=subject` (phase 2) gives
per-subject serial processing at the documented cost of head-of-line blocking.
Not guaranteed: exactly-once, global cross-subject ordering, delivery of retention-expired
messages, survival of the disk.

The recommended consumer-side dedup key is `stream/seq` (stable across every redelivery and
redrive), not `msg_id` (which is publisher-supplied and for publish-side dedup) — documented
with the canonical `INSERT … ON CONFLICT DO NOTHING`-in-the-same-transaction pattern (02).

---

## 7. API / protocol

HTTP/1.1 + JSON (D2). Listeners: `unix:///run/messq/messq.sock` (default, mode 0660) and
optional `--listen tcp://127.0.0.1:4390`. All errors share one envelope with stable machine
codes: `{"error":{"code":"stale_ack","message":"…","next":["messq …"],"trace_id":"…"}}`.
Codes are a closed, documented enum and part of the 1.0 compatibility contract.

```
# health / meta
GET    /healthz   /readyz   /metrics
GET    /v1/info                                   version, uptime, durability, db size

# streams
POST   /v1/streams                                {name, subjects[], retention, limits…}
GET    /v1/streams          GET /v1/streams/{s}   config + first/last seq, msgs, bytes
PATCH  /v1/streams/{s}      DELETE /v1/streams/{s}?confirm={name}
POST   /v1/streams/{s}/purge                      {up_to_seq?, subject?, keep?} (?dry_run=1)

# publish / inspect
POST   /v1/streams/{s}/messages                   raw body; ?subject= or Messq-Subject header
                                                  Messq-Msg-Id (dedup), Messq-Trace-Id /
                                                  traceparent, Messq-Header-* (user headers)
                                                  → 201 {stream, seq, id, trace_id, duplicate}
POST   /v1/streams/{s}/messages:batch             NDJSON in; one transaction
GET    /v1/streams/{s}/messages/{seq}             peek (no side effects); /{seq}/data → raw
GET    /v1/streams/{s}/messages?from_seq=&subject=&limit=

# consumers
POST   /v1/streams/{s}/consumers                  create/update (idempotent)
GET    /v1/streams/{s}/consumers[/{c}]            info incl. pending, backlog, oldest_pending_ms
DELETE /v1/streams/{s}/consumers/{c}?confirm={c}
POST   /v1/streams/{s}/consumers/{c}/seek         {to: start|new|seq:N|time:T} (?dry_run=1)
GET    /v1/streams/{s}/consumers/{c}/pending      ?older_than_ms=&limit=
POST   /v1/streams/{s}/consumers/{c}/pause|resume

# the hot path
POST   /v1/streams/{s}/consumers/{c}/fetch        {batch, wait_ms, max_bytes} — long-poll;
                                                  200 with messages[] (empty on timeout);
                                                  each carries ack_token, attempt/of, deadline,
                                                  body_b64, trace_id, headers
POST   /v1/ack     {tokens: […]}                  batch; per-token result (ok|stale|unknown)
POST   /v1/nak     {token, delay_ms?, reason?}    (also {items:[…]})
POST   /v1/term    {token, reason}
POST   /v1/extend  {tokens: […]}

# observability & recovery
GET    /v1/messages/{id}                          lookup by ULID
GET    /v1/messages/{id}/trace                    the life story (events for this id)
GET    /v1/events?msg_id=&trace_id=&stream=&consumer=&event=&since=&limit=&follow=1
POST   /v1/dlq/{s}/redrive                        {ids[]|limit, rate} (?dry_run=1)
POST   /v1/admin/log-level                        {"level":"debug"}
```

Wire conventions: publish takes a **raw body** (so `curl --data-binary @file` works); fetch
returns `body_b64` (one uniform, binary-safe shape; the raw-data endpoint exists when it
matters). `trace_id` is taken from `Messq-Trace-Id` or parsed from a W3C `traceparent`, else
minted at publish; it is stored on the message, echoed on every delivery, carried into the DLQ
copy, and stamped on every event — one grep across services. Response to publish/ack is sent
**only after the commit's fsync** (durability mode `full`).

The five-line shell worker is a README feature and a CI-executed golden test:

```bash
T=$(curl -s --unix-socket /run/messq/messq.sock -d '{"batch":1,"wait_ms":5000}' \
     http://localhost/v1/streams/jobs/consumers/w/fetch | jq -r '.messages[0].ack_token')
do_the_work
curl -s --unix-socket /run/messq/messq.sock -d "{\"tokens\":[\"$T\"]}" http://localhost/v1/ack
```

---

## 8. CLI design

One binary; `messq serve` is the daemon, everything else is a client (default: Unix socket,
`MESSQ_ADDR` override). Built with cobra (D9): `RunE` everywhere, `SilenceUsage`, persistent
`--addr/--output/--token-file` flags, dynamic shell completion of live stream/consumer names
(200 ms budget, silent failure — completion must never hang), generated man pages.

```
messq serve        --data-dir /var/lib/messq [--listen ADDR] [--durability full|relaxed]
                   [--log-format auto|human|json] [--log-level …] [--auth-file …] [--dev]
messq quickstart   # guided 60-second tour on a throwaway data dir; ends with a trace demo

# hot path — one word deep
messq pub <stream> <subject> [-d DATA|-f FILE|-] [--msg-id K] [-H k=v] [--count N]
messq sub <stream> <consumer> [--exec CMD --concurrency N] [--auto-ack|--manual] [--count N]
messq peek <stream> [--seq N|--last N|--subject PAT] [--raw]
messq trace <msg-id | --trace-id T>
messq pending <stream> <consumer> [--older-than 60s]
messq lag          # backlog table across all consumers; the incident overview

# manual transitions — symmetry with the daemon
messq ack|nak|term|extend <token…>  [--delay 5m] [--reason "…"]
messq seek <stream> <consumer> --to start|new|seq:N|time:T [--dry-run]
messq replay <stream> --from-seq A --to-seq B [--subject PAT] [--to-stream X]

# management
messq stream    add|ls|info|edit|rm|purge
messq consumer  add|ls|info|edit|rm|pause|resume
messq dlq       ls|show|redrive|purge
messq events    [--follow] [--verb …] [--consumer …]   # live/historical journal
messq doctor    # opinionated health & misconfiguration diagnosis in prose
messq verify    [--deep]        # the invariant checker, runnable against live or copied data
messq backup <path> | messq bench | messq completion <shell> | messq version
```

Rules (CI-enforced where possible):
- **Human output on a TTY, machine output otherwise** (`--output table|json|ndjson`), never a
  third mode. JSON field names are frozen at 1.0 and schema-tested.
- **Data to stdout, narration to stderr.** Exit codes are a documented contract
  (0 ok · 1 error · 2 usage · 3 not found · 4 conflict/stale · 5 empty/timeout ·
  6 daemon unreachable · 7 permission).
- **Every inspect command ends with the next useful command; every error names the fix**
  (04's teaching-error format: what happened / why / what to type next).
- **Every destructive command has `--dry-run` (exact preview, same renderer as the real run),
  and confirmation by name** (`--confirm orders`), plus an `admin.*` audit event with actor.
- `messq sub --exec`: payload on stdin, metadata in `MESSQ_*` env (+ `traceparent`); exit code
  is the ack — `0` ack, `75` (EX_TEMPFAIL) nak-with-backoff, `65` (EX_DATAERR) term, other
  non-zero nak; child stderr (first 4 KiB) becomes the nak reason visible in `trace` and the
  DLQ; automatic `extend` heartbeat at `ack_wait/2` while the child runs. This is the widest
  funnel in the product: a durable job queue for a bash script with zero client code.

`messq trace` is the flagship output (golden-tested byte-for-byte):

```
message 01J8ZQ4K2M9V0X7Y3B5N6C8D1E   stream=orders subject=orders.created seq=10493
trace 4bf92f35…  1.2 KiB  published 2026-08-21T09:14:02.114Z

  09:14:02.114  publish
  09:14:02.190  deliver   worker  attempt 1/5           deadline 09:14:32.190
  09:14:32.191  timeout   worker  attempt 1/5           waited 30.0s
  09:14:33.190  deliver   worker  attempt 2/5  cause=timeout
  09:14:41.002  ack.stale worker  token attempt 1       REJECTED (attempt 2 in flight)
  09:14:44.771  nak       worker  attempt 2/5           reason="upstream 503"  retry in 5s
  09:14:54.780  deliver   worker  attempt 3/5  cause=nak
  09:14:55.310  ack       worker  attempt 3/5           resolved in 53.2s, 3 attempts

  duplicates: 2 — all accounted for (1× timeout, 1× nak)
```

---

## 9. Observability & logging design

Logging is the reason to choose messq over a Redis list — a product feature with an API
contract, not instrumentation.

### 9.1 Three surfaces, one vocabulary

1. **Durable events table** (§4.2) — the source of truth, written in-transaction, never
   sampled. Powers `trace`, `events`, `/v1/events`.
2. **Structured logs** — `log/slog`; JSON handler for machines, a ~180-line custom human
   handler (aligned columns, color on TTY) for people. `slog.LevelVar` + SIGHUP = runtime
   level changes with no restart. Handler validated with `testing/slogtest`.
3. **Metrics** — Prometheus text on `/metrics`, custom registry only.

All three use the same event identifiers, so grep, SQL and PromQL speak one language.

### 9.2 Event vocabulary (closed set; renaming is a breaking change; golden-tested)

```
server.start server.stop server.reload recovery.unclean recovery.reclaimed storage.fatal
stream.create stream.update stream.delete stream.purge retention.expire retention.blocked
consumer.create consumer.update consumer.delete consumer.seek consumer.pause consumer.lag
msg.publish msg.dup msg.deliver msg.ack msg.ack_dup msg.ack_stale
msg.nak msg.term msg.extend msg.timeout msg.dead
dlq.redrive flow.blocked disk.degraded auth.denied api.error admin.action
```

Every message event carries: `event, ts, stream, subject, msg_id, seq, consumer, attempt,
max_deliver, trace_id` (+ `reason`, `delay_ms`, `held_ms`, `size` where relevant). The log
field schema is versioned like an API (`docs/log-schema.md`) — operators build alerts on it.

### 9.3 Volume control (the honest answer to "first-class logging at 5k msg/s")

Hot-path events (`publish`, `deliver`, `ack`, `extend`) log at DEBUG; problem events (`nak`,
`timeout`, `dead`, `ack_stale`, `flow.blocked`, `retention.blocked`, `storage.fatal`) log at
WARN/ERROR and are **never sampleable** (allow-list enforced in code). Default level INFO: out
of the box you see problems and admin actions, not traffic — the full history is still in the
events table. `--log-sample 1/N` samples **per message** (`hash(trace_id) % N`), so a sampled
message shows its *entire* lifecycle — half a story is worse than none (07/10).
`--trace-subjects PAT` forces full logging for matching subjects during an incident.

### 9.4 Metrics (labels: `stream`, `consumer` only — never subject, never ids)

```
messq_published_total{stream}            messq_duplicates_total{stream}
messq_delivered_total{stream,consumer}   messq_redelivered_total{stream,consumer,cause}
messq_acked_total / naked_total / termed_total / timeouts_total / dead_total{stream,consumer}
messq_stale_acks_total{stream,consumer}      ← alert on any nonzero rate (ack_wait too short)
messq_pending{stream,consumer}  messq_inflight{stream,consumer}  messq_backlog{stream,consumer}
messq_oldest_pending_age_seconds{stream,consumer}   ← THE user-facing SLI; alert on it
messq_dlq_depth{stream}
messq_ack_latency_seconds / messq_commit_duration_seconds / messq_commit_batch_size (hist)
messq_db_bytes / messq_wal_bytes / messq_events_rows / messq_disk_free_bytes
messq_build_info{version,commit,durability}
```

Backlog/depth gauges are computed by a custom `Collector` from SQLite at scrape time (5 s
cache) — a gauge recomputed from the source of truth cannot drift after a restart or purge
(11). Shipped artifacts: `contrib/prometheus/alerts.yml` (DLQ growth, oldest-pending-age > 15 m,
stale-ack rate, retention.blocked, disk ETA) and `contrib/grafana/messq.json`.

---

## 10. Security model

Threat posture: an internal broker for a trusted-ish network, secure by default, no
unauthenticated network path (D12).

- **Default listener is the Unix socket** (0660, group `messq`); filesystem permissions are the
  ACL. Data dir 0700, DB 0600 — verified at startup, refuse to run otherwise.
- **TCP requires auth**: `Authorization: Bearer <token>`; tokens live in a 0600 file
  (`--auth-file`), stored as SHA-256 of a 256-bit random secret, compared constant-time,
  reloaded on SIGHUP. Roles per token: `publish`/`consume`/`admin`, scoped to stream-name
  patterns. A non-loopback bind without auth is a **fatal startup error**. Token ids (not
  secrets) appear in logs; secrets implement `slog.LogValuer` → structurally unloggable.
- **No TLS in core at 1.0**: terminate TLS in nginx/caddy/Envoy *into the Unix socket*, or use
  WireGuard/Tailscale — one documented paragraph instead of a certificate subsystem. Native TLS
  (stdlib `crypto/tls`) is phase 2.
- **Payloads never enter logs** (redaction type + `ReplaceAttr` denylist + a CI canary-leak
  test that greps all output for a published canary string).
- Hardened systemd unit (`Type=notify`, `ProtectSystem=strict`, `NoNewPrivileges`,
  `StateDirectory=messq`, syscall filter, empty capability set). `--exec` is CLI-side only;
  the daemon has no exec capability. `SECURITY.md` + `govulncheck` in CI + `gitleaks` rule for
  the token prefix.

---

## 11. Testing strategy

The value proposition is "trustworthy"; the test suite is the product (D13). Ground rules:
`-race` everywhere; **no `time.Sleep` in tests** outside soak (lint-enforced; timing uses
`testing/synctest` bubbles or the Clock seam); every bug fix lands with a committed
reproduction artifact (rapid failfile, seed, or fuzz input); PR CI ≤ 10 minutes wall clock.

1. **Pure state-machine unit tests** — one table row per transition in §5.1, plus boundaries
   (`max_deliver` 0/1, empty backoff, `max_ack_pending=1`).
2. **Model-based property tests** (`pgregory.net/rapid`) — an independent ~300-line naive
   reference model driven alongside the real broker (real SQLite file, `t.TempDir()`, never
   `:memory:`) through random publish/fetch/ack/nak/term/extend/timeout/seek/purge/**restart**
   sequences; all §5.2 invariants checked after every action. Failfiles committed as the
   regression corpus. This is where the real bugs are found.
3. **Crash harness** — real `messq serve` subprocess, real data dir, SIGKILL at random points
   and at named fault points (`MESSQ_FAULTS` env grammar, build tag `messq_fault`), restart,
   assert against a **three-valued external ledger** (OK must exist / UNKNOWN either /
   FAILED must not), plus `verify` and `integrity_check` clean. Runs per-merge (fast subset)
   and nightly (full sweep, both durability modes, ext4+tmpfs).
4. **Fault injection** — ENOSPC on a loopback filesystem (must reach degraded-writes state,
   keep acking, recover without restart), EIO on commit (must latch read-only, never continue),
   clock jumps (monotonic deadlines unaffected), client disconnect mid-fetch, never-acking
   consumer (flow control holds, `ack.stale` observed).
5. **Golden log/metric/API tests** — scripted scenario asserts exact event sequences and field
   names; `/metrics` name+label set is golden (no accidental cardinality); every documented
   error code is exercised; the README curl transcript and every doc command block execute in
   CI (`testscript`) — documentation cannot rot.
6. **Fuzzing** — subject matcher (differential vs. naive reference), ack-token parser (no
   forgery parses), JSON decoders, header parsing. 60 s/target per PR, 30 min nightly, corpus
   committed.
7. **Soak** — nightly hour+ at load with 10 % naks, poison messages, kills every 5 min,
   retention on; final assertion is conservation: `published = acked + termed + dead + expired
   + still_pending`, flat RSS, flat goroutines.
8. **Upgrade tests** — committed golden data dirs per released schema version; current binary
   must migrate them, pass `verify --deep`, and keep delivering; newer-schema dirs must be
   refused with instructions.
9. **Benchmarks** — `messq bench` is a first-class command (publish/e2e/replay), open-loop;
   nightly benchstat comparison; published numbers always state hardware, durability mode, and
   methodology.

Coverage gates: `internal/queue` ≥ 90 %, `internal/store` ≥ 85 %; no vanity global number.

---

## 12. Performance targets (honest, measured, published)

Reference: commodity NVMe, ext4, 8 cores. Numbers are gates at the milestones named in §14 and
re-baselined per host by `messq doctor` (which measures the operator's actual fsync latency and
prints achievable rates — nobody has to trust our README).

| Metric | Target |
|---|---|
| Durable publish+ack round trip, 1 KiB, `full`, concurrent publishers | ≥ 5 000 msg/s (M2 gate; D1 revisit trigger below it) |
| `relaxed` mode | ≥ 20 000 msg/s |
| p99 publish→durable-ack at moderate load | ≤ 10 ms (≈ commit window + fsync) |
| Long-poll wake latency after publish | < 5 ms |
| Crash recovery | ≤ 2 s typical (WAL replay + one indexed UPDATE) |
| Idle daemon | < 0.5 % of one core |
| Event journaling overhead (`audit=full` vs `off`) | ≤ 10 % throughput (measured & published) |

If a workload needs 100k+/s, messq is the wrong tool and its docs say so.

---

## 13. Library choices (≤ 8 direct non-test deps; every addition needs an ADR)

| Dependency | Role | Why |
|---|---|---|
| `modernc.org/sqlite` | storage | Pure Go ⇒ `CGO_ENABLED=0` static cross-compiled binary; DSN pragma surface incl. `_txlock=immediate`; connection hook for pragma enforcement. `mattn/go-sqlite3` kept as a tested build-tag escape hatch. |
| `spf13/cobra` (+pflag) | CLI | Subcommands, RunE, dynamic completions, man pages. **No viper.** |
| `prometheus/client_golang` | metrics | Custom registry + promauto + `promhttp.HandlerFor(MaxRequestsInFlight)`; custom Collector for scrape-time gauges. |
| `oklog/ulid/v2` | ids | Spec-correct monotonic ULID factory. |
| `pgregory.net/rapid` | test-only | Stateful property testing with `""` invariant hook, shrinking, committed failfiles. |
| `rogpeppe/go-internal/testscript` | test-only | CLI golden tests; executable documentation. |
| `google/go-cmp` | test-only | Readable state diffs; no assertion DSL (no testify). |
| `golang.org/x/sys/unix` | syscalls | flock, statfs (already transitive via others). |

Standard library carries everything else: `net/http` + Go 1.22 ServeMux (no router),
`log/slog` + `testing/slogtest` (no zap/zerolog), `testing/synctest` (Go ≥ 1.25 pinned),
`database/sql`, `crypto/subtle`, `hash/crc32`, `os/signal`. Explicitly refused: viper, gRPC,
ORMs, OpenTelemetry SDK (we emit W3C trace ids and correlate; spans can bridge from logs),
web frameworks, YAML.

License: Apache-2.0, DCO (no CLA), no-relicensing commitment in `GOVERNANCE.md` at v0.9.

---

## 14. Roadmap

One developer, start **2026-08-24**, sequential with light overlap. Every milestone ends with
something runnable and a verifiable exit criterion; a milestone without its tests and docs is
not done. Full dependency-ordered backlog: issues #1-#42 in the tracker.

| Milestone | Due | Scope | Exit criterion |
|---|---|---|---|
| **M1: Foundations** | 2026-08-28 | Repo, CI gates, core primitives (ULID, subject matcher + fuzz, Clock seam, sentinels), normative SEMANTICS.md + ADRs | CI green; matcher fuzz-tested; spec merged before feature code |
| **M2: Durable store & publish** | 2026-09-10 | Schema+migrations+pragma enforcement, group-commit writer, durability modes, streams+publish+dedup+peek, crash harness + ledger oracle, `verify`, recovery | SIGKILL loop never loses an acknowledged publish; ≥ 5k durable msg/s on NVMe (D1 gate) |
| **M3: Delivery engine** | 2026-09-24 | Consumers, cursor, claim, flow control, ack/nak/term/extend + fencing, sweeper+backoff, DLQ stream, reference model + rapid suite | Poison message lands in DLQ after exactly `max_deliver` attempts; invariants green over 10⁵ random ops incl. restart |
| **M4: API & daemon** | 2026-10-06 | Full HTTP surface, error envelope, auth+listener policy, lifecycle/drain/systemd, contract + curl golden tests | `systemctl restart` mid-load loses nothing; five-line shell worker is a passing CI test |
| **M5: Observability** | 2026-10-14 | Events table wiring, slog handlers + sampling, `trace` + events API, metrics + alerts + dashboard | `messq trace` renders the §8 timeline for a timed-out/naked/acked message; golden log tests green |
| **M6: CLI & client** | 2026-10-26 | Go client pkg + Worker helper, cobra tree, core commands, `--exec`, quickstart, completions, testscript suite | Quickstart <60 s on a cold machine; `--exec` is a production-viable worker |
| **M7: Operations** | 2026-11-06 | Retention+housekeeping+disk safety, seek/replay + destructive-action discipline, DLQ ops with guard rails, backup+doctor, bench | An operator runs an entire incident (find→inspect→redrive→replay→purge) from the CLI alone; a week-long stream does not grow unbounded |
| **M8: Hardening & v1.0** | 2026-11-20 | Fault-injection matrix, ENOSPC/EIO tests, fuzz corpus + soak, upgrade fixtures, packaging (deb/rpm/container), 1.0 docs (guarantees, runbooks, comparisons, anti-pitch) | Every §5.2 invariant has a named passing test; **tag v1.0.0**; `apt install` → quickstart works on a clean VM |
| **M9: Phase 2** | 2026-12-03 | Delayed delivery, ordered-by-subject consumers, per-consumer rate limiting, native TLS, worker attribution (groups-lite), audit export | Each feature ships with a new invariant, events, metrics, docs — or it does not ship |

**Deliberately not planned:** clustering/replication (design doc only, on demonstrated demand;
honest v2 shape is async log-shipping to a read-only follower with manual promotion — never
consensus), exactly-once, priorities-as-lanes, web UI in core, hosted service. Each is a line
in `docs/non-goals.md` so "can it do X" is a link, not a debate.
