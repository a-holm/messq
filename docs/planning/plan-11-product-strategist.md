# messq — Project Plan (Product Strategist lens)

**Author persona:** Product Strategist
**Date:** 2026-08-21
**Status:** Proposed. Every decision below is a commitment, not an option.

> One-line thesis: **messq is the queue you can hand to your on-call engineer at 3am — because it can tell you, per message ID, exactly what happened and why.** Durability is table stakes; *answerability* is the product.

---

## 1. Vision & positioning

### 1.1 The market gap, stated honestly

Every team that outgrows "just run it inline" hits the same fork:

| Option | What it costs them |
|---|---|
| Cron + a `jobs` table in Postgres | Free to start, then no ack timeout, no attempt counters, no DLQ, no visibility. Each team reinvents it badly. |
| Redis LPUSH / BRPOPLPUSH | Fast, zero durability guarantees people actually understand. Redis Streams fixes that, but PEL + `XAUTOCLAIM` reaping is a subtle, self-inflicted distributed system. Each consumer must be its own reaper; millions of unacked entries degrade `XREADGROUP` and blow memory. |
| beanstalkd | Genuinely lovely primitives (`reserve`/`bury`/`kick`/TTR/tubes). Effectively unmaintained, no first-class durability story, no replay, no per-message history. |
| NSQ | No replication by design, "messages may be delivered multiple times — dedupe is your problem", no ordering, and the project has drifted. |
| RabbitMQ | Powerful, but the mental model (exchanges × bindings × routing keys × queue args × policies × quorum queues) is a week of study and a permanent ops liability. |
| NATS JetStream | The best semantics in the field — and a real cluster, a real config surface, subject-explosion footguns, and **no one-line DLQ**: you compose it from max-deliver advisories + direct-get + republish. Small teams report exactly this bill coming due. |
| Kafka | The queue *is* the system. Correct at scale; absurd for 200 msg/s of internal integration traffic. |

The gap is not "another broker." The gap is: **a durable, ack-based queue whose entire operational story fits in one binary, one file, and one CLI — and which answers forensic questions natively instead of forcing you to bolt on a log pipeline.**

### 1.2 Positioning statement

> For **small platform and product teams (3–15 engineers) running internal services on Linux without a dedicated SRE function**,
> who **need reliable retries, dead-lettering and replay but have been told the next step is Kafka or NATS**,
> **messq** is a **single-binary queue daemon with at-least-once delivery and a durable per-message journal**.
> Unlike **NATS JetStream, Redis Streams, or a hand-rolled jobs table**, messq **records every state transition of every message durably and shows it to you as one command: `messq trace <id>`**.

### 1.3 Ideal Customer Profile (and the anti-ICP)

**Primary ICP — "the two-pizza platform team."**
3–15 engineers. 5–40 internal services. Deployed on a handful of Linux VMs or a small k8s cluster. Already runs Postgres. No dedicated queue owner. Currently on cron + DB table, or a Redis list, or a RabbitMQ instance nobody understands. Throughput: 10–5,000 msg/s sustained, bursts to 20k. Message sizes 200B–256KB. Their real pain is *"a webhook didn't get processed last Tuesday and we cannot prove what happened."*

**Secondary ICPs, in priority order:**
1. **Integration / ETL shops.** Retries against flaky third-party APIs; need attempt counters, backoff and a DLQ they can inspect and re-drive by hand.
2. **Solo & small-SaaS founders.** Want a job queue that survives a reboot, without a Redis bill or an ops surface.
3. **On-prem / appliance vendors.** Ship software to a customer's single box. Cannot require Kafka. Need one static binary and one data file.
4. **Regulated small shops.** Need an auditable "what happened to this event" record for a compliance reviewer.
5. **Homelab / self-hosters.** The most efficient early-adoption and word-of-mouth engine for infra tools. They test the install path for free.

**Anti-ICP — written into the README, section "When *not* to use messq":**
- You need >50k msg/s sustained on one node, or terabyte retention.
- You need automatic failover / multi-region / quorum replication.
- You need stream processing (joins, windows, exactly-once transactional pipelines).
- You need a connector ecosystem (Debezium, sinks, schema registry).
- You need fan-out to thousands of concurrent subscribers with core-NATS latency.

Publishing the anti-pitch is a *conversion tactic*, not modesty. Infra buyers trust a project that names its limits; it is the single highest-signal trust artifact a small broker can ship, and it pre-empts the "this is a toy" objection by making the boundary our claim rather than a critic's discovery.

### 1.4 Feature triage: table stakes vs. differentiators

This is the sharpest strategic call in the plan. Building differentiators before table stakes is death; building *only* table stakes is invisibility.

**Table stakes (buy the right to be considered; zero marketing value; must be flawless):**
durable publish · at-least-once delivery · explicit ack/nak · ack timeout + redelivery · max-delivery cap · dead-letter destination · durable consumer cursors · subject routing · flow control · replay from a cursor · a metrics endpoint.

**Differentiators (the only things we say out loud):**
1. **The journal.** Every state transition of every message is written durably *in the same transaction as the state change*, and is queryable forever (subject to retention). Nobody else does this natively. JetStream emits advisories on a subject you must capture yourself; Redis Streams has a PEL, not a history; beanstalkd has `stats-job` and no history at all.
2. **`messq trace <message-id>`.** One command, human-readable, end-to-end. This is the demo. This is the screenshot on the landing page.
3. **CLI-as-primary-interface.** The CLI is not an admin afterthought; every daemon capability is a subcommand, and the daemon has no feature the CLI cannot reach.
4. **One static binary, one file, zero schema migration step for the user.** cgo-free, cross-compiled, `messq serve` works with no config file.
5. **`messq consume --exec`.** A shell-level consumer: run a program per message, exit code 0 = ack, 75 = nak, 78 = term. Teams with cron jobs get a real queue *without writing any code*. This is the widest possible top-of-funnel.
6. **A published log-field schema, versioned like an API.** Your Loki/Splunk queries do not break on upgrade.

**Explicitly deferred, and why:** clustering, replication, exactly-once, transactions across streams, schema registry, a web console in core. Each would convert "understandable in an evening" into "a worse NATS."

### 1.5 The permanent product promise

Three promises we will never break, printed in `GOVERNANCE.md`:

1. **Single node, honestly.** messq will not grow a consensus protocol. Warm standby / read replica is the ceiling. If you need HA, we will tell you to use JetStream and we will ship a migration guide.
2. **Understandable in an evening.** The core (`internal/broker`, `internal/store`, `internal/journal`) has a hard budget: **≤ 6,000 lines of non-test Go**. A PR that exceeds it must delete something or be rejected. This is measured in CI.
3. **Apache-2.0 forever, no CLA.** See §11.6.

---

## 2. Architecture overview

### 2.1 Process model

One process. One binary. `messq` is both daemon and client; `messq serve` is the daemon, everything else is a client that speaks HTTP to it (unix socket by default).

```
                         ┌──────────────────────────── messq serve (1 process) ────────────────────────────┐
                         │                                                                                 │
 publishers ──HTTP──▶    │  ┌──────────┐    ┌───────────────┐    ┌─────────────────────────────────────┐   │
 (curl, libs)            │  │ API layer│───▶│  Broker core  │───▶│  Store (single writer goroutine)    │   │
                         │  │ net/http │    │  (per-stream  │    │  ┌───────────────────────────────┐  │   │
 consumers  ──HTTP──▶    │  │ ServeMux │◀───│   + per-      │◀───│  │ SQLite (WAL), 1 write conn,   │  │   │
 (long-poll fetch)       │  └──────────┘    │   consumer    │    │  │ N read conns                  │  │   │
                         │        │         │   goroutines) │    │  └───────────────────────────────┘  │   │
 CLI ──unix socket──▶    │        │         └───────┬───────┘    └───────────────┬─────────────────────┘   │
                         │        │                 │                            │                         │
 Prometheus ──HTTP──▶    │  ┌─────▼──────┐   ┌──────▼────────┐            ┌──────▼──────┐                  │
                         │  │ /metrics   │   │ Timer wheel   │            │  Journal    │──▶ stdout (slog) │
                         │  │ /healthz   │   │ (ack deadlines│            │  writer     │                  │
                         │  └────────────┘   │  + nak delays)│            └─────────────┘                  │
                         │                   └───────────────┘                                             │
                         │  ┌────────────────┐  ┌──────────────────┐  ┌───────────────────┐                │
                         │  │ Retention GC   │  │ Checkpointer     │  │ SSE broadcaster   │                │
                         │  └────────────────┘  └──────────────────┘  └───────────────────┘                │
                         └─────────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Goroutines (exact inventory)

| Goroutine | Count | Responsibility |
|---|---|---|
| `http.Server` handlers | per-connection (stdlib) | Parse, validate, hand a command to the broker over a channel, wait for the reply. |
| **Store writer** | **exactly 1** | Owns the single SQLite write connection. Consumes a command channel; performs **group commit**: drains up to 256 commands or 2ms, wraps them in one `BEGIN IMMEDIATE … COMMIT`, replies to each waiter. This is where the fsync amortization lives. |
| Store readers | `GOMAXPROCS` conns in a pool | Read-only queries (peek, trace, list, lag). Never block the writer thanks to WAL. |
| Consumer dispatcher | 1 per active consumer | Owns that consumer's in-memory in-flight index and `max_ack_pending` accounting; serves `fetch` waiters; enforces per-subject ordering when enabled. |
| Timer wheel | 1 | Hierarchical timing wheel (100ms tick, 3 levels). Fires ack-deadline expiries and nak-delay releases. Emits redelivery commands to the store writer. |
| Journal fan-out | 1 | Takes committed journal events, writes them to `slog`, publishes to SSE subscribers, updates Prometheus counters. Never on the commit path's critical section — but the *durable* journal row is written inside the transaction. |
| Retention GC | 1 | Deletes messages/journal rows past retention; runs `PRAGMA wal_checkpoint(TRUNCATE)` when WAL exceeds a threshold. |
| Signal / lifecycle | 1 | SIGTERM → `http.Server.Shutdown(ctx)` → drain in-flight fetches → final checkpoint. |

Total steady-state goroutines: ~`7 + consumers + connections`. A reader can hold the whole thing in their head. That is a feature, not an accident.

### 2.3 Data flow, publish → ack

1. `POST /v1/pub/orders.created` arrives. Handler assigns a **ULID** message ID, extracts `traceparent`, `Messq-Msg-Id` (dedupe key), headers.
2. Handler enqueues a `PublishCmd` on the store-writer channel and blocks.
3. Store writer batches it, opens `BEGIN IMMEDIATE`, inserts into `message`, inserts one `pending` row per matching consumer, inserts a `journal` row (`event='published'`), commits (fsync per §3.3), replies.
4. Handler returns `202` with `{id, seq}`. Journal fan-out logs `msg=published id=01J… subject=orders.created`.
5. A consumer's long-poll `fetch` is parked on its dispatcher. The dispatcher wakes, claims up to `batch` pending rows via the store writer (`state=pending → inflight`, `attempt++`, `ack_deadline=now+ack_wait`, journal `delivered`), arms timer-wheel entries, returns the batch with opaque **delivery tokens**.
6. Consumer processes, calls `POST /v1/consumers/w1/ack {tokens:[…]}`. Store writer sets `state=acked`, journal `acked`, cancels the timer entries, advances the consumer cursor over the contiguous acked prefix.

Everything else in §4 is a variation on step 6.

---

## 3. Storage & durability design

### 3.1 Engine choice: SQLite via `modernc.org/sqlite`

**Decision: SQLite in WAL mode, driven by the pure-Go `modernc.org/sqlite` driver.**

Why, in strategy terms first:
- **cgo-free is a product requirement, not a preference.** The "download one static binary, `chmod +x`, run" promise dies the moment we need a C toolchain to cross-compile for arm64. `modernc.org/sqlite` is a CGo-free port (currently tracking SQLite 3.53.x, Go 1.18+), so `GOOS=linux GOARCH=arm64 go build` from any laptop produces a working artifact. `mattn/go-sqlite3` is faster but costs us the funnel.
- **A real query engine is what makes the journal a product.** `messq trace`, `messq lag`, `messq dlq ls --subject=… --since=1h`, `messq peek --filter` are all SQL. With bbolt we would hand-roll every secondary index and every scan, and the differentiator would become the expensive part.
- **No schema migration step for the user.** The daemon owns its own schema, migrates on boot, and the user never runs a migration command. Contrast with a Postgres-backed job queue, where the user's DBA is now in the loop.

**bbolt, evaluated and rejected.** Its docs are explicit about the constraints that hurt us: all access must occur inside transactions, retrieved data is only valid for the transaction's lifetime, objects are not goroutine-safe, and bulk-loading >100k keys into a new bucket in a single transaction is discouraged because pages split at commit. Combined with "no queries, only cursors and `Seek` range scans", every index for the journal would be a bucket we maintain by hand. bbolt is the right choice for a raft log; it is the wrong choice for an inspectable broker.

**Escape hatch.** `internal/store` is an interface (`Store`), and SQLite is one implementation. If we ever hit the ceiling (§10), a segment-file + index implementation slots in behind it without touching the broker.

### 3.2 Schema

```sql
PRAGMA journal_mode  = WAL;
PRAGMA synchronous   = FULL;      -- default; see §3.3
PRAGMA busy_timeout  = 5000;
PRAGMA foreign_keys  = ON;
PRAGMA wal_autocheckpoint = 0;    -- we checkpoint on our own schedule

CREATE TABLE stream (
  name          TEXT PRIMARY KEY,
  subjects      TEXT NOT NULL,          -- JSON array of patterns, e.g. ["orders.*"]
  max_age_sec   INTEGER,                -- NULL = forever
  max_bytes     INTEGER,
  max_msgs      INTEGER,
  discard       TEXT NOT NULL DEFAULT 'old',   -- old | new
  created_at    INTEGER NOT NULL
);

CREATE TABLE message (
  id            BLOB PRIMARY KEY,       -- 16-byte ULID
  stream        TEXT NOT NULL REFERENCES stream(name) ON DELETE CASCADE,
  seq           INTEGER NOT NULL,       -- per-stream monotonic
  subject       TEXT NOT NULL,
  payload       BLOB NOT NULL,
  headers       BLOB,                   -- msgpack-ish: compact JSON object
  trace_id      TEXT,                   -- 32-hex from W3C traceparent
  dedupe_key    TEXT,
  published_at  INTEGER NOT NULL,       -- unix micros
  size_bytes    INTEGER NOT NULL
);
CREATE UNIQUE INDEX message_stream_seq ON message(stream, seq);
CREATE INDEX        message_subject    ON message(stream, subject, seq);
CREATE UNIQUE INDEX message_dedupe     ON message(stream, dedupe_key)
       WHERE dedupe_key IS NOT NULL;

CREATE TABLE consumer (
  name            TEXT NOT NULL,
  stream          TEXT NOT NULL REFERENCES stream(name) ON DELETE CASCADE,
  filter_subject  TEXT NOT NULL DEFAULT '>',
  ack_wait_ms     INTEGER NOT NULL DEFAULT 30000,
  max_deliver     INTEGER NOT NULL DEFAULT 5,      -- -1 = unlimited
  backoff_ms      TEXT,                            -- JSON array; overrides ack_wait
  max_ack_pending INTEGER NOT NULL DEFAULT 1000,
  ordered_by      TEXT NOT NULL DEFAULT 'none',    -- none | subject
  dlq             TEXT NOT NULL DEFAULT 'auto',    -- auto | none | <stream>
  ack_floor_seq   INTEGER NOT NULL DEFAULT 0,      -- contiguous acked prefix
  delivered_seq   INTEGER NOT NULL DEFAULT 0,      -- highest seq ever handed out
  created_at      INTEGER NOT NULL,
  PRIMARY KEY (stream, name)
);

CREATE TABLE delivery (                            -- one row per (message, consumer)
  stream        TEXT NOT NULL,
  consumer      TEXT NOT NULL,
  msg_id        BLOB NOT NULL,
  seq           INTEGER NOT NULL,
  subject       TEXT NOT NULL,
  state         INTEGER NOT NULL,   -- 0 pending 1 inflight 2 acked 3 dead
  attempt       INTEGER NOT NULL DEFAULT 0,
  due_at        INTEGER NOT NULL,   -- pending: earliest delivery; inflight: ack deadline
  token         BLOB,               -- 16 random bytes, valid only while inflight
  last_reason   TEXT,
  PRIMARY KEY (stream, consumer, msg_id)
);
CREATE INDEX delivery_ready   ON delivery(stream, consumer, state, due_at, seq);
CREATE INDEX delivery_inflight ON delivery(state, due_at) WHERE state = 1;
CREATE INDEX delivery_by_msg  ON delivery(msg_id);

CREATE TABLE journal (                             -- THE differentiator
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  at         INTEGER NOT NULL,        -- unix micros
  event      TEXT NOT NULL,           -- published|delivered|acked|naked|expired|termed|dead|replayed|purged|restart_requeue|seek
  msg_id     BLOB,
  stream     TEXT,
  consumer   TEXT,
  subject    TEXT,
  attempt    INTEGER,
  trace_id   TEXT,
  reason     TEXT,
  detail     BLOB                     -- JSON: deadline, delay_ms, actor, remote_addr, latency_us
);
CREATE INDEX journal_msg  ON journal(msg_id, id);
CREATE INDEX journal_time ON journal(at);
CREATE INDEX journal_cons ON journal(stream, consumer, at);

CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT);    -- schema_version, node_id, created_at
```

Notes that matter:
- **The journal row is written in the same transaction as the state change.** There is no window in which the state and its explanation disagree. This is the invariant that makes `messq trace` trustworthy, and it is why the journal lives in SQLite rather than in a log file.
- **The DLQ is a real stream, not a graveyard.** `dlq='auto'` creates/uses stream `dlq.<stream>`; a dead message is *republished* into it with `Messq-Dead-Reason`, `Messq-Dead-Attempts`, `Messq-Origin-Id`, `Messq-Origin-Seq` headers, and its journal is linked to the original ID. Brand line: **"dead letters are just messages in another stream"** — so `messq sub`, `messq trace`, replay and retention all work on the DLQ with zero new concepts. This is the JetStream composability lesson, delivered as one config value instead of an advisory-capture pipeline.

### 3.3 fsync policy

Three named durability modes, chosen with one flag. Naming them is a product decision — users must be able to say which one they run.

| Mode | Flag | SQLite | Guarantee | Cost |
|---|---|---|---|---|
| **strict** (default) | `--durability=strict` | `synchronous=FULL`, WAL fsync per commit | A `202` from `/v1/pub` means the message survives power loss. | ~1 fsync per group commit. With 2ms/256-cmd group commit: 500 commits/s ≈ 10k–100k msg/s depending on disk. |
| **relaxed** | `--durability=relaxed` | `synchronous=NORMAL` | Never corrupt, always consistent; **may lose the last commits on power loss / kernel panic** (not on process crash). | ~3–5× publish throughput. |
| **volatile** | `--durability=volatile` | `synchronous=OFF` | Benchmarks and CI only. Daemon refuses to start without `--i-know` and logs `WARN` every 60s. | Fastest. |

**We default to strict and say so on the landing page.** The competitive point is that most "just use Redis/SQLite" advice quietly ships the relaxed semantics; SQLite's own default (`synchronous=NORMAL` in WAL) does not fsync per commit, which surprises people. messq makes the choice explicit, names it, logs it at startup, and exposes it at `GET /v1/info` and in `messq doctor`. Honesty about durability is a marketing asset in this category.

Group commit is what makes strict affordable: the store writer drains its channel, so N concurrent publishers share one fsync. We publish the measured fsync-amortization curve on the benchmarks page.

### 3.4 Crash recovery

State lives entirely in SQLite; SQLite's WAL recovers itself. messq's recovery is therefore about *semantics*, not bytes:

1. Open DB, run `PRAGMA integrity_check` if `--fsck` or if the previous shutdown was unclean (tracked in `meta`).
2. Apply schema migrations (embedded, forward-only, recorded in `meta.schema_version`).
3. **In-flight deliveries keep their stored `ack_deadline`.** They are *not* reset to attempt 0, and they are *not* all redelivered instantly. Deadlines already in the past become immediately due; future deadlines are re-armed in the timer wheel. A crash therefore does not silently multiply the redelivery storm, and attempt counters remain truthful.
4. For every delivery re-armed or made due, write journal event `restart_requeue` with `reason="broker restarted"`. **This is deliberate:** six months later, `messq trace` will explain that Tuesday's duplicate came from a restart, not from a consumer bug. No competitor can answer this question.
5. Rebuild per-consumer in-memory indexes and `max_ack_pending` counters by scanning `delivery WHERE state IN (0,1)` for each consumer (indexed; bounded by backlog, not by history).
6. Log one `startup` line with: version, data dir, durability mode, stream count, consumer count, total backlog, total in-flight re-armed, recovery duration.

**Crash-safety claim we will make and test (§8):** with `--durability=strict`, no acknowledged publish is ever lost, and no acked message is ever redelivered, across arbitrary `SIGKILL` at any point.

---

## 4. Delivery semantics & message lifecycle

### 4.1 Vocabulary — a deliberate branding decision

**We adopt NATS JetStream's vocabulary verbatim: stream, subject, consumer, ack, nak, term, ack-wait, max-deliver, backoff, max-ack-pending, deliver policy.** Not one synonym invented.

Three strategic reasons:
1. **Zero teaching cost.** Anyone who has read the JetStream docs already knows messq. Familiar vocabulary is the cheapest onboarding mechanism that exists.
2. **Graduation is a feature, not a defection.** "If you outgrow messq, JetStream is a config change, not a rewrite" is a *reason to adopt*, because it removes the lock-in objection that kills small-broker evaluations. We will ship `docs/graduating-to-jetstream.md` in v1.0. Being the honest on-ramp to a bigger system is a durable position; pretending to be the destination is not.
3. **SEO and conversation.** Every JetStream/Redis-Streams comparison thread becomes a place where messq is intelligible in one sentence.

Only **two** terms are ours, and they carry the whole brand:
- **journal** — the durable, queryable history of every transition.
- **trace** — the verb that reads it. `messq trace <id>`.

Rejected: "tube" (beanstalkd), "channel" (NSQ), "topic" (Kafka — invites the wrong scale expectations), "job" (narrows us to background work and cedes event distribution).

### 4.2 State machine

State is per **(message, consumer)** pair. A message with three consumers has three independent lifecycles — this is what makes messq a pub/sub broker rather than a work queue, and it is why the DLQ is per-consumer.

```
                       ┌──────────────────────── nak(delay) / expire ────────────────────────┐
                       │                                                                     │
   publish             ▼                fetch                       ack                      │
  ─────────▶ [ PENDING ] ───────────────────────▶ [ INFLIGHT ] ───────────────▶ [ ACKED ]    │
                  │  ▲                                  │                        (terminal)  │
                  │  │                                  │                                    │
                  │  └──────── restart_requeue ─────────┤                                    │
                  │                                     │                                    │
                  │                    progress (extend deadline) ──┐                        │
                  │                                     │◀──────────┘                        │
                  │                                     │                                    │
                  │                    term(reason)     │    attempt >= max_deliver          │
                  │                          ┌──────────┴──────────┐                         │
                  │                          ▼                     ▼                         │
                  │                     [ DEAD ] ◀───────────────────                        │
                  │                    (terminal)                                            │
                  │                          │                                               │
                  │                          └── republish into dlq.<stream> ──▶ new lifecycle│
                  │                                                                          │
                  └──────────────────────────────────────────────────────────────────────────┘
                                        seek / replay resets PENDING
```

### 4.3 Transition table (normative)

| From | Trigger | To | Side effects | Journal event |
|---|---|---|---|---|
| — | `POST /v1/pub` | PENDING (one row per matching consumer) | assign ULID + per-stream seq; dedupe check | `published` |
| PENDING | `fetch` claims it (`due_at <= now`, `inflight < max_ack_pending`) | INFLIGHT | `attempt++`; `due_at = now + ack_wait` (or `backoff[attempt-1]`); fresh random `token`; arm timer | `delivered` |
| INFLIGHT | `ack(token)` | ACKED | cancel timer; advance `ack_floor_seq` over contiguous acked prefix; retention may now delete | `acked` |
| INFLIGHT | `nak(token, delay?)` | PENDING | `due_at = now + delay` (default 0 → immediate); attempt already counted; record `reason` | `naked` |
| INFLIGHT | `progress(token)` | INFLIGHT | `due_at = now + ack_wait`; **does not** increment attempt | `progress` (debug level) |
| INFLIGHT | timer fires (`now > due_at`) | PENDING | `reason="ack_wait expired"` | `expired` |
| INFLIGHT | `term(token, reason)` | DEAD | skip remaining attempts; to DLQ | `termed` then `dead` |
| PENDING/INFLIGHT | `attempt >= max_deliver` at claim time | DEAD | to DLQ | `dead` |
| INFLIGHT | broker restart | INFLIGHT (deadline preserved) or PENDING (deadline passed) | re-arm timer | `restart_requeue` |
| ACKED/DEAD | `messq replay` / `seek` | PENDING | `attempt` reset to 0, **original journal preserved**, new `replayed` event links the epochs | `replayed` |
| any | retention / `purge` | (row deleted) | | `purged` |

**Precise semantics we commit to, in the docs, as a table:**
- `nak` does **not** apply the backoff schedule; it applies its own `delay` (default 0). Backoff applies to *ack-wait expiry* only. This mirrors JetStream exactly, including the surprise, because divergence here would be worse than the surprise.
- `backoff` overrides `ack_wait`; `backoff[0]` becomes the effective first ack-wait.
- `max_ack_pending` is per-consumer, shared across all workers pulling that consumer. `max_ack_pending=0` means unlimited (and `messq doctor` warns about it).
- Ordering: `ordered_by=subject` makes the dispatcher refuse to hand out message *n+1* on a subject while *n* is in-flight or pending-retry on that subject. Default `none`, because ordering costs head-of-line blocking and most users do not need it. **Opt-in, per consumer, documented with the cost.**
- **A `token` is single-use.** Ack of a stale token (from a previous attempt) returns `409 stale_token` and journals `stale_ack` — so the trace shows the double-delivery race that a slow consumer just lost, instead of silently mis-acking a newer attempt. This is a small correctness detail with outsized trust value.

### 4.4 Flow control

Three layers, all visible:
1. **`max_ack_pending`** — bounds in-flight per consumer; the dispatcher simply stops handing out work.
2. **`fetch` long-poll with `max` and `wait`** — consumers pull; there is no push to overwhelm.
3. **Stream limits** (`max_msgs`, `max_bytes`, `max_age`) with `discard=old|new`. `discard=new` makes `/v1/pub` return `429` with `Retry-After` — publisher backpressure that a client library can honour.

---

## 5. API / protocol

### 5.1 The decision: HTTP/1.1 + JSON, and nothing else in v1.0

This is a positioning decision as much as an engineering one. The evaluator's first session decides everything, and in that session the winning move is that `curl` already works. A custom binary protocol or gRPC would mean: no `curl`, a `protoc` toolchain, no ingress/proxy compatibility, no browser, and a client library required before the first message. That trades the entire top of the funnel for latency we do not need at 5k msg/s.

- **Transport:** HTTP/1.1 over a **unix socket by default** (`$XDG_RUNTIME_DIR/messq.sock`, mode 0600) and optionally TCP (`--listen=127.0.0.1:4747`).
- **Routing:** Go stdlib `http.ServeMux` with method+wildcard patterns (`mux.HandleFunc("POST /v1/pub/{subject...}", …)`). No router dependency.
- **Shutdown:** `http.Server.Shutdown(ctx)` on SIGTERM, draining long-polls first.
- **Bodies:** raw bytes for payloads (`Content-Type` preserved as a message header), JSON for control operations.
- **Contract:** an **OpenAPI 3.1 document generated from the handlers and checked into the repo**, from which we generate the Python and TypeScript clients. The Go client is hand-written.
- **Phase 2:** an SSE `deliver` endpoint for lower-latency push-style consumption, and h2c. gRPC only if three real users ask.

### 5.2 Endpoints

```
# --- publish -------------------------------------------------------------
POST   /v1/pub/{subject}
       Body: raw payload
       Headers: Messq-Msg-Id (dedupe), Messq-Stream (override routing),
                traceparent, Messq-Header-* (user metadata)
       → 202 {"id":"01JB…","stream":"orders","seq":48213,"duplicate":false}
       → 429 {"error":"stream_full","retry_after_ms":250}
POST   /v1/pub/batch            Body: JSON array; one transaction, all-or-nothing

# --- consume -------------------------------------------------------------
GET    /v1/consumers/{stream}/{name}/fetch?max=10&wait=30s
       → 200 {"messages":[{"token":"…","id":"01JB…","seq":48213,
                           "subject":"orders.created","attempt":2,
                           "delivered_at":"…","deadline":"…",
                           "headers":{…},"trace_id":"…",
                           "payload_b64":"…"}]}
       → 204 (long-poll expired, no work)
POST   /v1/consumers/{stream}/{name}/ack        {"tokens":["…"]}
POST   /v1/consumers/{stream}/{name}/nak        {"token":"…","delay":"5s","reason":"upstream 503"}
POST   /v1/consumers/{stream}/{name}/term       {"token":"…","reason":"schema invalid"}
POST   /v1/consumers/{stream}/{name}/progress   {"tokens":["…"]}

# --- admin ---------------------------------------------------------------
POST   /v1/streams              {"name":"orders","subjects":["orders.*"],"max_age":"168h"}
GET    /v1/streams  ·  GET /v1/streams/{s}  ·  DELETE /v1/streams/{s}
POST   /v1/consumers/{stream}   {"name":"billing","filter_subject":"orders.created",
                                 "ack_wait":"30s","max_deliver":5,
                                 "backoff":["5s","30s","5m","1h"],
                                 "max_ack_pending":500,"ordered_by":"subject"}
PATCH  /v1/consumers/{stream}/{name}    # ack_wait, max_deliver, backoff, max_ack_pending are editable
DELETE /v1/consumers/{stream}/{name}

# --- inspection & recovery — the differentiator --------------------------
GET    /v1/messages/{id}                     # metadata + payload
GET    /v1/messages/{id}/journal             # THE trace
GET    /v1/journal?stream=&consumer=&event=&since=&until=&limit=
GET    /v1/events                            # SSE live journal stream (messq tail)
GET    /v1/consumers/{stream}/{name}/pending?limit=50   # peek without consuming
POST   /v1/consumers/{stream}/{name}/seek    {"to":"start"|"now"|{"seq":N}|{"time":"…"}}
POST   /v1/consumers/{stream}/{name}/replay  {"ids":[…]} | {"from_seq":N,"to_seq":M} | {"dlq":true,"filter":{…}}
POST   /v1/streams/{s}/purge                 {"subject":"orders.test.*","before":"…"}   # requires confirm token

# --- ops -----------------------------------------------------------------
GET    /v1/info        # version, durability mode, uptime, data dir, limits
GET    /healthz  ·  GET /readyz  ·  GET /metrics
```

### 5.3 Auth

- **Unix socket + file permissions is the default and covers the ICP.** No token, no TLS, no config, on day one. This is a deliberate friction removal: authentication is the #1 quickstart killer for infra tools.
- TCP listener requires `--auth=token` with tokens in `messq.tokens` (bcrypt-hashed), scoped `publish|consume|admin` per stream pattern. Refuse to bind a non-loopback address without auth configured — fail loudly at startup rather than shipping an open broker.
- TLS: terminate at a proxy, or `--tls-cert/--tls-key`. mTLS in Phase 2.

### 5.4 Idempotency and tracing

- `Messq-Msg-Id` gives publish-side dedupe within a configurable window (default 2 minutes, `dedupe_window` per stream). Returns `202 {"duplicate":true}` with the original ID.
- `traceparent` (W3C Trace Context) is extracted on publish, its 32-hex trace-id stored on the message, echoed on every delivery, and stamped on **every journal row and every log line**. Message queues have no standard header slot for trace context — Kafka has record headers, AMQP has properties — so messq standardises on the HTTP header that already exists, and the operator gets one ID that spans "the HTTP request that published it" and "the worker that finally acked it". If no `traceparent` arrives, messq generates one and returns it, so a `curl` user gets tracing for free.

---

## 6. CLI & developer experience

### 6.1 The 60-second aha moment

Three commands, and the third is the money shot. This exact sequence is the README, the landing page above the fold, and the asciinema recording.

```console
$ curl -fsSL https://messq.dev/install.sh | sh        # or: go install go.messq.dev/messq@latest
$ messq quickstart
```

`messq quickstart` is a real subcommand, not a doc page. It:
1. starts an ephemeral daemon in a temp dir (nothing to clean up),
2. creates stream `demo` and consumer `worker` with `ack_wait=2s max_deliver=3`,
3. publishes 5 messages,
4. runs a built-in fake worker that acks 3, naks 1 once then acks it, and **lets one time out three times until it dead-letters**,
5. streams the human log to the terminal as it happens,
6. prints, unprompted:

```
  Message 01JB8Q0T7K5X2M9R4V6C3H1F2D never got an ack. Here is its life story:

  $ messq trace 01JB8Q0T7K5X2M9R4V6C3H1F2D

  01JB8Q0T7K5X2M9R4V6C3H1F2D   demo / jobs.email   142 B   trace 4bf92f3577b34da6
  ─────────────────────────────────────────────────────────────────────────────────
  12:04:31.118  published        by 127.0.0.1              seq 4
  12:04:31.204  delivered        → worker   attempt 1      deadline 12:04:33.204
  12:04:33.205  expired          ack_wait 2s elapsed       no ack received
  12:04:33.206  delivered        → worker   attempt 2      deadline 12:04:35.206
  12:04:35.207  expired          ack_wait 2s elapsed       no ack received
  12:04:35.208  delivered        → worker   attempt 3      deadline 12:04:37.208
  12:04:37.209  expired          ack_wait 2s elapsed       no ack received
  12:04:37.210  dead             max_deliver 3 reached     → dlq.demo (seq 1)
  ─────────────────────────────────────────────────────────────────────────────────
  in system 6.09s · 3 attempts · 0 acks · terminal state DEAD

  Re-drive it when the bug is fixed:   messq dlq replay demo --id 01JB8Q0T…
```

That output *is* the product. A reader who has ever debugged a stuck queue understands the entire value proposition in the time it takes to read it. Total elapsed: well under 60 seconds, zero config files, zero client libraries, zero code written.

Second aha, ~5 minutes in, for the cron crowd — a durable retrying queue with **no code at all**:

```console
$ messq serve &
$ messq stream add jobs --subjects 'jobs.*'
$ messq consumer add jobs mailer --ack-wait 30s --max-deliver 5 --backoff 5s,1m,10m
$ messq consume jobs mailer --exec ./send-email.sh     # exit 0 = ack, 75 = nak, 78 = term
$ echo '{"to":"a@example.com"}' | messq pub jobs.email -
```

`--exec` (payload on stdin, metadata in `MESSQ_*` env vars, `EX_TEMPFAIL=75` / `EX_CONFIG=78` as the nak/term signals) is our widest funnel: it converts anybody with a shell script into a messq user without a client library, and it is the single most shareable demo in the project.

### 6.2 Command surface (cobra)

`spf13/cobra` v1.9.x: subcommand tree, POSIX flags, persistent flags on root, and `GenBashCompletion` / `GenZshCompletion` / `GenFishCompletion` / `GenPowerShellCompletionWithDesc` behind `messq completion <shell>`. We use `SetHelpTemplate` to brand the help output and cobra's doc generation to produce the CLI reference pages for the docs site from the same source of truth, so help text and docs can never drift.

```
messq serve            [--data-dir --listen --socket --durability --log-format --config]
messq quickstart       # the 60-second demo
messq pub              <subject> [file|-] [--header k=v --msg-id --count --rate]
messq sub              <stream> <consumer> [--exec CMD | --print] [--max-in-flight]
messq consume          # alias of sub, for the job-queue mental model

messq stream           add|ls|info|rm|purge
messq consumer         add|ls|info|edit|rm|seek
messq trace            <message-id> [--json]              # ★
messq tail             [--stream --consumer --event --subject]   # ★ live journal, SSE
messq peek             <stream> <consumer> [--n 20]       # pending, without consuming
messq lag              [--stream --consumer --watch]      # backlog, in-flight, oldest-unacked age
messq dlq              ls|show|replay|drop
messq replay           <stream> <consumer> --from-seq|--since|--ids
messq doctor           # opinionated health diagnosis in prose
messq bench            [--publishers --consumers --size --duration]
messq completion       bash|zsh|fish|powershell
messq version          # --json includes commit, go version, durability default
```

### 6.3 Output contract

- **Human-readable by default**, aligned columns, colour only when stdout is a TTY, relative times ("4m ago") with absolute on `--utc`.
- **`--json` on every single command**, emitting one JSON object or NDJSON stream. Non-negotiable; scripts are how ops tools get adopted.
- Exit codes are a documented contract: `0` ok, `1` error, `2` usage, `3` not found, `4` daemon unreachable.
- **Every error message names the fix.** `error: consumer "mailer" not found in stream "jobs"` + `hint: messq consumer add jobs mailer --ack-wait 30s`. Error copy is reviewed as product copy in code review.

### 6.4 Client libraries

| Language | Status | Approach |
|---|---|---|
| Go | v1.0, in-repo, hand-written | `messq.Consume(ctx, stream, consumer, func(m *Message) error)` — return `nil`→ack, `messq.ErrRetry`→nak, `messq.ErrTerm`→term. Auto-`progress` for long handlers. |
| Python | v1.0 | Generated from OpenAPI, thin hand-written ergonomic layer, `@app.consumer("jobs.email")` decorator. |
| TypeScript | v1.0 | Same. |
| Rust, Java, PHP, Ruby | community | We ship the conformance suite (§8.5) and a `CONTRIBUTING-CLIENTS.md`; we do not maintain them. |

The Go client must be *thinner than the CLI's own usage of it* — the CLI consumes the same package, guaranteeing it is real.

### 6.5 Docs strategy

Structured by **reader intent**, not by feature, and every page must survive the "would this be in the top 3 Google results for the question a stuck operator types at 3am?" test.

1. **README (the 30-second decision).** What it is (2 sentences) → the `messq trace` output block → 3-command quickstart → "When *not* to use messq" → comparison table → install. No badges above the fold, no architecture diagram, no roadmap. The README's only job is to make the reader run `messq quickstart`.
2. **`messq.dev` landing page.** Above the fold: the `messq trace` screenshot and one sentence. Below: the three-command quickstart and the asciinema. Docs are a plain static site (Hugo, in-repo, no CMS).
3. **Tutorials (learning):** "Your first queue in 5 minutes" · "A retrying webhook processor" · "Turn a cron script into a durable queue with `--exec`".
4. **The incident-shaped tutorial — our signature doc:** *"A message went missing. Find it."* The reader is walked through `messq lag` → `messq peek` → `messq trace` → `messq dlq ls` → fix → `messq dlq replay`. This is the page that gets bookmarked and shared internally, because it teaches the product through the emotion that made the reader search.
5. **Reference:** CLI (generated by cobra), HTTP API (generated from OpenAPI), **log field schema**, **metrics catalogue**, config reference.
6. **Explanation:** "Delivery guarantees, precisely" (the §4.3 table verbatim, with worked examples of every duplicate scenario) · "Durability modes and what a `202` means" · "Why messq is single-node".
7. **Comparisons, written fairly:** vs JetStream · vs Redis Streams · vs beanstalkd · vs a Postgres jobs table · vs Kafka. Each ends with "choose *them* if…". Fair comparison pages are the highest-converting content an infra project can publish, because the evaluator is already writing that table and we save them the work.
8. **`docs/graduating-to-jetstream.md`** — the anti-lock-in page. Removes the biggest objection at zero cost.

**Docs are CI-tested.** Every command block in the tutorials runs in CI against a real daemon (`testscript`). A broken quickstart is a P1 bug, treated as an outage.

---

## 7. Observability & logging design

Logging is not instrumentation here; it is **the same data structure as the journal**, rendered to a different sink. One event type, three destinations: the durable `journal` table, the `slog` stream, and the Prometheus counters.

### 7.1 Logging

Standard library `log/slog`, no third-party logger. Two handlers:

- `--log-format=text` (**default when stdout is a TTY**): a custom `slog.Handler` producing aligned, colourised, human-scannable lines. Human-friendliness is a stated product feature, and stdlib `TextHandler`'s `k=v` output is not it.
- `--log-format=json`: `slog.NewJSONHandler` for Loki/ES/Splunk.

`slog.HandlerOptions.ReplaceAttr` is used for two jobs that matter: (a) **enforcing the stable field-name contract** (rename built-ins, drop `source` in production, render times as RFC3339 micros), and (b) **redaction** — payloads are never logged; only `size`, `sha256[:8]`, and, when `--log-payloads=headers`, the user headers. `slog.Group` nests `consumer.*` and `delivery.*` attributes in JSON while the text handler flattens them.

**Human output:**
```
12:04:33.205  WARN   expired    demo/jobs.email  01JB8Q0T…  worker  attempt 2/3  waited 2.0s  next in 0s
12:04:37.210  ERROR  dead       demo/jobs.email  01JB8Q0T…  worker  attempt 3/3  → dlq.demo   reason=max_deliver
```

**JSON output:**
```json
{"time":"2026-08-21T12:04:37.210114Z","level":"ERROR","msg":"dead",
 "event":"dead","msg_id":"01JB8Q0T7K5X2M9R4V6C3H1F2D","stream":"demo",
 "subject":"jobs.email","consumer":"worker","attempt":3,"max_deliver":3,
 "reason":"max_deliver","dlq":"dlq.demo","dlq_seq":1,
 "trace_id":"4bf92f3577b34da6a3ce929d0e0e4736","in_system_ms":6092}
```

**The log field schema is a versioned API.** `docs/reference/log-schema.md` lists every `event` value and every field, and the schema is semver'd independently (`log_schema=1` on the startup line). Removing or renaming a field is a breaking change requiring a major release. Rationale: our users will build alerts on `event="dead"`; breaking their alerts would destroy exactly the trust we are selling.

**Events (the complete v1 list):** `published`, `delivered`, `acked`, `naked`, `progress`, `expired`, `termed`, `dead`, `stale_ack`, `replayed`, `seek`, `purged`, `restart_requeue`, `consumer_created`, `consumer_updated`, `consumer_deleted`, `stream_created`, `stream_deleted`, `limit_hit`, `startup`, `shutdown`.

Levels: `DEBUG` = `delivered`/`progress` (high volume, off by default at scale); `INFO` = `published`/`acked`/`naked`/`replayed`/lifecycle; `WARN` = `expired`/`limit_hit`/`stale_ack`; `ERROR` = `dead`.

Sampling: at >2k msg/s the `delivered`/`acked` log lines are sampled (`--log-sample=1/100`, counters remain exact, **the durable journal is never sampled**). The sampled-away count is reported once per minute so the log is honest about what it dropped.

### 7.2 Metrics

`prometheus/client_golang` on a **custom `prometheus.NewRegistry()`** (never the default registry — we choose exactly what we expose), served by `promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg, MaxRequestsInFlight: 20})`.

Counters/histograms updated in the journal fan-out:
```
messq_published_total{stream,subject}
messq_delivered_total{stream,consumer}          # includes redeliveries
messq_acked_total{stream,consumer}
messq_naked_total{stream,consumer}
messq_expired_total{stream,consumer}
messq_dead_total{stream,consumer,reason}
messq_ack_latency_seconds{stream,consumer}      # native histogram, factor 1.1
messq_publish_commit_seconds{durability}        # native histogram — the fsync cost, visible
messq_delivery_attempts{stream,consumer}        # histogram, buckets 1..max_deliver
```

Backlog gauges are **not** kept in memory and mirrored — they are a **custom `Collector`** (`Describe`/`Collect` + `prometheus.NewConstMetric`) that queries SQLite at scrape time on a read connection, with a 5-second internal cache:
```
messq_backlog{stream,consumer}                  # pending
messq_inflight{stream,consumer}
messq_oldest_pending_age_seconds{stream,consumer}   # ← the alert that actually matters
messq_dlq_depth{stream}
messq_journal_rows / messq_db_bytes / messq_wal_bytes
```
This design choice is deliberate: a gauge that is recomputed from the source of truth at scrape time cannot drift from reality after a restart or a manual purge, which is precisely the class of bug that makes operators stop trusting a broker's dashboards.

We ship `contrib/grafana/messq.json` and `contrib/prometheus/alerts.yml` with three opinionated alerts: `MessqBacklogGrowing`, `MessqOldestPendingAge > 15m`, `MessqDeadLetterRate > 0`.

### 7.3 `messq doctor`

Reads metrics + journal + config and prints prose:

```
messq doctor — 3 findings

  ⚠  consumer jobs/mailer has 12,004 pending and 0 acks in the last 5m.
     Oldest pending message is 41m old (01JB8Q0T…).
     Likely: no consumer is fetching. Check that your worker is running.
     Investigate: messq trace 01JB8Q0T…

  ⚠  consumer jobs/mailer has max_deliver=-1 (unlimited) with no DLQ.
     A poison message will retry forever and never surface.
     Fix: messq consumer edit jobs mailer --max-deliver 5

  ℹ  durability=relaxed. A `202` from publish may be lost on power loss.
     Fix: restart with --durability=strict
```

`doctor` is a marketing surface disguised as a diagnostic tool. It is the thing screenshotted on social media, and it teaches the product's semantics at exactly the moment the user cares.

---

## 8. Testing strategy

The strategic point: **for an infrastructure product from an unknown author, the test suite is a marketing asset.** We do not claim robustness; we publish the artefacts that demonstrate it, and we link them from the landing page.

### 8.1 Unit + deterministic simulation
The broker core takes a `Clock` interface. All ack-wait, backoff and timer-wheel behaviour is tested with a fake clock at nanosecond determinism — no `time.Sleep` in unit tests, no flakes. Target: broker/store/journal packages ≥ 85% line coverage, enforced in CI.

### 8.2 Property-based / model-based testing (`pgregory.net/rapid`)
`rapid`'s stateful testing (`t.Repeat` with a map of named actions, plus the `""` invariant action that runs after every step) is a near-perfect fit for a delivery state machine. We run a reference model in memory against the real store and assert after every action:

- **I1** every message reaches exactly one terminal state (ACKED or DEAD) per consumer, or is still live;
- **I2** `attempt` is monotonically non-decreasing and never exceeds `max_deliver`;
- **I3** an acked message is never delivered again (unless explicitly replayed);
- **I4** in-flight count never exceeds `max_ack_pending`;
- **I5** `ack_floor_seq` never regresses;
- **I6** the journal for any message is a valid path through the §4.3 transition table — this is the invariant that guarantees `messq trace` never lies;
- **I7** with `ordered_by=subject`, delivered sequence numbers per subject are non-decreasing across the in-flight window;
- **I8** no message is lost: `published == acked + dead + pending + inflight + purged`.

CI runs `-rapid.checks=1000`; nightly runs `-rapid.checks=200000 -rapid.shrinktime=5m`. Failure files (`.rapid/`) are committed as regression tests.

### 8.3 Crash torture
A harness publishes at load while `SIGKILL`-ing the daemon at random intervals (10ms–2s), restarting, and verifying I1–I8 across restarts, with a ledger of client-observed `202`s and acks. Runs in CI for 60s per PR, 4 hours nightly. **Claim under test: with `--durability=strict`, no acknowledged publish is ever lost.**

### 8.4 Fault injection
Nightly, in a VM: `dm-flakey` / `charybdefs` under the data dir to inject write errors, torn writes and dropped fsyncs, plus ENOSPC, EIO, and clock-jump (`libfaketime`, including backwards jumps — relevant to ULID monotonicity and to timer-wheel correctness). Results are published as `docs/reliability/report.md` with the date and the exact scenarios, refreshed each release. **Publishing an honest fault-injection report is a differentiator, because almost no small broker does it.**

### 8.5 Conformance suite
`messq-conformance` — a black-box HTTP test binary asserting the full §4.3 table against any running daemon. Uses: (a) CI for the daemon; (b) validates community client libraries; (c) it is the artefact that lets a skeptical reader verify our semantic claims themselves.

### 8.6 Docs & CLI tests
Every command block in the README and tutorials is executed in CI via `rogpeppe/go-internal/testscript` against a real daemon; golden-file tests pin CLI human output (including `messq trace`) so the demo in the README can never rot. CLI help text is snapshot-tested.

### 8.7 Performance regression
`messq bench` runs in nightly CI on fixed hardware; publish/ack throughput and p99 ack latency per durability mode are tracked over time and published on `messq.dev/benchmarks` **with the methodology and the hardware**. A >15% regression fails the build. We publish honest numbers rather than best-case numbers — in this category, a modest number with a reproducible method beats a big number nobody trusts.

---

## 9. Roadmap: empty repository → ideal product

Each milestone has a scope, an exit criterion, and — because this is a product plan — the funnel metric it moves.

### M0 — Foundation (week 1)
**Scope:** Go module `go.messq.dev/messq` (vanity import path, so hosting can change without breaking users); `LICENSE` (Apache-2.0), `NOTICE`, `GOVERNANCE.md` (the three permanent promises), `CONTRIBUTING.md` (DCO, no CLA), `SECURITY.md`; CI (build, vet, staticcheck, test, cross-compile linux/{amd64,arm64}); `docs/adr/` with ADR-0001 SQLite, ADR-0002 HTTP+JSON, ADR-0003 JetStream vocabulary, ADR-0004 single-node forever, ADR-0005 Apache-2.0/no-CLA; the 6,000-line core budget check.
**Exit:** `go build ./...` produces a static binary that prints `messq version --json`. Repo is legally and structurally unambiguous to a first-time contributor.
**Metric:** none yet — this is the cost of being takeable seriously later.

### M1 — Durable spine (weeks 2–3)
**Scope:** SQLite store + schema + forward-only migrations; the single store-writer goroutine with group commit; publish; subject matching; fetch (claim); ack; the `journal` table written in the same transaction; `messq serve`, `messq pub`, `messq sub --print`; unix socket transport.
**Exit:** the crash-torture harness (§8.3) runs 30 minutes at 1k msg/s with `SIGKILL` every ~2s and loses zero acknowledged publishes; invariants I1, I3, I8 hold.
**Metric:** internal — we do not talk publicly yet.

### M2 — The full lifecycle (weeks 4–5)
**Scope:** `nak` (+delay), `progress`, `term`; ack-wait timer wheel; `backoff` schedules; `max_deliver`; `max_ack_pending`; DLQ as a real stream (`dlq.<stream>`); stale-token detection; stream limits and `discard` policy; crash recovery with deadline preservation and `restart_requeue`.
**Exit:** the full §4.3 transition table is implemented and covered by `rapid` state-machine tests (I1–I8) at 100k checks.
**Metric:** internal.

### M3 — The differentiator (weeks 6–7)
**Scope:** `messq trace` (human + `--json`); `messq tail` over SSE; the journal query API; the custom `slog` human handler; the versioned log-field schema; `messq peek`, `messq lag`.
**Exit:** the `messq trace` golden-file output in §6.1 is byte-stable in CI, and `docs/reference/log-schema.md` v1 is frozen.
**Metric:** internal — but this is the milestone that makes the launch possible. Nothing before M3 is worth showing anyone.

### M4 — Ops surface (weeks 8–9)
**Scope:** complete cobra CLI (`stream`/`consumer`/`dlq`/`replay`/`seek`/`purge`/`doctor`/`bench`/`completion`); admin API; Prometheus metrics incl. the scrape-time backlog collector; `/healthz`, `/readyz`, `/v1/info`; token auth for TCP; systemd unit + `.deb`/`.rpm`/`apk` + `Dockerfile` (distroless, ~15MB); Grafana dashboard + alert rules.
**Exit:** a fresh Linux VM goes from nothing to a monitored, alerting, systemd-managed messq in under 5 minutes following only `docs/install/`.
**Metric:** install-to-running success rate = 100% across Debian 12/13, Ubuntu 24.04, RHEL 9, Alpine 3.20.

### M5 — The 60-second aha, then launch (weeks 10–12) — **v0.9 public**
**Scope:** `messq quickstart`; `install.sh`; `messq.dev` (landing + Hugo docs); README rewrite to the §6.5 structure; the incident-shaped tutorial; five comparison pages; `graduating-to-jetstream.md`; asciinema recording; Go client v0.9; `messq consume --exec`; docs-in-CI.
**Exit & launch:** Show HN / r/golang / r/selfhosted / Lobsters, one launch post titled around the differentiator ("*I got tired of not knowing what happened to a message*"), with the `messq trace` output as the first thing in the post.
**Metrics (the ones we actually watch):** time-to-first-`trace` from a cold `curl | sh` < 60s on a 2-vCPU VM; ≥ 40% of `install.sh` runs reach `quickstart` completion; ≥ 5 docs pages viewed per first session; ≥ 15 unsolicited issues/discussions in the first month.

### M6 — Trust artefacts (weeks 13–18) — **v1.0**
**Scope:** `messq-conformance` published; fault-injection report published; benchmarks page with methodology; a third-party durability review (paid if necessary); Go client v1.0 + generated Python/TS clients + OpenAPI 3.1 frozen; API and log-schema stability guarantee; a documented upgrade/downgrade path; `messq migrate-from` importers for a Postgres jobs table and beanstalkd.
**Exit:** v1.0 tagged with a written compatibility promise (HTTP API, CLI `--json` shapes, log schema, on-disk format all semver'd) and a public support matrix.
**Metrics:** ≥ 3 named production users willing to be quoted; ≥ 2 external contributors with merged non-trivial PRs; zero data-loss reports.

### M7 — Earned features (months 5–9) — **v1.x**
Built strictly in the order that real, filed user requests demand — the roadmap issue is public and votes are visible:
priority lanes per subject · scheduled/delayed publish (`Messq-Deliver-At`) · per-consumer rate limiting · consumer groups with worker leases and per-worker attribution in the trace · payload compression (zstd above a size threshold) · retention policies incl. per-subject compaction (`last value per subject`) · signed audit-trail export (NDJSON + JSONL manifest with a hash chain, for the compliance ICP) · OTLP export of journal events as spans · SSE push delivery · mTLS.
**Exit:** each feature ships with docs, metrics, journal events and conformance tests, or it does not ship.
**Metrics:** retention — ≥ 60% of instances that were live at 30 days are live at 90.

### M8 — The ceiling (months 10+) — **v2 candidates, only if demanded**
- **Warm standby.** Async journal shipping to a follower, manual promotion, documented RPO. **Explicitly not consensus, not automatic failover.** If a user needs automatic failover, the honest answer stays "use JetStream", and we keep the migration guide current.
- **`messq console`** — a small read-only web UI over the journal (search by ID, trace view, backlog charts). This is the natural first *commercial* artefact (see §11.6), sold as a separate product; the core stays Apache-2.0 and complete without it.

**Anti-roadmap, published:** clustering with consensus; exactly-once semantics; a stream-processing DSL; a connector ecosystem; a plugin system; a hosted SaaS.

---

## 10. Risks & open questions

| # | Risk | Severity | Mitigation / decision |
|---|---|---|---|
| R1 | **"Yet another queue" fatigue.** The category is crowded and skepticism is the default reaction. | High | Never lead with "a queue". Lead with `messq trace`. The launch post is about a debugging problem, not a broker. The anti-pitch section pre-empts the top comment. |
| R2 | **Name collision.** `abhishekkr/messQ` (a dormant socket toy) and `mess-query/messq` already exist on GitHub. | Medium | Own the identity, not the word: register `messq.dev`, use the vanity module path `go.messq.dev/messq` from commit #1, prefer a GitHub org we control (`messq-dev` if `messq` is taken), and file a word-mark for "messq" for software once there is traction. Always write it lowercase; always pair it with the tagline in metadata. Decision: **keep the name** — it is short, pronounceable, describes the domain, and the collisions are inactive. |
| R3 | **SQLite write ceiling.** A single writer with `synchronous=FULL` will cap publish throughput on slow disks. | Medium | Group commit; publish the honest curve; document the ceiling in the anti-ICP; keep `Store` an interface so a segment-file engine can replace SQLite without touching the broker. If two ICP users hit the ceiling in production, that engine becomes M8 work — not before. |
| R4 | **Journal growth.** The differentiator is also unbounded write amplification: ~3–6 journal rows per message. | High | Journal retention is a first-class, separately configurable setting (`--journal-retention=30d`, per-stream override), with an option to keep *terminal* events (`dead`, `termed`, `purged`) longer than routine ones. `messq doctor` warns when the journal exceeds 40% of DB size. Compact `detail` blobs. **Open question:** should acked messages' full journal collapse to a single summary row after N days? Proposed default: yes, after 7 days, keeping `published`/`acked` and the attempt count. Decide with real data at M6. |
| R5 | **"No replication = toy" objection.** The most likely killer objection from a technical evaluator. | High | Answer it first and in public: the anti-ICP, the fault-injection report, the durability-mode table, and the graduation guide. Reframe: single-node with a truthful RPO beats a misconfigured three-node cluster the team cannot operate — and say it with the JetStream war-story evidence, not as an assertion. |
| R6 | **Bus factor / abandonment fear.** Evaluators have been burned (beanstalkd, NSQ). | High | Small, bounded, *finishable* scope is itself the mitigation, and we say so: "this project is designed to be done." Plus the 6k-line budget, an ADR trail, a conformance suite (so a fork can be verified), a public release cadence, and — by M6 — at least one non-author maintainer with commit rights. |
| R7 | **`--exec` is a footgun.** A shell-executing consumer on a broker is a lateral-movement vector. | Medium | `--exec` is CLI-only and never remotely triggerable; the daemon has no exec capability and no plugin hooks. Documented in `SECURITY.md`. Payload arrives on stdin, never in `argv`. |
| R8 | **Long-poll latency and connection count.** One parked HTTP connection per waiting worker. | Low | Go handles tens of thousands of parked connections; `fetch` with `wait` amortises. SSE delivery in M7 if p99 latency complaints appear. Measure before optimising. |
| R9 | **Competitive response.** JetStream ships a one-line DLQ; Redis Streams ships better inspection. | Medium | Our moat is not the DLQ (table stakes) — it is the *journal + CLI + single-binary* combination, which a distributed system cannot cheaply replicate because it requires a queryable local database on the hot path. Keep widening the trace, not the feature list. |
| R10 | **Licensing pressure later.** If it succeeds, there will be a temptation to relicense. | Medium | Pre-commit publicly in `GOVERNANCE.md`, at v0.9, before any incentive exists. The 2023–2026 relicensing wave (HashiCorp, Redis) showed communities route around license flips and well-governed forks (OpenTofu, Valkey) win. A pre-commitment is worth more than a promise made under pressure. |
| R11 | **Ordering semantics confusion.** Users will assume per-subject ordering is on. | Medium | Default `none`, loud in `consumer add` output, documented with its head-of-line-blocking cost, surfaced by `messq consumer info`, and flagged by `doctor` when a consumer looks order-sensitive (`max_ack_pending=1` + `ordered_by=none`). |
| R12 | **Clock skew / backwards jumps.** ULID monotonic generation must decide what to do when the clock moves backwards; ack deadlines must not be affected. | Low | All deadline arithmetic uses a monotonic clock; only journal timestamps use wall time. The ULID factory, per the spec's guidance, holds the last timestamp rather than failing, and logs `WARN clock_backwards`. |

### Open questions, with a proposed default and a decision date

1. **Should the DLQ be per-consumer or per-stream?** *Proposed: per-stream destination (`dlq.<stream>`), with the originating consumer recorded in headers and journal.* Simpler mental model, one place to look. Decide at M2.
2. **Should `fetch` support a "peek-and-hold" batch that acks atomically?** *Proposed: no in v1* — `max_ack_pending` + per-token acks cover it, and JetStream's `MaxAckPending=1` batching problem is a known wart we should not import. Revisit if the integration ICP asks.
3. **Do we support wildcard consumers across streams?** *Proposed: no.* A consumer belongs to exactly one stream. Cross-stream fan-in is a publisher concern. Prevents subject-explosion pathologies.
4. **Payload size cap?** *Proposed: 8 MB default, configurable, `413` above it, with a documented "store the blob elsewhere, queue the pointer" pattern.* Decide at M4.
5. **Config file, or flags + env only?** *Proposed: flags + `MESSQ_*` env only through M5*, adding an optional TOML file at M6 once the flag set stabilises. Avoids a config surface calcifying before the product does.
6. **Do we ever ship a hosted service?** *Proposed: no.* It would distort every technical decision toward multi-tenancy. Commercial path, if any, is `messq console` + support contracts.

---

## 11. Library choices, justified against the docs fetched

Dependency budget: **≤ 8 direct non-test dependencies in the daemon.** Every addition needs an ADR. This is a product constraint (audit surface, supply-chain risk, "understandable in an evening"), not asceticism.

### 11.1 `modernc.org/sqlite` — storage engine (**chosen**)
The docs confirm exactly what the single-binary promise needs: a **pure-Go, CGo-free** driver, Go 1.18+, tracking SQLite 3.53.x, usable through `database/sql`. DSN configuration is first-class, which is how we express the durability modes without a C build:
```go
db, _ := sql.Open("sqlite",
  "file:///var/lib/messq/messq.db?_journal=WAL&_synchronous=FULL"+
  "&_txlock=immediate&_timeout=5000&_foreign_keys=1"+
  "&_pragma=wal_autocheckpoint(0)&_time_format=sqlite&_timezone=UTC")
```
`_txlock=immediate` is important for our single-writer design: it takes the write lock at `BEGIN` and avoids the deferred-transaction upgrade deadlock class entirely. `_timeout` maps to `busy_timeout`. The driver also exposes `sqlite.Limit(conn, sqlite3.SQLITE_LIMIT_…)` — we use it to cap `SQLITE_LIMIT_LENGTH` as a defence against oversized payload blobs. Rejected `mattn/go-sqlite3` (cgo kills cross-compilation and the "one binary" funnel) and `glebarez/sqlite`/GORM (an ORM buys nothing when there are twelve hand-written queries on the hot path).

### 11.2 `etcd-io/bbolt` — **evaluated, rejected**
Its own documentation states the disqualifying constraints for our use: everything happens inside transactions, returned data is valid only for the transaction's lifetime, `Tx`/`Bucket` are not goroutine-safe, and bulk-loading >100k keys into a new bucket in one transaction is discouraged because pages split at commit; iteration is `Cursor.Seek` range scans over a single sorted keyspace. We would hand-build every index behind `messq trace`, `messq lag` and journal filtering. bbolt is excellent for a single sorted keyspace; the journal is a relational query workload.

### 11.3 `spf13/cobra` v1.9.x — CLI (**chosen**)
The docs give us exactly the three things the CLI-first strategy needs: (a) a subcommand tree with `AddCommand` and POSIX flags including `PersistentFlags()` for root-level `--socket/--json/--log-format`; (b) shell completion generation for bash/zsh/fish/PowerShell (`GenBashCompletion`, `GenZshCompletion`, `GenFishCompletion`, `GenPowerShellCompletionWithDesc`) behind `messq completion <shell>` — completion is a real adoption lever for an ops tool; (c) `SetHelpTemplate`/`SetHelpFunc` so `messq --help` looks like a product rather than a generated dump, plus cobra's docs generation to build the CLI reference pages from the same source. `charmbracelet/fang` (cobra + fancy output + manpages) was tempting and is deferred — it is a whole extra dependency for polish we can hand-roll in the help template.

### 11.4 `log/slog` (stdlib) — logging (**chosen, zero dependencies**)
Everything the logging design needs is in the standard library: `slog.New(slog.NewJSONHandler(w, opts))` for machine output, the `Handler` interface for our custom human formatter, `slog.Group` for nesting, and `HandlerOptions.ReplaceAttr` — which, per the docs, is called for every non-group attribute after value resolution, receives the built-ins `time`/`level`/`msg`/`source`, and **discards an attribute when it returns a zero `Attr`**. That is precisely the hook for enforcing the stable field-name contract and for redacting payloads. Rejected zerolog/zap: marginal performance on a path that is already fsync-bound, and adopting a third-party logger would make our log schema — which is a documented product contract — depend on someone else's formatting choices.

### 11.5 `prometheus/client_golang` — metrics (**chosen**)
The docs confirm the three patterns the design relies on: `prometheus.NewRegistry()` + `MustRegister` (we never touch the default registry) exposed via `promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg, MaxRequestsInFlight: 20})`; **native histograms** via `HistogramOpts{NativeHistogramBucketFactor: 1.1}` for ack-latency and commit-latency (high resolution without cardinality explosion); and the **custom `Collector`** pattern (`Describe`/`Collect` emitting `prometheus.NewConstMetric` against a `prometheus.NewDesc`) — this is what lets backlog/lag/DLQ-depth gauges be computed from SQLite at scrape time instead of being mirrored in memory where they could drift.

### 11.6 `oklog/ulid` (ULID spec) — message IDs (**chosen**)
Per the spec: 128 bits = 48-bit millisecond timestamp + 80 bits of randomness, encoded as **26 characters of Crockford base32** — case-insensitive, URL-safe, no ambiguous characters. Three properties we need: (a) **lexicographic = chronological**, so IDs are good primary keys and `journal_msg` scans stay local; (b) the 16-byte binary form is stored as a `BLOB` primary key; (c) the **monotonic factory** (increment randomness within the same millisecond, error on 80-bit overflow, defined behaviour on backwards clock) keeps ordering stable under burst publishing. The human factor decided it over UUIDv4: an operator copy-pastes `01JB8Q0T7K5X2M9R4V6C3H1F2D` out of a log and into `messq trace`, and its prefix sorts by time so IDs from the same incident cluster visually. That is a product property, not a database one.

### 11.7 `pgregory.net/rapid` — property testing (**chosen, test-only**)
The docs show the exact API our §8.2 invariant work needs: `rapid.Check(t, func(t *rapid.T){…})` with automatic minimisation of failing cases, `t.Repeat(map[string]func(*rapid.T){…})` where the `""` key runs as an invariant after every action (our I1–I8), `rapid.StateMachineActions` for struct-based machines, `t.Skip` for guard conditions, and flags (`-rapid.checks`, `-rapid.seed`, `-rapid.shrinktime`, `-rapid.failfile`) that make a nightly deep run and a committed regression file trivial. Shrinking is the decisive feature: a minimised counterexample to "an acked message was redelivered" is exactly the artefact that turns a queue bug into a one-line fix.

### 11.8 Go standard library `net/http` — server & routing (**chosen**)
Go 1.22+ `ServeMux` supports method-and-wildcard patterns (`"POST /v1/pub/{subject...}"`) parsed into a host→method→segment routing tree, which covers the entire API in §5.2 without chi/gin/echo. `http.Server.Serve(net.Listener)` gives us the unix-socket listener, and `Shutdown(ctx)` gives graceful drain of parked long-polls on SIGTERM. Zero routing dependency in a security-sensitive binary is worth more than middleware convenience.

### 11.9 Rejected outright
`grpc-go` (protoc toolchain, no `curl`, no proxy story — a direct hit on the 60-second aha); `spf13/viper` (config-source sprawl before the flag set has stabilised — see open question 5); any ORM; any plugin/scripting runtime; `nats.go` (we borrow the *vocabulary*, never the dependency).

### 11.10 Licensing, governance and the commercial question

- **License: Apache-2.0.** Not MIT: the explicit patent grant and patent-termination clause are why CNCF projects and enterprise legal teams default to it, and our ICP's employer runs an OSS review. Not AGPL/BSL/SSPL: any source-available restriction breaks "download the binary and put it on the box", which *is* the adoption path.
- **No CLA. DCO sign-off only.** A CLA on an infrastructure project from an unknown author reads as "preparing to relicense" and suppresses exactly the early contributors we need.
- **A written no-relicensing commitment in `GOVERNANCE.md`, published at v0.9** — before there is any incentive to break it. The 2023–2026 relicensing wave taught the market to check for this; the projects that survived a flip were the ones with community governance, and the forks (OpenTofu, Valkey) proved the community routes around a rug-pull.
- **Trademark:** the name and logo are held separately from the code, with a permissive usage policy (forks may fork; they may not call themselves messq). This is the standard way to keep a permissive license and a coherent identity.
- **Commercial path, if ever:** `messq console` (the web UI, sold separately) and support/retainer contracts. **Never a license flip, never a feature removed from core to create an upsell.** Anything in the anti-roadmap is fair game for a commercial product; nothing in the guarantees list ever is.

---

## Appendix A — the one-paragraph pitch (for the README, HN, and every conversation)

> **messq** is a single-binary queue daemon for Linux. Publish to a subject, consume with explicit ack/nak, get ack timeouts, backoff, delivery caps and a dead-letter stream — the semantics you already know from NATS JetStream, in one process with one file and no cluster. What makes it different: messq durably records every state transition of every message, so when something goes wrong you run `messq trace <message-id>` and read the whole story — published, delivered, timed out, retried, dead-lettered — with timestamps and reasons. It is small enough to understand in an evening and honest about what it is not: single node, no consensus, not a Kafka replacement. If you outgrow it, we wrote the guide for moving to JetStream.

## Appendix B — the metrics that define success

| Funnel stage | Metric | v1.0 target |
|---|---|---|
| Discovery | GitHub stars in first 90 days | 1,500 (a proxy, tracked but never optimised for) |
| Activation | `install.sh` → `quickstart` completion | ≥ 40% |
| Activation | Time to first `messq trace` on a cold machine | < 60s |
| Comprehension | Docs pages per first session | ≥ 5 |
| Adoption | Instances still reporting a heartbeat at day 30 | ≥ 25% of activations |
| Retention | Day-30 instances still live at day 90 | ≥ 60% |
| Trust | Reported data-loss incidents | 0 |
| Trust | Named production references willing to be quoted | ≥ 3 |
| Community | External contributors with merged non-trivial PRs | ≥ 2 |
| Health | Median time-to-first-response on issues | < 48h |

*(Heartbeat telemetry is opt-in, off by default, one line in the docs explaining exactly what it sends. An infra tool that phones home by default forfeits the trust this entire plan is built on.)*

---

## Sources consulted

- [Reliable Message Delivery in NATS JetStream: Acks, Retries, Dead Letters, and Replay — Synadia](https://www.synadia.com/blog/jetstream-reliable-delivery-dlq-replay)
- [Consumers — NATS Docs](https://docs.nats.io/nats-concepts/jetstream/consumers)
- [Pull consumers in depth — NATS Docs](https://docs.nats.io/learn/jetstream/pull-consumers)
- [I Ran NATS JetStream as Our Only Queue for 90 Days — Stackademic](https://blog.stackademic.com/i-ran-nats-jetstream-as-our-only-queue-for-90-days-week-six-almost-sent-everything-back-to-a3b450032c88)
- [NSQ Features & Guarantees](https://nsq.io/overview/features_and_guarantees.html) · [NSQ Design](https://nsq.io/overview/design.html)
- [beanstalkd protocol](https://github.com/beanstalkd/beanstalkd/blob/master/doc/protocol.txt) · [beanstalkd FAQ](https://github.com/beanstalkd/beanstalkd/wiki/faq)
- [Streams Consumer Group Patterns — antirez](https://redis.antirez.com/fundamental/streams-consumer-patterns.html) · [How to Implement Redis Streams Consumer Groups](https://oneuptime.com/blog/post/2026-01-30-redis-streams-consumer-groups/view)
- [River — background jobs for Go and Postgres](https://riverqueue.com/) · [SQLite for River](https://riverqueue.com/blog/sqlite-and-pro-dbsql-durable-periodic-jobs-performance-boosts)
- [SQLite's Durability Settings are a Mess — agwa](https://www.agwa.name/blog/post/sqlite_durability) · [SQLite commits are not durable under default settings](https://avi.im/blag/2025/sqlite-fsync/) · [synchronous=NORMAL vs FULL trade-off matrix](https://www.productionhardening.org/wal-optimization-concurrency-tuning/synchronous-normal-vs-full-trade-off-matrix/)
- [Kafka is fast — I'll use Postgres](https://topicpartition.io/blog/postgres-pubsub-queue-benchmarks) · ["You Don't Need Kafka, Just Use Postgres" Considered Harmful — Gunnar Morling](https://www.morling.dev/blog/you-dont-need-kafka-just-use-postgres-considered-harmful/)
- [W3C Trace Context Explained — Dash0](https://www.dash0.com/knowledge/w3c-trace-context-traceparent-tracestate) · [Context propagation — OpenTelemetry](https://opentelemetry.io/docs/concepts/context-propagation/)
- [OSS Licensing: MIT vs Apache vs AGPL — OSSAlt](https://ossalt.com/guides/oss-licensing-guide-mit-apache-agpl-2026) · [Open Source Licensing for Startups — Promise Legal](https://promise.legal/startup-legal-guide/ip/open-source)
- [Open source to PLG — Product Marketing Alliance](https://www.productmarketingalliance.com/developer-marketing/open-source-to-plg/) · [Open Source Marketing Strategy — Infrasity](https://www.infrasity.com/blog/open-source-marketing-strategy)
- Library docs via context7: `/gitlab_cznic/sqlite`, `/etcd-io/bbolt`, `/spf13/cobra`, `/golang/go` (`log/slog`, `net/http`), `/prometheus/client_golang`, `/ulid/spec`, `/flyingmutant/rapid`, `/nats-io/nats.docs`
