# messq — Project Plan (Plan 01: The Pragmatic Shipper)

> **Thesis:** messq is one Go binary, one SQLite file, one HTTP/JSON API, and one durable
> `(consumer, message)` state machine. Everything else is deferred until it earns its
> complexity. A team should be able to `apt install`, write a systemd unit, and put real
> internal work through it in an afternoon — and still be able to read the whole codebase
> in an evening.

**Target:** v0.1 usable in ~3.5 focused weeks, v1.0 "run it in production" in ~5–6 weeks,
from an empty repository. Every milestone below ships something a person can use.

---

## 1. Vision & positioning

### 1.1 The gap we are filling

Teams reach for Kafka because they want redelivery and durability, and then pay for
ZooKeeper/KRaft, partitions, consumer-group rebalances, and a full-time operator.
Teams reach for Redis Streams and discover the Pending Entries List is a footgun: a
consumer that only reads `>` leaves zombie entries in the PEL forever unless it
also sweeps history with `XAUTOCLAIM`, and every application re-implements that
orchestration slightly wrong. Teams reach for beanstalkd and get delightful
simplicity but a text protocol, a memory-first store, and no replay. Teams reach for
RabbitMQ and inherit an Erlang cluster and a mnesia recovery story.

NATS JetStream got the *semantics* right — `ack`, `nak` (with delay), `ack_wait`,
`max_deliver`, `backoff` arrays, `max_ack_pending` — and those primitives are the ones
worth copying. What JetStream does *not* give a five-person team is a
single 20 MB binary whose entire durable state is one file they can `cp` to a backup.

**messq is that binary.** Kafka-minimum semantics, beanstalkd-grade operational
simplicity, and an audit trail good enough that "what happened to message X?" is a
one-command answer.

### 1.2 What messq is

- A **single-node**, single-binary queue daemon for Linux.
- **At-least-once**, always. Never claims more.
- **Explicit ack** with server-enforced ack timeout, bounded redelivery, and a
  dead-letter stream.
- **Replayable**: the stream is an append-only log; consumers are cursors over it.
- **Observable by construction**: every state transition is written to a durable
  events table *inside the same transaction as the state change*, and emitted as a
  structured log line. The log cannot disagree with reality.
- **CLI-first**: the CLI is the primary UI. `messq sub jobs worker --exec ./handler.sh`
  turns any shell script into a reliable worker.

### 1.3 What messq is explicitly NOT (v1)

No clustering. No replication. No consensus. No partitions. No exactly-once. No
priorities. No compression. No push/webhook delivery. No TLS termination (put it behind
nginx/caddy or use the Unix socket). No plugin system. No web UI. No stream
transformations, no filters-with-code, no schema registry.

If your throughput requirement starts with a "1" and ends in "00,000 msg/s", or you need
multi-datacenter durability, **use Kafka or JetStream**. This document says so on the
front page of the README too. A tool that tells you when not to use it earns trust.

### 1.4 Success criteria for v1.0

| Criterion | Target |
|---|---|
| Time from `curl -O` to first acked message | < 3 minutes, no config file |
| Runtime dependencies | 4 Go modules, 0 C libraries, `CGO_ENABLED=0` static binary |
| Durable throughput, 1 KiB payloads, NVMe, `synchronous=FULL` | ≥ 3 000 msg/s publish+ack round trip |
| Non-durable (`--durability=relaxed`) | ≥ 25 000 msg/s |
| p99 fetch latency with a waiting long-poll consumer | < 5 ms after publish |
| Crash test: SIGKILL at any point | 0 acked-then-redelivered-as-lost, 0 lost publishes that returned 200 |
| Codebase | < 8 000 lines of non-test Go |
| "What happened to message X" | `messq trace <id>` — one command, full timeline |

---

## 2. Architecture overview

### 2.1 Processes

Exactly one: `messq serve`. One binary, `messq`, contains both the daemon and the CLI.
The CLI talks to the daemon over HTTP (TCP or Unix socket). There is no embedded/library
mode in v1, no sidecar, no separate admin process.

```
                       ┌──────────────────────────── messq serve ────────────────────────────┐
 publishers            │                                                                     │
  curl / SDK ──HTTP──▶ │  net/http server (TCP :4390  +  unix:///run/messq/messq.sock)        │
                       │        │                                                            │
 consumers             │        │  handlers validate, build a Cmd, send it on a channel       │
  messq sub  ──HTTP──▶ │        ▼                                                            │
  worker app           │  ┌────────────────┐   cmdCh (buffered)                              │
                       │  │  writer        │◀──────────────────────────┐                      │
                       │  │  goroutine     │                           │                      │
                       │  │  (group commit)│    ┌─────────────────┐    │                      │
                       │  └────────┬───────┘    │ sweeper         │────┘                      │
                       │           │            │ tick 250ms      │                           │
                       │           ▼            └─────────────────┘                           │
                       │   ┌───────────────┐    ┌─────────────────┐                           │
                       │   │ SQLite (WAL)  │    │ retention       │───────────────────────────┤
                       │   │ messq.db      │    │ tick 60s        │                           │
                       │   └───────┬───────┘    └─────────────────┘                           │
                       │           │                                                          │
                       │           │  read-only pool (WAL: readers never block the writer)     │
                       │           ▼                                                          │
                       │   list / peek / trace / stats handlers                                │
                       │                                                                      │
                       │   waiters registry ──▶ wakes long-poll fetches on publish/redelivery  │
                       │   obs: slog handler (JSON | console) + prometheus registry            │
                       └──────────────────────────────────────────────────────────────────────┘
```

### 2.2 Goroutines (the complete list)

| Goroutine | Count | Job |
|---|---|---|
| `http.Server` accept + handler | 1 + N | Parse, authorize, marshal. Never touches SQLite directly for writes. |
| `writer` | **exactly 1** | Owns the single read-write SQLite connection. Drains `cmdCh`, groups commands into one transaction per commit window, commits, then publishes results + events. |
| `sweeper` | 1 | Every 250 ms: expire ack timeouts → ready or dead; wake waiters. Runs as commands through the writer, not as an independent writer. |
| `retention` | 1 | Every 60 s: enforce `max_age`/`max_msgs`/`max_bytes`, workqueue reaping, audit trim, WAL checkpoint if the WAL exceeds `--wal-max-bytes`. |
| `waiters` | 0 (data structure) | `map[consumerKey][]chan struct{}` guarded by a mutex; the long-poll handler goroutine parks on its own channel. |
| `signal` | 1 | SIGTERM/SIGINT → graceful drain. SIGHUP → reload auth tokens + log level. |

That is it. No worker pools, no per-message timers, no priority heaps in memory. NSQ runs
two priority queues plus two goroutines *per channel* to track in-flight and deferred
messages; we replace all of it with an indexed `visible_at` column and one ticker. The
tradeoff is 250 ms of timeout granularity, which nobody's ack-wait budget cares about.

### 2.3 The single-writer decision

SQLite in WAL mode allows one writer and many concurrent readers. Rather than fight
`SQLITE_BUSY` with retries and `busy_timeout` guesswork across dozens of handler
goroutines, we serialize writes in the application, which is the pattern the SQLite
community converged on for write-heavy services. Benefits:

1. **Group commit for free.** The writer drains everything queued within
   `--commit-window` (default 2 ms) into one transaction. 200 concurrent publishes become
   one `fsync`. This is what makes `synchronous=FULL` affordable.
2. **No lock contention, no retry loops, no busy timeout tuning.** We still set
   `_timeout=5000` and `_txlock=immediate` as a belt-and-braces measure for the read pool
   and for any future path that opens its own transaction.
3. **Atomic multi-table transitions.** Ack = delete a delivery row + insert an event row.
   Dead-letter = insert into the DLQ stream + delete the delivery row + insert an event
   row. All in one transaction, so a crash can never leave the audit trail lying.
4. **Trivially reasoned-about ordering.** Sequence assignment, cursor advancement and
   claim are all in the writer, so there is exactly one place where "who gets this
   message" is decided.

### 2.4 Data flow: publish

1. `POST /v1/streams/jobs/messages?subject=jobs.email` with a raw body.
2. Handler authorizes, checks size against the stream's `max_msg_size`, extracts or mints
   `msg_id` (ULID) and `trace_id`, builds `cmdPublish`, sends it on `cmdCh`, waits on a
   reply channel.
3. Writer batches it with whatever else arrived within the commit window. In one tx:
   allocate `seq` from `stream_seq`, insert into `messages` (dedup via a partial unique
   index), insert an `msg.publish` event row.
4. Commit (one `fsync` for the whole batch).
5. Writer replies to each waiting handler with `{seq, id, duplicate}`, emits the slog
   lines, bumps counters, and notifies the waiter registry for every consumer of that
   stream whose filter matches.
6. Handler returns `201 Created` with `Messq-Seq`, `Messq-Msg-Id`, `Messq-Trace-Id`.

**The response is only sent after the commit.** A 201 means it is on disk (with
`--durability=full`). A 5xx or a dropped connection means "unknown, retry with the same
`Messq-Msg-Id`" — that is what publish dedup exists for.

### 2.5 Data flow: fetch → ack

1. `POST /v1/streams/jobs/consumers/worker/fetch {"batch":16,"wait_ms":30000}`.
2. Handler builds `cmdFetch`, writer executes it in the batch transaction:
   a. **Top-up:** if ready rows for this consumer < `batch`, scan up to `--scan-limit`
      (4096) rows of `messages` with `seq >= consumer.cursor_seq` (a pure primary-key
      range scan), filter them in Go with the compiled subject matcher, insert
      `deliveries` rows for matches, advance `cursor_seq` past everything scanned —
      **including non-matches**, so a narrow filter on a busy stream is amortized O(1)
      per message, not O(stream) per fetch. Stop early if
      `pending >= max_ack_pending` and emit `flow.blocked`.
   b. **Claim:** `UPDATE deliveries SET state=1, attempts=attempts+1,
      visible_at=now+ack_wait, lease=:nonce, delivered_at=now WHERE ... ORDER BY seq
      LIMIT :batch RETURNING seq`.
   c. Insert one `msg.deliver` event per claimed message.
3. If zero claimed and `wait_ms > 0`: register a waiter, park, retry once on wake or
   timeout. Return `200` with an empty `messages` array on timeout (not 204 — clients
   parse one shape).
4. Response carries per-message `ack_token`, `attempt`, `max_deliver`, `trace_id`,
   `body_b64`, plus `Messq-Pending` / `Messq-Backlog` headers so the worker can
   self-throttle.
5. Worker does its job, `POST /v1/ack {"tokens":["..."]}`. Writer validates the token's
   lease against the row, deletes the row, writes `msg.ack`. Batch acks are one tx.

---

## 3. Storage & durability design

### 3.1 Engine choice: SQLite via `modernc.org/sqlite`

**Decision: one SQLite database file per messq node, accessed through the pure-Go
`modernc.org/sqlite` driver.**

Rejected alternatives and why:

- **Custom segmented log files (Kafka-style).** This is the "fun" answer and the wrong
  one. We would be writing our own index, our own crash recovery, our own compaction, our
  own torn-write detection, and our own tests for all of it. That is 3 extra weeks and a
  permanent tail of corruption bugs. SQLite's recovery code has more production hours than
  anything we will ever write.
- **bbolt.** Genuinely appealing: ACID, MVCC, one file, pure Go. But bbolt's own README
  says random writes can be slow and recommends `DB.Batch()` or a write-ahead log in
  front — and we would then be hand-rolling secondary indexes (ready-by-visible_at,
  by-subject, by-msg-id, by-trace-id) as extra buckets that we must keep consistent
  ourselves. SQLite gives us those indexes and ad-hoc queries (`messq trace`, `messq
  pending --older-than 5m`) for free. Ad-hoc query capability is a *product feature* here,
  not a convenience.
- **`mattn/go-sqlite3`.** Faster, but requires cgo, which kills the static
  cross-compiled single binary. **Escape hatch:** the store speaks plain `database/sql`
  with vanilla SQL, and a `//go:build cgosqlite` file swaps the driver import. If we ever
  hit a modernc bug or a throughput wall, that is a one-line build-tag change, and the
  A/B is a CI matrix entry from M1 onward.

DSN (confirmed against the driver's documented query parameters):

```
file:/var/lib/messq/messq.db?_journal=WAL&_synchronous=FULL&_timeout=5000&_txlock=immediate&_foreign_keys=1
```

Plus, on open:

```sql
PRAGMA wal_autocheckpoint = 4000;   -- fewer, larger checkpoints; we also checkpoint manually
PRAGMA cache_size = -65536;         -- 64 MiB page cache
PRAGMA mmap_size = 268435456;       -- 256 MiB
PRAGMA temp_store = MEMORY;
PRAGMA optimize;                    -- on close
```

Two connections/pools:
- `rw`: `MaxOpenConns(1)`, owned exclusively by the writer goroutine.
- `ro`: `MaxOpenConns(runtime.NumCPU())`, `PRAGMA query_only=ON`, for list/peek/trace/stats.

### 3.2 Schema

All tables are `STRICT` (typed columns, real errors on mismatch). Timestamps are Unix
**milliseconds** as `INTEGER`. Migrations are numbered, embedded via `go:embed`, applied
in a transaction, tracked in `schema_version`; **forward-only, no down migrations** —
`messq backup` before upgrading is the documented rollback.

```sql
CREATE TABLE streams (
  name             TEXT PRIMARY KEY,
  subjects         TEXT    NOT NULL,          -- JSON array of accepted publish patterns
  retention        TEXT    NOT NULL DEFAULT 'limits',   -- 'limits' | 'workqueue'
  max_msgs         INTEGER NOT NULL DEFAULT 0,          -- 0 = unlimited
  max_bytes        INTEGER NOT NULL DEFAULT 0,
  max_age_ms       INTEGER NOT NULL DEFAULT 0,
  max_msg_size     INTEGER NOT NULL DEFAULT 1048576,    -- 1 MiB
  dedup_window_ms  INTEGER NOT NULL DEFAULT 120000,     -- JetStream's 2 min default
  created_at       INTEGER NOT NULL
) STRICT;

CREATE TABLE stream_seq (                     -- monotonic even across purge
  stream TEXT PRIMARY KEY REFERENCES streams(name) ON DELETE CASCADE,
  next   INTEGER NOT NULL
) STRICT;

CREATE TABLE messages (
  stream       TEXT    NOT NULL,
  seq          INTEGER NOT NULL,              -- per-stream, monotonic, gapless on the happy path
  id           TEXT    NOT NULL,              -- ULID, 26 chars, stable forever
  subject      TEXT    NOT NULL,
  hdr          TEXT,                          -- JSON object, user headers, nullable
  body         BLOB    NOT NULL,
  size         INTEGER NOT NULL,
  published_at INTEGER NOT NULL,
  trace_id     TEXT    NOT NULL,
  dedup_key    TEXT,                          -- from Messq-Msg-Id
  PRIMARY KEY (stream, seq)
) STRICT;
CREATE UNIQUE INDEX messages_id     ON messages(id);
CREATE UNIQUE INDEX messages_dedup  ON messages(stream, dedup_key) WHERE dedup_key IS NOT NULL;
CREATE INDEX        messages_subj   ON messages(stream, subject, seq);
CREATE INDEX        messages_age    ON messages(stream, published_at);

CREATE TABLE consumers (
  stream          TEXT    NOT NULL,
  name            TEXT    NOT NULL,
  filters         TEXT    NOT NULL DEFAULT '[">"]',  -- JSON array; match = any
  ack_wait_ms     INTEGER NOT NULL DEFAULT 30000,
  max_deliver     INTEGER NOT NULL DEFAULT 5,        -- 0 = unlimited
  max_ack_pending INTEGER NOT NULL DEFAULT 1000,
  backoff_ms      TEXT    NOT NULL DEFAULT '[]',     -- JSON array, e.g. [1000,5000,30000]
  ordered         INTEGER NOT NULL DEFAULT 0,        -- 1 = per-subject serial delivery
  dead_policy     TEXT    NOT NULL DEFAULT 'dlq',    -- 'dlq' | 'drop'
  cursor_seq      INTEGER NOT NULL DEFAULT 1,        -- next stream seq to consider
  created_at      INTEGER NOT NULL,
  PRIMARY KEY (stream, name)
) STRICT;

CREATE TABLE deliveries (
  stream     TEXT    NOT NULL,
  consumer   TEXT    NOT NULL,
  seq        INTEGER NOT NULL,
  subject    TEXT    NOT NULL,                -- denormalized: makes ordered-mode a WHERE clause
  state      INTEGER NOT NULL,                -- 0 = READY, 1 = INFLIGHT
  attempts   INTEGER NOT NULL DEFAULT 0,
  visible_at INTEGER NOT NULL DEFAULT 0,      -- READY when now >= visible_at
  lease      TEXT,                            -- 16-byte nonce, rotated on every delivery
  delivered_at INTEGER,
  last_error TEXT,
  PRIMARY KEY (stream, consumer, seq)
) STRICT;
CREATE INDEX deliveries_ready   ON deliveries(stream, consumer, state, visible_at, seq);
CREATE INDEX deliveries_subject ON deliveries(stream, consumer, state, subject);
CREATE INDEX deliveries_seq     ON deliveries(stream, seq);   -- for workqueue reaping

CREATE TABLE events (
  id       INTEGER PRIMARY KEY,               -- rowid, monotonic
  ts       INTEGER NOT NULL,
  event    TEXT    NOT NULL,                  -- 'msg.ack', 'msg.timeout', ...
  stream   TEXT, consumer TEXT, subject TEXT,
  msg_id   TEXT, seq INTEGER, attempt INTEGER,
  trace_id TEXT,
  detail   TEXT                               -- JSON, small; error strings, delays, counts
) STRICT;
CREATE INDEX events_msg   ON events(msg_id, id);
CREATE INDEX events_trace ON events(trace_id, id);
CREATE INDEX events_ts    ON events(ts);
```

**Design notes that matter:**

- **An ack is a `DELETE`.** `deliveries` only ever holds work that is not finished, so it
  stays small and every "pending" query is fast regardless of stream size. Redis Streams'
  PEL grows into a performance problem precisely because the pending set is a parallel
  structure that nobody prunes; here the pending set *is* the row and finishing removes it.
- **Payloads are inline BLOBs, capped at 1 MiB by default.** No blob sidecar store. This
  keeps crash recovery at exactly "SQLite replayed the WAL, done" with zero bespoke
  recovery code to write or test. Big-payload users are told to put an S3/URL reference in
  the message; that is the correct answer anyway.
- **`subject` is duplicated into `deliveries`** so per-subject ordering is a `NOT IN`
  subquery instead of a join against `messages`.
- **`lease` rotates on every delivery.** A late ack from an attempt that already timed
  out is rejected (`409`) and logged as `msg.ack_stale` with both attempt numbers. This
  turns the single most confusing at-least-once failure mode ("my worker acked but the
  message was processed twice") into a visible, alertable event instead of a silent shrug.

### 3.3 fsync policy

Two modes, one flag, honest names:

| `--durability` | SQLite | Meaning |
|---|---|---|
| `full` (**default**) | `journal_mode=WAL`, `synchronous=FULL` | A `2xx` on publish/ack means it survived a power cut. |
| `relaxed` | `journal_mode=WAL`, `synchronous=NORMAL` | The last few commits may be lost on power loss or OS crash. **Never corrupts.** Survives a process crash (SIGKILL) fine. For CI, dev, and workloads that can replay from source. |

There is no third mode and no per-message durability flag. Two options an operator can
hold in their head beat six they will misconfigure.

**Group commit is what makes `full` viable.** The writer accumulates commands for up to
`--commit-window` (2 ms) or `--commit-max-batch` (512 commands), whichever comes first,
then commits once. Batching turns N fsyncs into one, which is the difference between
~150 msg/s and several thousand on the same disk. `messq_commit_batch_size` and
`messq_commit_duration_seconds` are exported so operators can see the batching working.

Explicit non-goal: we do not attempt to detect a lying disk controller. We document
`--durability=full` as "as durable as SQLite with `synchronous=FULL`", which is a
well-understood, well-documented contract, and we link to it.

### 3.4 Crash recovery

There is deliberately **no bespoke recovery code**. Startup:

1. Open the DB. SQLite replays/rolls back the WAL. Any torn commit is gone; any complete
   commit is present — including its `events` rows, because they were in the same
   transaction.
2. Run migrations if `schema_version` is behind.
3. `PRAGMA quick_check` (fast); `--fsck` flag runs the full `integrity_check`.
4. **Reclaim leases**, one statement per consumer:
   ```sql
   UPDATE deliveries
      SET state = 0, lease = NULL, visible_at = :now + abs(random() % :jitter_ms)
    WHERE state = 1;
   ```
   Every in-flight lease is void after a restart — no client can still hold a live
   connection. `attempts` is **not** reset (a delivery that was in flight already counted).
   The random jitter (`--reclaim-jitter`, default 1000 ms) exists specifically to avoid the
   thundering-herd/retry-storm pattern where a recovering broker dumps its entire in-flight
   set on a downstream that is also just coming back.
5. Emit `recovery.reclaimed count=N duration_ms=… ` and `server.start` with version,
   durability mode, DB size, WAL size, and per-stream counts.

Recovery time is bounded by SQLite's WAL replay plus one indexed UPDATE. For a 10 GiB
database with 5 000 in-flight messages this is well under a second.

**Graceful shutdown uses the same lease-release path**, so there is one code path, tested
constantly, rather than a rarely-exercised crash path.

### 3.5 Retention, purge, backup

- `retention=limits`: keep messages until `max_msgs` / `max_bytes` / `max_age_ms` is
  exceeded, then delete oldest-first. A message with an outstanding delivery is **not**
  deleted; instead the retention sweep emits `retention.blocked` (an alertable condition —
  it means a consumer is stuck and disk is about to be your problem).
- `retention=workqueue`: delete a message once every consumer has moved past it and no
  delivery rows remain. This is the job-queue default that keeps the file small.
  Implemented in the 60 s sweep, not per-ack, so acks stay cheap:
  ```sql
  DELETE FROM messages
   WHERE stream = :s
     AND seq < (SELECT min(cursor_seq) FROM consumers WHERE stream = :s)
     AND NOT EXISTS (SELECT 1 FROM deliveries d WHERE d.stream = :s AND d.seq = messages.seq);
  ```
- Audit trim: `events` older than `--audit-retention` (default 72 h) or beyond
  `--audit-max-rows` (default 20 M) are deleted oldest-first.
- Free-space: the sweep runs `PRAGMA incremental_vacuum` when `freelist_count` exceeds a
  threshold (auto_vacuum=INCREMENTAL is set at creation), and
  `PRAGMA wal_checkpoint(TRUNCATE)` when the WAL exceeds `--wal-max-bytes` (default 256 MiB).
- `messq backup /path/snap.db` runs `VACUUM INTO` on the read pool — a consistent online
  snapshot with no downtime. Restore is `cp`. This is the entire backup story and it fits
  in a sentence, which is the point.

---

## 4. Delivery semantics & message lifecycle

### 4.1 Guarantee statement (goes in the README verbatim)

> messq delivers each message to each consumer **at least once**. Duplicates are possible
> whenever a worker completes work but its ack does not reach the server: after an ack
> timeout, a nak, a network failure, or a broker restart. **Consumers must be idempotent.**
> messq helps you: `Messq-Msg-Id` deduplicates publishes within a window, every delivery
> carries an attempt number, and a stale ack is reported rather than silently accepted.
> messq does not offer exactly-once and will not pretend to.

Ordering: within a consumer, ready messages are always claimed in ascending `seq`. With
`ordered=false` (default), several messages can be in flight simultaneously, so
*processing* order is not guaranteed. With `ordered=true`, at most one message per subject
is in flight, which gives per-subject serial processing at the cost of throughput.

### 4.2 The state machine

State lives in one row: `deliveries(stream, consumer, seq)`. `state ∈ {READY, INFLIGHT}`.
Terminal states are **row deletion** (ACKED) or **migration to the DLQ stream** (DEAD).

```
                      top-up from messages
                      (seq >= cursor_seq, filter matches)
                                 │
                                 ▼
                          ┌────────────┐
              ┌──────────▶│   READY    │◀────────────┐
              │           │ visible_at │             │
              │           └──────┬─────┘             │
              │                  │ fetch: now >= visible_at,
              │                  │ pending < max_ack_pending,
              │                  │ (ordered ⇒ no INFLIGHT for this subject
              │                  │            and this is min(seq) for the subject)
              │                  │ attempts++ ; lease = new nonce
              │                  ▼
              │           ┌────────────┐
              │           │  INFLIGHT  │  visible_at = now + ack_wait
              │           └─┬───┬───┬──┘
              │   nak(delay)│   │   │ ack(token)
              │             │   │   └────────────────────▶ ● ACKED  (row DELETEd)
              │             │   │
              │             │   │ term(token)   ─────────▶ ◆ DEAD   (→ DLQ stream)
              │             │   │
              │             │   │ extend(token, +ms) ──────┐ visible_at += ms
              │             │   │                          └─▶ stays INFLIGHT
              │             │   │
              │             │   │ now > visible_at (sweeper) : TIMEOUT
              │             ▼   ▼
              │        ┌──────────────────────────────┐
              │        │ attempts >= max_deliver ?    │
              │        └───────┬──────────────┬───────┘
              │            no  │              │ yes
              └────────────────┘              ▼
               visible_at = now + backoff  ◆ DEAD
                                            dead_policy=dlq  → publish to _dlq stream
                                            dead_policy=drop → delete + event only
```

### 4.3 Transition rules, precisely

**Attempts.** `attempts` increments exactly once, at the moment of delivery (the claim
UPDATE). `max_deliver = 5` therefore means *exactly five deliveries*, then dead — the same
arithmetic as JetStream. `max_deliver = 0` means unlimited (and is documented as "you are
choosing to let a poison message retry forever; you probably want `max_deliver` and a DLQ
alert instead").

**Ack.** Requires a token whose `(stream, consumer, seq)` matches an `INFLIGHT` row **and**
whose lease equals `deliveries.lease`.
- Match → delete row, `msg.ack` event, workqueue reap eligible.
- Row is `INFLIGHT` but lease differs → `409 Conflict`, `msg.ack_stale` event carrying
  `stale_attempt` and `current_attempt`. The work was probably done twice.
- Row does not exist → `410 Gone`, `msg.ack_orphan` event. Either already acked, or the
  message was dead-lettered/purged under the worker.

**Nak.** `POST /v1/nak {"token":..., "delay_ms":n, "error":"..."}`.
- `delay_ms` present → `visible_at = now + delay_ms`.
- absent and `backoff_ms` configured → `visible_at = now + backoff[min(attempts-1, len-1)]`
  (JetStream's rule: the last entry repeats).
- absent and no backoff → immediate (`visible_at = now`), which wakes long-polling waiters.
- **Jitter:** ±`--nak-jitter` (default 20 %) is applied to any non-zero delay. Twenty
  workers naking simultaneously because a downstream is down must not resynchronize into a
  retry storm.
- `error` is stored in `deliveries.last_error` and in the event's `detail`. This is what
  makes `messq pending` and `messq dlq peek` actually diagnostic instead of just a count.
- If `attempts >= max_deliver`, a nak goes straight to DEAD rather than back to READY.

**Term.** Explicit "this will never succeed" — skips remaining attempts, goes straight to
DEAD. This is the classification the retry literature keeps asking for: transient errors
nak with backoff, permanent errors (validation, 4xx from a downstream, unparseable
payload) term immediately instead of burning five attempts and five ack-waits.

**Extend** (`touch` in beanstalkd, `WorkingOnIt` in JetStream). `visible_at += extra_ms`,
capped at `--max-ack-wait` (default 1 h). For legitimately long jobs. Emits `msg.extend`
so a consumer that extends forever is visible.

**Timeout.** The sweeper, every 250 ms, per consumer:
```sql
-- redeliverable
UPDATE deliveries SET state=0, lease=NULL, visible_at=:now + :backoff
 WHERE stream=:s AND consumer=:c AND state=1 AND visible_at<=:now
   AND (:max_deliver = 0 OR attempts < :max_deliver)
 RETURNING seq, attempts;
-- exhausted
SELECT seq, attempts, last_error FROM deliveries
 WHERE stream=:s AND consumer=:c AND state=1 AND visible_at<=:now
   AND :max_deliver > 0 AND attempts >= :max_deliver;
```
Each redelivery emits `msg.timeout` with `lease_age_ms` and `attempt`; each exhaustion
emits `msg.dead`.

**Dead-letter.** *The DLQ is a real stream, not a special state.* On DEAD with
`dead_policy=dlq`, in one transaction: publish the payload into the auto-created `_dlq`
stream under subject `dlq.<stream>.<consumer>.<original-subject>` with headers
`Messq-Origin-Stream`, `Messq-Origin-Consumer`, `Messq-Origin-Seq`, `Messq-Origin-Id`,
`Messq-Attempts`, `Messq-Last-Error`, and **the original `trace_id` preserved**; then
delete the delivery row; then write `msg.dead`.

Why a stream: redrive becomes "consume from `_dlq` and republish", which is code we
already have. Inspection becomes `messq peek`, which we already have. Alerting becomes
"backlog of `_dlq` > 0", which is a metric we already have. One mechanism, four features.
Contrast with a `state=DEAD` flag, which would need its own listing, its own purge, its
own retention and its own redrive path.

**Ordered consumers.** `ordered=1` adds two conditions to the claim: the subject must have
no `INFLIGHT` row, and the candidate must be the lowest outstanding `seq` for its subject
(so a naked message that is waiting out its backoff blocks later messages on the same
subject, which is what "ordered" must mean). Implemented with SQLite's documented bare-column
`min()` behaviour:
```sql
SELECT seq, subject FROM deliveries
 WHERE stream=:s AND consumer=:c AND state IN (0,1)
 GROUP BY subject
HAVING min(seq)=seq AND state=0 AND visible_at<=:now
 ORDER BY seq LIMIT :batch;
```
This is a slightly obscure SQLite feature, so it gets a comment, a dedicated test, and a
fallback plain-Go path behind `--no-sqlite-minhack` if it ever misbehaves.

**Flow control.** The top-up refuses to create ready rows once
`count(deliveries WHERE consumer=c) >= max_ack_pending`. This bounds memory, bounds the
pending table, and gives natural backpressure: a slow consumer simply stops accumulating.
`flow.blocked` is emitted (rate-limited to once per 10 s per consumer) so the operator sees
*why* the backlog is growing.

**Publish dedup.** `Messq-Msg-Id: <key>` → `INSERT ... ON CONFLICT(stream, dedup_key) DO
NOTHING`. On conflict the existing `seq`/`id` is returned with `"duplicate": true` and a
`msg.dup` event. Keys are cleared (set to NULL) by the retention sweep after
`dedup_window_ms`, so the unique index stays bounded. This makes publisher retries safe,
which is the other half of at-least-once that most small brokers omit.

### 4.4 Replay and seek — two different tools

| Operation | Effect |
|---|---|
| `messq consumer seek S C --seq N` | Sets `cursor_seq = N`. Optionally `--drop-pending` deletes outstanding delivery rows. Rewinds *one consumer* over the *existing* log. Emits `consumer.seek`. |
| `messq consumer seek S C --time 2026-08-20T10:00:00Z` | Binary-searches `messages_age` for the first seq at/after that time, then as above. |
| `messq consumer seek S C --start` / `--new` | Cursor to 1 / to head+1. |
| `messq replay S --from-seq A --to-seq B [--to-stream X] [--subject-filter P]` | **Re-publishes copies** as new messages (new seq, new id, `Messq-Replay-Of` header, original `trace_id` preserved). Feeds *all* consumers of the target stream. This is the "reprocess yesterday" button. |
| `messq dlq redrive S C [--limit N] [--to-subject P]` | Consumes `_dlq` and republishes to the origin stream/subject. `--rate N/s` throttles, because the reason the DLQ filled is usually that the downstream was down and is now fragile. |

---

## 5. API / protocol

**Decision: HTTP/1.1 + JSON, over TCP and a Unix socket. Pull-based fetch with long-poll.
No gRPC, no custom binary protocol, no WebSocket in v1.**

Rationale, in persona: every language has an HTTP client and nobody needs to generate
stubs; `curl` is a first-class client, which makes the docs testable and the 3 a.m.
debugging session survivable; long-poll needs no special infrastructure and gives natural
backpressure; a reverse proxy gives us TLS, auth integration and rate limiting for free.
gRPC would buy maybe 2× on the wire and cost us protoc in the build, a code-gen story, and
"can I curl it?" — a bad trade at this scale. The gRPC/streaming door stays open for v2
without breaking anything, because the pull model is the same either way.

Pull, not push, is also the correct semantic: the consumer's own fetch rate *is* the flow
control, exactly as it is for Pub/Sub streaming pull and NSQ's RDY.

### 5.1 Endpoints

```
# health / meta
GET    /healthz                                    → 200 always if the process is up
GET    /readyz                                     → 200 when DB open and migrations done
GET    /metrics                                    → prometheus text format
GET    /v1/info                                    → version, uptime, durability, db size

# streams
POST   /v1/streams                                 {name, subjects[], retention, limits...}
GET    /v1/streams
GET    /v1/streams/{stream}                        → config + first_seq, last_seq, msgs, bytes
PATCH  /v1/streams/{stream}                        limits only
DELETE /v1/streams/{stream}
POST   /v1/streams/{stream}/purge                  {up_to_seq?, subject?, keep?}

# publish / inspect
POST   /v1/streams/{stream}/messages               raw body; ?subject= or Messq-Subject
POST   /v1/streams/{stream}/messages:batch         JSON array of {subject, body_b64, hdr, msg_id}
GET    /v1/streams/{stream}/messages/{seq}
GET    /v1/messages/{msg_id}
GET    /v1/streams/{stream}/messages?from_seq=&limit=&subject=   (peek/list, no side effects)

# consumers
POST   /v1/streams/{stream}/consumers              create or update (idempotent)
GET    /v1/streams/{stream}/consumers
GET    /v1/streams/{stream}/consumers/{consumer}   → config + pending, inflight, backlog, oldest_pending_ms
DELETE /v1/streams/{stream}/consumers/{consumer}
POST   /v1/streams/{stream}/consumers/{consumer}/seek     {seq|time|start|new, drop_pending}
GET    /v1/streams/{stream}/consumers/{consumer}/pending  ?older_than_ms=&limit=

# the hot path
POST   /v1/streams/{stream}/consumers/{consumer}/fetch    {batch, wait_ms, max_bytes}
POST   /v1/ack     {tokens: [...]}
POST   /v1/nak     {token, delay_ms?, error?}   (also accepts {items:[...]})
POST   /v1/term    {token, error?}
POST   /v1/extend  {token, extra_ms}

# observability
GET    /v1/events?msg_id=&trace_id=&stream=&consumer=&event=&since=&limit=
GET    /v1/stats                                   → per-stream/consumer counters and gauges
```

### 5.2 Wire shapes

Publish — raw body, metadata in headers, so `curl --data-binary @file` works:

```
POST /v1/streams/jobs/messages?subject=jobs.email HTTP/1.1
Authorization: Bearer pub_xxx
Content-Type: application/octet-stream
Messq-Msg-Id: order-4711-confirm       # optional, dedup key
Messq-Trace-Id: 7f3a...                # optional, else minted (traceparent also accepted)
Messq-Header-Tenant: acme              # optional user headers, Messq-Header-* → hdr JSON

← 201 Created
{"stream":"jobs","seq":90412,"id":"01K3F2QHZ8N4T6M0X9V2C7B5RD",
 "trace_id":"7f3a...","duplicate":false}
```

Fetch:

```
POST /v1/streams/jobs/consumers/mailer/fetch
{"batch":16,"wait_ms":30000,"max_bytes":1048576}

← 200 OK
Messq-Pending: 41
Messq-Backlog: 1203
{"messages":[
  {"ack_token":"AY29...","stream":"jobs","seq":90412,
   "id":"01K3F2QHZ8N4T6M0X9V2C7B5RD","subject":"jobs.email",
   "hdr":{"tenant":"acme"},"body_b64":"eyJ0byI6...",
   "attempt":2,"max_deliver":5,"published_at":"2026-08-21T09:14:02.113Z",
   "ack_deadline":"2026-08-21T09:14:35.902Z","trace_id":"7f3a..."}
]}
```

`body_b64` is always base64 — one field, one shape, no content-type guessing, no ambiguity
about binary payloads. The CLI decodes it; that is the CLI's job.

Errors are a single shape everywhere:
`{"error":{"code":"stale_ack","message":"...","msg_id":"...","trace_id":"..."}}` with
stable machine-readable `code` values (`not_found`, `stale_ack`, `orphan_ack`,
`flow_blocked`, `too_large`, `no_stream`, `bad_subject`, `unauthorized`, `shutting_down`).

### 5.3 Auth

- **Unix socket** (`--unix /run/messq/messq.sock`, mode 0660, group `messq`): no auth.
  Filesystem permissions *are* the ACL. This is how the CLI talks to a local daemon and how
  most single-host deployments will run.
- **TCP**: `Authorization: Bearer <token>`. Tokens live in a line-oriented file
  (`--auth-file`), reloaded on SIGHUP:
  ```
  # token             role      streams
  pub_7f2a...         publish   jobs,events
  sub_91bd...         consume   jobs
  adm_c40e...         admin     *
  ```
  Roles: `publish`, `consume`, `admin`. Three roles, one file, ~60 lines of code, no
  RBAC engine. Tokens are compared with `subtle.ConstantTimeCompare` and never logged
  (a `token_id` = first 8 chars of the SHA-256 is logged instead).
- **TLS**: not implemented. Documented: bind to localhost or the Unix socket and put
  caddy/nginx in front, or use WireGuard/Tailscale. This is one paragraph of docs instead
  of a certificate-reload subsystem.

### 5.4 Client libraries

**None in v1, on purpose.** The README ships a 40-line Go snippet, a 25-line Python
snippet and a `curl` transcript, all copy-pasteable, all covered by an executable docs
test. A "real" SDK is a maintenance surface that competes with shipping the broker. If
adoption demands it, `messq-go` is a separate repo after v1.0.

---

## 6. CLI & developer experience

One binary. `messq serve` is the daemon; everything else is a client. Built with
`spf13/cobra` (`RunE` for real error propagation, `PersistentFlags` for
`--addr`/`--token`/`--output`, generated bash/zsh/fish completions, and man pages via
`cobra/doc`). **No viper** — flags and `MESSQ_*` environment variables only.

**There is no config file in v1.** Flags + env, plus a systemd `EnvironmentFile`. This
kills a dependency, a parser, and the entire "which setting wins" bug class. Server
settings that must be dynamic (log level, auth tokens) reload on SIGHUP.

### 6.1 Command surface

```
messq serve            --data-dir /var/lib/messq --addr :4390 --unix /run/messq/messq.sock
                       --durability full --log-format console --auth-file /etc/messq/tokens

messq stream add jobs --subjects 'jobs.>' --retention workqueue --max-age 168h
messq stream ls | info jobs | rm jobs | purge jobs --up-to-seq 90000

messq consumer add jobs mailer --filter 'jobs.email' --ack-wait 30s \
                               --max-deliver 5 --backoff 1s,5s,30s --max-ack-pending 200
messq consumer ls jobs | info jobs mailer | rm jobs mailer
messq consumer seek jobs mailer --time 2026-08-20T10:00:00Z --drop-pending

messq pub jobs jobs.email --data '{"to":"a@b.c"}'        # or --file x.json, or - for stdin
messq pub jobs jobs.email --file batch.ndjson --ndjson   # one message per line
messq sub jobs mailer                                     # print + auto-ack (dev)
messq sub jobs mailer --manual                            # print token, do not ack
messq sub jobs mailer --exec ./handler.sh --concurrency 4 # ← the important one

messq peek jobs --seq 90412 | messq peek --id 01K3F2...
messq trace 01K3F2QHZ8N4T6M0X9V2C7B5RD                    # full timeline, one message
messq pending jobs mailer --older-than 60s
messq dlq ls | dlq peek jobs mailer | dlq redrive jobs mailer --limit 100 --rate 10/s
messq replay jobs --from-seq 88000 --to-seq 90000
messq top                                                  # live refreshing table
messq bench --publishers 8 --consumers 4 --size 1k --duration 30s
messq backup /var/backups/messq-$(date +%F).db
messq version | completion | doc man
```

### 6.2 `messq sub --exec` — the feature that sells it

```
messq sub jobs mailer --exec ./send.sh --concurrency 4
```

Each message is piped to `./send.sh` on stdin, with `MESSQ_SUBJECT`, `MESSQ_MSG_ID`,
`MESSQ_SEQ`, `MESSQ_ATTEMPT`, `MESSQ_MAX_DELIVER`, `MESSQ_TRACE_ID` and
`MESSQ_HDR_<NAME>` in the environment. Exit code decides the transition, using the
`sysexits.h` convention operators already know:

| Exit | Action |
|---|---|
| `0` | ack |
| `75` (`EX_TEMPFAIL`) | nak with the consumer's backoff |
| `64`–`74`, `76`–`78` | term → straight to DLQ (permanent error) |
| anything else | nak with backoff |
| killed by signal / exceeds `--exec-timeout` | let ack-wait expire; the message redelivers |

Long-running scripts are covered by an automatic `extend` at 50 % of ack-wait, up to
`--exec-timeout`. Stdout/stderr of the child are captured, truncated to 4 KiB, and attached
as `last_error` on failure — so `messq pending` and `messq dlq peek` show you the actual
stack trace.

This means a team gets a durable, retrying, dead-lettering job queue for a bash script or a
Python one-liner **without writing a client**. It is the shortest path from "we have a
cron job that silently fails" to "we have a queue".

### 6.3 Output modes

`--output table` (default, aligned, colour when TTY), `--output json` (one JSON object,
for `jq`), `--output ndjson` (streaming). Every list command supports all three. Exit codes
are meaningful (`0` ok, `1` error, `2` usage, `4` not found, `5` server unreachable) so
CLI calls compose in shell scripts.

`messq trace` is the flagship:

```
$ messq trace 01K3F2QHZ8N4T6M0X9V2C7B5RD
msg 01K3F2QHZ8N4T6M0X9V2C7B5RD  stream=jobs subject=jobs.email seq=90412  trace=7f3a91c2
  1 KiB  published 2026-08-21 09:14:02.113  by token pub_7f2a…

  09:14:02.113  publish                                            size=1024
  09:14:02.118  deliver   mailer  attempt 1/5   deadline 09:14:32.118
  09:14:32.121  timeout   mailer  attempt 1/5   lease_age=30.003s
  09:14:33.004  deliver   mailer  attempt 2/5   deadline 09:15:03.004
  09:14:35.902  nak       mailer  attempt 2/5   delay=5.4s  err="smtp 421 try later"
  09:14:41.310  deliver   mailer  attempt 3/5   deadline 09:15:11.310
  09:14:41.884  ack       mailer  attempt 3/5   held=574ms
  ─ done, 39.8s end to end, 3 attempts, 1 timeout, 1 nak
```

### 6.4 Packaging

- `make release` → static `linux/amd64` + `linux/arm64` binaries (`CGO_ENABLED=0`,
  `-trimpath`, version/commit/date via `-ldflags -X`). No cross-compilation toolchain
  needed, because there is no cgo.
- `.deb` and `.rpm` via `nfpm`, containing the binary, the systemd unit, a
  `messq` system user, `/var/lib/messq` (0750), and `/etc/messq/tokens` (0640).
- A `scratch`-based container image (~20 MB) for people who want it.
- Shipped systemd unit:
  ```ini
  [Service]
  ExecStart=/usr/bin/messq serve --data-dir /var/lib/messq
  User=messq
  Restart=always
  TimeoutStopSec=30
  ProtectSystem=strict
  ReadWritePaths=/var/lib/messq /run/messq
  NoNewPrivileges=true
  PrivateTmp=true
  LimitNOFILE=65536
  ```

---

## 7. Observability & logging design

Logging is not a debugging aid here; it is the reason to choose messq over a Redis list.

### 7.1 Three surfaces, one vocabulary

1. **Structured logs** — `log/slog`, JSON (`--log-format json`, default for non-TTY) or a
   custom compact console handler (default when stderr is a TTY).
2. **Durable audit trail** — the `events` table, written **in the same transaction as the
   state change**. Queryable via `messq trace` / `messq events` / `GET /v1/events`.
   Retention `--audit-retention` (default 72 h). `--audit=full|transitions|off` for
   operators who measure the write amplification and decide they want less; default `full`.
3. **Metrics** — Prometheus at `/metrics`.

All three use **the same `event` identifiers**, so "grep the log", "query the audit table"
and "look at the metric" all speak one language. That is the single highest-leverage
observability decision in this plan.

### 7.2 Event vocabulary (stable, versioned, tested)

```
server.start  server.stop  server.reload  recovery.reclaimed
stream.create stream.update stream.delete stream.purge  retention.expire retention.blocked
consumer.create consumer.update consumer.delete consumer.seek consumer.lag
msg.publish   msg.dup
msg.deliver   msg.ack   msg.ack_stale  msg.ack_orphan
msg.nak       msg.term  msg.extend     msg.timeout  msg.dead
dlq.redrive   flow.blocked
auth.denied   api.error
```

Every message-scoped event carries: `event`, `ts`, `stream`, `subject`, `msg_id`, `seq`,
`consumer`, `attempt`, `max_deliver`, `trace_id`, and where relevant `lease_age_ms`,
`delay_ms`, `held_ms`, `err`, `size`. Field names never change without a major version;
a golden-file test locks them.

`trace_id` is taken from `Messq-Trace-Id`, or parsed out of a W3C `traceparent` header if
present, else minted at publish. It is stored on the message, echoed on every delivery,
carried into the DLQ copy, and present on every event — so a message that was published by
service A, dead-lettered, redriven and finally processed by service C is one `grep
trace_id=…` away, across all of it.

### 7.3 Console format (default on a TTY)

```
09:14:33.004 INFO  msg.deliver   jobs/mailer  seq=90412 id=01K3F2QH… attempt=2/5 subject=jobs.email deadline=+30s
09:14:35.902 WARN  msg.nak       jobs/mailer  seq=90412 id=01K3F2QH… attempt=2/5 delay=5.4s err="smtp 421 try later"
09:14:41.884 INFO  msg.ack       jobs/mailer  seq=90412 id=01K3F2QH… attempt=3/5 held=574ms
09:15:00.000 WARN  consumer.lag  jobs/mailer  backlog=12043 pending=200/200 oldest_pending=4m12s
```

Aligned columns, colour on TTY, `id` truncated to 8 chars in console mode (full in JSON).
~150 lines implementing `slog.Handler`. It earns its place: this is what the "more readable
ops than a traditional broker" claim actually means.

### 7.4 Log volume control

Per-delivery logging at 5 000 msg/s is 5 000 lines/s, which is unacceptable as a default
for a high-throughput stream and essential for a low-throughput one. Resolution:

- `--log-level` (`debug|info|warn|error`) via a `slog.LevelVar`, changeable at runtime with
  SIGHUP — no restart to turn on debug during an incident.
- Hot-path events (`msg.publish`, `msg.deliver`, `msg.ack`) log at `DEBUG`;
  problem events (`msg.nak`, `msg.timeout`, `msg.dead`, `msg.ack_stale`, `flow.blocked`,
  `retention.blocked`) log at `WARN`; everything else at `INFO`.
- Default level is `INFO`, so out of the box you see **problems and admin actions**, not
  traffic — and the full per-message history is still in the audit table for `messq trace`.
- `--log-sample N` logs 1-in-N hot-path events when you do want them at volume.

That combination is the whole answer to "first-class logging without drowning".

### 7.5 Metrics

Custom `prometheus.NewRegistry()` (never the default registry, so we control what appears)
with `promauto.With(reg)` and `promhttp.HandlerFor(reg, promhttp.HandlerOpts{})`.

**Label discipline: only `stream` and `consumer` are ever labels. Never `subject`, never
`msg_id`.** Subject cardinality is user-controlled and would blow up any Prometheus.

```
messq_published_total{stream}
messq_publish_bytes_total{stream}
messq_duplicates_total{stream}
messq_delivered_total{stream,consumer}
messq_acked_total{stream,consumer}
messq_naked_total{stream,consumer}
messq_terminated_total{stream,consumer}
messq_timeouts_total{stream,consumer}
messq_dead_total{stream,consumer}
messq_stale_acks_total{stream,consumer}

messq_pending{stream,consumer}                    gauge  (ready + inflight)
messq_inflight{stream,consumer}                   gauge
messq_backlog{stream,consumer}                    gauge  (stream head − cursor)
messq_oldest_pending_age_seconds{stream,consumer} gauge  ← the alert that matters
messq_dlq_depth{stream,consumer}                  gauge

messq_ack_latency_seconds{stream,consumer}        histogram (deliver → ack)
messq_commit_duration_seconds                     histogram
messq_commit_batch_size                           histogram
messq_fetch_wait_seconds                          histogram
messq_stream_bytes{stream} / messq_db_bytes / messq_wal_bytes
messq_build_info{version,commit}
```

Gauges are refreshed by the 60 s retention sweep and on demand at scrape time via a
`prometheus.Collector` that runs three cheap indexed `COUNT`s, so `/metrics` never lies by
more than a scrape interval.

Ship `docs/alerts.yml` with four rules that actually matter:
`messq_dlq_depth > 0`, `messq_oldest_pending_age_seconds > 300`,
`rate(messq_timeouts_total[5m]) > 0.1 * rate(messq_delivered_total[5m])`,
`messq_backlog` growing for 15 min.

---

## 8. Testing strategy

The whole value proposition is "trustworthy". The test suite is the product.

**1. State machine unit tests.** The transition function is pure:
`step(row, event, cfg, now) → (newRow, sideEffects)`. Exhaustive table test over every
(state × event × attempts-vs-max_deliver × backoff-config) combination. Fast, no I/O,
run on every save.

**2. Store tests against a real file.** Never `:memory:` — WAL, checkpointing, and
`busy_timeout` paths only exist for file-backed databases. `t.TempDir()`, real fsync.

**3. Injected clock.** A `Clock` interface (`Now()`, `After()`, `Tick()`) with a
`fakeClock` in tests. Ack-timeout, backoff and retention tests run in microseconds of wall
time, so we can afford thousands of them. Production uses `realClock`. **No `time.Sleep` in
any test** — this is enforced by a CI grep.

**4. Model-based invariant testing.** A pure in-memory reference implementation of the
semantics, driven alongside the real store by a randomized operation generator (publish,
fetch, ack, nak, term, extend, timeout-tick, restart, seek, purge) with shrinking on
failure. Invariants checked after every step:
- every published message is eventually acked, dead-lettered or still outstanding — never
  silently vanished;
- `attempts` never exceeds `max_deliver`;
- `pending ≤ max_ack_pending`;
- no `seq` is `INFLIGHT` for two different leases simultaneously;
- for `ordered=true`, at most one in-flight `seq` per subject, and it is the minimum
  outstanding one;
- the `events` table replayed from scratch reproduces the current state.
This is where the real bugs are found. It runs 10 s in normal CI and 10 min nightly.

**5. Crash injection.** A harness starts `messq serve` as a subprocess with a real data
dir, drives load through the HTTP API, `SIGKILL`s it at a randomized point (biased toward
mid-commit by hooking a `--fault-inject=after-insert-before-commit` test-only flag),
restarts, and asserts:
- every publish that returned `201` is present;
- every ack that returned `200` is not redelivered;
- no message is in a state that violates the invariants;
- `PRAGMA integrity_check` is clean.
Runs in both `--durability=full` and `relaxed` (with the appropriately weaker
publish-durability assertion). Matrix over ext4, xfs and tmpfs in nightly CI.

**6. Golden log tests.** A scripted scenario (publish → deliver → timeout → deliver → nak
→ deliver → dead → redrive → ack) asserts the exact ordered sequence of event names and
their required fields against a golden file. This makes the observability contract a
tested API, so a refactor cannot quietly stop emitting `msg.timeout`.

**7. HTTP contract tests.** `httptest` + real store, covering every endpoint's success and
every documented error `code`. The README's curl transcript is executed as a test
(`docs_test.go`) so the documentation cannot rot.

**8. Fuzzing.** Go native fuzzing for the subject matcher (`FuzzMatch` against a slow
reference implementation), the ack-token codec (must never panic, never accept a forged
token), and the JSON request decoders.

**9. Concurrency.** Everything under `-race` in CI. A 30-minute soak (`messq bench` with
chaotic naks, restarts and seeks) nightly, asserting flat memory and goroutine counts.

**10. Performance as a gate.** `messq bench` is a first-class command, not a script, and
CI runs it on tmpfs with a floor assertion (`≥ X msg/s`, `p99 ≤ Y ms`). Regressions fail
the build. The same command is documented so operators can measure *their* disk rather than
trusting our numbers.

**11. Upgrade tests.** For each released version, a test opens a data directory created by
version N−1, runs migrations, and asserts the invariants hold.

Coverage target: **90 % on `internal/queue` and `internal/store`**, no target elsewhere
(coverage numbers on HTTP glue are theatre).

---

## 9. Roadmap

Estimates assume one focused engineer. Every milestone ends with something demoable and
tagged.

### M0 — Skeleton (2 days)
- `go.mod` (Go 1.24+), package layout:
  `cmd/messq`, `internal/api`, `internal/store`, `internal/queue`, `internal/obs`,
  `internal/cli`, `internal/testutil`.
- CI: build, `go vet`, `staticcheck`, `go test -race`, `gofumpt` check.
- `make release` producing static amd64/arm64 binaries; `messq version`.
- `Clock` interface, ULID generator (26-char Crockford base32, 48-bit ms + 80-bit
  crypto/rand entropy, monotonic within a millisecond by incrementing the random component
  — ~50 lines, no dependency), subject matcher with `*`/`>` plus its fuzz test.
- ADR files for the five decisions in §3.1, §5, §4.3-DLQ, §6-config, §7.
- **Exit:** `messq version` runs; CI green; matcher is fuzz-tested.

### M1 — Store core & publish (5 days)
- Embedded migrations, schema from §3.2, `STRICT` tables, `schema_version`.
- Writer goroutine with group commit; `rw`/`ro` pool split.
- Publish (with dedup), seq allocation, peek by seq/id, stream CRUD, purge.
- Store unit tests + first crash test (publish under SIGKILL).
- **Exit:** `messq pub` / `messq peek` against a running `messq serve`; a `kill -9` loop
  never loses an acknowledged publish.

### M2 — Delivery engine (6 days) — *the heart*
- Consumers CRUD; cursor + inline top-up; claim with `RETURNING`; ack/nak/term/extend;
  lease rotation and stale-ack detection.
- Sweeper: ack timeout → redeliver or dead; backoff arrays with jitter.
- `max_ack_pending` flow control; `ordered=true` per-subject serialization.
- DLQ as the `_dlq` stream; `dead_policy`.
- Model-based invariant test suite.
- **Exit:** `messq sub jobs mailer --manual` shows redelivery after ack-wait, and a poison
  message lands in `_dlq` after exactly `max_deliver` attempts. Invariant tests pass on
  10 000 random operation sequences.

### M3 — HTTP surface & daemon hygiene (3 days)
- Full endpoint set from §5.1 on stdlib `net/http` `ServeMux` (Go 1.22 method+pattern
  routing — no router dependency).
- Long-poll fetch + waiter registry; TCP + Unix socket listeners.
- Bearer auth file with SIGHUP reload; role checks.
- Graceful shutdown: stop accepting → release long-polls → drain handlers (10 s) →
  release leases → `wal_checkpoint(TRUNCATE)` → close.
- systemd unit; HTTP contract tests; executable README curl transcript.
- **Exit:** `systemctl restart messq` mid-load loses nothing and logs
  `recovery.reclaimed`.

### M4 — Observability (3 days)
- `log/slog` wiring, `LevelVar` + SIGHUP, console handler, `--log-sample`.
- Event vocabulary; durable `events` table written in-transaction; `--audit` modes.
- `messq trace`, `messq events`, `GET /v1/events`.
- Prometheus registry, all metrics from §7.5, `docs/alerts.yml`.
- Golden log tests.
- **Exit:** `messq trace <id>` prints the §6.3 timeline for a message that timed out,
  was naked and finally acked. **Tag `v0.1.0` — usable. (~3.5 weeks in.)**

### M5 — Operations completeness (4 days)
- Retention (`limits` + `workqueue`), dedup-window trimming, incremental vacuum, WAL
  checkpoint policy, `retention.blocked`.
- `seek` (seq/time/start/new, `--drop-pending`), `replay`, `dlq redrive` with rate limit.
- `messq pending --older-than`, `messq top`, `messq bench`, `messq backup`.
- Shell completions + man pages.
- **Exit:** the full §6.1 CLI works; a stream running for a week does not grow unbounded.

### M6 — Hardening & release (5 days)
- Crash-injection matrix (fs × durability × fault point) in nightly CI; 30-min soak;
  fuzz corpus committed; upgrade test from v0.1.
- Benchmark floor as a CI gate; published benchmark methodology and numbers.
- Documentation: README (guarantees front and centre, including what messq is *not*),
  `docs/operations.md` (sizing, backup/restore, alerting, upgrade), `docs/semantics.md`
  (the state machine diagram + the exact rules from §4.3), `docs/faq.md`
  ("why did my message get delivered twice?").
- `.deb`/`.rpm` via nfpm, container image, checksums, signed release.
- **Exit:** **Tag `v1.0.0`. (~5.5–6 weeks in.)** A team can install it from a package and
  run it.

### M7 — Earned extensions (post-1.0, only what users ask for)
Ordered by expected demand, each independently shippable:
1. Delayed publish (`Messq-Delay-Ms`) — the `visible_at` machinery already exists.
2. Per-consumer delivery rate limit (`--rate 100/s`) — the single most requested thing
   after a DLQ redrive goes badly.
3. Audit export: `messq events --since … --output ndjson` streaming to a file/pipe, plus
   an optional `--audit-sink /path/audit.ndjson` for compliance workflows.
4. Batch/chunked fetch over a single streaming HTTP response (`Transfer-Encoding: chunked`,
   NDJSON) for consumers that want lower per-message overhead without gRPC.
5. `messq consumer info --explain` — why is this consumer not receiving? (filter matched 0,
   flow blocked, ordered-blocked on subject X, cursor ahead of head).

### M8 — Deliberately not now
Clustering/replication (the honest v2 design is a read-only follower fed by shipping the
`messages` table, with manual promotion — *not* consensus), priorities, compression,
push/webhook delivery, consumer-group leases beyond "N workers share one consumer", schema
registry, web UI, multi-tenancy. Each of these is a line in `docs/non-goals.md` with the
reason, so the answer to "can it do X" is a link rather than a debate.

---

## 10. Risks & open questions

| # | Risk | Severity | Mitigation |
|---|---|---|---|
| R1 | **SQLite write throughput ceiling.** Serialized writes + `synchronous=FULL` cap us in the low thousands of msg/s on decent NVMe, far less on a cloud network disk. | High | Group commit (the main lever). `messq bench` as a first-class command so operators measure their own disk. Publish honest numbers with methodology. Document `--durability=relaxed` for replayable workloads. Position explicitly below Kafka. If we hit the wall: batch-publish endpoint (already in M1) amortizes further. |
| R2 | **`modernc.org/sqlite` is a transpiled port**, not the C original — possible subtle behaviour or performance differences. | Medium | Keep all SQL vanilla and portable. `//go:build cgosqlite` alternative driver wired from M1 and exercised in a CI matrix job, so switching is a build tag, not a rewrite. Static single binary is worth this risk. |
| R3 | **Audit table write amplification** roughly doubles write volume. | Medium | `--audit=full\|transitions\|off`, age + row-count trimming, and it is measured by `messq bench` in both modes so the cost is a published number, not a surprise. |
| R4 | **`ordered=true` head-of-line blocking.** One poison message stalls its subject until `max_deliver` is exhausted. | Medium | Documented as the explicit tradeoff. `messq consumer info --explain` (M7) names the blocking subject. `term` gives the operator an immediate unblock. |
| R5 | **Clock jumps / VM suspend** cause mass ack-timeouts on resume. | Medium | Reclaim jitter on startup; nak/backoff jitter; `msg.timeout` events carry `lease_age_ms` so a mass event is instantly recognizable as a clock artifact. NTP is a documented prerequisite. |
| R6 | **Long-poll goroutine/FD pressure** with many idle consumers. | Low | One goroutine + one FD per waiting fetch; fine to ~10 k. `--max-waiters` returns `503` with `Retry-After` beyond it. `LimitNOFILE=65536` in the unit file. |
| R7 | **At-least-once duplicates surprise users.** The single most common broker support question. | High (adoption) | Guarantee statement in the README's first screen; `attempt` on every delivery; `Messq-Msg-Id` publish dedup; explicit `msg.ack_stale` events; a FAQ entry titled "why did my message get delivered twice?". |
| R8 | **Unbounded disk growth** when a consumer is stuck and retention is `limits`. | Medium | `retention.blocked` event + metric + shipped alert rule; `messq stream info` shows bytes and the oldest blocking consumer; documented `max_bytes` guidance. |
| R9 | **The `GROUP BY … HAVING min(seq)=seq` trick** relies on SQLite's bare-column behaviour. | Low | Dedicated test, explanatory comment, and a plain-Go fallback path behind a flag. |
| R10 | **Scope creep toward a distributed system.** Every user will eventually ask for HA. | High (project) | `docs/non-goals.md` written in M6, before the requests arrive. The honest HA answer for v1 is "run it on a machine with a UPS, back it up hourly with `VACUUM INTO`, and accept a restart-length outage" — which is genuinely acceptable for internal workflows and should be stated confidently rather than apologized for. |

### Open questions (to be closed by the end of M2, with a written ADR each)

1. **Should ack accept `(stream, consumer, seq)` without a lease token?** Leaning **no** —
   the token is what makes stale-ack detection possible, and that detection is a
   differentiator. But a `--allow-untokenized-ack` escape hatch may be needed for clients
   that cannot store opaque strings. Decide after writing the Python example.
2. **Multi-consumer fetch in one call** (fetch from several consumers/streams in one
   round trip). Real ergonomic win for a worker handling five queues; real complexity in
   the writer. Deferred to M7 unless the `--exec` worker experience demands it.
3. **Should `_dlq` be one shared stream or one per origin stream?** Currently one shared
   stream with structured subjects. If DLQ volume becomes large enough that retention
   policies need to differ per origin, split it. Revisit with real usage.
4. **Default `max_deliver`.** 5 matches most people's intuition; JetStream defaults to
   unlimited. We deliberately differ, because unlimited-by-default is how you get a poison
   message that retries for a month. Confirm no user is surprised.
5. **`--exec` concurrency and ordering.** With `--concurrency 4` and `ordered=true` the
   consumer already serializes per subject, but the CLI must not reorder within a subject
   either. Needs a test, and possibly per-subject worker affinity.
6. **Commit-window tuning.** Is 2 ms the right default, or should it adapt (grow under
   load, shrink when idle)? Measure in M6; a fixed default that is documented beats an
   adaptive one that is mysterious, unless the numbers are compelling.

---

## 11. Library choices

**Total runtime dependencies: 3 third-party modules + the standard library.** Every one is
justified below against documentation fetched for this plan.

### 11.1 `modernc.org/sqlite` — storage engine (driver)

Pure-Go, cgo-free SQLite. This is what makes `CGO_ENABLED=0`, `-trimpath`, static
cross-compiled linux/amd64+arm64 binaries possible, which is the whole "single binary"
promise. Its documentation confirms the DSN parameters we depend on:
`_journal=WAL`, `_synchronous=NORMAL|FULL`, `_timeout` (alias `_busy_timeout`),
`_foreign_keys`, `_txlock=immediate`, and `_pragma=…` for anything else — so our entire
connection configuration is one DSN string, no post-open PRAGMA dance to get wrong on a
reconnect. The docs also spell out the `SQLITE_BUSY` handling story (busy timeout or
explicit backoff retry), which we largely sidestep by serializing writes but still
configure defensively for the read pool.

`_txlock=immediate` is specifically important: it takes the write lock at `BEGIN` rather
than at first write, eliminating the deadlock-on-upgrade class of `SQLITE_BUSY` failures
should any future path open its own write transaction.

From the SQLite documentation itself we take: `PRAGMA journal_mode=WAL` (readers never
block the writer — the reason our `ro` pool can run `messq trace` during peak load),
`PRAGMA wal_autocheckpoint` tuning plus explicit `wal_checkpoint(TRUNCATE)` on shutdown,
`synchronous=FULL` as the durable setting under WAL, and batching many statements into one
transaction as the documented way to collapse fsyncs — which is exactly our group-commit
design.

*Rejected:* `mattn/go-sqlite3` (needs cgo → no static cross-compiled binary; kept as a
build-tag fallback), `glebarez/sqlite` (a GORM driver — we do not want an ORM anywhere near
a hot path we intend to hand-tune).

### 11.2 `spf13/cobra` — CLI framework

We need ~25 subcommands with nested groups, persistent flags, generated shell completions
and man pages. Cobra's documentation confirms all of it: `RunE` for real error returns
(so our exit-code contract in §6.3 is implementable without `os.Exit` scattered through
command bodies), `PersistentFlags()` on the root for `--addr`/`--token`/`--output`,
`AddCommand` composition, and `GenBashCompletion` / `GenZshCompletion` /
`GenFishCompletion` / `GenPowerShellCompletionWithDesc` for `messq completion`.

Notably, Cobra's own documentation demonstrates wiring with **viper**; we decline it. Viper
brings config-file formats, remote config and precedence rules we do not want, and §6 has
already ruled out a config file. We use Cobra plus its `pflag` dependency and read
environment variables directly with a 20-line helper. This is a deliberate subtraction from
the framework's suggested path.

*Rejected:* stdlib `flag` (no subcommand tree, no completions — would cost more code than
Cobra saves), `urfave/cli` (equivalent, less ubiquitous), `charmbracelet/fang` (adds
styling and an opinion layer we do not need on top of Cobra).

### 11.3 `prometheus/client_golang` — metrics

The de-facto standard, and the docs confirm the exact pattern we want: build a
**custom registry** with `prometheus.NewRegistry()`, create metrics through
`promauto.With(reg)` bound to that registry, and serve with
`promhttp.HandlerFor(reg, promhttp.HandlerOpts{})`. Using a custom registry rather than the
default one matters here: it keeps `/metrics` to *our* declared surface, so the label
discipline in §7.5 is enforceable and there are no surprise collectors. The docs also cover
`HandlerOpts{MaxRequestsInFlight}` (we set it, so a scrape storm cannot become a
denial-of-service against the daemon) and `HistogramOpts.Buckets` with
`ExponentialBuckets`/`LinearBuckets`, which we use for `messq_ack_latency_seconds`
(exponential, 1 ms → 1 h — ack latency spans orders of magnitude) and
`messq_commit_duration_seconds` (exponential, 100 µs → 1 s).

*Rejected:* OpenTelemetry metrics (heavier dependency tree, and a Prometheus endpoint is
what a small team's existing scraper already wants), hand-rolled counters (we would
reimplement the exposition format and get it subtly wrong).

### 11.4 Standard library — deliberately, for everything else

- **`log/slog`** for structured logging. The docs confirm the two features the design leans
  on: `slog.NewJSONHandler(w, &slog.HandlerOptions{Level: programLevel})` where
  `programLevel` is a `*slog.LevelVar`, giving us **runtime log-level changes on SIGHUP
  with no restart** (critical during an incident); and the `Handler` interface
  (`Enabled`, `Handle`, `WithAttrs`, `WithGroup`) which our ~150-line console handler
  implements. `Logger.With()` pre-binds `stream`/`consumer` so hot-path call sites stay
  clean. No zerolog, no zap: slog is in the standard library, it is fast enough for a
  workload bounded by `fsync`, and it means zero dependency risk on the one subsystem we
  are marketing.
- **`net/http`** with `ServeMux`'s method-and-pattern routing (`POST /v1/streams/{stream}/…`)
  from Go 1.22+. No chi, gin, echo or fiber. Our route table is ~25 entries; a router
  dependency would buy nothing and cost a middleware ecosystem we would then be tempted to
  use.
- **`database/sql`** as the store interface — which is what makes the cgo escape hatch in
  R2 a one-line change. No sqlc, no ORM; the queries are hand-written, few, and
  performance-critical.
- **`crypto/rand` + `encoding/base32`** for ULID generation. Rather than take a
  dependency, we implement the published ULID format directly: 26 characters of Crockford
  base32 (`0123456789ABCDEFGHJKMNPQRSTVWXYZ`, chosen to exclude visually confusable
  characters), a 10-character 48-bit millisecond timestamp component and a 16-character
  80-bit random component, with monotonicity inside a millisecond obtained by incrementing
  the random component. ~50 lines plus a fuzz test that asserts sortability, uniqueness and
  round-tripping. Message IDs are on every log line and in every support conversation, so
  "sortable, timestamped, unambiguous when read aloud from a screenshot" is a real product
  requirement, not a style choice.
- **`encoding/json`, `testing`, `testing/fstest`, `os/signal`, `os/exec`** — no wrappers.

### 11.5 Development-only tooling

`staticcheck`, `gofumpt`, `govulncheck`, `nfpm` (packaging). None ship in the binary. Test
code uses only the standard library plus, if it earns it, `google/go-cmp` for diffs in
golden tests.

---

## Appendix A — Repository layout

```
cmd/messq/main.go              ~40 lines: build info, cli.Execute()
internal/cli/                  cobra commands, output formatting, --exec worker
internal/api/                  routes, handlers, auth, long-poll waiters, error shapes
internal/queue/                state machine, subject matcher, ids, clock, config types
internal/store/                schema/*.sql (embedded), writer, queries, migrations
internal/obs/                  slog handlers, event vocabulary, metrics registry
internal/testutil/             fake clock, crash harness, model checker, load generator
docs/                          README companions: semantics, operations, non-goals, faq, alerts.yml
packaging/                     systemd unit, nfpm.yaml, Dockerfile
```

## Appendix B — Configuration surface (complete)

```
--data-dir            /var/lib/messq      directory holding messq.db
--addr                :4390               TCP listen ("" disables)
--unix                ""                  Unix socket path ("" disables)
--auth-file           ""                  bearer token file ("" = no TCP auth; refuses non-loopback bind)
--durability          full                full | relaxed
--commit-window       2ms                 group-commit accumulation window
--commit-max-batch    512                 commands per transaction
--scan-limit          4096                messages scanned per fetch top-up
--sweep-interval      250ms               ack-timeout resolution
--retention-interval  60s
--reclaim-jitter      1s                  startup lease-release spread
--nak-jitter          0.2                 fraction applied to non-zero delays
--max-ack-wait        1h                  cap on extend
--max-waiters         10000               concurrent long-polls
--drain-timeout       10s                 graceful shutdown budget
--wal-max-bytes       256MiB              manual checkpoint threshold
--audit               full                full | transitions | off
--audit-retention     72h
--audit-max-rows      20000000
--log-level           info                debug | info | warn | error (SIGHUP reloadable)
--log-format          auto                auto | console | json
--log-sample          1                   log 1-in-N hot-path events
```

Every flag also reads `MESSQ_<UPPER_SNAKE>`. That is the whole configuration surface, and
it fits on one screen — which is the test it had to pass.
