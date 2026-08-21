# messq — Project Plan (Plan 09: The Testing & Quality Advocate)

> **Thesis:** the test strategy *is* the architecture. Every structural decision below is
> chosen because it makes a correctness claim mechanically checkable. messq's product
> differentiator is not throughput and not features — it is that it can **prove itself**,
> on the user's machine, from a single binary.

---

## 1. Vision & positioning

### 1.1 The problem, seen from the quality seat

Teams reach for Kafka not because they need partitions and consensus, but because they are
scared. They have been burned by a queue that lost a job, delivered it twice with no
explanation, or silently wedged behind a poison message. The industry's own war stories are
consistent: the failures that hurt are *operational and semantic*, not throughput-related —
a visibility timeout tuned below p99 job duration causing permanent mid-flight redelivery; a
DLQ with no alarm becoming "a silent black hole"; a bulk redrive re-poisoning the main queue;
an ack that races a redelivery and quietly retires work a second worker is still doing.

None of those need a distributed log to fix. They need **precise semantics, checkable
invariants, and an audit trail**. That is a testing problem wearing a product costume.

### 1.2 What messq is

messq is a single-binary, single-node, at-least-once queue daemon for Linux with
NATS-JetStream-shaped primitives (stream, subject, consumer, ack, nak, term, ack-wait,
max-deliver, DLQ, replay, cursors, max-ack-pending) and one unusual property: **the
correctness argument ships with the binary.**

Concretely, three things no comparable small broker offers:

1. **`messq verify`** — the same invariant checker the test suite uses, runnable against a
   live or copied data directory. If messq's state is inconsistent, an operator finds out in
   two seconds, not two weeks.
2. **`messq trace <msg-id>`** — a complete, durable, causally-annotated lifecycle for any
   message: every delivery carries a `cause` (`first`, `nak`, `ack_timeout`,
   `crash_recovery`, `replay`, `redrive`). *Every duplicate has a named reason, or it is a
   bug.*
3. **`messq sim run --seed N`** — the deterministic simulator is a shipped subcommand. Bug
   reports become seeds. "It happened once in production" becomes a reproducible artifact.

### 1.3 What messq is not

Not a Kafka replacement. No consensus, no quorum replication, no partitions across machines,
no exactly-once. One node, one process, local disk. Horizontal scale is explicitly out of
scope for v1 — because a small team cannot honestly test a replication protocol, and shipping
an untestable guarantee is worse than shipping no guarantee.

### 1.4 The positioning sentence

> **A broker small enough to read in an evening, and paranoid enough to trust in production —
> because it can show you, on demand, that its own state is sound.**

### 1.5 What "proven correct enough" means (the contract)

This is the definition the rest of the plan serves. messq is "correct enough" when **all five**
hold — they multiply, they do not add:

| Pillar | Meaning | Evidence artifact |
|---|---|---|
| **Executable invariants** | Every advertised guarantee is a predicate in `internal/invariant`, not prose | `messq verify` exits non-zero |
| **Deterministic reproduction** | Any failure is replayable from a seed on any machine | committed `.rapid/` failfiles + sim seeds |
| **Fault coverage** | Nine enumerated fault families, each with a named green CI job | CI job matrix |
| **Durability evidence** | Crash + power-loss oracles, parameterized by durability mode | `crash-kill9`, `nightly-lazyfs` jobs |
| **Tested observability** | Replaying the audit log reconstructs the persisted state exactly | `TestLogIsTheModel` |

Anything a guarantee table claims that is not backed by a row above gets deleted from the
guarantee table. That rule is non-negotiable and applies to marketing copy too.

---

## 2. Architecture overview

### 2.1 The three seams

All nondeterminism is confined to exactly three interfaces. This is *the* load-bearing
architectural decision; determinism is bought by design, not by runtime interception (which is
why we do **not** adopt `gosim` or a source-translating simulator — see §11.9).

```go
// internal/seam
type Clock interface {
    Now() time.Time          // wall clock: log timestamps, delayed delivery only
    Since(Instant) Duration  // monotonic: ALL lease deadlines
    NewTimer(d Duration) Timer
}
type Rand interface { Uint64() uint64 }
type Store interface { /* §3.4 */ }
```

Everything else in `internal/core` is pure.

### 2.2 The pure core

```go
// internal/core — no I/O, no goroutines, no time.Now(), no map iteration order dependence.
func (s *State) Apply(cmd Command, now Instant) (Effects, []Event, error)
```

`State` is the full broker model (streams, consumers, pending sets, cursors). `Apply` is a
total function: same state + same command + same `now` ⇒ byte-identical outputs, always.
`Effects` are *requests* to do I/O (persist these rows, send these deliveries, arm this
timer). `Events` are audit records.

This single design choice is what makes property testing, model-based testing, deterministic
simulation, and invariant checking all cheap. It is worth the indirection.

### 2.3 Process and goroutine model

One process. Goroutines, by role:

```
┌─────────────────────────────────────────────────────────────────────┐
│ messq serve (single process)                                        │
│                                                                     │
│  net/http (h2c) ──┬─ Connect handlers (N goroutines, one per RPC)   │
│                   │       │ send Command on stream inbox chan       │
│                   │       ▼                                         │
│                   │  ┌──────────────────────────────────────────┐   │
│                   │  │ Stream Actor  (1 goroutine per stream)    │   │
│                   │  │  ─ owns core.State for that stream        │   │
│                   │  │  ─ serial: recv cmd → Apply → batch →     │   │
│                   │  │    commit tx → reply → emit events        │   │
│                   │  │  ─ NO shared mutable state escapes        │   │
│                   │  └──────────────┬───────────────────────────┘   │
│                   │                 │ (grouped write batch)          │
│                   │                 ▼                                │
│                   │        ┌────────────────────┐                    │
│                   │        │ Store (SQLite, WAL)│  1 write conn      │
│                   │        │ + N read conns     │                    │
│                   │        └────────────────────┘                    │
│                   │                                                  │
│  Timer wheel goroutine ──► ExpireLeases cmd ──► stream actor         │
│  Retention goroutine   ──► Retention cmd    ──► stream actor         │
│  Checkpoint goroutine  ──► PRAGMA wal_checkpoint(PASSIVE)            │
│  Audit sink goroutine  ──► slog JSON/console (events already durable) │
│  Metrics: promhttp on the same listener, /metrics                    │
└─────────────────────────────────────────────────────────────────────┘
```

**Why one actor per stream, not a mutex-protected shared map:** the actor loop's body is a
synchronous, single-threaded function. The deterministic simulator can call it *directly*,
with zero goroutine scheduling involved. Concurrency bugs are pushed to the boundary
(inbox channels), which is a few hundred lines that `-race` and `testing/synctest` cover
completely. We buy determinism with an architectural constraint instead of a test framework.

**Consequence, accepted:** per-stream write throughput is bounded by one goroutine + one
SQLite write connection. Measured target: ≥ 20k small publishes/s in `group` durability mode
on NVMe, ≥ 2k/s in `strict`. That is far above the "internal workflows" bar and we will not
trade the testability for more.

### 2.4 Data flow: a publish

1. Connect handler decodes `PublishRequest`, validates, assigns `trace_id` (from header
   `messq-trace-id` or generated), sends `CmdPublish` + reply channel to the stream actor.
2. Actor drains its inbox up to the batch window (default 2 ms / 256 commands).
3. For each command: `state.Apply(cmd, clock.Now())` → effects + events. Nothing is durable
   yet; `Apply` mutates a *shadow* copy.
4. Actor opens one `BEGIN IMMEDIATE` transaction, writes all effect rows **and all event
   rows**, commits. `synchronous=FULL` ⇒ exactly one fsync per batch.
5. Only after commit: shadow state is promoted to live state, reply channels are closed with
   success, events are pushed to the audit sink for stdout logging, timers are armed.

The ordering in step 5 is the whole crash-safety story: **no client ever observes state that
is not durable, and no in-memory state diverges from disk.** Fault point
`store.tx.after_commit_before_reply` exists precisely to test the one window where a crash
produces a client-visible UNKNOWN.

### 2.5 Data flow: a fetch/ack round trip

```
client ──Fetch(consumer, max_msgs=32, max_bytes=1MiB, expires=30s)──► handler
        handler → CmdFetch → actor
        actor: honours max_ack_pending (flow control), leases up to N seqs,
               delivery_count++, state=INFLIGHT, deadline=now+ack_wait,
               writes pending rows + msg.deliver events in ONE tx
        actor → handler: deliveries with AckToken = c<cid>.<seq>.<attempt>.<gen>
        handler streams Delivery messages; if the stream breaks, leases simply expire.
client ──Ack([tokens], outcome=ACK|NAK|TERM)──► handler → CmdAck → actor
        actor: validates token epoch (§4.4), applies, one tx, replies per-token status
```

Pull-only. No server-side push subscription state. Flow control is inherent: the consumer
asks for exactly what it can handle. This removes an entire category of untestable
back-pressure code.

### 2.6 Package layout

```
cmd/messq/                 main, cobra wiring
internal/core/             pure state machine  (target ≥95% stmt coverage, mutation-tested)
internal/core/model/       reference implementation used as the test oracle
internal/invariant/        the 13 invariants, as predicates over a StateView
internal/store/            SQLite store + migrations + recovery
internal/store/memstore/   in-memory Store for sim/logic
internal/seam/             Clock, Rand
internal/fault/            named fault points (build tag `messq_fault`)
internal/audit/            event → slog handlers (json, console)
internal/server/           Connect handlers, interceptors, flow control
internal/sim/              deterministic simulator (logic + durable tiers)
internal/cli/              subcommands
api/messq/v1/              .proto + buf config
test/conform/              black-box conformance suite (the executable spec)
test/crash/                subprocess kill-9 harness + external ledger oracle
test/script/               testscript .txtar CLI golden tests
test/soak/                 long-running load, the only place time.Sleep is allowed
```

---

## 3. Storage & durability design

### 3.1 Engine decision: one SQLite database, `modernc.org/sqlite`

**Decision:** a single SQLite database file per data directory, accessed through the pure-Go
`modernc.org/sqlite` driver. No second engine. No separate segment files for payloads.

**Why one engine (the testability argument):** a design with an append-only payload log plus
a metadata store has *two* durability boundaries and therefore a two-phase commit between
them. The crash matrix of a two-phase commit — crash after log fsync but before metadata
commit, after metadata but before log, during recovery of either — is a combinatorial space
that a small team cannot exhaustively test. One transactional boundary means every state
transition is a single atomic fact, and crash recovery is "whatever SQLite recovered is the
truth." That collapses the entire crash matrix into: *did the transaction commit or not?*
Two outcomes, both testable.

**Why SQLite over bbolt** (bbolt was seriously considered; its `StrictMode` post-commit
consistency check and `DB.Batch` group-commit are genuinely attractive for this persona):
the pending set needs at least three access paths — by `(consumer, seq)`, by
`(consumer, state, deadline)` for lease expiry, and by `(state, deadline)` globally for the
timer wheel refill. In bbolt each of those is a hand-maintained secondary bucket, and every
hand-maintained index is an invariant *we* must write, test, and repair. SQLite maintains
them, and `PRAGMA integrity_check` verifies them for free. Additionally, `messq verify`'s
invariants become SQL queries, which any operator can run with the stock `sqlite3` CLI during
an incident — forensics without our binary. bbolt's advantage (raw write speed, no SQL
parsing) does not outweigh that for our throughput target.

**Why `modernc.org/sqlite` over `mattn/go-sqlite3`:**
- **cgo breaks our test strategy.** The Go race detector does not instrument C code; a data
  race inside a cgo SQLite handle is invisible to `-race`. Pure Go means `-race` and
  `go test -fuzz` cover the *entire* stack including the storage engine.
- Single static binary, trivial cross-compilation, no libc/toolchain variance between the
  developer's machine, CI, and production — which means "reproduce the failure" actually works.
- No cgo means no `runtime.LockOSThread` interactions and no stack-growth surprises under
  the deterministic simulator.

Accepted cost: modernc's transpiled SQLite is ~1.5–2× slower than the C library on write-heavy
workloads. Given our 20k msg/s target, acceptable. Mitigation for the "transpilation bug" risk
is in §10.

### 3.2 PRAGMA / DSN configuration (grounded in the driver docs)

The `modernc.org/sqlite` driver takes pragmas via DSN parameters. Our write connection:

```
file:/var/lib/messq/messq.db?
  _journal=WAL&
  _synchronous=FULL&          # or NORMAL in relaxed mode, see §3.6
  _txlock=immediate&          # critical: see below
  _timeout=5000&              # busy_timeout
  _foreign_keys=1&
  _pragma=wal_autocheckpoint(4000)&
  _pragma=cache_size(-64000)&
  _pragma=temp_store(MEMORY)
```

`_txlock=immediate` is load-bearing, not cosmetic: with the default `deferred`, a transaction
that starts by reading and then writes must *upgrade* its lock, and an upgrade that loses the
race returns `SQLITE_BUSY` which `busy_timeout` **does not** retry (it would deadlock). Taking
the write lock at `BEGIN IMMEDIATE` makes contention a wait instead of an error. Note also
that the driver forces plain `BEGIN` for read-only transactions regardless of `_txlock`, which
is exactly what we want for our reader pool.

Connections: **exactly one** write connection (`db.SetMaxOpenConns(1)` on a dedicated
`*sql.DB`) — SQLite has one writer anyway, and an application-level write queue is strictly
better than relying on `SQLITE_BUSY`. A separate read-only `*sql.DB` with N connections serves
`Peek`, `Trace`, `List*`, and `/metrics`. WAL mode gives those readers snapshot isolation
without ever blocking the writer.

Checkpointing runs on its own connection in its own goroutine (`wal_checkpoint(PASSIVE)`
every 5 s or 4000 pages) so a checkpoint never lands inside a publish's latency.

### 3.3 Schema

`STRICT` tables throughout (type enforcement is a free invariant). Times are integer
microseconds since epoch.

```sql
CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT NOT NULL) STRICT;
-- schema_version, node_id, durability_mode_last_used, created_at

CREATE TABLE stream (
  id          INTEGER PRIMARY KEY,
  name        TEXT NOT NULL UNIQUE,
  created_at  INTEGER NOT NULL,
  retention   TEXT NOT NULL DEFAULT 'limits',   -- 'limits' | 'all_resolved'
  max_age_us  INTEGER NOT NULL DEFAULT 0,
  max_bytes   INTEGER NOT NULL DEFAULT 0,
  max_msgs    INTEGER NOT NULL DEFAULT 0,
  next_seq    INTEGER NOT NULL DEFAULT 1,
  first_seq   INTEGER NOT NULL DEFAULT 1        -- advances on retention/purge
) STRICT;

CREATE TABLE message (
  stream_id    INTEGER NOT NULL REFERENCES stream(id) ON DELETE CASCADE,
  seq          INTEGER NOT NULL,
  msg_id       TEXT    NOT NULL,                -- ULID, or client-supplied dedup key
  subject      TEXT    NOT NULL,
  published_at INTEGER NOT NULL,
  trace_id     TEXT    NOT NULL,
  headers      BLOB,                            -- protobuf-encoded
  body         BLOB    NOT NULL,
  body_len     INTEGER NOT NULL,
  PRIMARY KEY (stream_id, seq)
) STRICT, WITHOUT ROWID;

CREATE UNIQUE INDEX message_dedup   ON message(stream_id, msg_id);
CREATE        INDEX message_subject ON message(stream_id, subject, seq);

CREATE TABLE consumer (
  id              INTEGER PRIMARY KEY,
  stream_id       INTEGER NOT NULL REFERENCES stream(id) ON DELETE CASCADE,
  name            TEXT NOT NULL,
  filter_subject  TEXT NOT NULL DEFAULT '>',
  ack_wait_us     INTEGER NOT NULL DEFAULT 30000000,
  max_deliver     INTEGER NOT NULL DEFAULT 5,
  max_ack_pending INTEGER NOT NULL DEFAULT 1000,
  backoff_us      TEXT NOT NULL DEFAULT '[]',   -- JSON array of per-attempt delays
  ordered         INTEGER NOT NULL DEFAULT 0,
  dlq_stream_id   INTEGER REFERENCES stream(id),
  on_exhausted    TEXT NOT NULL DEFAULT 'dlq',  -- 'dlq' | 'hold'
  cursor_seq      INTEGER NOT NULL DEFAULT 0,   -- highest seq ever leased
  ack_floor_seq   INTEGER NOT NULL DEFAULT 0,   -- all seqs <= this are resolved
  generation      INTEGER NOT NULL DEFAULT 1,   -- bumped by seek/purge/reset
  created_at      INTEGER NOT NULL,
  UNIQUE (stream_id, name)
) STRICT;

CREATE TABLE pending (
  consumer_id        INTEGER NOT NULL REFERENCES consumer(id) ON DELETE CASCADE,
  seq                INTEGER NOT NULL,
  state              INTEGER NOT NULL,    -- 1=INFLIGHT 2=WAITING
  delivery_count     INTEGER NOT NULL,
  generation         INTEGER NOT NULL,
  deadline_us        INTEGER NOT NULL,    -- lease expiry (INFLIGHT) | redeliver_at (WAITING)
  first_delivered_us INTEGER NOT NULL,
  last_cause         INTEGER NOT NULL,
  PRIMARY KEY (consumer_id, seq)
) STRICT, WITHOUT ROWID;

CREATE INDEX pending_due      ON pending(state, deadline_us);
CREATE INDEX pending_consumer ON pending(consumer_id, state, deadline_us);

CREATE TABLE resolved (                    -- sparse set strictly above ack_floor_seq
  consumer_id INTEGER NOT NULL REFERENCES consumer(id) ON DELETE CASCADE,
  seq         INTEGER NOT NULL,
  outcome     INTEGER NOT NULL,            -- 1=ACK 2=TERM 3=DEAD 4=SKIPPED(filtered)
  at_us       INTEGER NOT NULL,
  PRIMARY KEY (consumer_id, seq)
) STRICT, WITHOUT ROWID;

CREATE TABLE event (                       -- durable audit log; written in the same tx
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  at_us       INTEGER NOT NULL,
  kind        INTEGER NOT NULL,
  stream_id   INTEGER, consumer_id INTEGER, seq INTEGER,
  msg_id      TEXT, trace_id TEXT,
  cause       INTEGER, attempt INTEGER,
  detail      TEXT                          -- JSON, small
) STRICT;
CREATE INDEX event_msg ON event(msg_id, id);
CREATE INDEX event_at  ON event(at_us);
```

**Why `event` lives in the same transaction as the state change:** it costs no additional
fsync (same commit), and it makes the audit log crash-consistent with the state by
construction. That is what licenses invariant **I13** ("the log is the model") to be checked
at runtime, and it makes `messq trace` work after a restart. Write amplification is ~2× in
bytes, ~0× in syncs. Default `audit=full`, event retention 7 days. `audit=off` is available,
logs a startup WARNING, and disables I13 checking — documented as such.

### 3.4 The `Store` interface

Deliberately narrow so `memstore` (used by `sim/logic` and property tests) is a faithful,
~300-line implementation:

```go
type Store interface {
    Load(ctx) (*core.State, error)          // full recovery snapshot
    Commit(ctx, batch WriteBatch) error     // atomic; returns only after durability
    ReadMessage(ctx, stream, seq) (Message, error)
    ScanMessages(ctx, stream, from, filter, limit) iter.Seq2[Message, error]
    ScanEvents(ctx, q EventQuery) iter.Seq2[Event, error]
    Snapshot(ctx) (StateView, error)        // for invariant checking
    IntegrityCheck(ctx, deep bool) error
    Close() error
}
```

`WriteBatch` is a plain struct of row deltas (`[]MessageInsert`, `[]PendingUpsert`,
`[]PendingDelete`, `[]ResolvedInsert`, `[]ConsumerUpdate`, `[]EventInsert`, `StreamUpdate`).
It is data, so it can be logged, diffed with `go-cmp`, replayed, and fuzzed.

**Loading full state at startup is intentional.** Pending sets and consumer metadata are
small (bounded by `max_ack_pending × consumers`); messages are *not* loaded, only indexed by
`(first_seq, next_seq)`. Startup cost is O(pending), not O(stream).

### 3.5 Crash recovery procedure

On `messq serve` open, in order, all logged at INFO with timings:

1. **Integrity.** `PRAGMA quick_check` (default) or `integrity_check` (`--deep-check`). Failure
   ⇒ refuse to start, print the exact recovery instructions, exit 70.
2. **Schema version.** Compare `meta.schema_version`; run forward migrations inside one
   transaction; refuse to open a newer schema. Every migration has a committed golden data
   dir fixture (§8.11).
3. **Load** streams, consumers, `pending`, `resolved` into `core.State`.
4. **Void crashed leases.** Every `pending` row with `state=INFLIGHT` belongs to a lease held
   by a process that no longer exists. Set `state=WAITING`, `deadline_us=now`,
   `last_cause=CRASH_RECOVERY`. **`delivery_count` is not incremented** — it was already
   incremented at delivery time, so `max_deliver` still bounds a crash-loop. Emit one
   `msg.lease_void` event per row.
5. **Verify.** Run the cheap invariant subset (I1–I7, I10–I12) over the loaded state. A
   violation ⇒ refuse to start unless `--force-start`, which logs at ERROR and records a
   `startup.forced` event. This is the difference between "we hope it's fine" and evidence.
6. **Rearm.** Rebuild the timer wheel from `pending_due`; start actors; open the listener last.

### 3.6 fsync policy: three named durability modes

| Mode | SQLite | Batching | Publish returns after | Survives SIGKILL | Survives power loss |
|---|---|---|---|---|---|
| `strict` | `synchronous=FULL` | none (1 tx per call) | its own fsync | yes | yes |
| `group` **(default)** | `synchronous=FULL` | 2 ms / 256 cmds | its batch's fsync | yes | yes |
| `relaxed` | `synchronous=NORMAL` | 2 ms / 256 cmds | WAL write, no fsync | yes | **no** |

`group` gives the *same guarantee* as `strict` — every acknowledged publish is fsynced before
the client is told — while amortising the fsync across a batch. This is the right default:
strong and fast, no semantic asterisk.

`relaxed` maps to the documented behaviour of WAL + `synchronous=NORMAL`: transactions remain
durable across *application* crashes but a committed transaction may roll back after an OS
crash or power loss. It is opt-in, prints a WARNING banner at startup, is reported by
`messq doctor`, and — critically — **the crash-test oracle is parameterized by mode** (§8.7):
under SIGKILL, `relaxed` must lose nothing; only under injected power-loss (LazyFS) may it
lose unsynced commits, and even then it must not corrupt.

Message size cap: 1 MiB default, 8 MiB hard maximum, enforced at the API edge and asserted by
a fuzz target. Larger payloads belong in object storage with a pointer in the message; we say
so in the docs rather than growing an untestable blob-chunking path.

---

## 4. Delivery semantics & message lifecycle

### 4.1 Sequence and identity

- `seq`: per-stream, gap-free, monotonically increasing from 1. Assigned inside the commit
  transaction. Never reused, even after purge (`first_seq` advances; `next_seq` never rewinds).
- `msg_id`: ULID assigned by the server, or supplied by the client for publish idempotency.
  A duplicate `msg_id` within a stream returns the original `seq` with `duplicate=true` and
  logs `publish.dedup`.
- `trace_id`: propagated from the `messq-trace-id` header or generated; stamped on the message
  and on **every** event about it.

### 4.2 State machine (per consumer × per message)

The state of message `seq` for consumer `c` is exactly one of six. This is a **partition** —
that is invariant I1.

```
                 seq >= c.cursor_seq
                 (never leased)
                 ┌───────────────┐
                 │  UNDELIVERED  │
                 └───────┬───────┘
                         │ Fetch  [inflight < max_ack_pending]
                         │ [ordered ⇒ no unresolved lower seq on same subject]
                         │ delivery_count = 1, cause=first
                         ▼
        ┌────────────────────────────────┐
   ┌───►│           INFLIGHT             │◄──┐
   │    │ lease: deadline = now+ack_wait │   │
   │    └──┬────────┬────────┬───────┬───┘   │
   │       │Ack     │Nak     │Term   │Extend │  Fetch (redelivery)
   │       │        │        │       └───────┘  delivery_count++
   │       │        │        │                  cause = nak|ack_timeout|crash_recovery
   │       ▼        │        ▼                  │
   │  ┌────────┐    │   ┌────────┐              │
   │  │ ACKED  │    │   │  TERM  │              │
   │  └────────┘    │   └────────┘              │
   │                │  (terminal, no redeliver) │
   │                ▼                           │
   │      delivery_count >= max_deliver?        │
   │        no │            │ yes               │
   │           ▼            ▼                   │
   │   ┌──────────────┐  ┌─────────────────┐    │
   └───┤   WAITING    │  │      DEAD       │    │
       │ redeliver_at │  │ copied to DLQ   │    │
       │ = now+backoff│  │ (or held)       │    │
       └──────┬───────┘  └─────────────────┘    │
              │ clock reaches redeliver_at      │
              └─────────────────────────────────┘

  ack_wait elapses while INFLIGHT ──► same branch as Nak, cause=ack_timeout
  process crash while INFLIGHT    ──► WAITING, redeliver_at=now, cause=crash_recovery
                                       (delivery_count unchanged)
```

Terminal states: `ACKED`, `TERM`, `DEAD`. Terminal states advance `ack_floor_seq` when they
close the gap from the floor; entries above the floor live in `resolved`.

### 4.3 Transition table (the normative spec — mirrored 1:1 by the conformance suite)

| From | Trigger | Guard | To | `delivery_count` | Event |
|---|---|---|---|---|---|
| UNDELIVERED | Fetch | inflight < max_ack_pending; ordering ok; subject matches filter | INFLIGHT | := 1 | `msg.deliver cause=first` |
| UNDELIVERED | Fetch | subject does not match filter | SKIPPED (resolved) | — | `msg.skip` |
| INFLIGHT | Ack(token) | token epoch current | ACKED | — | `msg.ack` |
| INFLIGHT | Ack(token) | token epoch stale | *no change* | — | `ack.stale` (WARN) |
| INFLIGHT | Nak(token, delay) | epoch current, `dc < max_deliver` | WAITING | — | `msg.nak cause=nak` |
| INFLIGHT | Nak(token, delay) | epoch current, `dc >= max_deliver` | DEAD | — | `msg.dead cause=max_deliver` |
| INFLIGHT | Term(token) | epoch current | TERM | — | `msg.term` |
| INFLIGHT | Extend(token, d) | epoch current, total ≤ `max_ack_wait_total` | INFLIGHT | — | `msg.extend` |
| INFLIGHT | deadline reached | `dc < max_deliver` | WAITING | — | `msg.timeout cause=ack_timeout` |
| INFLIGHT | deadline reached | `dc >= max_deliver` | DEAD | — | `msg.dead cause=max_deliver` |
| INFLIGHT | recovery | (startup) | WAITING(now) | unchanged | `msg.lease_void cause=crash_recovery` |
| WAITING | redeliver_at reached + Fetch | flow control ok | INFLIGHT | += 1 | `msg.deliver cause=<last_cause>` |
| ACKED/TERM/DEAD | any ack | — | *no change*, returns OK | — | `ack.duplicate` (DEBUG) |
| any | Seek/Reset | — | UNDELIVERED (gen++) | := 0 | `consumer.seek` |

`max_deliver` is checked **at the point of failure**, not at the point of redelivery. This
matters: with `max_deliver=1` a message is delivered once and then dead-lettered on
timeout/nak, which is what users expect.

**Divergence from JetStream, deliberate:** in JetStream, exceeding `MaxDeliver` merely stops
redelivery and the message remains in the stream. messq instead defaults to
`on_exhausted=dlq`: the message is copied to the configured DLQ stream (a normal stream, so
DLQ inspection and redrive are the same features as everything else) and marked DEAD for the
consumer. `on_exhausted=hold` reproduces the JetStream behaviour for people who want it.
Rationale: the documented operational failure is DLQs that nobody watches; making the DLQ a
first-class stream means lag metrics and alerts apply to it automatically.

### 4.4 Ack tokens are epoch-qualified — the single most important safety rule

```
AckToken := "c" <consumer_id> "." <seq> "." <delivery_count> "." <generation>
e.g.       c7.10493.2.1
```

An ack is applied **only if** `delivery_count` and `generation` match the current `pending`
row. Otherwise: `CodeFailedPrecondition` with `reason=STALE_TOKEN`, and a `ack.stale` WARN
event.

Why this is non-negotiable: worker A is handed attempt 1, stalls past `ack_wait`, the message
is redelivered to worker B as attempt 2. Worker A finally finishes and acks. With an
unqualified `(consumer, seq)` ack, A's ack retires the message **while B is still processing
it** — B's subsequent ack is a no-op, and if B fails, the failure is invisible. This is the
classic at-least-once double-processing bug and it is *silent*. Epoch-qualified tokens make it
loud, observable (`ack.stale` metric), and testable (invariant I11, and a dedicated
`TestStaleAckDoesNotRetireLiveLease` property).

The token is human-readable on purpose: an operator debugging a stuck consumer can read
attempt numbers straight out of a log line, and `messq trace` can be driven from a token.

### 4.5 Ordering

`ordered=true` on a consumer means: **at most one unresolved message per subject at a time**.
The actor will not lease `seq` for subject `s` while any lower `seq'` on subject `s` is
UNDELIVERED/INFLIGHT/WAITING. Head-of-line blocking is the explicit, documented cost;
`max_deliver` + DLQ bounds it. This is invariant I8 and it is checked continuously in the
simulator.

### 4.6 Flow control

- `max_ack_pending`: hard cap on INFLIGHT per consumer. Fetch returns fewer messages, or zero
  with `reason=MAX_ACK_PENDING`, incrementing `messq_flow_control_holds_total`.
- `max_bytes` / `max_messages` per Fetch: client-side cap.
- `expires`: long-poll duration; server returns an empty batch at expiry rather than hanging.
- Backoff schedule per consumer (`backoff_us` JSON array) applied per attempt, last value
  repeating — mirroring JetStream's `BackOff`, with the documented warning to size
  `max_deliver` against the schedule length.

### 4.7 The duplicate contract

messq is at-least-once. Duplicates are permitted, but **only with a recorded cause.** The
normative statement, which is a test:

> In a run with no injected faults, where every delivery is acked strictly within `ack_wait`,
> every message is delivered **exactly once** to each consumer. Any `delivery_count > 1`
> must be preceded in the event log by a `msg.nak`, `msg.timeout`, or `msg.lease_void` event
> for that `(consumer, seq)`.

`TestNoDuplicateWithoutCause` asserts this over every simulator run. It is the single
strongest thing we can say about an at-least-once system, and it is exactly what operators
actually want to know.

### 4.8 The thirteen invariants

Implemented in `internal/invariant`, each as `func(StateView) []Violation`, each with a stable
ID used in `messq verify` output, log messages, and test names.

| ID | Name | Statement |
|---|---|---|
| **I1** | Partition | For every consumer `c` and every `seq` in `[stream.first_seq, stream.next_seq)`, `seq` is in exactly one of: UNDELIVERED, INFLIGHT, WAITING, ACKED, TERM, DEAD |
| **I2** | No premature GC | A `message` row exists for every `seq` referenced by any `pending` row |
| **I3** | Retention safety | `stream.first_seq` only advances past `seq` when `retention='limits'` and a limit was hit, or every consumer has resolved `seq` (`retention='all_resolved'`) |
| **I4** | Lease exclusivity | At most one INFLIGHT row per `(consumer, seq)`; `deadline_us > 0` |
| **I5** | Attempt monotonicity | `delivery_count` never decreases within a generation, and `1 <= delivery_count <= max_deliver` for any non-terminal row |
| **I6** | Delivery bound | No `msg.deliver` event exists with `attempt > max_deliver` at the time of emission |
| **I7** | Flow control | `count(pending where state=INFLIGHT and consumer=c) <= c.max_ack_pending` |
| **I8** | Ordering | For an `ordered` consumer, no two unresolved `seq` share a subject |
| **I9** | Durability | Every `seq` for which a `PublishResponse` was returned OK is present after recovery (checked by the crash harness against the external ledger) |
| **I10** | Floor monotonicity | `ack_floor_seq` never decreases within a generation; no `pending`/`resolved` row exists at or below it |
| **I11** | Epoch safety | No ACK/TERM was applied whose token epoch differed from the row's `(delivery_count, generation)` |
| **I12** | DLQ conservation | Each `(consumer, seq)` enters DEAD at most once, and each DEAD has exactly one corresponding DLQ message (when `on_exhausted=dlq`) |
| **I13** | Log ≡ state | Folding the full `event` table from the beginning reproduces the persisted `(consumer, pending, resolved, stream)` state exactly |

I1–I8, I10, I12 are pure state predicates and are cheap enough to run after **every**
transition in the simulator. I13 is O(events) and runs at end-of-scenario and in
`messq verify --deep`. I9 and I11 need history and are checked by the crash harness and the
simulator's event log respectively.

---

## 5. API / protocol

### 5.1 Decision: Connect-RPC over HTTP, protobuf schema, pull-based delivery

**One protocol, one port, one schema.** `connectrpc.com/connect` serves the Connect protocol,
gRPC, and gRPC-Web from the same `net/http` handler, which means:

- **Testability:** handlers are plain `http.Handler`s, so `httptest.Server` covers the whole
  transport with no special harness, and the same tests run over h2c and HTTP/1.1.
- **Ops:** every RPC is also a plain `POST` with a JSON body, so `curl` is a first-class
  client and every log line in the docs can be reproduced without our binary.
- **No hand-rolled framing:** framing bugs are a classic broker CVE source. We delete that
  category and keep only protobuf decoding as a fuzz target.
- **Schema compatibility becomes a CI gate** via `buf breaking` against the `main` branch.

Server setup follows the documented pattern: generated `NewMessqServiceHandler` mounted on an
`http.ServeMux`, with `http.Protocols` configured for HTTP/1 and unencrypted HTTP/2 so gRPC
clients work without TLS on localhost.

**Delivery is pull-only.** `Fetch` is a server-streaming RPC; there is no push subscription.
This is a testability decision as much as a semantics one: push requires server-side
per-subscriber state, credit accounting, and a slow-consumer eviction policy — three stateful
subsystems whose failure modes are hard to enumerate. Pull makes flow control an argument to
a request. Long-polling (`expires`) gives push-like latency (sub-millisecond when messages are
waiting) without the state.

### 5.2 Service sketch

```protobuf
// api/messq/v1/messq.proto
syntax = "proto3";
package messq.v1;

service MessqService {
  // --- admin ---
  rpc CreateStream(CreateStreamRequest) returns (Stream);
  rpc GetStream(GetStreamRequest) returns (Stream);
  rpc ListStreams(ListStreamsRequest) returns (ListStreamsResponse);
  rpc DeleteStream(DeleteStreamRequest) returns (DeleteStreamResponse);
  rpc CreateConsumer(CreateConsumerRequest) returns (Consumer);
  rpc GetConsumer(GetConsumerRequest) returns (Consumer);   // includes lag/backlog
  rpc ListConsumers(ListConsumersRequest) returns (ListConsumersResponse);
  rpc DeleteConsumer(DeleteConsumerRequest) returns (DeleteConsumerResponse);

  // --- data plane ---
  rpc Publish(PublishRequest) returns (PublishResponse);          // batch
  rpc Fetch(FetchRequest) returns (stream Delivery);              // pull, long-poll
  rpc Ack(AckRequest) returns (AckResponse);                      // batch, per-token status
  rpc Extend(ExtendRequest) returns (ExtendResponse);             // "in progress"

  // --- operations ---
  rpc Seek(SeekRequest) returns (SeekResponse);
  rpc Purge(PurgeRequest) returns (PurgeResponse);
  rpc Redrive(RedriveRequest) returns (stream RedriveProgress);   // DLQ → stream, rate-limited
  rpc Peek(PeekRequest) returns (stream Message);                 // non-destructive
  rpc Trace(TraceRequest) returns (stream Event);                 // full lifecycle by msg_id/seq
  rpc Verify(VerifyRequest) returns (VerifyResponse);             // run invariants online
  rpc Info(InfoRequest) returns (InfoResponse);                   // version, mode, uptime, gates
}

message PublishRequest {
  string stream = 1;
  repeated PublishMessage messages = 2;     // atomic: all or none
  string trace_id = 3;
}
message PublishMessage {
  string subject = 1;
  bytes  body = 2;
  map<string,string> headers = 3;
  optional string msg_id = 4;               // dedup key
}
message PublishResponse {
  repeated PublishResult results = 1;       // { seq, msg_id, duplicate }
  uint64 stream_first_seq = 2;
  uint64 stream_next_seq  = 3;
}

message FetchRequest {
  string stream = 1; string consumer = 2;
  uint32 max_messages = 3;                  // default 1, max 1024
  uint64 max_bytes = 4;                     // default 4 MiB
  google.protobuf.Duration expires = 5;     // long-poll, default 30s, max 5m
  bool   no_wait = 6;
}
message Delivery {
  Message msg = 1;
  string  ack_token = 2;                    // "c7.10493.2.1"
  uint32  delivery_count = 3;
  DeliveryCause cause = 4;                  // FIRST | NAK | ACK_TIMEOUT | CRASH_RECOVERY | REPLAY | REDRIVE
  google.protobuf.Timestamp lease_deadline = 5;
}
message AckRequest {
  repeated AckItem items = 1;               // { ack_token, outcome, optional delay, optional reason }
}
message AckResponse {
  repeated AckItemResult results = 1;       // { status: OK | STALE | UNKNOWN | ALREADY_RESOLVED }
}
```

`Ack` returning **per-item status** rather than failing the whole batch is deliberate: a
partial batch failure that aborts the rest is how ack storms start.

### 5.3 Error model

Connect's gRPC-compatible codes, used precisely and asserted in the conformance suite:

| Situation | Code | `reason` detail |
|---|---|---|
| Unknown stream/consumer | `NotFound` | `STREAM_NOT_FOUND` / `CONSUMER_NOT_FOUND` |
| Stale ack token | `FailedPrecondition` | `STALE_TOKEN` |
| Malformed token / bad subject | `InvalidArgument` | `BAD_TOKEN` / `BAD_SUBJECT` |
| Payload over limit | `InvalidArgument` | `MESSAGE_TOO_LARGE` |
| `max_ack_pending` reached | *(not an error)* | empty batch, `hold_reason=MAX_ACK_PENDING` |
| Disk full / store failure | `ResourceExhausted` | `STORE_FULL` |
| Store integrity failure | `DataLoss` | `INTEGRITY` |
| Shutting down | `Unavailable` | `DRAINING` |

Interceptors, in order: recovery → trace-id extraction/injection → protovalidate → structured
access log → metrics. Each is independently unit-tested; the ordering itself is asserted by a
test, because "we reordered the interceptors and lost all trace IDs" is a real class of bug.

### 5.4 Subject syntax

Dot-separated tokens, `*` matches one token, `>` matches one-or-more trailing tokens
(JetStream-compatible, so people's mental models transfer). The matcher is a pure function
with a dedicated fuzz target and a property test (`match(pattern, subject)` against a
deliberately naive reference implementation).

---

## 6. CLI & developer experience

`messq` is one binary; the daemon is `messq serve`. Built with `spf13/cobra`.

### 6.1 Command surface

```
messq serve      --data-dir /var/lib/messq --listen 127.0.0.1:4711
                 --durability group|strict|relaxed --audit full|transitions|off
                 --log-format console|json --dev
messq stream     create|list|info|delete|purge
messq consumer   create|list|info|delete|seek|reset
messq pub        <stream> <subject> [--data @file|-] [--id KEY] [--header k=v] [--count N]
messq sub        <stream> <consumer> [--batch N] [--exec CMD] [--json] [--ack-wait 30s]
messq peek       <stream> [--seq N] [--count N] [--subject pat] [--since 1h]
messq trace      <msg-id|seq|ack-token>
messq lag        [<stream> [<consumer>]]
messq dlq        list|inspect|redrive
messq replay     <stream> <consumer> --from seq:N|time:T|start
messq verify     [--data-dir D | --addr URL] [--deep] [--json]
messq doctor
messq sim        run|replay|shrink
messq conform    <addr>
messq bench      publish|roundtrip
messq completion bash|zsh|fish
```

### 6.2 The DX decisions that matter

**`messq sub --exec` — shell-first consumers.** Exit code is the ack decision, following
`sysexits.h` so it composes with existing scripts:

```
exit 0   → ACK
exit 75  → NAK  (EX_TEMPFAIL, honours the consumer's backoff)
exit 65  → TERM (EX_DATAERR, poison message, no redelivery)
any other→ NAK
```

```bash
messq sub orders workers --batch 8 --exec ./handle.sh
# handle.sh receives the body on stdin and metadata in MESSQ_* env vars:
#   MESSQ_MSG_ID MESSQ_SEQ MESSQ_SUBJECT MESSQ_DELIVERY_COUNT MESSQ_CAUSE MESSQ_TRACE_ID
```

This makes the "better than shell pipes" use case a one-liner on day one and gives us an
end-to-end test surface that exercises the full stack with a 3-line shell script.

**`messq trace` is the flagship.** Output is a timeline, not a log dump:

```
$ messq trace 01J8ZQ4K2M9V0X7Y3B5N6C8D1E
message 01J8ZQ4K2M9V0X7Y3B5N6C8D1E   stream=orders subject=orders.created seq=10493
trace_id=4f1c…9ab  size=412B  published 2026-08-21T09:14:02.114Z

  09:14:02.114  publish        seq=10493
  09:14:02.190  deliver        consumer=workers attempt=1 cause=first        lease→09:14:32.190
  09:14:32.190  timeout        consumer=workers attempt=1 cause=ack_timeout   (no ack in 30s)
  09:14:33.190  deliver        consumer=workers attempt=2 cause=ack_timeout  lease→09:15:03.190
  09:14:41.002  ack.stale      consumer=workers token=c7.10493.1.1  REJECTED (attempt 1 is stale)
  09:14:44.771  nak            consumer=workers attempt=2 reason="upstream 503"  retry→09:14:54
  09:14:54.780  deliver        consumer=workers attempt=3 cause=nak          lease→09:15:24.780
  09:14:55.310  ack            consumer=workers attempt=3   ✓ resolved in 53.2s, 3 attempts

  duplicates: 2  — all accounted for (1× ack_timeout, 1× nak)
```

That last line is the product in one sentence. The `ack.stale` line is a bug in the *user's*
worker that most brokers would never surface.

**`messq doctor`** runs: integrity check, all invariants, durability-mode sanity, disk space
headroom vs. retention, consumers whose `ack_wait` is below observed p99 processing time
(the documented visibility-timeout foot-gun), DLQ depth with no recent drain, and consumers
with `max_deliver` inconsistent with their backoff schedule length. Human-readable output with
a `--json` mode for monitoring.

**`messq serve --dev`** — in-memory store, auto-create streams/consumers on first use, console
logging at DEBUG, `ack_wait=5s`. Zero-config first five minutes.

**`messq sim replay <seedfile>`** — an operator or contributor can attach a seed to an issue,
and any maintainer reproduces the exact failure locally. Bug reports become executable.

### 6.3 CLI quality rules

- Every command supports `--json`; human output is never parsed by our own tests, the JSON is.
- Exit codes are a documented, tested contract: `0` ok, `1` generic, `2` usage, `3`
  invariant violation, `4` not found, `5` server unavailable, `70` data integrity.
- No command mutates state without either an explicit `--yes` or an interactive confirmation;
  `purge`, `seek`, `delete`, and `redrive` print a dry-run summary first.
- `messq redrive` is **rate-limited by default** (`--rate 10/s`) and defaults to
  `--limit 100`. The documented failure mode is bulk-redriving a DLQ back into a still-broken
  system; the default should make that hard, not easy.
- Shell completion for all four shells via cobra's generators, with completion output itself
  covered by a `testscript` golden test.

---

## 7. Observability & logging design

### 7.1 Events are data first, logs second

The canonical event is a row in the `event` table, written transactionally with the state
change. The logger is a *projection* of that stream. This inverts the usual arrangement and
buys us I13.

Event kinds (closed enum, `exhaustive` linter enforces every switch handles all of them):

```
publish  publish.dedup  publish.reject
deliver  ack  nak  term  extend  timeout  lease_void  skip  dead  redrive
ack.stale  ack.duplicate
consumer.create  consumer.delete  consumer.seek  consumer.reset
stream.create  stream.delete  stream.purge  retention.delete
startup  startup.forced  shutdown  integrity.fail  invariant.violation
```

### 7.2 Canonical field set

Every event carries, where applicable: `msg_id`, `trace_id`, `stream`, `subject`, `seq`,
`consumer`, `attempt`, `cause`, `lease_deadline`, `outcome`, `reason`, `duration_ms`,
`ack_token`. Field names are frozen at v1.0 and covered by a golden test — renaming a log
field breaks operators' dashboards and must be a deliberate, versioned act.

### 7.3 Two handlers, both tested

- **`json`** (default for `serve`): `slog.NewJSONHandler` with a `ReplaceAttr` that normalises
  levels and pins the time format. Line-delimited, journald/Loki friendly.
- **`console`**: a custom `slog.Handler` producing the aligned, colourised, human-first output
  used in `messq trace` and `--dev`.

The custom handler is validated with **`testing/slogtest`** — `slogtest.TestHandler` checks
the whole `slog.Handler` contract (group qualification, empty-group elision, zero-time
handling, attribute resolution). Writing a handler without it is how subtly-wrong logs happen.

### 7.4 Sampling and the anti-flood rule

High-volume events (`deliver`, `ack`) are always written to the `event` table but are
**sampled** in the stdout log when rate exceeds `--log-sample-threshold` (default 1000/s),
emitting a `log.sampled` summary line with counts. Low-volume, high-value events (`nak`,
`timeout`, `dead`, `ack.stale`, `invariant.violation`, `integrity.fail`) are **never** sampled.
The sampler is a pure function with its own property test: *no event in the never-sample set
is ever dropped, for any input rate*.

### 7.5 Metrics

`prometheus/client_golang` with an **explicit, non-default registry** created in `main` and
injected — never `prometheus.DefaultRegisterer`. That makes metrics unit-testable in isolation
and prevents accidental global state in tests.

```
messq_published_total{stream,subject}
messq_delivered_total{stream,consumer,cause}          # cause label is the duplicate story
messq_acked_total{stream,consumer}
messq_nak_total{stream,consumer}
messq_ack_timeouts_total{stream,consumer}
messq_stale_acks_total{stream,consumer}               # alert on any nonzero rate
messq_dead_total{stream,consumer,reason}
messq_pending{stream,consumer,state}                  # gauge: inflight / waiting
messq_consumer_lag{stream,consumer}                   # next_seq - cursor_seq
messq_ack_floor_age_seconds{stream,consumer}          # oldest unresolved — the real SLI
messq_delivery_attempts_bucket{stream,consumer}       # histogram, catches redelivery storms
messq_publish_duration_seconds{durability_mode}       # histogram
messq_commit_fsync_duration_seconds
messq_store_size_bytes / messq_wal_size_bytes
messq_invariant_violations_total{id}                  # MUST be zero; page on any increase
messq_build_info{version,commit,go_version,durability_mode}
```

`messq_ack_floor_age_seconds` is the metric to alert on. Lag alone is misleading: a consumer
can have zero lag and one poisoned message from three days ago pinned below its floor.

Metrics are tested with `prometheus/testutil`: `CollectAndCompare` against golden exposition
text in unit tests, and `ScrapeAndCompare` against the live `/metrics` endpoint in the
conformance suite. A lint test asserts every metric has a non-empty `Help` and follows naming
conventions.

### 7.6 Ops runbook artifacts (shipped, not aspirational)

`docs/runbook.md` with one section per alert, each linking to the `messq` command that
diagnoses it. A `docs/guarantees.md` table where every row names the invariant ID and the CI
job that proves it. If a row cannot name both, the guarantee does not ship.

---

## 8. Testing strategy

This is the section the rest of the plan exists to enable. Twelve layers, each with an owner
package, a CI job, and a stated purpose. **A layer without a named CI job does not count.**

### 8.0 Ground rules

1. **`-race` is not optional.** `GOFLAGS=-race` for every test job. A package that cannot run
   under `-race` does not merge.
2. **`time.Sleep` is banned in tests** outside `test/soak`. Enforced by a `forbidigo` rule in
   `.golangci.yml` applying to `_test.go`. Timing is either the fake clock (`seam.Clock`) or a
   `testing/synctest` bubble. This single rule eliminates the dominant source of flakes.
3. **Zero flake tolerance.** A test that fails twice in 30 days without a corresponding code
   change is fixed within one week or deleted. No `t.Skip` on `main`. A nightly
   `go test -count=20 -race ./...` job exists specifically to expose flakes early.
4. **No fix without a harness-generated failing test.** Every bug fix must be preceded by a
   committed artifact — a `rapid` failfile, a sim seed, or a fuzz corpus entry — that
   reproduces it. The regression corpus grows monotonically and is committed to the repo.
5. **Coverage gates:** `internal/core` ≥ 95% statements, `internal/invariant` = 100%,
   `internal/store` ≥ 90%, repo ≥ 80%. Gates ratchet upward only.

### 8.1 Layer 0 — Static analysis

`golangci-lint` with a deliberately strong set: `errcheck`, `govet` (incl. `copylocks`,
`loopclosure`), `staticcheck`, `gosec`, `exhaustive` (critical — our state and event enums
must be handled exhaustively everywhere), `forbidigo` (bans `time.Sleep` in tests,
`prometheus.DefaultRegisterer`, `time.Now` outside `seam`), `nilerr`, `bodyclose`,
`sqlclosecheck`, `rowserrcheck`, `containedctx`, `errorlint`. Plus `go vet`, `govulncheck`,
and `buf lint` + `buf breaking` on the proto. The `time.Now`-outside-`seam` ban is what keeps
the three-seam architecture from eroding.

### 8.2 Layer 1 — Unit tests on the pure core

Table-driven, one case per row of the §4.3 transition table, plus boundary cases
(`max_deliver=0`, `max_deliver=1`, `max_ack_pending=1`, empty backoff, backoff shorter than
`max_deliver`). `go-cmp` for diffs — `cmp.Diff` on `State` structs gives failure output an
engineer can read, which is why we skip an assertion DSL entirely.

**Mutation testing** on `internal/core` and `internal/invariant` with `go-gremlins/gremlins`,
nightly, target mutation score ≥ 75%. Purpose: coverage says the line ran; mutation score says
the test would have *noticed*. It runs on smallish modules well, which our core is by
construction. (It is a 0.x tool; pinned by version, and a red score is advisory-with-review
rather than a hard merge block.)

### 8.3 Layer 2 — Property & model-based testing (`pgregory.net/rapid`)

The reference model (`internal/core/model`) is a naive, obviously-correct in-memory
implementation: maps and slices, no optimisation, no persistence, ~300 lines. Its only job is
to be *readable enough to be trusted by inspection*.

```go
func TestBrokerModel(t *testing.T) {
    rapid.Check(t, func(t *rapid.T) {
        sut, ref := newSUT(t), newRef()
        t.Repeat(map[string]func(*rapid.T){
            "publish":      func(t *rapid.T) { /* draw subject/body/msg_id, apply to both */ },
            "fetch":        func(t *rapid.T) { /* draw batch size, compare delivery sets */ },
            "ack":          func(t *rapid.T) { /* draw a token: valid, stale, or forged */ },
            "nak":          func(t *rapid.T) { /* draw delay */ },
            "term":         func(t *rapid.T) { /* … */ },
            "extend":       func(t *rapid.T) { /* … */ },
            "advance_time": func(t *rapid.T) { /* draw duration, tick both clocks */ },
            "restart":      func(t *rapid.T) { /* reload SUT from store, ref unchanged */ },
            "seek":         func(t *rapid.T) { /* … */ },
            "purge":        func(t *rapid.T) { /* … */ },
            "":             func(t *rapid.T) { // runs after every action
                invariant.CheckAll(t, sut.View())
                requireEquivalent(t, sut, ref)   // observational equivalence
            },
        })
    })
}
```

The `""` action is rapid's invariant hook — it runs after every other action, so all thirteen
invariants plus model equivalence are checked at *every* intermediate state, not just at the
end. `restart` inside the action set is the key move: it forces persistence and recovery into
the property loop rather than into a separate, weaker test.

Configuration: `-rapid.checks=200` on PRs (fast), `-rapid.checks=20000` nightly,
`-rapid.shrinktime=2m`. Rapid minimises failures automatically and writes a failfile under
`.rapid/`; **failfiles are committed** to `testdata/regressions/rapid/` and replayed on every
run via `-rapid.failfile`. That is the regression corpus from ground rule 4.

Additional standalone properties:
- `TestSubjectMatchProperty` — optimised matcher ≡ naive matcher.
- `TestAckTokenRoundTrip` — parse ∘ format = identity; no forged token parses.
- `TestBackoffSchedule` — attempt *n* delay is monotone non-decreasing, last value repeats.
- `TestWriteBatchIdempotent` — applying the same batch twice is a no-op (needed for recovery).

### 8.4 Layer 3 — Deterministic simulation testing

Two tiers, because honesty matters more than purity.

**Tier A: `sim/logic`** — fully deterministic, in-process, no OS. Virtual clock, seeded PRNG,
`memstore`. Because the stream actor's body is a synchronous function, the simulator calls it
**directly**; there is no goroutine scheduling, so determinism is total and speed is
~10⁶ operations/second. The simulator:

- generates a workload (publishers, consumers with varied `ack_wait`/`max_deliver`/`backoff`,
  ordered and unordered, subject fan-out),
- interleaves scheduled events (delivery, lease expiry, retention, restart) by virtual time,
- injects logical faults: client disconnect mid-fetch, slow consumer, duplicate ack, late ack,
  clock jump forward/backward, restart-from-store,
- runs `invariant.CheckAll` after every step,
- on violation, prints the seed and dumps a minimised event trace.

Run shape: PR = 50 seeds × 10 000 steps. Nightly = 5 000 seeds × 100 000 steps. Release gate =
cumulative 10⁷ operations clean.

**Tier B: `sim/durable`** — the same generated scenario script, replayed against the *real*
SQLite store in a subprocess, with crash injection at fault points. Slower (~10⁴ ops/s), not
perfectly deterministic (the OS and SQLite are real), but it is the only thing that tests the
actual durability path. Nightly + release gate.

The scenario script is a serialisable artifact, so a Tier A failure can be promoted to Tier B
and vice versa. `messq sim run --seed N --steps M [--fault-profile crash|fs|clock]` ships in
the binary.

**Why not `gosim` or a source-translating simulator:** gosim is impressive (it runs etcd) but
it works by rewriting Go source to replace concurrency, OS, and syscall primitives, which
means the thing under test is not byte-identical to the thing we ship, and the toolchain
becomes a dependency of every developer's inner loop. We chose to pay for determinism in
architecture (the three seams, the synchronous actor body) rather than in tooling. Revisit if
we ever add clustering, where the argument flips.

### 8.5 Layer 4 — Concurrency testing

- **`testing/synctest`** (stdlib, stable since Go 1.25) for everything involving goroutines
  and timers: the batching window, long-poll expiry, timer wheel, graceful drain, interceptor
  timeouts. Inside a bubble the `time` package uses a fake clock that advances only when every
  goroutine in the bubble is durably blocked, which makes "wait 30 seconds for an ack timeout"
  an instantaneous, deterministic assertion. This is what makes ground rule 2 (no
  `time.Sleep`) practical rather than aspirational.
- **`-race`** on every job.
- **Linearizability**: `anishathalye/porcupine` over histories from a concurrent-client test.
  The model is the per-consumer pending set as an abstract object with `fetch`/`ack`/`nak`
  operations. Porcupine is the same checker etcd uses in its robustness tests, and its
  visualiser output is attached to CI artifacts on failure. Scope note: messq is single-node,
  so this checks our *concurrency*, not a consensus protocol — it catches "two concurrent
  fetches leased the same seq" and "ack applied out of order against a concurrent seek".
- **Deadlock watchdog**: a `TestMain` helper that dumps all goroutine stacks and fails if a
  test exceeds its budget, so a hang is a diagnosable failure rather than a CI timeout.

### 8.6 Layer 5 — Fault injection (`internal/fault`)

A hand-rolled ~150-line package behind build tag `messq_fault`, adopting `pingcap/failpoint`'s
**activation grammar** while rejecting its **codegen**:

```go
fault.Point("store.tx.before_commit")           // no-op, inlined away, in production builds
```
```
MESSQ_FAULTS="store.tx.before_commit=1*panic;store.wal.fsync=50%return(EIO)"
```

Grammar `[<percent>%][<count>*]<action>[(args)]` with actions `panic`, `return`, `sleep`,
`pause`, `error`. Rationale for not taking the dependency: failpoint's main advantage is
zero-cost production binaries via a source-rewriting `enable`/`disable` build step; a Go build
tag achieves the same zero cost with no rewriting, no `failpoint-ctl` in the developer inner
loop, and no risk of a half-transformed working tree. We take the good idea (env-driven,
counted, probabilistic activation) and skip the machinery.

Named points, exhaustively:

```
store.open.after_integrity          store.tx.begin
store.tx.before_write               store.tx.before_commit
store.wal.fsync                     store.tx.after_commit_before_reply
store.checkpoint                    store.migrate.mid
core.apply.before_effects           lease.timer.fire
wire.fetch.before_send              wire.fetch.mid_stream
wire.ack.before_apply               retention.before_delete
recovery.after_load                 recovery.before_void_leases
```

A meta-test asserts that **every declared fault point is exercised by at least one test**;
adding a point without a test fails CI. This prevents the classic decay where fault points
accumulate and nobody triggers them.

### 8.7 Layer 6 — Crash testing (the nine fault families)

`test/crash` launches a real `messq serve` subprocess against a real data directory and runs a
load generator that maintains an **external append-only ledger** (its own file, fsynced) with
three-valued outcomes:

- `OK` — the server returned success; the message **must** exist after recovery.
- `UNKNOWN` — the RPC was in flight when the process died; either outcome is legal.
- `FAILED` — the server returned an error before commit; the message must **not** exist.

Three-valued reconciliation is the only correct oracle for at-least-once under crashes. A
two-valued oracle either produces false failures or hides real loss.

The nine fault families, each a named CI job:

| # | Family | Mechanism | Oracle |
|---|---|---|---|
| 1 | Process kill | `SIGKILL` at random times and at each fault point | ledger + `verify` + I9 |
| 2 | Lost unsynced writes | LazyFS (FUSE, drops non-fsynced data) | `strict`/`group`: no `OK` lost. `relaxed`: loss allowed, corruption not |
| 3 | Torn writes | LazyFS torn-write injection | `integrity_check` clean or clean refusal to start |
| 4 | I/O errors | `dm-flakey` / injected `EIO` at `store.wal.fsync` | publish returns error, no `OK` lost, no corruption |
| 5 | Disk full | 32 MiB loopback ext4, fill it | `ResourceExhausted`, clean recovery after space freed |
| 6 | Clock skew | virtual clock jumps ±1 h / ±1 y (sim), `libfaketime` (e2e) | leases unaffected (monotonic); delayed msgs never early |
| 7 | Client disconnect | kill client mid-`Fetch`, mid-`Ack` | leases expire, messages redelivered, no leak |
| 8 | Slow consumer | consumer that never acks / acks at 0.9× and 1.1× `ack_wait` | flow control holds, no unbounded memory, `ack.stale` observed |
| 9 | Malformed input | fuzzing (§8.8) | no panic, no hang, no memory blowup |

LazyFS is the right tool here: it is purpose-built to simulate losing data written but not
fsynced, has been used to find real data-loss bugs in PostgreSQL, etcd, ZooKeeper, Redis and
LevelDB, and integrates with Jepsen. Families 2–5 run nightly (they need privileged/FUSE
runners); family 1 runs on every merge to `main` with 100 kill cycles.

### 8.8 Layer 7 — Fuzzing

Native `go test -fuzz` targets, corpora committed under `testdata/fuzz/`:

1. `FuzzPublishDecode` — protobuf `PublishRequest` bytes.
2. `FuzzSubjectMatch` — pattern × subject, differential against the naive matcher.
3. `FuzzAckToken` — token parsing; no input may panic or produce a valid-looking forged token.
4. `FuzzConfig` — config file parser.
5. **`FuzzCommandSequence`** — the deep one: fuzz bytes are decoded into a sequence of core
   commands and applied to `core.State`, asserting all invariants after each step. This turns
   the fuzzer into an unguided model checker and is historically the cheapest way to find
   state-machine bugs.

PR: 30 s per target (smoke). Nightly: 30 min per target. Every crasher is committed to the
corpus permanently.

### 8.9 Layer 8 — CLI & golden testing

`rogpeppe/go-internal/testscript` with `.txtar` scripts under `test/script/`. It is the
framework the Go team uses to test the `go` command: shell-like scripts, stdout/stderr
assertions, coverage integration, and `-update` for regenerating golden files.

```
# test/script/dlq_redrive.txtar
exec messq serve --dev --data-dir $WORK/data &
exec messq stream create orders
exec messq consumer create orders workers --max-deliver 2 --dlq orders.dlq
exec messq pub orders orders.created --data 'boom'
exec messq sub orders workers --batch 1 --count 2 --exec 'exit 75'
exec messq dlq list orders.dlq
stdout 'orders.created.*attempts=2'
! exec messq dlq redrive orders.dlq --to orders   # must refuse without --yes
stderr 'refusing.*--yes'
```

Every documented CLI example in `README.md` and `docs/` is extracted and executed as a
testscript — documentation that lies fails CI.

### 8.10 Layer 9 — Conformance suite (the executable spec)

`test/conform` is a black-box suite that takes only an address and runs the entire §4.3
transition table plus the error model plus the metric names against a **running daemon**.
Shipped as `messq conform <addr>`.

Three jobs it does at once:
1. It *is* the normative specification — prose in `docs/semantics.md` links to test names.
2. It is the upgrade gate: run the previous release's conform binary against the new server.
3. It is the future compatibility harness if anyone ever writes a second implementation.

### 8.11 Layer 10 — Upgrade & migration testing

For every released schema version, a committed golden data directory fixture
(`testdata/datadirs/v3/`) containing a non-trivial state: pending messages, a DLQ, a
mid-backoff message, a consumer with a non-zero generation. Tests assert that the current
binary opens each fixture, migrates, passes `verify --deep`, and continues delivering
correctly. Downgrade is explicitly unsupported and must fail with a clear message — also
tested. This is the most commonly skipped test category and the most expensive to skip.

### 8.12 Layer 11 — Soak & performance

- `test/soak`: 4-hour run under `-race` with a mixed workload, random restarts, and continuous
  `verify`. The only package where `time.Sleep` is allowed. Nightly.
- Memory: `runtime/pprof` heap snapshots at intervals; a growth-rate assertion catches pending-
  set and goroutine leaks. A `TestNoGoroutineLeak` using `goleak`-style stack diffing after
  every server shutdown in integration tests.
- Benchmarks with `benchstat` comparison against the previous nightly. > 20% regression opens
  an issue automatically. **Not** a merge gate — benchmark noise as a merge gate trains people
  to ignore gates.

### 8.13 CI pipeline

| Job | Trigger | Contents | Budget |
|---|---|---|---|
| `lint` | PR | golangci-lint, buf lint+breaking, govulncheck, gofmt | 2 min |
| `unit` | PR | `go test -race ./...`, coverage gates | 4 min |
| `property` | PR | rapid `-rapid.checks=200` + committed failfiles | 3 min |
| `sim-logic` | PR | 50 seeds × 10k steps | 4 min |
| `script` | PR | testscript CLI goldens | 2 min |
| `fuzz-smoke` | PR | 30 s × 5 targets | 3 min |
| `build-matrix` | PR | linux/amd64, linux/arm64, static, reproducible | 2 min |
| `crash-kill9` | merge to main | 100 kill/restart cycles, all 3 durability modes | 15 min |
| `conform` | merge to main | conformance + previous-release conform | 5 min |
| `upgrade` | merge to main | all golden data dir fixtures | 3 min |
| `sim-logic-deep` | nightly | 5 000 seeds × 100k steps | 2 h |
| `sim-durable` | nightly | subprocess + crash injection | 1 h |
| `nightly-lazyfs` | nightly | families 2–3 (FUSE runner) | 45 min |
| `nightly-fsfault` | nightly | families 4–5 (privileged runner) | 30 min |
| `fuzz-long` | nightly | 30 min × 5 targets | 2.5 h |
| `mutation` | nightly | gremlins on core + invariant | 1 h |
| `soak` | nightly | 4 h under `-race` | 4 h |
| `bench` | nightly | benchstat vs. previous | 20 min |
| `flake-hunt` | nightly | `go test -count=20 -race ./...` | 40 min |

PR total ≤ 8 minutes wall clock with parallel jobs. That number is a design constraint: a slow
PR pipeline is a pipeline people learn to bypass.

### 8.14 The v1.0 release gate (the operational definition of "enough")

All must be true, and `messq info --gates` prints the recorded evidence for the shipped build:

- [ ] All 13 invariants implemented, each with ≥ 1 test that fails when it is deliberately broken (mutation-verified).
- [ ] Cumulative ≥ 10⁷ simulated operations with zero invariant violations across ≥ 10 000 distinct seeds.
- [ ] All nine fault families green, with the crash harness at ≥ 10 000 total kill/restart cycles across the three durability modes.
- [ ] `internal/core` coverage ≥ 95%, mutation score ≥ 75%; `internal/invariant` coverage 100%.
- [ ] Every historical bug has a committed regression artifact (failfile, seed, or fuzz input).
- [ ] Conformance suite passes against the release binary on linux/amd64 and linux/arm64.
- [ ] Upgrade tests pass from every prior schema fixture.
- [ ] 72-hour soak with zero invariant violations, zero goroutine growth, bounded heap.
- [ ] Zero known flaky tests; zero `t.Skip` on `main`.
- [ ] `docs/guarantees.md`: every row names an invariant ID **and** a CI job.

---

## 9. Roadmap

Ordered milestones from the empty repository. The ordering is deliberate and unusual: **the
test harness precedes the product**, because retrofitting determinism is an order of magnitude
more expensive than designing for it.

### M0 — Foundations (week 1)
Repo skeleton, `go.mod` (Go 1.26), `.golangci.yml` with the full linter set, Makefile,
GitHub Actions with `lint`/`unit`/`build-matrix`, `seam` package (Clock, Rand + fake
implementations), `internal/fault` skeleton, ADR directory.
**Exit:** an empty repo that already fails CI on a `time.Sleep` in a test file.

### M1 — The model, not the server (weeks 2–3)
`internal/core` state machine implementing the complete §4.3 table. `internal/core/model`
reference implementation. All 13 invariants in `internal/invariant`. `memstore`. The rapid
model-based test with the `""` invariant hook. `sim/logic` tier A.
**Exit:** an in-memory broker with no network, no disk, and no CLI that passes 50 seeds ×
10k steps with all invariants green. `core` coverage ≥ 95%.
*Rationale: this is the whole product's semantics, provable, in three weeks, before a single
byte touches a socket.*

### M2 — Durability (weeks 4–6)
SQLite store, schema, migrations, `WriteBatch`, recovery procedure, three durability modes,
`messq verify` (CLI + RPC). `sim/durable` tier B. First fault points and the `crash-kill9`
job with the three-valued ledger oracle.
**Exit:** family 1 green with 1 000 kill cycles in all three modes; `restart` action added to
the rapid property loop; invariant I9 checked.

### M3 — Wire (weeks 7–8)
`api/messq/v1` proto, buf toolchain with `breaking` gate, Connect handlers, interceptor chain,
error model, `Publish`/`Fetch`/`Ack`/`Extend`, flow control. `FuzzPublishDecode`,
`FuzzSubjectMatch`, `FuzzAckToken`. `testing/synctest` tests for batching and long-poll.
**Exit:** an end-to-end publish→fetch→ack round trip over h2c and over `curl`; fuzz smoke green.

### M4 — CLI & logging (weeks 9–10)
Cobra command tree, `serve`, `pub`, `sub --exec`, `stream`, `consumer`, `peek`, `lag`,
completion. `internal/audit` with the JSON and console handlers, `slogtest` validation, event
table, `messq trace`. testscript golden suite.
**Exit:** the README's every example runs as a testscript; `messq trace` reproduces the §6.2
output; invariant I13 (log ≡ state) implemented and checked.

### M5 — The at-least-once machinery, hardened (weeks 11–12)
Ack timeout timer wheel, nak with backoff schedules, `max_deliver`, DLQ as a first-class
stream, `on_exhausted` policy, `Extend`. Families 6, 7, 8 (clock skew, disconnect, slow
consumer). `FuzzCommandSequence`.
**Exit:** `TestNoDuplicateWithoutCause` green over 500 seeds; DLQ conservation (I12) checked;
`messq dlq` commands with rate-limited redrive.

### M6 — Filesystem paranoia (weeks 13–14)
LazyFS integration, `nightly-lazyfs` and `nightly-fsfault` jobs, families 2–5. `ENOSPC`
handling audit across every write path. `messq doctor`.
**Exit:** all nine families have named green jobs. Durability-mode-parameterized oracle
implemented and demonstrated to *catch* a deliberately-introduced `synchronous=OFF`.

### M7 — Operations (weeks 15–16)
Replay, seek, purge, retention policies, `Redrive`, `Peek`, `Trace` RPC. Prometheus metrics
with `testutil` golden tests. Upgrade fixtures and the `upgrade` job. Conformance suite +
`messq conform`.
**Exit:** MVP feature-complete per the project brief; conformance suite is the spec;
`docs/guarantees.md` complete with invariant IDs and job names.

### M8 — v1.0 (weeks 17–19)
Mutation testing, soak, benchmarks, `messq sim` shipped as a subcommand, runbook, packaging
(systemd unit, static binaries, reproducible builds, SBOM). Grind the §8.14 gate list to zero.
**Exit:** every checkbox in §8.14 ticked; `messq info --gates` prints the evidence.

### M9+ — Phase 2, each feature with its own invariant
Every phase-2 feature ships **with a new numbered invariant and a new simulator action**, or
it does not ship:

| Feature | New invariant |
|---|---|
| Per-subject ordering (promote from opt-in to hardened) | I8 extended to concurrent fetches |
| Delayed delivery (`publish --at`) | I14: no message delivered before `deliver_at`, across restarts and clock jumps |
| Priority channels | I15: no lower-priority message leased while a higher-priority one is available to the same consumer |
| Rate limiting | I16: deliveries in any window ≤ configured rate + burst |
| Consumer groups with lease | I17: at most one group member holds a lease for a given seq |
| Compression | I18: decompress ∘ compress = identity (fuzz-verified), applied at rest only |
| Audit trail export | I19: exported stream folds to the same state as the live event table |
| Metrics endpoint hardening | already covered by testutil goldens |

Clustering remains explicitly out of scope. It would invalidate the "single durability
boundary" argument in §3.1 and require a testing investment (Jepsen, a real consensus
simulator) larger than the entire rest of this plan. If it is ever attempted, the first
milestone is a deterministic network simulator, not a replication protocol — and that is the
point at which `gosim` gets reconsidered.

---

## 10. Risks & open questions

### Risks

**R1 — `modernc.org/sqlite` is a transpilation of SQLite's C source.** A miscompiled edge case
would be a data-loss bug we did not write and cannot easily read.
*Mitigation:* pin the exact version; a nightly job that runs a SQLite-level torture workload
(large blobs, concurrent readers during checkpoint, forced WAL wraparound) through the driver
and then `PRAGMA integrity_check`; `quick_check` on every startup; keep the `Store` interface
narrow enough that swapping to `mattn/go-sqlite3` behind a build tag is a two-day job, with a
CI job that runs the store test suite against both. That escape hatch is itself tested.

**R2 — Single-writer throughput ceiling.** One goroutine and one SQLite write connection per
stream caps write rate.
*Mitigation:* group commit (default) amortises fsync; benchmarks published honestly; the
positioning explicitly says "not Kafka at scale". If a real user hits the ceiling, the fix is
more streams (independent actors and independent SQLite... — see R3), not a redesign.

**R3 — One database file for all streams means one write lock for all streams.** Actors are
per-stream but they contend on the single writer connection.
*Open question, must be resolved by M2.* Two candidate answers: (a) a shared commit
coordinator that merges batches from multiple actors into one transaction — preserves the
single durability boundary, adds a fair-queueing component that needs its own tests; (b) one
database file per stream — removes contention but reintroduces cross-stream atomicity
questions for DLQ writes (a DEAD message writes to two streams). **Leaning (a)**, because DLQ
atomicity is a guarantee we want to keep, and a merged commit coordinator is a ~200-line
component with a clean property test (`merge(batches)` applied ≡ batches applied in order).
Decision required with a benchmark before M2 exits.

**R4 — Event-table write amplification.** `audit=full` roughly doubles bytes written.
*Mitigation:* same transaction ⇒ no extra fsync; event retention default 7 days; measured in
the benchmark suite; `audit=transitions` and `audit=off` available with documented loss of
I13 checking.

**R5 — Invariant checkers can be wrong.** A checker that always returns "ok" gives false
confidence — the worst possible outcome for this plan.
*Mitigation:* every invariant has a paired "poison" test that deliberately corrupts state and
asserts the checker *fires*; mutation testing on `internal/invariant` at 100% coverage; and a
CI job that runs the suite against intentionally-broken builds (a `bugs/` directory of
sabotaged variants — e.g. ack without epoch check, `synchronous=OFF`, `max_deliver` off by
one) and **fails if the suite passes**. This is a test suite for the test suite, and it is the
single most important item in this document.

**R6 — Test suite runtime becomes a productivity tax.** Nightly budget is already ~12 hours of
compute.
*Mitigation:* the 8-minute PR budget is a hard constraint; deep work is nightly and does not
block anyone; sim seeds are sharded across runners; nightly failures create issues rather than
blocking merges, except for the release gate.

**R7 — LazyFS and privileged CI runners.** FUSE and device-mapper are awkward on hosted CI.
*Mitigation:* run families 2–5 in a self-hosted or privileged container nightly; if that is
unavailable at M6, a local VM image (`make faults-vm`) that a maintainer runs before each
release, with the result recorded as a signed artifact in the release checklist. The gate does
not get dropped; the automation might be manual for a while.

**R8 — `pending` table growth with a stuck consumer.** A consumer with high
`max_ack_pending` and no acks leaves a large WAITING set.
*Mitigation:* `max_ack_pending` is a hard cap and is invariant I7; `messq doctor` flags
consumers whose `ack_floor_age` exceeds a threshold; the soak test includes a permanently
stuck consumer.

### Open questions

1. **Should `Publish` be atomic across a batch?** Current answer: yes (one transaction,
   all-or-nothing) because it is easier to test and easier to reason about. Open: whether
   partial success would be more useful in practice for large batches. Revisit at M7 with
   user feedback; changing it later is an API break, so it must be settled before v1.0.
2. **Dedup window semantics.** `message_dedup` is a unique index over the whole stream, so
   dedup is effectively unbounded until retention removes the row. A time-bounded window would
   be more like JetStream but adds a state-expiry path that needs its own invariant. Current
   answer: keep it retention-bounded and document it. Revisit only if memory/index size becomes
   a real problem.
3. **`ack_wait` extension cap.** `Extend` needs a total cap or a slow worker can hold a lease
   forever. Proposed: `max_ack_wait_total = 10 × ack_wait`, configurable. Needs a decision plus
   invariant text before M5.
4. **Should `verify --deep` (I13, O(events)) be runnable online without blocking writes?** WAL
   snapshot isolation says yes on a reader connection, but the fold must handle events
   arriving during the scan. Design needed at M7.
5. **Retention vs. unresolved messages.** `retention='limits'` can delete a message that a
   consumer has not resolved. Current answer: allowed, logged at WARN, counted in
   `messq_retention_forced_total`, and reported by `doctor` — but should it be *refused* by
   default instead? Leaning toward refusing when any consumer has it pending, with an explicit
   `--allow-unresolved-deletion`. Decide at M7.
6. **Do we need `porcupine` at all for a single-node broker?** Its value is real but narrow
   (concurrent-fetch/ack races). If the actor model makes those races structurally impossible,
   the checker may be ceremony. Decide at M4 based on whether it has caught anything.

---

## 11. Library choices

Every choice below was checked against current documentation via context7 (or the project's
own README where context7 had no entry), and every one is justified by what it does for
*testability* first.

### 11.1 `modernc.org/sqlite` — storage engine driver
Pure-Go SQLite, `database/sql` compatible. Docs confirm the DSN pragma surface we depend on:
`_journal=WAL`, `_synchronous=FULL|NORMAL`, `_timeout` (busy_timeout), `_txlock=immediate`,
`_foreign_keys=1`, and arbitrary `_pragma=name(value)` parameters. The driver source shows
`_txlock` is applied as `BEGIN IMMEDIATE` for write transactions and deliberately ignored for
read-only transactions (`newTx` uses plain `begin` when `opts.ReadOnly`), which is exactly the
behaviour our one-writer/N-reader split needs.
**Chosen because:** cgo would make the race detector blind to the storage engine and would
make "reproduce it on your machine" depend on the local toolchain. Pure Go keeps `-race`,
`-fuzz`, deterministic cross-compilation, and a genuinely static single binary.

### 11.2 `etcd-io/bbolt` — evaluated, rejected
Docs confirm the attractive features: `DB.Batch` opportunistic group commit (with the caveat
that the function must be idempotent because it may be retried), `StrictMode` post-commit
consistency checking, and `NoSync`/`NoFreelistSync` durability knobs. Rejected because the
pending set needs three access paths that would become hand-maintained secondary buckets —
each one an invariant we must write and test — where SQLite maintains and verifies its own
indexes. `StrictMode` is a genuinely good idea we steal in spirit: our equivalent is
`verify --quick` after recovery and after every simulator step.

### 11.3 `connectrpc.com/connect` — RPC framework
Docs confirm the exact server shape we want: generated handlers are plain `net/http` handlers
mounted on a `ServeMux`, with `http.Protocols` configured via `SetHTTP1(true)` and
`SetUnencryptedHTTP2(true)` so gRPC clients work without TLS; `NewServerStreamHandler` gives
the `Fetch` streaming shape; the `Code` enum is gRPC-compatible (`CodeNotFound`,
`CodeFailedPrecondition`, `CodeResourceExhausted`, `CodeDataLoss`, `CodeUnavailable` — all used
in §5.3); and the `Interceptor` interface (`WrapUnary` / `WrapStreamingHandler`) supports our
trace-id + logging + metrics chain. Protovalidate integration is documented as the recommended
validation path, which moves request validation into the schema where it can be fuzzed.
**Chosen because:** one handler serves gRPC, gRPC-Web, and plain JSON-over-HTTP, so `curl` is a
first-class client and `httptest.Server` covers the entire transport with no bespoke harness.

### 11.4 `pgregory.net/rapid` — property & model-based testing
Docs confirm `rapid.Check`, generics-based type-safe generators, `t.Repeat(map[string]func(*rapid.T))`
for stateful testing with the `""` key running **after every other action** as the invariant
hook (exactly the shape §8.3 needs), `rapid.StateMachineActions` for the struct-based variant,
automatic minimisation with **no user-written shrinkers**, and the flag surface we build CI on:
`-rapid.checks`, `-rapid.seed`, `-rapid.failfile`, `-rapid.shrinktime`, plus automatic
failfile persistence under `.rapid/` and automatic halving of checks under `-short`.
**Chosen over gopter because:** automatic full minimisation without user code, no dependencies
outside the standard library, and the failfile mechanism gives us a committed regression
corpus for free — which is ground rule 4 of our testing discipline.

### 11.5 `testing/synctest` (standard library, Go 1.25+)
Stable since Go 1.25. Inside a bubble, the `time` package uses a per-bubble fake clock starting
at 2000-01-01 UTC, and time advances **only when every goroutine in the bubble is durably
blocked**; `synctest.Test` waits for all bubble goroutines to exit. That is precisely what
makes "assert the ack timeout fires at exactly `ack_wait`" instantaneous and deterministic,
and it is what makes our ban on `time.Sleep` in tests enforceable rather than aspirational.
Zero dependencies. Go 1.26 is the project's minimum.

### 11.6 `log/slog` + `testing/slogtest` (standard library)
`slog.NewJSONHandler` for the machine format; `HandlerOptions.ReplaceAttr` for level/time
normalisation (documented pattern); `slog.Group` for the message/consumer attribute groups.
Our console handler implements `slog.Handler` and is validated by `slogtest.TestHandler(h, results)`,
which checks the full documented handler contract — zero-time elision, group qualification via
`WithGroup`, empty-group elision, attribute resolution. Writing a custom handler without
`slogtest` is how subtly-broken structured logs ship.

### 11.7 `prometheus/client_golang` — metrics
Docs confirm the patterns we adopt: an explicit `prometheus.NewRegistry()` with
`promauto.With(reg)` for construction (never the default registerer — enforced by a
`forbidigo` lint rule), `promhttp.HandlerFor(reg, HandlerOpts{...})` for exposition, and the
`testutil` package for verification. `testutil.CollectAndCompare` gives golden-file unit tests
of metric output, and `testutil.ScrapeAndCompare` performs a real HTTP scrape and parses the
text exposition format — used in the conformance suite so metric names are a tested contract,
not an accident.

### 11.8 `spf13/cobra` — CLI
Docs confirm `SetArgs`/`SetOut`/`SetErr`, which make every command testable in-process with
captured output and no subprocess, and the four-shell completion generators
(`GenBashCompletion`, `GenZshCompletion`, `GenFishCompletion`,
`GenPowerShellCompletionWithDesc`) plus the hidden `__complete` command for debugging
completion logic — which we drive from a testscript golden test so completions cannot silently
rot. `ShellCompDirective` flags give us proper completion of stream and consumer names.

### 11.9 `rogpeppe/go-internal/testscript` — CLI golden tests
The framework extracted from the Go team's own tooling and used to test the `go` command:
shell-like `.txtar` scripts, stdout/stderr assertions, negation with `!`, `go test` and
coverage integration, build-tag awareness, and `-update` for regenerating goldens. This is the
right tool for a CLI-first product: our documented examples become executable tests.

### 11.10 `anishathalye/porcupine` — linearizability checking
A fast linearizability checker taking an executable model plus a history; reported as
1 000×–10 000× faster than Knossos with a much smaller memory footprint, with history
visualisation for debugging. Used by etcd's robustness tests and by PingCAP for TiDB —
i.e. it is battle-tested in exactly our problem domain. Scoped narrowly here (§8.5) to the
per-consumer pending set under concurrent clients; see open question 6.

### 11.11 `google/go-cmp` — diffs
`cmp.Diff` on `core.State` and `WriteBatch` values. **No assertion library** (no testify): our
failure messages come from readable struct diffs, and one fewer abstraction between the
engineer and the failure is worth more than terser test code.

### 11.12 `go-gremlins/gremlins` — mutation testing
A Go mutation tester inspired by PITest, explicitly designed to work well on smallish modules
and usable as a CI quality gate — which matches `internal/core` exactly. Pinned by version
(it is pre-1.0 and only the current minor is maintained); runs nightly on `core` and
`invariant` only, never on the whole repo (documented to be slow on large modules).

### 11.13 LazyFS — filesystem fault injection *(external tool, not a Go dependency)*
A FUSE filesystem with its own page cache, purpose-built to inject **lost unsynced writes** and
**torn writes** with precise control (e.g. "clear unsynced data after the sixth fsync to this
file"), plus profiling of the operation and persistence flow. Its published evaluation
reproduced known data-loss bugs and found eight new ones across PostgreSQL, etcd, ZooKeeper,
Redis, LevelDB and PebblesDB, and it is integrated with Jepsen. This is the tool that turns
"we set `synchronous=FULL`" into evidence.

### 11.14 `pingcap/failpoint` — evaluated, grammar adopted, dependency rejected
Its docs describe marker functions (`failpoint.Inject`, `failpoint.Eval`), env-driven
activation with the grammar `[<percent>%][<count>*]<type>[(args...)]` via `GO_FAILPOINTS`, and
a three-stage `failpoint-ctl enable` → build → `disable` codegen workflow whose selling point
is that failpoint code does not appear in the final binary. We adopt the **activation grammar**
verbatim (it is well designed: probabilistic, counted, composable) and reject the **codegen**:
a Go build tag gives the same zero-cost production binary with no source rewriting, no extra
tool in the inner loop, and no risk of committing a half-transformed tree. `internal/fault` is
~150 lines and has no dependencies.

### 11.15 `buf` — protobuf toolchain *(build-time tool)*
Codegen plus `buf lint` and, critically, `buf breaking` against the `main` branch — which makes
wire compatibility a mechanical CI gate rather than a review-time judgement call.

### 11.16 Rejected: `gosim`
Genuinely impressive — deterministic goroutine interleavings, simulated network and disk, and
it has run etcd. Rejected because it achieves determinism by source-translating Go to replace
concurrency and syscall primitives, which means the binary under test differs from the binary
we ship, and the translation step becomes a dependency of every developer's inner loop. We buy
determinism architecturally instead (three seams + a synchronous actor body). Explicitly
flagged for reconsideration if clustering is ever attempted, where the argument reverses.

---

## Appendix A — Sources consulted

- [Reliable Message Delivery in NATS JetStream: Acks, Retries, Dead Letters, and Replay (Synadia)](https://www.synadia.com/blog/jetstream-reliable-delivery-dlq-replay)
- [Consumers | NATS Docs](https://docs.nats.io/nats-concepts/jetstream/consumers)
- [JetStream Model Deep Dive | NATS Docs](https://docs.nats.io/using-nats/developer/develop_jetstream/model_deep_dive)
- [Visibility Timeout Deep Dive | task-queues.com](https://www.task-queues.com/queue-fundamentals-architecture/visibility-timeout-deep-dive/)
- [Dead-Letter Queues & Poison-Message Handling | task-queues.com](https://www.task-queues.com/queue-fundamentals-architecture/dead-letter-queues-poison-messages/)
- [Streams Consumer Group Patterns (antirez)](https://redis.antirez.com/fundamental/streams-consumer-patterns.html)
- [XAUTOCLAIM | Redis Docs](https://redis.io/docs/latest/commands/xautoclaim/)
- [SQLite's Durability Settings are a Mess (Andrew Ayer)](https://www.agwa.name/blog/post/sqlite_durability)
- [SQLite commits are not durable under default settings (avi.im)](https://avi.im/blag/2025/sqlite-fsync/)
- [SQLite performance tuning (phiresky)](https://phiresky.github.io/blog/2020/sqlite-performance-tuning/)
- [The Write Stuff: Concurrent Write Transactions in SQLite (oldmoe)](https://oldmoe.blog/2024/07/08/the-write-stuff-concurrent-write-transactions-in-sqlite/)
- [Diving into FoundationDB's Simulation Framework (Pierre Zemb)](https://pierrezemb.fr/posts/diving-into-foundationdb-simulation/)
- [Deterministic Simulation Testing for Our Entire SaaS (WarpStream)](https://www.warpstream.com/blog/deterministic-simulation-testing-for-our-entire-saas)
- [(Mostly) Deterministic Simulation Testing in Go (Polar Signals)](https://www.polarsignals.com/blog/posts/2024/05/28/mostly-dst-in-go)
- [gosim: simulation testing for Go](https://github.com/jellevandenhooff/gosim)
- [awesome-deterministic-simulation-testing](https://github.com/ivanyu/awesome-deterministic-simulation-testing)
- [LazyFS](https://github.com/dsrhaslab/lazyfs) · [When Amnesia Strikes (VLDB 2024)](https://www.vldb.org/pvldb/vol17/p3017-ramos.pdf)
- [Files are hard (Dan Luu)](https://danluu.com/file-consistency/)
- [Design and Implementation of Golang Failpoints (PingCAP)](https://www.pingcap.com/blog/design-and-implementation-of-golang-failpoints/)
- [porcupine: a fast linearizability checker](https://github.com/anishathalye/porcupine) · [Testing Distributed Systems for Linearizability](https://anishathalye.com/testing-distributed-systems-for-linearizability/)
- [rapid: modern Go property-based testing](https://github.com/flyingmutant/rapid)
- [testing/synctest — Go Packages](https://pkg.go.dev/testing/synctest) · [Testing Time (Go blog)](https://go.dev/blog/testing-time)
- [gremlins: mutation testing for Go](https://github.com/go-gremlins/gremlins)
- [golangci-lint](https://github.com/golangci/golangci-lint)
