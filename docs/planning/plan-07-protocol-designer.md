# messq — Project Plan (Protocol & API Designer)

**Author persona:** the person who owns the wire. Transport choice, RPC surface, streaming
consumption model, ack/nak/flow-control framing, SDK ergonomics, versioning policy, auth hooks.
Every other section exists and is decided, but the depth is spent where the wire lives:
sections 4, 5, 6 and 11.

**Status:** plan. Repository currently contains `README.md` only.

---

## 1. Vision & positioning

### 1.1 The thesis

**The protocol *is* the product.** Every guarantee messq claims — at-least-once, explicit ack,
ack timeout, max-deliver, dead-letter, replay, flow control — must be *expressible*, *observable*
and *testable on the wire*, or it is folklore. A broker whose guarantees only exist inside its own
source code is a broker you cannot operate.

Kafka's guarantees are real but its protocol is a research project; you cannot `curl` it, you
cannot read a frame dump, and its consumer-group barrier rebalance has generated a decade of
operational war stories (consumers evicted for exceeding `max.poll.interval.ms`, triggering a
rebalance, rejoining, triggering another). RabbitMQ's AMQP 1.0 link credit is theoretically
beautiful and, by RabbitMQ's own admission, layered flow control is "challenging to get right".
Redis Streams pushes the entire redelivery orchestration into every client, where each application
reimplements `XAUTOCLAIM` reaping "often subtly wrong".

NATS JetStream got the *vocabulary* right — `ack`, `nak`, `term`, `working` (`AckProgress`),
`AckWait`, `MaxDeliver`, `Backoff`, `MaxAckPending`, `DeliverPolicy` — and messq adopts that
vocabulary deliberately and almost verbatim. What messq adds is a wire you can read with your eyes.

### 1.2 What messq is

A single Linux binary, `messq`, that is both daemon and CLI. It speaks **one RPC service over the
Connect protocol**, which means the same handler answers three wire protocols — Connect
(HTTP/1.1 + JSON, `curl`-able), gRPC, and gRPC-Web — with no gateway, no sidecar, no second
listener. Metrics, health and RPC all share one `net/http` mux and one Unix socket.

Positioning in one line: **the queue whose protocol you can read in an afternoon and whose message
history you can reconstruct with one command.**

### 1.3 Design commitments (the things I will not trade away)

| # | Commitment | Consequence |
|---|---|---|
| C1 | One RPC service, ≤ 24 methods, exactly 2 streaming methods | Any new method must justify itself against the budget or displace one |
| C2 | Consumption is **pull-with-credit**, never server-push | Backpressure is structural, not configured |
| C3 | Ack is by **opaque signed token**, not by connection state | You can ack from another process, another connection, ten minutes later |
| C4 | Four settle dispositions: `ACK`, `NAK`, `TERM`, `WORKING` | "retry later" and "poison, stop trying" are distinguishable by operators |
| C5 | `msg_id` and `trace_id` are first-class protocol fields | Every log line, every event, every delivery carries them |
| C6 | Additive-only proto evolution for the life of `messq.v1` | CI gate (`buf breaking`) enforces it from commit #1 |
| C7 | Auth is a hook in the protocol from v1, even when the default is "none" | Retrofitting auth into a wire protocol breaks everyone |
| C8 | Retry is safe **by construction**, not by client discipline | Dedup key on publish; ack by token is idempotent |
| C9 | No `AckAll` | Deliberately dropped from the JetStream vocabulary: cumulative ack makes "which message actually completed?" unanswerable, which contradicts C5 |

### 1.4 Explicit non-goals

Quorum replication. Distributed consensus. Partition assignment protocols. Exactly-once end-to-end
(we provide idempotent publish within a dedup window and at-least-once delivery; effectively-once
is built by the consumer, as in every honest system). A browser SDK. Multi-tenant SaaS. Being fast
at a million messages per second.

---

## 2. Architecture overview

### 2.1 Processes

One. `messq serve` runs the daemon; every other subcommand is a client of it over the same RPC
surface the SDK uses. There is no admin backdoor: `messq stream create` calls
`messq.v1.MessqService/CreateStream`. This is a hard rule — it guarantees the CLI can never do
something the protocol cannot express, which keeps the CLI honest as a documentation artefact.

Default listeners:

- `unix:///run/messq/messq.sock` (mode 0660, group `messq`) — always on
- `tcp://127.0.0.1:7442` — opt-in via config; serves RPC **and** `/metrics`, `/healthz`, `/readyz`
  on the same mux, because Connect runs on stock `net/http`
- h2c (cleartext HTTP/2) is enabled on both so gRPC clients work without TLS locally

### 2.2 Goroutine topology

```
                    ┌────────────────────────────────────────────────┐
   clients ────────►│  net/http server (goroutine per connection)     │
   (SDK/CLI/curl)   │  ├─ auth interceptor  ├─ log interceptor        │
                    │  └─ Connect handler → service methods           │
                    └───────┬──────────────────────────┬─────────────┘
                            │ writeOp chan             │ attach/credit/settle
                            ▼                          ▼
                    ┌───────────────┐          ┌────────────────────────┐
                    │ store writer  │          │ dispatcher goroutine   │
                    │ (exactly 1)   │          │  one PER CONSUMER      │
                    │ group commit  │◄─────────┤  owns: cursor, credit, │
                    │ + fsync       │  ack/    │  in-flight map,        │
                    └───────┬───────┘  pending │  lease min-heap        │
                            │          flush   └────────┬───────────────┘
                            ▼                           │ Delivery
                    ┌───────────────┐                   ▼
                    │ SQLite (WAL)  │          attached Consume streams
                    └───────┬───────┘
                            │
   ┌────────────────────────┴──────────────┬─────────────────────────┐
   ▼                                       ▼                         ▼
┌──────────────┐                 ┌──────────────────┐      ┌──────────────────┐
│ retention    │                 │ event bus        │─────►│ slog handlers    │
│ goroutine    │                 │ (1 goroutine,    │─────►│ events table     │
│ per stream   │                 │  ring buffers)   │─────►│ WatchEvents subs │
└──────────────┘                 └──────────────────┘─────►│ prometheus       │
                                                            └──────────────────┘
```

**Single store writer.** All writes funnel through one goroutine that batches operations into a
single SQLite transaction — up to `store.max_batch` (default 256) ops or `store.max_batch_delay`
(default 2 ms), whichever comes first. One `fsync` per batch instead of per publish. This is the
same trick bbolt's `db.Batch` uses ("multiple batches are opportunistically combined"); we
reimplement it on SQLite because we want the query surface (§3).

**One dispatcher goroutine per consumer.** All mutable consumer state — cursor, credit balance,
in-flight map, lease expiry heap — lives inside one goroutine and is never locked. Consumers number
in the tens or hundreds, not millions, so this is the right trade: zero lock reasoning, trivially
testable as a pure state machine (§8). The dispatcher blocks on a `select` over: new-message
signal, credit grant, settle batch, next lease expiry (`time.Timer` from a `container/heap`),
config change, shutdown.

**Event bus never blocks the data path.** Each subscriber (slog sink, events-table writer,
`WatchEvents` stream, metrics) has a bounded ring buffer. Overflow drops, increments
`messq_events_dropped_total`, and — for `WatchEvents` — sends a `LOST_EVENTS` status frame with a
count. Failing loud beats a silent gap in an audit trail.

### 2.3 Data flow: publish

1. Connection goroutine: decode → validate subject/size → authorize (`publish` verb, subject pattern)
2. Dedup check against in-memory LRU keyed by `(stream, dedup_key)` with TTL, backed by the `dedup`
   table. Hit → return the original `seq` with `duplicate: true`, no write.
3. Optional `expect` preconditions (`last_seq`, `last_subject_seq`) checked *inside* the writer
   transaction, not before it — otherwise the check races.
4. `writeOp` → store writer → group commit → fsync → `seq` and `msg_id` (ULID) assigned
5. Respond. Signal every dispatcher whose filter matches the subject.
6. Emit `publish` event.

The response is only sent after fsync. **A `PublishAck` is a durability promise.**

### 2.4 Data flow: deliver

Dispatcher wakes → selects the next candidate under this precedence:

1. **Redeliveries** whose scheduled time has passed, oldest first
2. **New messages** from the cursor forward that match the filter

Guard on every send: `credit_messages > 0 ∧ credit_bytes ≥ size ∧ in_flight < max_ack_pending`.
If a candidate exists but the guard fails, emit `flow.stalled` once (edge-triggered) and send a
`FLOW_STALLED` status frame. This is the single most valuable operational signal in the system: it
tells the operator *the consumer is the bottleneck*, as distinct from `NO_MESSAGES`, which tells
them the queue is empty. NSQ, JetStream and AMQP all learned this the hard way; messq surfaces it
at frame level from day one.

---

## 3. Storage & durability design

### 3.1 Engine: SQLite via `modernc.org/sqlite`

**Decision: SQLite, cgo-free driver, one database file.**

Rationale, in priority order:

1. **Operators can query it.** `sqlite3 /var/lib/messq/messq.db "select seq, subject, deliveries
   from pending join ..."` is a forensic tool that ships for free and works when the daemon is
   down. For a product positioned on *readable operations*, this is not a nice-to-have; it is the
   thesis.
2. **cgo-free ⇒ genuinely single static binary.** `modernc.org/sqlite` is a pure-Go port;
   `mattn/go-sqlite3` would require cgo and destroy trivial cross-compilation.
3. **WAL gives concurrent readers + one writer**, which is exactly our goroutine topology.
4. **Crash recovery is someone else's solved problem.** WAL replay on open, `PRAGMA
   integrity_check` available on demand.

**bbolt was seriously evaluated and rejected.** It has a cleaner durability story and its `Batch`
API is precisely our group-commit pattern. But it is a pure key/value B+tree: DLQ listing, pending
scans, per-subject scans and "last message per subject" each need a hand-rolled index bucket, and
none of them are queryable by a human at 3am. Rejected on operability, not correctness.

### 3.2 DSN and fsync policy

```
file:/var/lib/messq/messq.db
  ?_journal=WAL
  &_synchronous=FULL          # durability=strict (default)
  &_timeout=5000              # busy_timeout, ms
  &_txlock=immediate          # writer conn: take the write lock at BEGIN
  &_pragma=foreign_keys(1)
  &_pragma=cache_size(-65536) # 64 MiB
```

Two connections: one writer (`MaxOpenConns=1`, `_txlock=immediate`) and a read pool
(`_query_only=1`, `MaxOpenConns=runtime.NumCPU()`).

| `durability` setting | `synchronous` | Promise |
|---|---|---|
| `strict` (default) | `FULL` | A returned `PublishAck` survives power loss |
| `relaxed` | `NORMAL` | A returned `PublishAck` survives process crash; up to one checkpoint interval may be lost on power loss |

`messq doctor` prints the effective setting in plain English and refuses to be silent about it.
Group commit means `strict` costs one fsync per *batch*, not per message — with a 2 ms batch delay,
a busy stream pays roughly 500 fsyncs/second regardless of message rate.

### 3.3 Schema (v1)

```sql
PRAGMA user_version = 1;

CREATE TABLE streams (
  id              INTEGER PRIMARY KEY,
  name            TEXT NOT NULL UNIQUE,
  subjects        TEXT NOT NULL,          -- JSON array of patterns
  max_msgs        INTEGER, max_bytes INTEGER, max_age_ms INTEGER,
  dedup_window_ms INTEGER NOT NULL DEFAULT 120000,
  discard         TEXT NOT NULL DEFAULT 'old',   -- old | new
  max_msg_size    INTEGER NOT NULL DEFAULT 1048576,
  created_ms      INTEGER NOT NULL,
  config_json     TEXT NOT NULL           -- full proto as JSON; source of truth for round-trip
);

CREATE TABLE messages (
  stream_id   INTEGER NOT NULL,
  seq         INTEGER NOT NULL,           -- per-stream monotonic
  msg_id      TEXT    NOT NULL,           -- ULID, 26 chars
  subject     TEXT    NOT NULL,
  subject_seq INTEGER NOT NULL,           -- per-subject monotonic
  ts_ms       INTEGER NOT NULL,
  headers     BLOB,                       -- proto-encoded map<string,string>
  payload     BLOB    NOT NULL,
  trace_id    TEXT,
  size        INTEGER NOT NULL,
  PRIMARY KEY (stream_id, seq)
) WITHOUT ROWID;
CREATE INDEX messages_subject ON messages(stream_id, subject, seq);
CREATE UNIQUE INDEX messages_msgid ON messages(msg_id);

CREATE TABLE dedup (
  stream_id INTEGER NOT NULL, dedup_key TEXT NOT NULL,
  seq INTEGER NOT NULL, expires_ms INTEGER NOT NULL,
  PRIMARY KEY (stream_id, dedup_key)
) WITHOUT ROWID;
CREATE INDEX dedup_expiry ON dedup(expires_ms);

CREATE TABLE consumers (
  id              INTEGER PRIMARY KEY,
  stream_id       INTEGER NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
  name            TEXT NOT NULL,
  filter_subjects TEXT NOT NULL,          -- JSON array
  ack_wait_ms     INTEGER NOT NULL DEFAULT 30000,
  max_ack_wait_total_ms INTEGER NOT NULL DEFAULT 3600000,  -- WORKING extension ceiling
  max_deliver     INTEGER NOT NULL DEFAULT 5,
  backoff_ms      TEXT,                   -- JSON array; overrides ack_wait per attempt
  max_ack_pending INTEGER NOT NULL DEFAULT 1000,
  max_ack_pending_bytes INTEGER NOT NULL DEFAULT 67108864,
  deliver_policy  TEXT NOT NULL DEFAULT 'all',  -- all|new|by_seq|by_time|last_per_subject
  opt_start_seq   INTEGER, opt_start_ms INTEGER,
  ordered         INTEGER NOT NULL DEFAULT 0,   -- per-subject ordering
  dlq_stream      TEXT,
  epoch           INTEGER NOT NULL DEFAULT 0,   -- bumped by Seek/Purge; invalidates ack tokens
  ack_floor       INTEGER NOT NULL DEFAULT 0,   -- all seq <= this are settled
  delivered_seq   INTEGER NOT NULL DEFAULT 0,   -- cursor
  next_delivery_seq INTEGER NOT NULL DEFAULT 1, -- monotonic across all deliveries
  UNIQUE (stream_id, name)
);

CREATE TABLE pending (
  consumer_id        INTEGER NOT NULL,
  seq                INTEGER NOT NULL,
  delivery_seq       INTEGER NOT NULL,   -- token freshness discriminator
  deliveries         INTEGER NOT NULL,
  lease_expires_ms   INTEGER NOT NULL,   -- or scheduled redelivery time when state='scheduled'
  state              TEXT    NOT NULL,   -- in_flight | scheduled
  first_delivered_ms INTEGER NOT NULL,
  last_reason        TEXT,
  PRIMARY KEY (consumer_id, seq)
) WITHOUT ROWID;
CREATE INDEX pending_lease ON pending(consumer_id, lease_expires_ms);

CREATE TABLE dlq (
  consumer_id INTEGER NOT NULL, seq INTEGER NOT NULL,
  msg_id TEXT NOT NULL, reason TEXT NOT NULL, cause TEXT NOT NULL, -- cause: max_deliver|term
  deliveries INTEGER NOT NULL, dead_ms INTEGER NOT NULL,
  last_error TEXT, trace_id TEXT,
  PRIMARY KEY (consumer_id, seq)
) WITHOUT ROWID;

CREATE TABLE events (                     -- powers `messq trace`; own retention
  id INTEGER PRIMARY KEY,
  ts_ms INTEGER NOT NULL, event TEXT NOT NULL, level TEXT NOT NULL,
  stream TEXT, consumer TEXT, subject TEXT,
  msg_id TEXT, seq INTEGER, trace_id TEXT,
  deliveries INTEGER, principal TEXT, client TEXT, dur_ms INTEGER, reason TEXT,
  extra_json TEXT
);
CREATE INDEX events_msgid ON events(msg_id, id);
CREATE INDEX events_trace ON events(trace_id, id);
CREATE INDEX events_ts    ON events(ts_ms);

CREATE TABLE meta (k TEXT PRIMARY KEY, v BLOB);
-- keys: schema_version, node_id, ack_hmac_key_current, ack_hmac_key_previous, clean_shutdown
```

### 3.4 The interesting durability decision: delivery state

Writing a `pending` row synchronously on every delivery doubles write amplification and puts an
fsync on the delivery hot path. Decision:

- **Acks, naks and terms are durable before the response.** Always. They go through group commit
  and the `Settled` frame is sent afterwards. An ack you waited for is a promise.
- **Delivery records are buffered and flushed within `state_flush_interval` (default 250 ms)**, or
  sooner if the batch fills.

The consequence, stated openly in `PROTOCOL.md`:

> **After an unclean shutdown, delivery counts are a lower bound.** A message may be delivered
> slightly more than `max_deliver` times across a crash. It will never be dead-lettered *early*.

That asymmetry is the whole point: undercounting deliveries costs a duplicate; overcounting would
poison a healthy message. We choose the safe direction and say so. (A `strict_delivery_counts=true`
mode that makes pending writes synchronous is designed but deferred — see §10.)

### 3.5 Crash recovery

On `messq serve` start:

1. If `meta.clean_shutdown` is absent, log `recovery.unclean` WARN and run `PRAGMA quick_check`.
2. Open with WAL; SQLite replays the log.
3. Clear `meta.clean_shutdown`.
4. Per consumer, load `pending` rows into the dispatcher's in-flight map. **All in-flight leases
   are treated as expired immediately** — nobody holds them, every client is disconnected.
   There is no redelivery stampede, because delivery is gated on client-granted credit: the
   dispatcher physically cannot send faster than workers ask. Credit-based flow control solves
   restart stampede for free, which is a nice consequence of C2.
5. Delete expired `dedup` rows; delete `events` older than `event_retention`.
6. Emit a single `recovered` event: streams, consumers, last seq per stream, pending restored per
   consumer, DLQ depth. This event is the first thing an operator reads after an incident.

On graceful shutdown: stop accepting attaches, send `DRAINING` status to all `Consume` streams,
wait up to `drain_timeout` (default 30 s) for in-flight settles, flush pending state, checkpoint
WAL, set `meta.clean_shutdown`, exit 0.

### 3.6 Retention

Per stream: `max_msgs`, `max_bytes`, `max_age`. A message is eligible for deletion when its `seq`
is below `min(ack_floor)` across all consumers of the stream. If a limit is breached while messages
are still un-acked and `discard=old`, we delete anyway and emit `retention.forced_discard` at WARN
with the count and the blocking consumer name — losing data silently is the one thing a queue must
never do. `discard=new` instead rejects publishes with `RESOURCE_EXHAUSTED` and a `retry_after`.

---

## 4. Delivery semantics & message lifecycle

### 4.1 Guarantees, precisely

- **Publish:** a returned `PublishAck` means the message is durable at the configured level and has
  a stable `seq` and `msg_id`. A publish with a `dedup_key` seen within the stream's dedup window
  returns the *original* ack with `duplicate: true` and writes nothing.
- **Delivery:** at-least-once. A message matching a consumer's filter, at or after its cursor, is
  delivered until acked, terminated, or dead-lettered.
- **Ack:** idempotent by token. Acking an already-settled message succeeds.
- **Ordering:** by default, messages from a single subject are *offered* in `seq` order but may be
  concurrently in flight. With `ordered=true`, at most one message per subject is in flight for
  that consumer, giving strict per-subject ordering at the cost of per-subject head-of-line
  blocking.
- **Not guaranteed:** exactly-once delivery, global ordering across subjects, ordering after a nak
  with delay (a delayed message is re-offered later than its successors — documented, not hidden).

### 4.2 State machine

Per `(consumer, seq)`. This is implemented as a **pure function**
`transition(State, Event, Config, now) -> (State, []Effect)` with no I/O, which is what makes §8
possible.

```
                            ┌──────────────────────────────────────────┐
                            │                                          │
   ┌─────────┐   deliver    ▼         ack                              │
   │ UNSEEN  ├────────────► IN_FLIGHT ──────────► ACKED (terminal)     │
   └────┬────┘  guard: credit  │  │  │                                 │
        │       & !max_pending │  │  │ term(reason)                    │
        │                      │  │  └──────────► DEAD ───────────┐    │
        │            working   │  │                               │    │
        │        (lease+=wait) │  │  nak(delay) | lease timeout    │    │
        │        bounded by    │  │                                │    │
        │        max_ack_wait  │  ▼                                │    │
        │             ▲        │  deliveries >= max_deliver ? ─yes─┘    │
        │             └────────┘         │ no                           │
        │                                ▼                              │
        │                        SCHEDULED(at) ─── timer fires ─────────┘
        │                                │
        └── seek/epoch bump ─────────────┴──► UNSEEN (in-flight dropped, counted)

   DEAD ── Requeue ──► UNSEEN  (deliveries reset or preserved, caller's choice)
```

### 4.3 Transition table

| Event | Guard | Effects |
|---|---|---|
| `deliver` | `credit_msgs > 0` ∧ `credit_bytes ≥ size` ∧ `in_flight < max_ack_pending` ∧ `in_flight_bytes + size ≤ max_ack_pending_bytes` ∧ (`!ordered` ∨ no in-flight for subject) | `deliveries++`; `delivery_seq = next_delivery_seq++`; `lease_expires_at = now + ackWait(deliveries)`; mint ack token; decrement credit; emit `deliver` |
| `ack(token)` | token HMAC valid ∧ `epoch` matches ∧ `delivery_seq` == current | delete pending; advance `ack_floor` over the contiguous prefix; emit `ack` with `dur_ms = now − first_delivered` |
| `ack(token)` on unknown/settled seq | token HMAC valid ∧ epoch matches | **success** (idempotent); emit `ack.duplicate` at DEBUG |
| `ack(token)` with stale `delivery_seq` | — | reject `ABORTED`; message stays in flight; emit `ack.stale` at WARN with both delivery_seqs |
| `ack(token)` with wrong epoch | — | reject `ABORTED` + `CONSUMER_CHANGED` hint; emit `ack.epoch_mismatch` WARN |
| `nak(token, delay?, reason?)` | as `ack` | if `deliveries ≥ max_deliver` → DEAD(cause=`max_deliver`); else SCHEDULED at `now + (delay ?? backoff[deliveries])`; emit `nak` INFO |
| `term(token, reason)` | as `ack` | → DEAD(cause=`term`); emit `dead` ERROR |
| `working(token)` | as `ack` ∧ `now − first_delivered < max_ack_wait_total` | `lease_expires_at = now + ack_wait`; emit `working` DEBUG; return new deadline |
| `working(token)` past ceiling | — | reject `FAILED_PRECONDITION`; force lease expiry; emit `working.exhausted` WARN. *A wedged worker cannot hold a message forever.* |
| `lease timeout` | `now ≥ lease_expires_at` | identical to `nak` with no delay, but emits `timeout` at **WARN** including `held_for`, and the `principal`, `client_name` and `client_addr` of the session that took the delivery |
| `redelivery timer` | `now ≥ scheduled_at` | → UNSEEN, front of the redelivery queue |
| `seek` / `purge` | — | `epoch++`; drop in-flight (counted); reset cursor; emit `seek` INFO with old/new |

**Backoff.** `ackWait(n)` = `backoff_ms[min(n-1, len-1)]` if `backoff_ms` is set, else `ack_wait_ms`.
Adopted directly from JetStream, where `Backoff` overrides `AckWait` and its first element sets the
initial wait. Default for new consumers: `ack_wait=30s`, `max_deliver=5`, no backoff. A recommended
production profile printed by `messq consumer create --explain`: `backoff=[5s,30s,5m,1h]`,
`max_deliver=5`.

### 4.4 Dead-letter handling

On DEAD, atomically in one transaction:

1. Insert into `dlq` with `reason`, `cause`, `deliveries`, `last_error`, `trace_id`
2. Delete from `pending`; advance `ack_floor` (a dead message no longer blocks retention)
3. If `dlq_stream` is configured, publish the payload into that stream under subject
   `<original-subject>` with server-set headers:

```
Messq-Origin-Stream: jobs
Messq-Origin-Seq:    918273
Messq-Origin-Msg-Id: 01JBQ7Z4K8VMH3E2Q9XW5T6R7Y
Messq-Origin-Consumer: emailer
Messq-Deliveries:    5
Messq-Cause:         max_deliver
Messq-Reason:        smtp: 550 mailbox unavailable
Messq-Trace-Id:      4bf92f3577b34da6a3ce929d0e0e4736
Messq-Dead-At:       2026-08-21T09:14:22.881Z
```

The `Messq-` header prefix is **reserved**; user headers starting with it are rejected with
`INVALID_ARGUMENT`. This keeps the DLQ envelope forgeable-proof and makes DLQ contents
self-describing when read by an unrelated tool.

`Requeue` moves a DLQ entry back to UNSEEN (`--reset-deliveries` or preserve), emitting `requeue`
with the operator's principal — a DLQ replay is an audited action.

### 4.5 Ack tokens — the load-bearing design

```
AckToken := base64url( body || tag )
body     := "mq1" (3B) || consumer_id (u64 LE) || seq (u64 LE)
                       || delivery_seq (u64 LE) || epoch (u32 LE)
tag      := HMAC-SHA256(K, body)[0:8]
```

35 bytes of body + 8 bytes tag → 58 characters base64url. Opaque to clients; the SDK never parses
it and the spec says so.

Why every field is there:

- **`consumer_id` + `seq`** — addresses the delivery without any session context. This is what makes
  C3 true: a worker can crash, reconnect on a new stream, and ack work it took before the
  disconnect. It is also what makes `messq ack <token>` a usable CLI command and what makes
  fire-and-forget acks over a *different* connection legal.
- **`delivery_seq`** — a per-consumer monotonic counter across all deliveries. It makes a *stale*
  ack detectable: if attempt 3 times out, is redelivered as attempt 4, and then attempt 3's worker
  finally wakes up and acks, the token carries the old `delivery_seq` and is rejected `ABORTED`
  with an `ack.stale` warning. Ack-by-sequence-number systems silently accept that ack and lose
  the second worker's result. This is the single most valuable field in the token.
- **`epoch`** — invalidates every outstanding token after a `Seek` or `Purge`, so a replay cannot be
  corrupted by acks belonging to the previous cursor generation.
- **HMAC tag** — lets the server reject garbage or forged tokens with a memcmp, no database hit, and
  prevents a client from acking a message it was never handed. Key lives in `meta`, generated on
  first boot, 32 random bytes. Rotation verifies against `current` and `previous`.

**Token durability across restart:** tokens survive, because the key is persisted and
`(consumer_id, seq, delivery_seq, epoch)` are all durable-ish. A token minted for a delivery whose
`pending` row was lost in an unclean shutdown will be accepted as a duplicate ack (idempotent path)
— harmless.

---

## 5. API / protocol

### 5.1 Decision: Connect protocol over `net/http`, via `connectrpc.com/connect`

**Not raw gRPC. Not hand-rolled TCP framing. Not REST/JSON-only.**

The requirement is that one wire serve three audiences simultaneously — a human with `curl`, a Go
service with a typed SDK, and the `messq` CLI — without a translation layer. connect-go is the only
option that does this, because a single `connect.Handler` answers the Connect protocol (HTTP/1.1
*and* HTTP/2, JSON *or* binary protobuf), the gRPC protocol, and gRPC-Web, all from the same
mounted path. Consequences we get for free:

- `curl` works on unary RPCs with plain JSON bodies — no binary framing to fight
- RPC, `/metrics`, `/healthz` and pprof share one `net/http` mux, one port, one Unix socket
- Existing gRPC tooling (`grpcurl`, generated clients in other languages) works unchanged
- Standard `net/http` middleware applies; no parallel interceptor universe for HTTP concerns
- connect-go follows semantic versioning, which grpc-go historically has not

**The cost, stated honestly:** the Connect protocol's *bidirectional* streaming requires HTTP/2.
HTTP/1.1-only clients cannot use `Consume`. That is precisely why `Fetch` + `Ack` exist as unary
RPCs — the universal fallback that works from any language with an HTTP client and from `curl`.
This is documented in `PROTOCOL.md` as a first-class alternative, not a degraded mode.

**Custom TCP framing was considered and rejected.** It would be smaller on the wire and it is what
NSQ and beanstalkd did. But it means writing and maintaining a client for every language, and it
means no `curl`, no HTTP proxy, no standard tooling. For a product whose selling point is
operability, a bespoke binary protocol is a liability, not an asset.

### 5.2 Package, path, generation

- Proto package `messq.v1`, Go package `github.com/messq/messq/gen/messq/v1`
- Procedures at `/messq.v1.MessqService/<Method>`
- Generated with `buf` + `protoc-gen-go` + `protoc-gen-connect-go`
- Reads (`Get*`, `List*`) are annotated `idempotency_level = NO_SIDE_EFFECTS`, which makes
  connect-go serve them over HTTP **GET** — cacheable, bookmarkable, trivially curl-able

### 5.3 The service — 22 methods, 2 streaming

```protobuf
syntax = "proto3";
package messq.v1;

import "google/protobuf/duration.proto";
import "google/protobuf/timestamp.proto";

service MessqService {
  // ---- data plane (4) ----
  rpc Publish(PublishRequest) returns (PublishResponse);
  rpc Fetch(FetchRequest) returns (FetchResponse);                 // unary pull; the curl path
  rpc Consume(stream ConsumeRequest) returns (stream ConsumeResponse); // HTTP/2 only
  rpc Settle(SettleRequest) returns (SettleResponse);              // out-of-band ack/nak/term/working

  // ---- streams (6) ----
  rpc CreateStream(CreateStreamRequest) returns (StreamInfo);
  rpc UpdateStream(UpdateStreamRequest) returns (StreamInfo);
  rpc DeleteStream(DeleteStreamRequest) returns (DeleteStreamResponse);
  rpc GetStream(GetStreamRequest) returns (StreamInfo) { option idempotency_level = NO_SIDE_EFFECTS; }
  rpc ListStreams(ListStreamsRequest) returns (ListStreamsResponse) { option idempotency_level = NO_SIDE_EFFECTS; }
  rpc PurgeStream(PurgeStreamRequest) returns (PurgeStreamResponse);

  // ---- consumers (6) ----
  rpc CreateConsumer(CreateConsumerRequest) returns (ConsumerInfo);
  rpc UpdateConsumer(UpdateConsumerRequest) returns (ConsumerInfo);
  rpc DeleteConsumer(DeleteConsumerRequest) returns (DeleteConsumerResponse);
  rpc GetConsumer(GetConsumerRequest) returns (ConsumerInfo) { option idempotency_level = NO_SIDE_EFFECTS; }
  rpc ListConsumers(ListConsumersRequest) returns (ListConsumersResponse) { option idempotency_level = NO_SIDE_EFFECTS; }
  rpc SeekConsumer(SeekConsumerRequest) returns (ConsumerInfo);

  // ---- inspection (4) ----
  rpc GetMessage(GetMessageRequest) returns (GetMessageResponse) { option idempotency_level = NO_SIDE_EFFECTS; }
  rpc ListMessages(ListMessagesRequest) returns (ListMessagesResponse) { option idempotency_level = NO_SIDE_EFFECTS; }
  rpc ListPending(ListPendingRequest) returns (ListPendingResponse) { option idempotency_level = NO_SIDE_EFFECTS; }
  rpc Requeue(RequeueRequest) returns (RequeueResponse);           // DLQ -> stream

  // ---- observability (2) ----
  rpc WatchEvents(WatchEventsRequest) returns (stream Event);      // live + historical replay
  rpc GetServerInfo(GetServerInfoRequest) returns (ServerInfo) { option idempotency_level = NO_SIDE_EFFECTS; }
}
```

That is the whole surface. C1 says any 23rd method must displace one of these or be argued for in
an ADR.

### 5.4 Publish

```protobuf
message PublishRequest {
  string stream = 1;
  repeated NewMessage messages = 2;      // batch; all-or-nothing within one transaction
  Expect expect = 3;                     // optimistic concurrency, evaluated inside the txn
  reserved 100 to 199;                   // future core
}

message NewMessage {
  string subject = 1;
  bytes  payload = 2;
  map<string, string> headers = 3;       // "Messq-*" prefix reserved, rejected if used
  string dedup_key = 4;                  // idempotent publish within stream dedup window
  string trace_id  = 5;                  // 32 hex chars (W3C trace-id); generated if empty
}

message Expect {
  optional uint64 last_seq         = 1;  // stream's last seq must equal this
  optional uint64 last_subject_seq = 2;  // this subject's last seq must equal this
  optional bool   stream_exists    = 3;
}

message PublishResponse { repeated PublishAck acks = 1; }

message PublishAck {
  uint64 seq = 1;
  string msg_id = 2;                     // ULID
  uint64 subject_seq = 3;
  bool   duplicate = 4;                  // dedup_key hit; seq points at the original
  google.protobuf.Timestamp published_at = 5;
  string trace_id = 6;
}
```

Curl:

```bash
curl --unix-socket /run/messq/messq.sock \
  -H 'Content-Type: application/json' \
  -d '{"stream":"jobs","messages":[
        {"subject":"jobs.email","payload":"aGVsbG8=","dedup_key":"invoice-4711"}]}' \
  http://localhost/messq.v1.MessqService/Publish
# {"acks":[{"seq":"918273","msgId":"01JBQ7Z4K8VMH3E2Q9XW5T6R7Y","subjectSeq":"411",
#           "publishedAt":"2026-08-21T09:14:22.881Z","traceId":"4bf92f35..."}]}
```

### 5.5 Consume — the streaming model

Bidirectional. Client frames drive; server frames respond. Modelled on NSQ's `RDY` and AMQP 1.0's
link credit, with one deliberate difference: **credit is additive, not absolute.**

```protobuf
message ConsumeRequest {
  oneof frame {
    Attach attach = 1;
    Credit credit = 2;
    Settle settle = 3;
    Drain  drain  = 4;
    Ping   ping   = 5;
  }
  reserved 100 to 199;
}

message Attach {
  string stream = 1;
  string consumer = 2;                   // must already exist (server-side config, C: §2)
  Credit initial_credit = 3;
  string client_name = 4;                // shows up in `consumer info` and in timeout logs
  string client_version = 5;
  string client_instance = 6;            // hostname/pid; disambiguates replicas
}

message Credit {                         // ADDITIVE grant
  uint32 messages = 1;
  uint64 bytes = 2;                      // 0 = unlimited bytes, messages still bounded
}

message Drain { bool close_after_settled = 1; }  // zero remaining credit

message Settle {
  uint64 correlation_id = 1;             // echoed in Settled; omit for fire-and-forget
  repeated Disposition dispositions = 2;
}

message Disposition {
  string ack_token = 1;
  AckKind kind = 2;
  google.protobuf.Duration delay = 3;    // NAK only; overrides backoff
  string reason = 4;                     // logged, stored on DLQ entries
  google.protobuf.Duration handler_time = 5;  // optional; feeds ack-latency histogram
}

enum AckKind {
  ACK_KIND_UNSPECIFIED = 0;
  ACK_KIND_ACK     = 1;
  ACK_KIND_NAK     = 2;
  ACK_KIND_TERM    = 3;
  ACK_KIND_WORKING = 4;
}

message ConsumeResponse {
  oneof frame {
    Attached attached = 1;
    Delivery delivery = 2;
    Settled  settled  = 3;
    Status   status   = 4;
    Pong     pong     = 5;
  }
  reserved 100 to 199;
}

message Attached {
  ConsumerInfo consumer = 1;
  uint64 epoch = 2;
  google.protobuf.Timestamp server_time = 3;
  google.protobuf.Duration  ack_wait = 4;      // effective, after config resolution
  uint32 max_ack_pending = 5;
  repeated string capabilities = 6;            // server feature flags (see §5.9)
}

message Delivery {
  string ack_token = 1;                        // opaque; never parse
  string msg_id = 2;
  uint64 seq = 3;
  string subject = 4;
  uint64 subject_seq = 5;
  bytes  payload = 6;
  map<string, string> headers = 7;
  string trace_id = 8;
  uint32 deliveries = 9;                       // 1 on first attempt
  uint64 delivery_seq = 10;
  google.protobuf.Timestamp published_at = 11;
  google.protobuf.Timestamp lease_expires_at = 12;  // absolute, SERVER clock
  google.protobuf.Timestamp server_time = 13;       // so the client can measure its own skew
  uint64 stream_last_seq = 14;                 // free lag signal on every delivery
  uint64 pending_after = 15;                   // backlog remaining for this consumer
  reserved 16 to 99;
  reserved 100 to 199;
}

message Settled {
  uint64 correlation_id = 1;
  repeated SettleResult results = 2;           // sent only AFTER durable commit
}
message SettleResult { string ack_token = 1; uint32 code = 2; string message = 3; }

message Status {
  StatusCode code = 1;
  string message = 2;
  uint64 detail = 3;                           // e.g. events lost, backlog size
}

enum StatusCode {
  STATUS_CODE_UNSPECIFIED = 0;
  STATUS_CODE_IDLE_HEARTBEAT   = 1;  // no traffic; connection is alive
  STATUS_CODE_NO_MESSAGES      = 2;  // credit outstanding, nothing to send
  STATUS_CODE_FLOW_STALLED     = 3;  // messages available, credit or max_ack_pending exhausted
  STATUS_CODE_FLOW_RESUMED     = 4;
  STATUS_CODE_DRAINING         = 5;  // graceful shutdown; settle and go
  STATUS_CODE_CONSUMER_CHANGED = 6;  // config or epoch changed; re-attach
  STATUS_CODE_LOST_EVENTS      = 7;  // WatchEvents only; detail = count
}
```

**Why additive credit rather than NSQ's absolute `RDY`.** With absolute RDY the client must reason
about how many messages are already in flight when it lowers the value, and the classic race
(client sets RDY 0 while the server has already dispatched three) needs client-side bookkeeping.
Additive credit removes the race: the client grants exactly one credit per settled message and
never computes a balance. The only thing absolute RDY does better is "stop now", so we add an
explicit `Drain` frame for that. Server-side, credit is clamped by `max_ack_pending` anyway, so an
over-granting client cannot hurt the broker — the ceiling is server policy, the pace is client
policy.

**Heartbeats.** When idle, the server sends `IDLE_HEARTBEAT` every `heartbeat` (default 15 s). A
client that misses two consecutive heartbeats must reconnect. This is the NSQ lesson: without a
heartbeat, "the queue is empty" and "the connection is dead" are indistinguishable, and workers sit
silently forever. Additionally, HTTP/2 keepalive pings are configured server-side
(`MaxConnectionIdle`, ping enforcement) to reap half-open TCP connections long before the kernel's
user timeout would.

**Frame ordering rules (normative):**

1. `Attach` must be the first frame; anything else → `FAILED_PRECONDITION`.
2. The server sends exactly one `Attached` before any `Delivery`.
3. `Settle` may be sent at any time, including after `Drain` and including for tokens issued on a
   *previous* connection.
4. `Settled` is sent only after the settle batch is durable. **A `Settle` you did not wait for is a
   hint, not a promise** — if the daemon crashes before commit, the message will be redelivered.
   The SDK's default is fire-and-forget with a `Drain`-time flush; `WithSyncAck()` waits.
5. On `CONSUMER_CHANGED`, the client should re-attach; outstanding tokens may be `ABORTED`.

### 5.6 Fetch + Settle — the universal fallback

```protobuf
message FetchRequest {
  string stream = 1;
  string consumer = 2;
  uint32 max_messages = 3;               // default 1, cap 1024
  uint64 max_bytes = 4;
  google.protobuf.Duration wait = 5;     // long-poll up to this long; default 0 = return now
  bool   no_wait = 6;
}
message FetchResponse {
  repeated Delivery deliveries = 1;
  StatusCode status = 2;                 // NO_MESSAGES / FLOW_STALLED when empty
}
message SettleRequest  { repeated Disposition dispositions = 1; }
message SettleResponse { repeated SettleResult results = 1; }
```

`Fetch` grants exactly `max_messages` credit for the duration of the call and takes it back on
return. Long-polling with `wait` gives cheap latency without a stream. `Settle` is unary, batched,
idempotent, and takes tokens from *any* source — so this works:

```bash
TOKEN=$(curl -s --unix-socket /run/messq/messq.sock -H 'Content-Type: application/json' \
  -d '{"stream":"jobs","consumer":"emailer","max_messages":1,"wait":"5s"}' \
  http://localhost/messq.v1.MessqService/Fetch | jq -r '.deliveries[0].ackToken')

do_the_work
curl -s --unix-socket /run/messq/messq.sock -H 'Content-Type: application/json' \
  -d "{\"dispositions\":[{\"ackToken\":\"$TOKEN\",\"kind\":\"ACK_KIND_ACK\"}]}" \
  http://localhost/messq.v1.MessqService/Settle
```

A complete, correct, at-least-once worker in five lines of shell. That snippet is in the README and
it is a CI-verified golden test (§8).

### 5.7 Errors

Connect codes, chosen per connect-go's own guidance, plus a structured detail on every error:

```protobuf
message ErrorDetail {
  Reason reason = 1;
  string stream = 2; string consumer = 3; uint64 seq = 4; string msg_id = 5;
  google.protobuf.Duration retry_after = 6;
  uint64 expected = 7; uint64 actual = 8;   // for Expect failures
}
enum Reason {
  REASON_UNSPECIFIED = 0;
  REASON_STREAM_NOT_FOUND = 1;    REASON_CONSUMER_NOT_FOUND = 2;
  REASON_MESSAGE_NOT_FOUND = 3;   REASON_ALREADY_EXISTS = 4;
  REASON_EXPECT_MISMATCH = 5;     REASON_MAX_PENDING = 6;
  REASON_RATE_LIMITED = 7;        REASON_MESSAGE_TOO_LARGE = 8;
  REASON_STALE_ACK_TOKEN = 9;     REASON_EPOCH_MISMATCH = 10;
  REASON_INVALID_ACK_TOKEN = 11;  REASON_PROTOCOL_VERSION = 12;
  REASON_SUBJECT_INVALID = 13;    REASON_STORAGE_FULL = 14;
  REASON_DRAINING = 15;           REASON_WORKING_EXHAUSTED = 16;
}
```

| Situation | Code | Client action |
|---|---|---|
| Unknown stream/consumer/message | `NOT_FOUND` | fix config |
| Create conflict | `ALREADY_EXISTS` | idempotent create → treat as success if config matches |
| `Expect` mismatch, protocol version, `WORKING` past ceiling | `FAILED_PRECONDITION` | do not retry blindly |
| `max_ack_pending`, rate limit, disk full, message too large on a full stream | `RESOURCE_EXHAUSTED` | back off by `retry_after` |
| Stale/epoch-mismatched ack token | `ABORTED` | drop the result, do not retry |
| Bad subject, oversized message, reserved header | `INVALID_ARGUMENT` | fix the caller |
| Missing/invalid credential | `UNAUTHENTICATED` | re-auth |
| Authenticated but not permitted | `PERMISSION_DENIED` | fix ACL |
| Draining or restarting | `UNAVAILABLE` | retry with backoff |

The SDK's retry predicate is exactly `UNAVAILABLE | RESOURCE_EXHAUSTED | DEADLINE_EXCEEDED`, which
matches connect-go's documented retryable set.

### 5.8 Auth hooks

Auth lives in the protocol from v1 even though the default deployment is unauthenticated.

**Transport-level principals.**

| Listener | Mechanism | Principal |
|---|---|---|
| Unix socket (default) | `SO_PEERCRED` on the accepted conn | `unix:<username>` (uid resolved), plus gid list |
| TCP | `Authorization: Bearer messq_<keyid>_<secret>` | `token:<keyid>` — Connect headers are gRPC metadata are HTTP headers; one mechanism, three protocols |
| TCP + mTLS | client certificate | `cert:<SAN or CN>` |

Tokens are stored as argon2id hashes in `/etc/messq/tokens.d/*.toml` with `expires_at` and a scope
list. The `Token` type implements `slog.LogValuer` returning `"REDACTED"`, so a secret cannot reach
a log line even through `%v`.

**Authorization is one interface, wired as a connect interceptor** (unary *and* streaming, since
`WithInterceptors` composes onion-style over both):

```go
type Verb string // publish, consume, settle, inspect, replay, purge, admin

type Authorizer interface {
    Authorize(ctx context.Context, p Principal, v Verb, r Resource) error
}
type Resource struct { Stream, Subject, Consumer string }
```

- `Publish` authorizes per message subject (so `svc-a` may publish `jobs.email.>` but not `jobs.billing.>`)
- `Consume` authorizes once at `Attach`, then re-checks when the credential's `exp` passes — never
  per delivery, because per-message authorization on a hot path is how brokers get slow
- Every mutating RPC records `principal` into the event log → the audit trail is a by-product

v1 ships three implementations: `none`, `file` (tokens + a TOML ACL), `mtls`. An `exec` authorizer
that shells out to an external policy program is Phase 3. **No OIDC, no JWT validation, no
Vault integration in the daemon** — that is what a reverse proxy is for, and Connect runs behind
one unchanged.

### 5.9 Versioning & compatibility strategy

**The promise:** `messq.v1` is additive-only for its entire life. A client built against any
`messq.v1` proto works against any later 1.x server, and a 1.x client works against a later server
(unknown fields are preserved by the protobuf runtime and ignored).

Mechanics:

1. **Handshake header.** Every request carries `messq-protocol-version: 1`. A server that does not
   support the major version replies `FAILED_PRECONDITION` / `REASON_PROTOCOL_VERSION` with the
   supported set in the detail. The header travels identically over Connect, gRPC and gRPC-Web.
2. **Capability flags, not version numbers.** `GetServerInfo` and `Attached` return
   `repeated string capabilities` — `"delayed-delivery"`, `"priority"`, `"compression:zstd"`,
   `"consumer-groups"`, `"rate-limit"`. Phase 2 features (§9, M8) ship as new *optional fields*
   guarded by a capability string. No `v2` package is needed for any planned feature.
3. **Field-number discipline.** Tags 1–15 (one-byte tags) are reserved for hot-path fields on
   `Delivery`, `NewMessage` and `Event`. Every message reserves `100..199` for "future core" and
   treats `200+` as experimental. Tags are never reused; removals get `reserved`.
4. **Enum discipline.** Every enum has `_UNSPECIFIED = 0`. Clients must treat an unknown enum value
   as `_UNSPECIFIED` **and log it at DEBUG with the numeric value** — so a forward-compatibility gap
   is diagnosable rather than mysterious. The SDK does this for you.
5. **Semantics are frozen with the tag.** A field's meaning never changes; a changed meaning gets a
   new field and the old one is deprecated. Default values never change.
6. **CI gate from commit #1.** `buf lint` (`STANDARD`) and
   `buf breaking --against '.git#branch=main'` with `use: [WIRE_JSON]`. `WIRE_JSON` rather than
   `FILE` because we reserve the right to move messages between `.proto` files while keeping both
   the binary and JSON encodings compatible.
7. **If v2 ever happens**, `messq.v2` is served *concurrently* from the same binary — different
   procedure paths, same handlers underneath — for at least two minor releases, with a
   `v1_deprecated` capability flag and a `WARN` log per v1 call.
8. **The SDK is versioned separately from the proto.** See §6.4: the public Go API never exposes a
   generated type, so a proto addition is never an SDK break.

`COMPATIBILITY.md` states this contract in the repository, and `PROTOCOL.md` is the normative wire
spec — both are release artefacts, not afterthoughts.

---

## 6. CLI & developer experience

### 6.1 One binary, `messq`

`messq serve` is the daemon; everything else is a client. `MESSQ_ADDR` defaults to
`unix:///run/messq/messq.sock`, falling back to `$XDG_RUNTIME_DIR/messq.sock` for rootless
development. Built with `spf13/cobra`.

```
messq serve [--config /etc/messq/messq.toml] [--dev]

messq stream    create|ls|info|update|rm|purge
messq consumer  create|ls|info|update|rm|seek
messq pub       <stream> <subject> [-d DATA | -f FILE | -]  [--dedup-key K] [-H k=v] [--trace-id T]
messq sub       <stream> <consumer> [--ack auto|manual|none] [--max-in-flight N] [--json]
messq fetch     <stream> <consumer> [-n 10] [--wait 5s] [--no-ack]
messq ack       <token>...
messq nak       <token> [--delay 30s] [--reason "..."]
messq term      <token> --reason "..."
messq peek      <stream> [--seq N | --msg-id ULID] [--subject P] [--last-per-subject] [--raw]
messq pending   <stream> <consumer> [--older-than 1m]
messq dlq       ls|show|requeue|purge
messq trace     <msg-id | --trace-id T>
messq tail      [--stream S] [--consumer C] [--event dead,timeout] [--since 1h] [-f]
messq replay    <stream> --from start|seq:N|time:T [--to ...] [--into <consumer> | --stdout]
messq bench     pub|sub
messq doctor
messq completion bash|zsh|fish
```

### 6.2 The three commands that define the product

**`messq trace <msg-id>` — reconstruct one message's whole life.** Reads the `events` table.

```
$ messq trace 01JBQ7Z4K8VMH3E2Q9XW5T6R7Y
msg_id  01JBQ7Z4K8VMH3E2Q9XW5T6R7Y   stream jobs   subject jobs.email   seq 918273
trace   4bf92f3577b34da6a3ce929d0e0e4736                    payload 412 B

  09:14:22.881  publish   by unix:deploybot          dedup_key=invoice-4711
  09:14:22.902  deliver   emailer  attempt 1/5       client=mailer-7f9c/pid31 lease=+30s
  09:14:52.903  timeout   emailer  attempt 1         held_for=30.0s  client=mailer-7f9c/pid31
  09:14:52.905  deliver   emailer  attempt 2/5       client=mailer-2ab1/pid18 lease=+30s
  09:14:55.113  nak       emailer  attempt 2         delay=30s reason="smtp: 451 try later"
  09:15:25.114  deliver   emailer  attempt 3/5       client=mailer-2ab1/pid18
  09:15:26.002  ack       emailer  attempt 3         handled_in=888ms  total=63.1s

  outcome: ACKED after 3 attempts, 63.1s end to end
```

That output is the reason someone chooses messq. It is impossible to produce unless `msg_id` and
`trace_id` are protocol fields (C5), which is why they are.

**`messq tail --event dead,timeout -f`** — live incident view over `WatchEvents`, filtered
server-side. Not a log tail; a structured event stream that also works when stdout logs are
sampled.

**`messq doctor`** — prints the effective durability level in prose ("a returned publish ack
survives power loss: YES"), verifies socket permissions, checks free disk against retention limits,
measures actual fsync latency on the data directory, and warns when `max_deliver` × `ack_wait`
exceeds `max_ack_wait_total`.

### 6.3 DX details that are cheap and disproportionately valuable

- `--output table|json|yaml` on **every** command, and `--json` implies stable field names. The CLI
  is scriptable by construction.
- **Dynamic shell completion**: cobra `ValidArgsFunction` that calls `ListStreams`/`ListConsumers`,
  so `messq sub jobs <TAB>` completes real consumer names.
- **Documented exit codes**: `0` ok, `1` error, `2` usage, `3` not found, `4` permission, `5`
  daemon unavailable, `6` failed precondition. Scripts can branch without parsing text.
- `messq serve --dev` — ephemeral data dir, verbose text logs, and a ready banner that prints three
  copy-pasteable commands. First useful message published within 30 seconds of `go install`.
- `messq consumer create --explain` prints the resulting redelivery schedule as a timeline
  ("attempt 1 at t=0, 2 at t=5s, 3 at t=35s, 4 at t=5m35s, 5 at t=1h5m35s, then dead-letter"),
  because nobody can compute a backoff array in their head.
- `messq pub --dry-run` validates subject and size against stream config without writing.

### 6.4 Go SDK

Module path `github.com/messq/messq/client`, importable as `messq`. **The public API never exposes a
generated protobuf type.** Generated code lives in `.../gen/`, is `internal`-adjacent by convention,
and every wire type is mapped to a hand-written struct. This is the rule that makes §5.9 point 8
true: adding a proto field never changes an SDK signature.

```go
c, err := messq.Dial(ctx, os.Getenv("MESSQ_ADDR"))    // unix:// or http(s)://
defer c.Close()

// publish
ack, err := c.Publish(ctx, "jobs", messq.Msg{
    Subject:  "jobs.email",
    Data:     body,
    DedupKey: invoiceID,             // retry-safe by construction
    Headers:  messq.H{"tenant": "acme"},
})

// consume: the 90% path
sub, err := c.Consume(ctx, "jobs", "emailer", messq.WithMaxInFlight(32))
err = sub.Handle(ctx, func(ctx context.Context, m *messq.Message) error {
    if err := send(ctx, m.Data); err != nil {
        if errors.Is(err, ErrBadAddress) {
            return messq.Terminal(err)      // -> TERM -> DLQ, no more attempts
        }
        return err                          // -> NAK with the consumer's backoff
    }
    return nil                              // -> ACK
})

// consume: manual control
for m := range sub.Messages() {             // channel; also sub.All() as an iter.Seq
    switch {
    case ok:        m.Ack()
    case retryable: m.Nak(messq.After(30*time.Second), messq.Reason(err.Error()))
    default:        m.Term(messq.Reason("poison"))
    }
}
```

Ergonomic decisions, each earning its keep:

1. **Auto-`WORKING`.** While a `Handle` callback runs, a goroutine extends the lease every
   `ack_wait/2` until `max_ack_wait_total`. This kills the number-one at-least-once bug — a slow
   handler causing a duplicate delivery — without the user knowing the mechanism exists.
   `WithoutAutoWorking()` opts out.
2. **Auto-credit.** One credit returned per settled message, window = `WithMaxInFlight(n)`. The user
   never sees the word "credit" unless they want to.
3. **Reconnect that keeps its promises.** On disconnect, the SDK re-`Attach`es with fresh credit and
   **replays outstanding settles by token over the new stream**. Nothing is lost because tokens are
   session-independent (C3). Exponential backoff with full jitter, capped at 30 s.
4. **Clock-skew-proof deadlines.** Every `Delivery` carries `lease_expires_at` *and* `server_time`;
   the SDK computes the offset once per connection and exposes `m.Deadline()` in local time and
   `m.TimeLeft()`.
5. **Batching publisher.** `p := c.Publisher("jobs", messq.WithBatchSize(128), messq.WithLinger(2*time.Millisecond))`
   with `p.PublishAsync(msg) (*Future, error)`, a bounded queue that returns
   `ErrPublisherFull` rather than growing without limit, and `p.Flush(ctx)`.
6. **`messqtest.NewServer(t)`** — spins an in-process daemon on a temp dir and Unix socket, returns
   a `*messq.Client`, cleans up via `t.Cleanup`. Shipping this is what makes the SDK adoptable;
   without it every user writes a worse version.
7. **Tracing without a dependency.** Core has no OpenTelemetry import. A separate
   `client/messqotel` module extracts `trace_id` from the ambient span and injects it, and creates
   a span per delivery. Users who do not want OTel never compile it.
8. **`messq.Message` is a struct with methods, not an interface.** Fields are additive-safe; an
   interface would not be.

### 6.5 Non-Go clients

There is no plan to hand-write SDKs for five languages. Instead:

- `PROTOCOL.md` documents Publish / Fetch / Settle as three HTTP POSTs with JSON bodies. Any
  language with an HTTP client is a first-class client in about 30 lines.
- Generated gRPC clients work for anyone who wants them (`buf generate` templates for Python,
  TypeScript and Java are committed, unsupported but present).
- A `contrib/` directory holds community clients with an explicit "not supported by us" banner.

---

## 7. Observability & logging design

### 7.1 The fixed field vocabulary

Logging is a product feature, so its schema is versioned like the protocol. Every message-lifecycle
record emits the same keys via `log/slog`, in this order, and nothing else at the top level:

```
ts  level  event  stream  subject  consumer  msg_id  seq  trace_id
deliveries  delivery_seq  principal  client  dur_ms  reason
```

Event names are a **closed, documented, greppable vocabulary**:

`publish` · `publish.duplicate` · `publish.rejected` · `deliver` · `ack` · `ack.duplicate` ·
`ack.stale` · `ack.epoch_mismatch` · `nak` · `working` · `working.exhausted` · `timeout` · `dead` ·
`requeue` · `seek` · `purge` · `flow.stalled` · `flow.resumed` · `consumer.attach` ·
`consumer.detach` · `lag.warn` · `retention.discard` · `retention.forced_discard` · `recovered` ·
`auth.denied`

Adding an event name is a documentation change. `PROTOCOL.md` carries the table.

`--log-format=json` (default) uses `slog.NewJSONHandler`; `--log-format=text` uses
`slog.NewTextHandler` with aligned columns for humans. Both are fed from one `slog.Handler` chain,
with a `ReplaceAttr` that normalises timestamps to RFC3339 with milliseconds and redacts anything
implementing `slog.LogValuer` (credentials do).

### 7.2 Sampling that does not lie

Per-message INFO logs at 5k msg/s are useless and expensive. But a sampled log that drops the *one*
event you needed is worse than no log. The rule:

- **Normal transitions** (`publish`, `deliver`, `ack`) are DEBUG and sampled at `--trace-sample=1/N`
  (default: 1/1 under 100 msg/s, automatically 1/100 above it, with the effective rate logged when
  it changes).
- **Abnormal transitions** (`nak`, `timeout`, `dead`, `ack.stale`, `working.exhausted`,
  `flow.stalled`, `retention.forced_discard`, `auth.denied`) are INFO/WARN/ERROR and are
  **never sampled**.
- **Sticky trace:** the first time a message experiences an abnormal transition, it is promoted to
  full-trace for the rest of its life. So the messages you actually investigate always have a
  complete history, and the boring ones do not.
- `--trace-subjects=jobs.billing.>` forces full trace for matching subjects regardless of sampling.

This is a genuinely distinctive design and it falls straight out of C5: because the event carries a
stable `msg_id`, the sampler can make a per-message decision and remember it.

### 7.3 Three sinks, one event

The event bus fans one `Event` struct to:

1. **`slog`** → stdout (and optionally a rotating JSONL file)
2. **`events` table** → powers `messq trace`, retention `--event-retention 72h` / `--max-events 5M`.
   This is what makes forensics possible without a log aggregation stack — which matters enormously
   for the target user, a small team running one binary.
3. **`WatchEvents` RPC** → live subscribers (`messq tail -f`, dashboards), with server-side filters
   on stream, consumer, subject, event name, `msg_id` and `trace_id`, plus `since` for historical
   replay out of the `events` table before switching to live.

Slow subscribers get a bounded ring buffer, then a `LOST_EVENTS` status frame with a count. The data
path is never blocked by a watcher.

### 7.4 Metrics

`prometheus/client_golang` with a **non-default registry** (`prometheus.NewRegistry()` +
`promauto.With(reg)`), exposed by `promhttp.HandlerFor(reg, promhttp.HandlerOpts{MaxRequestsInFlight: 8})`
on `/metrics` of the same mux.

```
messq_published_total{stream,subject_root}
messq_publish_duplicates_total{stream}
messq_delivered_total{stream,consumer,attempt_class}   # first | redelivery
messq_settled_total{stream,consumer,kind}              # ack|nak|term|working
messq_timeouts_total{stream,consumer}
messq_dead_total{stream,consumer,cause}                # max_deliver | term
messq_pending{stream,consumer}                         # gauge: in-flight + scheduled
messq_backlog{stream,consumer}                         # gauge: last_seq - ack_floor  (lag)
messq_oldest_pending_seconds{stream,consumer}
messq_flow_stalled_seconds_total{stream,consumer}      # THE consumer-is-the-bottleneck signal
messq_ack_latency_seconds{stream,consumer}             # histogram: deliver -> ack
messq_e2e_latency_seconds{stream,consumer}             # histogram: publish -> ack
messq_store_commit_seconds                             # histogram: group-commit incl. fsync
messq_store_batch_size                                 # histogram
messq_stream_bytes{stream} / messq_stream_messages{stream}
messq_events_dropped_total{sink}
messq_rpc_duration_seconds{procedure,code}             # from a connect interceptor
```

Cardinality discipline: `subject_root` is the first subject token, never the full subject. A full
`subject` label on a wildcard stream is an unbounded-cardinality trap and we refuse it.

`/healthz` = process alive. `/readyz` = store open, recovery complete, not draining. pprof only
behind `--debug-addr`.

---

## 8. Testing strategy

### 8.1 The state machine is a pure function

`transition(State, Event, Config, now) -> (State, []Effect)` has no I/O, no clock, no database. The
dispatcher only wires effects to the world. This is the highest-leverage testing decision in the
plan: the entire delivery semantics of §4 is covered by table-driven unit tests that run in
microseconds, and the transition table in §4.3 is *literally* the test table.

### 8.2 Deterministic simulation

A virtual clock, a seeded RNG, a single-goroutine scheduler, and an in-memory store. Ten thousand
randomised scenarios per CI run mixing publish / deliver / ack / nak / term / working / timeout /
credit-grant / drain / disconnect / crash / restart / seek / purge, asserting after **every** step:

- **I1** every published message eventually reaches ACKED or DEAD under fair scheduling
- **I2** `acks(m) ≤ deliveries(m)` for every message
- **I3** `ack_floor` is monotonically non-decreasing except across a `seek`
- **I4** `deliveries ≤ max_deliver` (with the documented post-crash undercount exemption)
- **I5** `pending ∩ dlq = ∅`, and `acked ∩ dlq = ∅`
- **I6** `in_flight ≤ max_ack_pending` and `delivered_unsettled ≤ granted_credit`
- **I7** with `ordered=true`, at most one in-flight message per subject, and per-subject `seq`
  arrives in increasing order across successful acks
- **I8** no ack token is ever accepted twice with a state change
- Failing seeds are printed and committed as regression cases.

### 8.3 Crash and durability

- **SIGKILL loop:** 100 iterations of publish-load → `SIGKILL` at a random point → restart → assert
  every acked message is absent from redelivery and every unacked message is present. Runs in CI.
- **Power-loss:** documented release-gate procedure using `dm-log-writes` over a loopback device,
  replaying the write log to arbitrary points and checking the invariants. Manual, per release, not
  per commit — but written down and reproducible.
- **fsync verification:** `messq doctor` measures fsync latency; a test asserts it is non-zero on a
  real filesystem, which catches "we silently opened with `synchronous=OFF`".

### 8.4 Protocol conformance

A black-box test binary drives a real daemon and asserts **identical observable behaviour across
all three wire protocols**: Connect+JSON, Connect+proto, gRPC. Same scenarios, same assertions,
three transports. Since one handler serves all three, this is mostly a regression net against
codec-specific bugs — and it is cheap.

**Curl golden tests.** Every `curl` example in `README.md` and `PROTOCOL.md` is extracted by a test
and executed against a fresh daemon, with the JSON output compared to a golden file (timestamps and
ULIDs normalised). The documentation cannot rot.

### 8.5 Compatibility gates

- `buf lint` (`STANDARD`) and `buf breaking --against '.git#branch=main'` (`WIRE_JSON`) on every PR
- **Cross-version matrix:** the previous three tagged client versions are compiled against the
  current server and run through the conformance suite; and the current client against the previous
  three servers. Additive-only is a promise we test, not one we assert.
- Golden wire fixtures: serialized `Delivery` / `PublishRequest` bytes from v1.0 are checked into
  `testdata/` and must still decode.

### 8.6 Everything else

- `-race` on all tests; goroutine-leak check (`goleak`) at the end of every daemon test
- Fuzzing (`go test -fuzz`) on: ack-token decoding, subject pattern matching, header decoding,
  protojson decoding of `PublishRequest`
- Nightly soak: 1 hour at 10k msg/s with 5% nak rate and 1% term rate, asserting flat memory and
  goroutine counts, with pprof artefacts retained
- Benchmark gates in CI with a regression budget: p99 publish latency, ack throughput, commit batch
  size, memory per in-flight message

---

## 9. Roadmap

Each milestone ends with a demoable acceptance test. Nothing is "done" without it.

### M0 — Contract first (no server) · ~1 week
`proto/messq/v1/{messq.proto,events.proto}`. `buf.yaml`, `buf.gen.yaml`, `buf.lock`. CI: `buf lint`,
`buf breaking`, `go vet`, `go build`. `Makefile`. ADR-0001 (Connect over gRPC/REST/custom TCP),
ADR-0002 (SQLite over bbolt), ADR-0003 (signed ack tokens), ADR-0004 (additive credit).
**Acceptance:** generated Go compiles; the breaking-change gate is live before a single line of
server code exists.

### M1 — Walking skeleton: publish, fetch, ack · ~2 weeks
SQLite schema v1 + migrations. Store writer with group commit and fsync. `Publish`, `Fetch`,
`Settle`, `CreateStream`, `GetStream`, `ListStreams`, `GetServerInfo`. Ack tokens with HMAC. `slog`
events for `publish`/`deliver`/`ack`. `messq serve`, `messq pub`, `messq fetch`, `messq ack`,
`messq stream`.
**Acceptance:** the five-line shell worker of §5.6 runs end to end via `curl --unix-socket`, and it
is a golden test in CI.

### M2 — Consumers, leases, redelivery, DLQ · ~3 weeks
Consumer CRUD. Dispatcher goroutine with lease min-heap. `ack_wait`, `backoff`, `max_deliver`,
`nak(delay)`, `term`, `working` with the `max_ack_wait_total` ceiling. `dlq` table + optional DLQ
stream with `Messq-*` headers. The pure transition function and its table tests.
CLI: `consumer`, `nak`, `term`, `dlq`, `pending`.
**Acceptance:** kill a worker mid-flight → redelivery after `ack_wait` with `deliveries=2`; exceed
`max_deliver` → DLQ entry carrying the reason from the last nak; `messq dlq requeue` returns it.

### M3 — Streaming Consume with credit flow control · ~3 weeks
Bidi `Consume`: `Attach`/`Credit`/`Settle`/`Drain`/`Ping` in, `Attached`/`Delivery`/`Settled`/
`Status`/`Pong` out. `max_ack_pending` enforcement, `FLOW_STALLED`/`NO_MESSAGES`/`IDLE_HEARTBEAT`,
HTTP/2 keepalive tuning. Ordered consumers (`ordered=true`).
Go SDK v0: `Dial`, `Publish`, `Consume`, `Handle`, auto-working, auto-credit, reconnect with
token replay, `messqtest`. CLI `messq sub`.
**Acceptance:** an SDK worker survives a daemon restart mid-batch with zero duplicate acks and zero
lost work; `flow.stalled` fires within one second of a worker stopping its credit grants and
`flow.resumed` when it resumes.

### M4 — Inspection, replay, and the trace story · ~3 weeks
`events` table + retention. `WatchEvents` with filters and `since`. `GetMessage`, `ListMessages`,
`ListPending`, `SeekConsumer`, `PurgeStream`, `Requeue`. Sampling + sticky-trace logic.
CLI: `peek`, `trace`, `tail`, `replay`, `seek`, `--output json` everywhere, dynamic completions.
**Acceptance:** `messq trace <msg-id>` reproduces the §6.2 output exactly, including the client
identity of the session that timed out.

### M5 — Durability hardening · ~2 weeks
`durability=strict|relaxed`, dirty marker, `recovered` event, `PRAGMA quick_check` path, dedup
window + `Expect` preconditions, retention enforcement with `forced_discard` warnings, `messq
doctor`. Deterministic simulation harness with I1–I8. SIGKILL CI job. `dm-log-writes` procedure
written up.
**Acceptance:** 10 000 simulated scenarios green across 20 seeds; 100 SIGKILL/restart cycles with
zero violations.

### M6 — Auth, limits, operational safety · ~2 weeks
`SO_PEERCRED` principals, bearer tokens with argon2id, mTLS, `Authorizer` interface + interceptors,
TOML ACLs, per-principal rate limits returning `retry_after`, `max_msg_size`, connect
`WithReadMaxBytes`, connection limits, graceful drain, systemd unit, config file + env + flag
precedence.
**Acceptance:** an unauthorized publish is denied, audited with the principal, and visible in
`messq tail --event auth.denied`; a drain completes with no in-flight work redelivered.

### M7 — 1.0 · ~3 weeks
Prometheus metrics, `/healthz`, `/readyz`, conformance suite across three protocols, curl golden
tests, cross-version compatibility matrix, GoReleaser (static tarball, `.deb`, `.rpm`, container),
`PROTOCOL.md`, `COMPATIBILITY.md`, `OPERATIONS.md`, benchmark numbers published in the README.
**Freeze `messq.v1`. Publish the additive-only promise.**

### M8 — Phase 2, all as capability flags, no v2 · ongoing
Each item is new optional proto fields plus a capability string, shipped independently:
`publish_at` delayed delivery · priority bands with weighted selection · per-consumer rate limiting
· consumer groups as **leases over one consumer** (multiple attachments, exclusive per-subject
leases, no partition assignment, no group barrier — explicitly not Kafka's model) ·
zstd payload compression · `retention=workqueue|interest` · `messq audit export --since` ·
OTel span export from the daemon.

### M9 — Deliberately last: replicas · only if asked
Async log shipping to a read-only follower for hot standby and offloaded replay. **Not consensus,
not quorum, not automatic failover.** Built only if real users ask for it, and shipped with an
honest statement of what it does not guarantee.

---

## 10. Risks & open questions

**R1 — SQLite write throughput.** Group commit with `synchronous=FULL` should land in the
5–20k small-messages/second range on NVMe, but this is the assumption the whole storage choice rests
on. *Mitigation:* benchmark in M1 and publish the number in the README before building on top of it.
*Escape hatch:* the schema already isolates `payload` in `messages`; moving payloads to append-only
segment files while SQLite keeps only the index is a contained change that does not touch the wire.

**R2 — Connect bidi streaming requires HTTP/2.** HTTP/1.1-only environments cannot use `Consume`.
*Mitigation:* `Fetch` + `Settle` is a fully supported, documented, CI-tested path — not a fallback
we are embarrassed about. Long-poll `wait` closes most of the latency gap.

**R3 — Delivery-count undercount after unclean shutdown** (§3.4). Acceptable for us, but a user who
treats `max_deliver` as a hard safety bound (say, "never charge a card more than 5 times") may
disagree. *Designed but deferred:* `strict_delivery_counts=true`, making `pending` writes
synchronous at roughly 2× write amplification. **Open question:** should this be the default for
consumers whose `max_deliver ≤ 3`, on the theory that a low limit signals a safety-critical
consumer? Leaning yes; decide with M5 benchmark data.

**R4 — Ack HMAC key rotation** invalidates in-flight tokens. *Mitigation:* verify against
`current` and `previous`; rotate only on explicit admin action; log a `WARN` with the count of
tokens that will be invalidated when `previous` is retired.

**R5 — Clock skew** between server and client on `lease_expires_at`. *Mitigation:* every `Delivery`
carries `server_time`; the SDK computes an offset per connection and exposes relative deadlines.
Servers with a jumping clock (NTP step) are a genuine hazard for lease expiry — *open question:*
use `CLOCK_MONOTONIC` internally for lease arithmetic and wall-clock only for display. Leaning yes;
it costs one type in the store layer and should be decided in M2 before leases exist.

**R6 — Ordered consumers can starve credit.** One stuck subject holds a slot in `max_ack_pending`
indefinitely. *Open question:* should `ordered=true` consumers get a per-subject in-flight cap
instead of a single global one? Leaning yes, decided with data in M3.

**R7 — `WatchEvents` back-pressure.** A slow watcher must never stall the data path. Ring buffer +
drop + `LOST_EVENTS` is decided. *Open question:* should exceeding a drop threshold terminate the
watcher stream (fail loud) rather than continue with gaps? Leaning fail-loud with an explicit
`LOST_EVENTS` count first, then termination if it recurs.

**R8 — Terminology drift.** "Consumer group" in M8 must not import Kafka's rebalance semantics; if
it does, we inherit exactly the operational pain we are positioning against. *Mitigation:* the ADR
for M8 must state, in its first paragraph, that groups are leases and there is no assignment
protocol and no group barrier.

**R9 — Scope creep through the RPC surface.** 22 methods is a budget, and budgets are broken one
reasonable request at a time. *Mitigation:* C1 is enforced by review: a 23rd method needs an ADR
that names the method it displaces or argues the budget was wrong.

**R10 — Single-binary auth expectations.** Users will ask for OIDC/JWT/LDAP. Saying no is correct
(a reverse proxy does this better) but must be said clearly in `OPERATIONS.md` with a worked nginx
example, or the `exec` authorizer becomes an unmaintainable plugin API.

---

## 11. Library choices

Every entry below was checked against current documentation via context7 before being committed to.

### 11.1 `connectrpc.com/connect` — RPC framework · **chosen**

The docs confirm exactly the properties the design needs:

- `NewBidiStreamHandler` gives the handler a `*BidiStreamForHandler[Req, Res]` with the
  `for stream.Receive() { ... stream.Send(...) } ; return stream.Err()` shape — a natural fit for
  the frame loop in §5.5, with no callback inversion.
- `NewServerStreamHandler` covers `WatchEvents`.
- `WithInterceptors` composes unary **and** streaming interceptors "like an onion, with the first
  interceptor provided being the outermost layer" — so one auth interceptor and one logging
  interceptor cover both the unary control plane and the `Consume` stream, which is precisely §5.8.
- The `Code` set is the gRPC set (`CodeResourceExhausted`, `CodeFailedPrecondition`, `CodeAborted`,
  `CodeUnauthenticated`…) and the docs' own guidance — "`CodeResourceExhausted` signals quota or
  capacity limits", "`CodeFailedPrecondition` indicates the system state is incompatible with the
  requested operation", "`CodeAborted` for transaction conflicts or concurrent modifications" —
  maps one-to-one onto the table in §5.7. The stale-ack-token case is genuinely a concurrent
  modification, so `ABORTED` is not a stretch.
- `Spec().IdempotencyLevel == IdempotencyLevelIdempotent` is available to clients for retry
  decisions, and `NO_SIDE_EFFECTS` methods are servable over HTTP GET — which is what makes the
  read side of §5.3 curl-able with query strings.
- Message-size limits produce `CodeResourceExhausted` from `sendMaxBytes` checks against *both*
  uncompressed and compressed sizes — so `max_msg_size` enforcement has a defined, documented
  failure mode at the transport layer as well as ours.
- The documented retry set (`Unavailable`, `ResourceExhausted`, `DeadlineExceeded`) is adopted
  verbatim as the SDK's retry predicate.

**Rejected: `google.golang.org/grpc`.** It cannot serve JSON over HTTP/1.1 from the same handler,
which kills the `curl` story and therefore the positioning; it needs a separate listener from the
`net/http` mux serving `/metrics` and `/healthz`; and its release history has repeatedly broken
backward compatibility, which is a poor foundation for a project whose central promise is
compatibility. **Rejected: hand-rolled TCP framing** (NSQ/beanstalkd style) — smaller on the wire,
but it means writing a client per language and forfeiting all HTTP tooling.

### 11.2 `google.golang.org/protobuf` — codec · **chosen**

`protojson.MarshalOptions` supplies exactly the knobs the CLI and docs need: `UseProtoNames` (so
JSON keys match the `.proto` and `messq trace --json` matches `PROTOCOL.md`), `EmitUnpopulated` /
`EmitDefaultValues` (so a JSON response has a stable shape for `jq` scripts), `Multiline`+`Indent`
for human output. `EmitUnknown: true` is used by `messq doctor --dump` to *show* fields sent by a
newer client that this server does not understand — a forward-compatibility debugging tool that
falls straight out of the runtime preserving unknown fields, and the reason §5.9's promise is
verifiable rather than aspirational.

### 11.3 `buf` (CI tool, not a runtime dependency) · **chosen**

The compatibility gate of §5.9 is `buf.yaml`:

```yaml
version: v2
modules: [{ path: proto }]
lint:     { use: [STANDARD] }
breaking: { use: [WIRE_JSON] }
```

with `buf breaking --against "https://github.com/messq/messq.git#branch=main,subdir=proto"` in CI.
`WIRE_JSON` rather than the BSR-default `FILE`: we want the freedom to move messages between files
while keeping both binary and JSON encodings compatible, and `FILE` forbids exactly that.
`buf.gen.yaml` runs `protoc-gen-go` and `protoc-gen-connect-go`, plus unsupported Python/TS/Java
templates for `contrib/`.

### 11.4 `modernc.org/sqlite` — storage engine · **chosen**

The DSN surface documented for this driver is what §3.2 relies on directly:
`_journal=WAL`, `_synchronous=NORMAL|FULL`, `_timeout=5000` (busy timeout),
`_txlock=immediate`, and generic `_pragma=foreign_keys(1)` / `_pragma=cache_size(...)` —
all settable in the connection string, so durability configuration is one string built from config
rather than a startup script of `PRAGMA` statements. The driver's documented `SQLITE_BUSY` handling
(busy timeout, or exponential-backoff retry against `sqlite.Error.Code()`) is the model for the
store layer's retry wrapper, though our single-writer topology should make contention rare by
construction.

**cgo-free is the deciding factor**: it preserves `CGO_ENABLED=0` static builds and trivial
cross-compilation, which is the "single binary" half of the product promise. `mattn/go-sqlite3`
would be faster and is more battle-tested, but requires cgo. **Rejected.**

### 11.5 `go.etcd.io/bbolt` — **evaluated, rejected, and borrowed from**

The bbolt docs describe `db.Batch` as executing "a read-write transaction within a batch" where
"multiple batches are opportunistically combined" and the function "must be idempotent; may be
called multiple times" — that *is* the group-commit design of §2.2, and the documented bulk-load
pattern (`NoSync = true` … `db.Sync()`) is the same amortised-fsync idea our
`max_batch`/`max_batch_delay` implements over SQLite. `NextSequence()` is the direct analogue of our
per-stream `seq`.

Rejected because bbolt is a pure key/value B+tree whose only scan primitive is a cursor
(`c.First(); k != nil; k, v = c.Next()`). Every operator question — "which messages are pending
longest?", "what is in the DLQ for this consumer?", "what was the last message on this subject?" —
becomes a hand-maintained index bucket, and none of it is answerable by a human with a shell while
the daemon is down. For a product positioned on readable operations, that is disqualifying.

### 11.6 `github.com/spf13/cobra` — CLI · **chosen**

Standard, and the pieces §6 needs are all documented: `PersistentFlags()` on the root for
`--addr`/`--output`/`--log-level`, `MarkFlagRequired`, `GenBashCompletion`/`GenZshCompletion`/
`GenFishCompletion`/`GenPowerShellCompletionWithDesc` behind a `messq completion` command, `ValidArgs`
+ `cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs)` for strict argument validation, and the
built-in `--version` handling driven by `Command.Version`. Dynamic completion via
`ValidArgsFunction` is what gives §6.3 real stream/consumer name completion.

**Not using viper.** Config is a small TOML file (`BurntSushi/toml`) with a documented
flag > env > file > default precedence. Viper's implicit key resolution makes "why is this setting
not taking effect?" a support burden, and for a daemon with ~30 settings the cost is unjustified.

### 11.7 `log/slog` (stdlib, Go ≥ 1.21) — logging · **chosen**

`NewJSONHandler(w, *HandlerOptions)` and `NewTextHandler(w, *HandlerOptions)` cover the two output
formats of §7.1 from one code path. `HandlerOptions.ReplaceAttr` normalises timestamps and enforces
the fixed key ordering. `LogAttrs(ctx, level, msg, attrs...)` is the allocation-free path used on
the delivery hot path. `slog.Group` nests the `messq.*` event payload without polluting the top
level.

Two stdlib features are load-bearing rather than convenient:

- **`slog.LogValuer`** — the docs' own example returns `"REDACTED_TOKEN"` from a secret type's
  `LogValue()`. Our `auth.Token` implements it, which makes it *structurally impossible* to leak a
  bearer token into a log line, even through `%v` or a careless `slog.Any`.
- **`slog.NewMultiHandler`** — tees the same record to stdout and to a rotating JSONL file without
  formatting twice.

No zap, no logrus, no zerolog. A stdlib logger means anyone can write a handler for messq's events
without importing our logging opinion.

### 11.8 `github.com/prometheus/client_golang` — metrics · **chosen**

Used exactly as the docs prescribe for a library-like component: a **non-default** registry via
`prometheus.NewRegistry()`, metric construction through `promauto.With(reg)` bound to a `Metrics`
struct built in one constructor, and exposition via
`promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg, MaxRequestsInFlight: 8})` mounted on
the same `net/http` mux as the RPC handler. Histogram buckets are chosen explicitly
(`prometheus.ExponentialBuckets` for latencies spanning milliseconds to an hour, since `ack_latency`
legitimately reaches `ack_wait`), never `DefBuckets`.

### 11.9 `github.com/oklog/ulid/v2` — message IDs · **chosen**

Per the ULID spec: 128 bits, a 48-bit millisecond timestamp, Crockford base32, 26 characters,
case-insensitive, lexicographically sortable, with monotonic entropy within a millisecond. Chosen
over UUIDv4 because `msg_id`s that sort by time make `grep`ped logs and `messq peek` output
readable; over UUIDv7 because the base32 text form is shorter and case-insensitive, which matters
when a human retypes an ID from a terminal into `messq trace`.

### 11.10 NATS JetStream — semantic reference, **not** a dependency

We ship no NATS code, but the consumer configuration surface is a deliberate, near-verbatim
adoption of the JetStream vocabulary documented at `nats-concepts/jetstream/consumers`: `AckWait`
("the duration the server will wait for an acknowledgment for any individual message once it has
been delivered"), `MaxDeliver`, `Backoff` (a sequence of redelivery delays that overrides `AckWait`,
with its length bounded by `MaxDeliver`), `MaxAckPending` as the backpressure ceiling, and
`DeliverPolicy` (`all` / `new` / `by_start_sequence` / `by_start_time` / `last_per_subject`). The
acknowledgement model — `AckAck`, `AckNak`, `AckProgress`, `AckTerm` — becomes our four
dispositions.

Two deliberate divergences:

- **`AckNext` is dropped.** JetStream's pull mode folds "ack and give me the next one" into one
  verb; our additive credit frame does that job more generally, and keeping settle and flow control
  as separate concerns makes both easier to reason about.
- **`AckAll` is dropped** (C9). Cumulative acknowledgement is efficient and makes "which message
  actually completed?" unanswerable. Given that reconstructing one message's history is the product,
  removing it is not a simplification we regret.

### 11.11 Remaining dependencies, and the ones we refuse

Also used: `golang.org/x/crypto/argon2` (token hashing), `github.com/BurntSushi/toml` (config),
`go.uber.org/goleak` (tests only). Everything else is the standard library — `net/http`,
`crypto/hmac`, `container/heap`, `database/sql`, `context`, `log/slog`.

Explicitly refused: any web framework, any ORM, any DI container, any OpenTelemetry import in the
core module, any clustering or consensus library. The dependency list is part of the "understandable
in an evening" promise, and it is reviewed as such.

---

## Appendix A — Sources consulted

- NATS JetStream consumers, ack model, `MaxAckPending` flow control, idempotent publish via
  `Nats-Msg-Id` and the stream dedup window (docs.nats.io, nats.io/blog, synadia.com)
- NSQ `RDY` state and heartbeat design; beanstalkd `reserve`/TTR/`DEADLINE_SOON` (nsq.io,
  beanstalkd protocol.txt)
- AMQP 1.0 link credit and its complexity trade-off (rabbitmq.com)
- Redis Streams `XPENDING`/`XAUTOCLAIM` consumer patterns and the "every client reimplements the
  orchestration, often subtly wrong" lesson (redis.io, redis.antirez.com)
- Kafka consumer-group rebalance operational history and KIP-848 (confluent.io and others)
- Connect protocol vs gRPC, HTTP/1.1 and curl compatibility, SemVer stability (buf.build,
  connectrpc.com)
- gRPC flow control and keepalive for detecting dead clients (grpc.io, grpc-go docs)
- Protobuf schema evolution: reserve tags, never reuse, never change meaning (protobuf.dev)
- context7 documentation for: connect-go, protobuf-go, buf, modernc.org/sqlite, bbolt, cobra,
  `log/slog`, prometheus/client_golang, nats.docs, the ULID spec
