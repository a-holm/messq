# messq — Project Plan 04: The CLI & DX Craftsman

> Persona lens: **the CLI is the product**. The daemon is an implementation detail that keeps
> promises; the command line is where those promises become visible, teachable and operable.
> Every design decision below is judged first by "what does this do to the person at the prompt
> at 03:00?" and only second by throughput.

Author: planner 04 · Status: proposal · Target: Linux, single static Go binary

---

## 1. Vision & positioning

### 1.1 The one-sentence pitch

**messq is a durable queue you can *interrogate*.** Publish, consume, ack — and then, when
something goes wrong, type `messq trace 01J8Z9K3QF7VB2M0RS4KQ7` and read the complete life story
of that one message: who published it, who got it, how many times, why it failed, where it is now,
and the exact command that fixes it.

### 1.2 What we are competing against, and on what axis

| System | What it gives | What it costs the operator |
|---|---|---|
| Kafka | scale, ordering, replication | ZooKeeper/KRaft, JVM, partitions, rebalancing, consumer-group folklore |
| RabbitMQ | routing power | exchanges/bindings/vhosts, Erlang ops, a UI you must learn |
| NATS JetStream | excellent ack primitives | a cluster concept, an ecosystem, a mental model bigger than the job |
| Redis Streams | small, fast | PEL semantics that leak into every consumer; you hand-roll the janitor |
| beanstalkd | genuinely small | almost no introspection, no replay, no audit trail |
| SQS/PubSub | zero ops | not local, not inspectable, not free, not yours |

We compete on **one axis only: how fast a competent engineer can understand what happened.**
Not throughput. Not scale. Not exactly-once. Understanding.

Concretely, our benchmark is not messages/second. It is:

- **60 seconds** from `messq serve` to a durable publish/consume/ack loop, no config file.
- **One command** from "a customer says their order never processed" to a complete audit trail.
- **One evening** to read the whole design doc and the state machine and be confident you know
  what the daemon will do under any failure.

### 1.3 The three product commitments

1. **Symmetry.** Anything the daemon can do to a message, a human can do to that message by hand:
   `messq ack`, `messq nak --delay 5m`, `messq term`, `messq seek`, `messq dlq redrive`. There is
   no privileged internal transition.
2. **Duality.** Every command has exactly two faces: a human face (aligned, coloured, relative
   times, teaching errors) and a machine face (NDJSON/JSON, stable field names, versioned schema,
   documented exit codes). Never a third, never a surprise.
3. **Honesty.** Defaults are the safe ones, not the fast ones. `synchronous=FULL` by default,
   `max_deliver=5` by default, DLQ on by default. Where we are slower than the alternative, the
   docs say so with numbers.

### 1.4 Explicit non-goals for 1.0

- No consensus, no quorum, no leader election, no multi-node writes.
- No exactly-once *processing* (we offer at-least-once + publish dedupe; the application owns
  idempotency, and `messq help concepts` explains exactly why).
- No plugin system, no scripting language, no embedded jq, no web UI.
- No AMQP/MQTT/Kafka wire compatibility.

---

## 2. Architecture overview

### 2.1 Processes

One binary, `messq`. Two roles selected by subcommand:

```
messq serve      -> the daemon (long-lived)
messq <anything> -> a client of the daemon over the same public HTTP API
```

Shipping one binary is a DX decision: there is no version-skew question between "the server
package" and "the CLI package", `messq version` prints both client and server versions and warns
on mismatch, and a user who has the daemon has the tools.

### 2.2 Goroutine topology inside `messq serve`

```
                      ┌──────────────────────────────────────────────┐
   unix socket ──────►│ http.Server (net/http, HTTP/1.1, keep-alive) │
   (optional tcp)     │  one goroutine per connection                │
                      └───────┬───────────────────────┬──────────────┘
                              │ commands              │ reads
                              ▼                       ▼
                    ┌───────────────────┐    ┌────────────────────┐
                    │  writer goroutine │    │ reader pool (N=8)  │
                    │  SINGLE, owns the │    │ *sql.DB read-only  │
                    │  write *sql.DB    │    │ WAL snapshot reads │
                    │  (MaxOpenConns=1) │    └────────────────────┘
                    │  group commit     │
                    └─────┬──────┬──────┘
                          │      │ emits events
              applies     │      ▼
              state       │  ┌───────────────────┐
              machine     │  │ event fan-out hub │──► slog handler (stderr/file)
                          │  └───────────────────┘──► /v1/events NDJSON subscribers
                          │                       ──► prometheus counters
                          ▼
                    ┌───────────────────────────────────────────┐
                    │ SQLite file (WAL)  messq.db               │
                    └───────────────────────────────────────────┘
                          ▲              ▲              ▲
             ┌────────────┴───┐  ┌───────┴──────┐  ┌────┴──────────────┐
             │ timeout scanner│  │ retry timer  │  │ janitor           │
             │ (ack deadlines)│  │ (backoff due)│  │ retention, dedupe │
             │ ticks 200ms    │  │ ticks 200ms  │  │ ticks 30s         │
             └────────────────┘  └──────────────┘  └───────────────────┘
                          │              │              │
                          └──────────────┴──────────────┘
                                  all submit commands to the writer
```

Rules that keep this comprehensible:

- **Exactly one goroutine writes.** Every mutation is a `Command` value sent on a channel to the
  writer. The writer batches whatever is queued (up to 256 commands / 5 ms), applies them inside a
  single SQLite transaction, fsyncs once, then replies to each caller. This is where ordering,
  durability and event emission are decided — one place, one file, ~400 lines.
- **The state machine is pure.** `broker.Apply(state Snapshot, cmd Command, now time.Time) (mutations []Mutation, events []Event, err error)`. It touches no I/O, so it is exhaustively
  table-testable and property-testable. The store turns `Mutation`s into SQL.
- **Readers never block writers.** WAL mode; the read pool serves `peek`, `pending`, `trace`,
  `lag`, `ls` from a snapshot.
- **Waiters are in memory.** Long-poll fetches park on a per-consumer condition variable; a
  publish or a retry-due signals it. No polling loop on the hot path.

### 2.3 Data flow of one message

```
messq pub orders.created '{...}'
      │  POST /v1/streams/orders/messages
      ▼
  writer: assign seq + ULID, insert messages row, emit publish event, fsync, reply 201
      │
      ▼  a consumer is parked in POST .../consumers/billing/fetch?max=10&wait=30s
  writer: pop from cursor (or from due-retry queue), insert inflight row
          (deliveries=1, deadline=now+ack_wait), emit deliver event, fsync, reply 200 NDJSON
      │
      ├── ack   -> DELETE inflight, emit ack
      ├── nak   -> DELETE inflight, INSERT redelivery(ready_at=now+backoff), emit nak
      ├── term  -> DELETE inflight, INSERT dlq(reason=terminated), emit term+dead
      ├── extend-> UPDATE inflight.deadline, emit extend
      └── (silence) -> scanner sees deadline < now, emits timeout, then nak-with-backoff
                       or, if deliveries == max_deliver, dlq(reason=max_deliver) + dead
```

---

## 3. Storage & durability design

### 3.1 Engine decision: SQLite via `modernc.org/sqlite`

**Decision: SQLite, single file, WAL mode, accessed through the pure-Go `modernc.org/sqlite`
driver.** Not bbolt, not a hand-rolled segment log, not CGO `mattn/go-sqlite3`.

Justification:

- **Single static binary is a hard requirement** (`CGO_ENABLED=0`, trivially cross-compiled,
  no glibc coupling). `modernc.org/sqlite` is a CGo-free port, so `go build` produces exactly the
  artifact we promise.
- **Inspectability is the product.** When an operator distrusts us, they can open `messq.db` with
  `sqlite3` and run a `SELECT`. A bespoke segment log or a bbolt file gives them nothing. This
  single property is worth more to this project than the throughput a custom log would buy.
- **Crash recovery is someone else's solved problem.** WAL replay, `PRAGMA integrity_check`,
  `VACUUM INTO` for consistent backups. We write zero recovery code for the storage layer.
- **A relational shape fits the queries we actually run.** "pending per consumer ordered by
  deadline", "events for msg id", "oldest unacked age" are indexes and joins, not key scans.

The cost is a single-writer ceiling. We accept it, engineer around it with group commit, and
publish honest numbers (§10).

### 3.2 Connection setup (exact)

Two `database/sql` handles over the same file:

```go
const pragmas = "?_pragma=journal_mode(WAL)" +
    "&_pragma=synchronous(FULL)" +      // overridden only by --durability=relaxed
    "&_pragma=busy_timeout(5000)" +
    "&_pragma=foreign_keys(1)" +
    "&_pragma=temp_store(MEMORY)" +
    "&_pragma=cache_size(-32000)" +     // 32 MiB page cache
    "&_pragma=wal_autocheckpoint(2000)" +
    "&_txlock=immediate"                // write txns take the write lock up front

write, _ := sql.Open("sqlite", "file:"+path+pragmas)
write.SetMaxOpenConns(1)                // the writer goroutine is the only user

read,  _ := sql.Open("sqlite", "file:"+path+pragmas+"&mode=ro")
read.SetMaxOpenConns(runtime.NumCPU())
```

Additionally we register a **connection hook** so that *every* pooled connection re-asserts the
durability pragmas and we verify them by reading them back:

```go
sqlite.RegisterConnectionHook(func(c sqlite.ExecQuerierContext, dsn string) error { ... })
```

This exists because the single most-reported SQLite durability trap is a driver or a pooled
connection silently running at `synchronous=NORMAL` (which, in WAL mode, does **not** fsync on
commit and loses recent commits on power loss or kernel panic). `messq doctor --durability` reads
the live pragma values back out of a real connection and prints them; if `synchronous` is not
`FULL` and `--durability=relaxed` was not requested, the daemon refuses to start.

### 3.3 Schema

Schema version is stored in `meta`; migrations are ordered, embedded (`//go:embed migrations/*.sql`),
forward-only, applied in one transaction at boot, and logged.

```sql
CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT NOT NULL);
-- schema_version, node_id (ULID), created_at, last_clean_shutdown

CREATE TABLE streams (
  id           INTEGER PRIMARY KEY,
  name         TEXT NOT NULL UNIQUE,
  subjects     TEXT NOT NULL,            -- JSON array of patterns, e.g. ["orders.>"]
  retention    TEXT NOT NULL,            -- 'limits' | 'workqueue'
  max_msgs     INTEGER, max_bytes INTEGER, max_age_ms INTEGER,
  max_msg_size INTEGER NOT NULL DEFAULT 1048576,
  discard      TEXT NOT NULL DEFAULT 'old',  -- 'old' | 'new'
  dedupe_ms    INTEGER NOT NULL DEFAULT 120000,
  created_at   INTEGER NOT NULL,
  revision     INTEGER NOT NULL DEFAULT 1   -- optimistic concurrency for edits
);

CREATE TABLE messages (
  stream_id    INTEGER NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
  seq          INTEGER NOT NULL,          -- per-stream, monotonic, gapless on publish
  id           TEXT NOT NULL,             -- ULID, 26 chars, globally unique
  subject      TEXT NOT NULL,
  ts_ms        INTEGER NOT NULL,
  size         INTEGER NOT NULL,
  headers      TEXT,                      -- JSON object, small
  trace_id     TEXT,                      -- 32 hex chars, or NULL -> defaults to id
  body         BLOB NOT NULL,
  PRIMARY KEY (stream_id, seq)
) STRICT;
CREATE UNIQUE INDEX messages_id      ON messages(id);
CREATE INDEX        messages_subject ON messages(stream_id, subject, seq);
CREATE INDEX        messages_ts      ON messages(stream_id, ts_ms);

CREATE TABLE consumers (
  id               INTEGER PRIMARY KEY,
  stream_id        INTEGER NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
  name             TEXT NOT NULL,
  filter           TEXT NOT NULL DEFAULT '>',
  ack_wait_ms      INTEGER NOT NULL DEFAULT 30000,
  max_deliver      INTEGER NOT NULL DEFAULT 5,
  max_ack_pending  INTEGER NOT NULL DEFAULT 256,
  max_ack_bytes    INTEGER NOT NULL DEFAULT 67108864,
  backoff          TEXT NOT NULL DEFAULT '["1s","5s","30s","2m","10m"]',
  ordered          INTEGER NOT NULL DEFAULT 0,   -- per-subject ordering
  dlq              INTEGER NOT NULL DEFAULT 1,
  paused           INTEGER NOT NULL DEFAULT 0,
  start_policy     TEXT NOT NULL DEFAULT 'new',  -- new|all|seq:N|time:RFC3339
  created_at       INTEGER NOT NULL,
  revision         INTEGER NOT NULL DEFAULT 1,
  UNIQUE (stream_id, name)
) STRICT;

-- the low watermark: everything below next_seq has been delivered at least once
CREATE TABLE cursor (consumer_id INTEGER PRIMARY KEY REFERENCES consumers(id) ON DELETE CASCADE,
                     next_seq INTEGER NOT NULL);

CREATE TABLE inflight (
  consumer_id INTEGER NOT NULL REFERENCES consumers(id) ON DELETE CASCADE,
  seq         INTEGER NOT NULL,
  msg_id      TEXT NOT NULL,
  subject     TEXT NOT NULL,
  deliveries  INTEGER NOT NULL,
  delivered_ms INTEGER NOT NULL,
  deadline_ms INTEGER NOT NULL,
  owner       TEXT,                        -- client hint: pid@host or lease id
  PRIMARY KEY (consumer_id, seq)
) STRICT;
CREATE INDEX inflight_deadline ON inflight(deadline_ms);
CREATE INDEX inflight_msg      ON inflight(msg_id);

CREATE TABLE redelivery (
  consumer_id INTEGER NOT NULL REFERENCES consumers(id) ON DELETE CASCADE,
  seq         INTEGER NOT NULL,
  msg_id      TEXT NOT NULL,
  subject     TEXT NOT NULL,
  deliveries  INTEGER NOT NULL,
  ready_at_ms INTEGER NOT NULL,
  reason      TEXT,
  PRIMARY KEY (consumer_id, seq)
) STRICT;
CREATE INDEX redelivery_ready ON redelivery(consumer_id, ready_at_ms);

CREATE TABLE dlq (
  id          INTEGER PRIMARY KEY,
  consumer_id INTEGER NOT NULL, stream_id INTEGER NOT NULL,
  seq         INTEGER NOT NULL, msg_id TEXT NOT NULL,
  reason      TEXT NOT NULL,             -- 'max_deliver' | 'terminated' | 'too_large' | 'filtered'
  deliveries  INTEGER NOT NULL,
  last_error  TEXT,
  dead_at_ms  INTEGER NOT NULL,
  redriven_at_ms INTEGER
) STRICT;
CREATE INDEX dlq_msg ON dlq(msg_id);

CREATE TABLE dedupe (
  stream_id  INTEGER NOT NULL, key TEXT NOT NULL,
  seq        INTEGER NOT NULL, expires_ms INTEGER NOT NULL,
  PRIMARY KEY (stream_id, key)
) STRICT;

-- THE AUDIT TRAIL. This table is the product.
CREATE TABLE events (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_ms       INTEGER NOT NULL,
  verb        TEXT NOT NULL,
  stream      TEXT, consumer TEXT, subject TEXT,
  msg_id      TEXT, seq INTEGER, trace_id TEXT,
  attempt     INTEGER, of INTEGER,
  latency_ms  INTEGER,
  reason      TEXT, detail TEXT           -- detail is small JSON
) STRICT;
CREATE INDEX events_msg      ON events(msg_id, id);
CREATE INDEX events_ts       ON events(ts_ms);
CREATE INDEX events_consumer ON events(consumer, ts_ms);
CREATE INDEX events_verb     ON events(verb, ts_ms);
```

**Invariant that makes the model explainable:** a message `seq` is *done for consumer C* exactly
when `seq < cursor.next_seq` **and** it has no row in `inflight` and no row in `redelivery` for C.
That is the whole bookkeeping. There is no per-message ack bitmap, because a delivery always
inserts the inflight row and advances the cursor **in the same transaction**.

### 3.4 fsync policy

`--durability` has three named settings; the name is printed at startup and by `messq doctor`.

| Setting | pragma | Commit behaviour | Loses on power cut | Default |
|---|---|---|---|---|
| `strict` | `synchronous=FULL` | one transaction per command, fsync each | nothing | no |
| `group` | `synchronous=FULL` | batch ≤256 commands / ≤5 ms into one txn, one fsync | nothing | **yes** |
| `relaxed` | `synchronous=NORMAL` | batched, no fsync per commit | last ~seconds of commits | no |

`group` is the default because it is fully durable *and* the fsync cost amortises across
concurrent publishers: 1 publisher sees one fsync of latency (≈0.5–3 ms on NVMe, ≈8–15 ms on
spinning rust or a cloud EBS volume), 200 concurrent publishers still see one fsync total per
batch. Publishers get their 201 only after the fsync — we never ack a publish we might lose.

`relaxed` prints a warning banner at startup and is flagged by `doctor`. It is safe against
process crash (SIGKILL) and unsafe against power loss / kernel panic; the help text says exactly
that in those words.

### 3.5 Crash recovery

On `messq serve` start:

1. Open the DB. SQLite replays/truncates the WAL. If `meta.last_clean_shutdown` is absent, log
   `recovery start reason=unclean_shutdown` at WARN and run `PRAGMA quick_check` (full
   `integrity_check` behind `--check-on-start`, and always after an unclean shutdown if the file
   is < 1 GiB).
2. Apply migrations if `schema_version` is behind. Refuse to start if it is *ahead* (a newer
   binary wrote this file) with a teaching error.
3. **Orphan sweep.** Every row in `inflight` belongs to a client connection that no longer exists.
   For each: increment `deliveries`, delete from `inflight`, insert into `redelivery` with
   `ready_at_ms = now` and `reason = 'server_restart'`, emit a `redeliver` event with that reason.
   The attempt counter *is* incremented (a restart still counts against `max_deliver`), because
   silently resetting it would let a poison message loop forever across restarts — but the reason
   is recorded so `messq trace` can show "this attempt was consumed by a restart, not by your
   handler".
4. Expire the `dedupe` table, run one retention pass, then start listeners.
5. Write `last_clean_shutdown=0`; on graceful shutdown (SIGTERM: stop accepting, drain in-flight
   fetches for up to `--drain 10s`, checkpoint WAL with `PRAGMA wal_checkpoint(TRUNCATE)`) set it
   to the timestamp.

Startup emits a single human-readable recovery summary — never silence:

```
recovery  db=/var/lib/messq/messq.db  schema=7  durability=group (synchronous=FULL)
recovery  unclean shutdown detected; quick_check ok in 41ms
recovery  4 streams, 9 consumers, 214,882 messages, 1.2 GiB
recovery  17 in-flight deliveries orphaned by restart -> requeued for immediate redelivery
recovery  ready in 128ms
```

### 3.6 Retention, backup, portability

- `retention=limits` (default): messages age out by `max_age`/`max_msgs`/`max_bytes`; consumers
  that fall behind get a `gap` event and their cursor is fast-forwarded (loudly logged, counted in
  `messq_dropped_total`, surfaced by `doctor`).
- `retention=workqueue`: a message is deleted once every consumer whose filter matches it is done
  with it. `messq stream info` shows which consumer is holding the floor.
- Backup: `messq backup /path/snap.db` runs `VACUUM INTO` — a consistent snapshot without stopping
  the daemon. `messq restore` is documented as "stop, copy, start".
- Portability: `messq stream export orders --since -24h > orders.ndjson` writes one JSON object
  per message with base64 body; `messq stream import` reads it. This is how you move between
  machines and how you keep an audit archive.

---

## 4. Delivery semantics & message lifecycle

### 4.1 Guarantees, stated as sentences an operator can repeat

1. A publish that returned 201 is on disk (in `group`/`strict` durability).
2. Every message matching a consumer's filter will be delivered to that consumer at least once,
   unless retention deletes it first (which is always logged and counted).
3. A message is delivered to a given consumer **at most `max_deliver` times**. The `max_deliver`-th
   failure dead-letters it. `max_deliver=5` means five handler invocations, not six.
4. Two different consumers on the same stream each get their own copy and their own ack state.
5. Within one consumer, a message is in flight to at most one fetcher at a time.
6. With `ordered=true`, at most one message per subject is in flight for that consumer, so
   per-subject order is preserved across retries (at the cost of head-of-line blocking, which we
   name and display).
7. Publish-side dedupe within `dedupe_ms` when the publisher supplies `Messq-Msg-Id`.

### 4.2 The state machine

Per (message, consumer) pair. Six states, nine transitions. This diagram is printed verbatim by
`messq help lifecycle`.

```
                    ┌───────────────┐
                    │  UNDELIVERED  │  seq >= cursor.next_seq
                    └───────┬───────┘
                            │ (1) deliver: cursor++, inflight+, deliveries=1
                            ▼
        (5) extend ──►┌───────────────┐◄── (4) deliver-retry: deliveries++
        deadline+=w   │   IN_FLIGHT   │        (from SCHEDULED when due)
                      └──┬──┬──┬──┬───┘
             (2) ack     │  │  │  │     (3) nak [delay]
            ┌────────────┘  │  │  └──────────────┐
            ▼               │  │                 ▼
      ┌───────────┐         │  │          ┌─────────────┐
      │   ACKED   │(final)  │  │          │  SCHEDULED  │ ready_at = now+backoff(n)
      └───────────┘         │  │          └──────┬──────┘
                            │  │                 │ due
        (6) timeout:        │  │                 └────► back to IN_FLIGHT  (4)
        deadline passed ────┘  │
        == implicit nak        │ (7) term  (8) deliveries == max_deliver
                               ▼
                        ┌─────────────┐
                        │    DEAD     │ (final)  -> dlq table, reason recorded
                        └──────┬──────┘
                               │ (9) redrive: operator command
                               └────► SCHEDULED with deliveries reset to 0
```

Transition details:

| # | Trigger | Effect | Event emitted |
|---|---|---|---|
| 1 | fetch, window has room | cursor++, insert inflight(deliveries=1, deadline=now+ack_wait) | `deliver` |
| 2 | `ack` | delete inflight | `ack` (with `latency_ms` = time since delivery) |
| 3 | `nak(delay?)` | delete inflight, insert redelivery(ready_at = now + delay|backoff[n]) | `nak` |
| 4 | retry timer, ready_at ≤ now, window has room | deliveries++, move redelivery → inflight | `redeliver` |
| 5 | `extend` ("still working") | deadline = now + ack_wait | `extend` |
| 6 | scanner: deadline < now | as (3) with `reason=ack_timeout` | `timeout` then `nak` |
| 7 | `term` | delete inflight, insert dlq(reason=terminated) | `term`, `dead` |
| 8 | (3)/(6) when deliveries ≥ max_deliver | delete inflight, insert dlq(reason=max_deliver) | `dead` |
| 9 | `dlq redrive` | delete dlq row, insert redelivery(deliveries=0, ready_at=now) | `redrive` |

Idempotence rules (these matter enormously for CLI usability):

- `ack` on an id that is not in flight returns `200 {"result":"stale"}`, **not** an error. The most
  common real-world case is "the handler finished just after the ack timeout fired". The CLI prints
  `ack 01J8Z…KQ7: already redelivered (attempt 2 is in flight)` on stderr and exits 0. Silence here
  would be a bug; a hard error here would be user-hostile.
- `nak` on a stale id: same treatment.
- Double `ack` of the same delivery: second is stale.
- `redrive` of an already-redriven DLQ entry: conflict (exit 4) with the timestamp of the first.

### 4.3 Backoff

Default backoff schedule: `1s, 5s, 30s, 2m, 10m`, each jittered ±20%, last value repeats if
`max_deliver` exceeds the list length. Exponential-ish rather than linear specifically to avoid the
thundering-herd retry storms that are the classic failure mode when a shared dependency (a
database, a payment API) goes down and every naked message retries in lockstep.

Explicit `nak --delay 5m` overrides the schedule for that attempt. This mirrors the distinction
that has proven necessary in practice: *an ack timeout means "we don't know what happened"* and
uses the schedule; *a nak means "the handler knows this failed"* and the handler may know better
than the schedule how long to wait.

### 4.4 Flow control

- `max_ack_pending` (count) and `max_ack_bytes`: the fetch endpoint returns fewer messages, or zero
  with `Messq-Flow: window-full`, when the in-flight window for that consumer is full.
- The CLI never hangs mysteriously: `messq sub` prints, once, to stderr:
  `flow control: 256/256 in-flight for consumer "billing"; waiting for acks. See: messq pending billing`
- `messq lag` and `messq top` show `inflight/max` as a bar so a full window is visible at a glance.
- Publish-side: when a stream is at `max_bytes` with `discard=new`, publish returns 507 with a
  teaching error naming the exact `messq stream edit` command to raise the limit.

### 4.5 Ordering

`ordered=true` on a consumer. Implementation: the delivery selector skips any candidate whose
subject already has an in-flight or scheduled entry for this consumer. `messq consumer info` shows:

```
ordering        per-subject (strict)
blocked         2 subjects held by in-flight messages
                orders.eu.created   01J8Z…KQ7  attempt 3/5  next retry in 27s
                orders.us.created   01J8Z…M12  attempt 1/5  ack-wait 12s remaining
```

Head-of-line blocking is not hidden; it is displayed as a first-class fact, because the number one
support question for ordered consumers is "why did everything stop?".

---

## 5. API / protocol

### 5.1 Decision: HTTP/1.1 + JSON over a Unix socket (TCP optional). Not gRPC.

Rationale, from the persona: **a protocol you cannot `curl` is a protocol you cannot debug at
03:00.** gRPC would force a toolchain (protoc, codegen, a client library) onto every integrator,
would make the wire opaque to `tcpdump`/`strace`, and would make "write a worker in bash" —
one of our stated use cases — impossible. HTTP/1.1 with keep-alive over a Unix socket sustains
well above the throughput a single SQLite writer can absorb, so gRPC would buy performance we
cannot use, at a DX price we refuse to pay.

Transport defaults:

- `unix:///run/messq/messq.sock` when running as root/systemd, mode `0660`, group `messq`.
- `unix://$XDG_RUNTIME_DIR/messq/messq.sock` when running as a user.
- `--listen tcp://127.0.0.1:4222` optional; TCP **requires** either `--auth-token-file` or
  `--allow-insecure` (which logs a warning every 60s). TLS via `--tls-cert/--tls-key`.

Authorization in 1.0 is deliberately minimal: filesystem permissions on the socket, or a single
bearer token for TCP. Per-subject ACLs are a Phase 3 question (§10).

### 5.2 Endpoints

All responses are JSON; all errors share one envelope. `Accept: application/x-ndjson` selects
streaming where offered.

```
GET    /v1/info                                     server version, uptime, durability, db path
GET    /healthz  /readyz  /metrics

POST   /v1/streams                                  create   {name, subjects[], retention, limits...}
GET    /v1/streams                                  list
GET    /v1/streams/{s}                              info + computed stats
PATCH  /v1/streams/{s}                              edit (If-Match: revision)
DELETE /v1/streams/{s}                              delete (?confirm=name)
POST   /v1/streams/{s}/purge                        {subject?, keep?, before_seq?, before_time?} (?dry_run=1)
POST   /v1/streams/{s}/export  GET .../export       NDJSON out / in

POST   /v1/streams/{s}/messages                     publish
       headers: Content-Type, Messq-Subject, Messq-Msg-Id (dedupe), Messq-Trace-Id,
                Messq-Header-<Name>: <value>
       201 {"id":"01J8Z…","seq":1042,"duplicate":false}
GET    /v1/streams/{s}/messages?seq=|id=|subject=&limit=&order=      peek (no state change)
GET    /v1/streams/{s}/messages/{seq|id}            single message + per-consumer status

POST   /v1/streams/{s}/consumers                    create
GET    /v1/streams/{s}/consumers                    list + lag
GET    /v1/streams/{s}/consumers/{c}                info
PATCH  /v1/streams/{s}/consumers/{c}                edit (If-Match)
DELETE /v1/streams/{s}/consumers/{c}
POST   /v1/streams/{s}/consumers/{c}/pause|resume

POST   /v1/streams/{s}/consumers/{c}/fetch?max=10&wait=30s
       long-poll batch fetch; 200 NDJSON of messages, or 204 on timeout
       response headers per message line carry attempt, deadline, deliveries
GET    /v1/streams/{s}/consumers/{c}/subscribe      chunked NDJSON push, credit = max_ack_pending
POST   /v1/streams/{s}/consumers/{c}/ack
       {"action":"ack|nak|term|extend","ids":["01J8Z…"],"delay":"5m","reason":"upstream 503"}
       200 {"applied":3,"stale":1,"results":[...]}
GET    /v1/streams/{s}/consumers/{c}/pending?order=deadline&limit=100
POST   /v1/streams/{s}/consumers/{c}/cursor         seek {to:"start|now|seq:N|time:…|id:…"} (?dry_run=1)
POST   /v1/streams/{s}/consumers/{c}/replay         {from,to,subject,speed,into} (?dry_run=1)

GET    /v1/dlq?consumer=&reason=&since=&limit=
GET    /v1/dlq/{msg_id}
POST   /v1/dlq/redrive                              {ids[]|filter, into?, reset_deliveries:true} (?dry_run=1)
DELETE /v1/dlq                                      {ids[]|filter} (?confirm=…)

GET    /v1/messages/{msg_id}/trace                  the life story (events for this id)
GET    /v1/events?follow=1&verb=&consumer=&subject=&msg=&since=&level=
                                                    NDJSON live tail or historical query
POST   /v1/admin/log-level                          {"level":"debug"}  (slog.LevelVar)
```

### 5.3 The error envelope (one shape, everywhere)

```json
{
  "error": {
    "code": "consumer_not_found",
    "message": "consumer \"orders-worker\" not found in stream \"orders\"",
    "because": "streams hold messages; consumers hold the cursor and ack state",
    "next": [
      "messq consumer ls orders",
      "messq consumer create orders orders-worker --ack-wait 30s"
    ],
    "help_topic": "concepts",
    "details": {"stream": "orders", "similar": ["billing-worker"]}
  }
}
```

The CLI never invents text for a server-side condition: it renders exactly these fields. That means
a fix to an error message ships in the daemon and improves every client, including `curl` users.
The full code list is a Go `const` block and a table in `docs/errors.md`; codes are part of the
1.0 compatibility contract.

### 5.4 Go client

`pkg/messq` — a tiny (< 800 LOC) client that the CLI itself uses. No codegen, no reflection, one
`Client`, one `Consumer` with `Fetch/Ack/Nak/Extend`, plus a `Worker` helper:

```go
w := messq.NewWorker(client, "orders", "billing", messq.WorkerOpts{Concurrency: 8})
w.Run(ctx, func(ctx context.Context, m *messq.Msg) error {
    if err := handle(m); err != nil {
        if errors.Is(err, ErrPermanent) { return messq.Terminate(err) }
        return messq.RetryAfter(30*time.Second, err)
    }
    return nil // ack
})
```

`Extend` is called automatically on a heartbeat at `ack_wait/2` while the handler runs — the
library removes the single most common footgun (jobs longer than `ack_wait`) by default.

---

## 6. CLI & developer experience

This is the section the rest of the plan exists to serve.

### 6.1 The 60-second quickstart (verbatim, and tested in CI)

```
$ messq serve &
messq 1.0.0  listening on unix:///run/user/1000/messq/messq.sock
db /home/you/.local/state/messq/messq.db   durability=group (synchronous=FULL)
tip: run `messq quickstart` for a guided tour, or `messq help concepts`

$ messq pub orders.created '{"id":42}'
created stream "orders" (subjects: orders.>)         # auto-provisioned; disable with --strict
published 01J8Z9K3QF7VB2M0RS4KQ7  seq=1  orders.created  9 B

$ messq sub orders --as billing --exec ./handle.sh
created consumer "billing" (ack-wait 30s, max-deliver 5, dlq on)
01J8Z9K3QF…  orders.created  attempt 1/5  -> ./handle.sh  exit 0  ack  (11ms)
waiting for messages... (ctrl-c to stop)
```

Three commands. No config file, no schema, no broker concepts learned in advance. Auto-provision is
a deliberate bet: strictness is a production flag (`--strict`, or `strict = true` in the server
config), not a beginner tax. To limit the blast radius of typos, `messq doctor` flags any stream
with zero consumers and fewer than 10 messages as "possibly created by a typo" and offers the
`messq stream rm` command.

`messq quickstart` runs a guided six-step tour against a throwaway database in a temp dir, printing
each command before it runs it, pausing for Enter, and ending with `you now know: publish, ack,
nak, redelivery, dlq, replay`. It deletes its database on exit. It is an executable tutorial, and
it is a testscript test, so it can never rot.

### 6.2 Command tree

Principle: **noun-verb for management, bare verb for the hot path.** The six things you do while
firefighting are one word deep; everything else is two.

```
messq
├── serve                        run the daemon
├── quickstart                   guided 60-second tour (throwaway db)
│
│   # hot path — one word deep, on purpose
├── pub <subject> [data]         publish (stdin | --file | --count | --header k=v | --msg-id)
├── sub <stream> [--as name]     consume (--exec | --print | --batch | --ack-mode | --count)
├── peek <stream>                look without consuming (--seq | --id | --subject | --last | --follow)
├── pending <consumer>           what is in flight, ordered by deadline
├── trace <msg-id>               the life story of one message
├── lag                          backlog table across all consumers
├── top                          live TUI dashboard
│
│   # manual transitions — symmetry with the daemon
├── ack   <msg-id...>
├── nak   <msg-id...> [--delay 5m] [--reason "..."]
├── term  <msg-id...> [--reason "..."]
├── seek     <consumer> --to start|now|seq:N|time:...|id:...|-1h
├── replay   --from ... --to ... [--subject ...] [--into consumer] [--speed original|max]
├── purge    <stream> [--subject ...] [--before ...] [--keep N]
│
│   # management — noun then verb
├── stream    ls | info | create | edit | rm | purge | export | import
├── consumer  ls | info | create | edit | rm | pause | resume | reset
├── dlq       ls | show | redrive | drop | export
├── context   ls | use | add | rm | current
│
│   # operations & introspection
├── events [--follow] [--verb ...] [--consumer ...] [--msg ...]   the audit trail
├── doctor                       health + misconfiguration checks
├── backup <path> | restore <path>
├── bench                        built-in load generator with honest numbers
├── metrics                      prometheus metrics as a human table
├── subject test <pattern> <subject>   tiny matcher tester
├── completion bash|zsh|fish
├── docs man|markdown            generate man pages / docs
└── version                      client + server, warns on skew
```

Aliases: `ls`≡`list`, `rm`≡`delete`≡`del`, `info`≡`show`, `pub`≡`publish`, `sub`≡`subscribe`≡`consume`.
Cobra's suggestion engine handles `messq consumers` → "Did you mean `consumer`?".

### 6.3 Global flags (persistent, on every command)

| Flag | Default | Notes |
|---|---|---|
| `-s, --server` | context, else well-known socket | `unix://…`, `http://host:port` |
| `-c, --context` | `default` | named server profiles, kubectl-style |
| `-o, --output` | `auto` | `auto\|table\|wide\|json\|ndjson\|template=<go-tpl>` |
| `--time` | `auto` | `relative\|rfc3339\|epoch`; auto = relative on TTY, rfc3339 in JSON |
| `--full-ids` | false | print full 26-char ULIDs instead of abbreviated |
| `--no-color` | auto | also honours `NO_COLOR`, `CLICOLOR_FORCE`, `TERM=dumb` |
| `-q, --quiet` | false | data only, no narration |
| `-v, --verbose` | 0 | repeatable; `-vv` shows the HTTP requests being made |
| `-y, --yes` | false | required for destructive ops when stdin is not a TTY |
| `--dry-run` | false | supported by every destructive/bulk command |
| `--timeout` | `30s` | client-side |

Configuration precedence is exactly three layers, hand-rolled in ~100 lines, no Viper:
**flag > `MESSQ_*` env var > context file (`$XDG_CONFIG_HOME/messq/contexts.toml`)**. Viper is
rejected specifically because its precedence surprises (config-vs-flag-vs-default interactions) are
a recurring source of "why is it connecting to the wrong server?" bugs — the opposite of this
project's purpose. `messq context current -o json` prints the effective configuration *and the
source of each value*:

```
server      unix:///run/messq/messq.sock     (context "prod")
output      json                             (env MESSQ_OUTPUT)
timeout     5s                               (flag --timeout)
```

### 6.4 Output design

**Auto mode** picks human if stdout is a TTY, otherwise the machine form of that command
(`ndjson` for streaming commands, `json` for single/list commands). Colour is applied only when the
colour profile detection says the terminal supports it, which also means colours vanish under
pipes and in CI without any flag.

Human list output — no borders, aligned, relative times, abbreviated IDs, units:

```
$ messq lag
CONSUMER          STREAM   FILTER          BACKLOG  INFLIGHT  SCHEDULED  DLQ   OLDEST UNACKED
billing           orders   orders.>          1,284    12/256          3    2   4m12s
audit-tap         orders   orders.>              0     0/64           0    0   -
invoices          orders   orders.invoice.>     17      2/32          0   41   2h07m   (!)
```

The `(!)` marker and the 41 DLQ entry are colourised red, and the footer teaches:

```
1 consumer needs attention:  invoices (41 dead-lettered, oldest unacked 2h07m)
  inspect:  messq dlq ls --consumer invoices
  why:      messq trace $(messq dlq ls --consumer invoices -o json | jq -r '.[0].msg_id')
```

**Every inspect command ends with the next useful command.** That is the single strongest DX
lever in the whole design: the CLI teaches its own command tree at the moment of need.

`-o wide` adds columns (owner, ack-wait, max-deliver, bytes) rather than wrapping.

Machine output — one object per line for streams, a single document otherwise, every record
carrying a schema tag:

```
$ messq pending billing -o ndjson
{"schema":"messq.v1.Pending","msg_id":"01J8Z9K3QF7VB2M0RS4KQ7","seq":1042,"subject":"orders.created","consumer":"billing","attempt":2,"of":5,"delivered_at":"2026-08-21T14:02:41.419Z","deadline":"2026-08-21T14:03:11.419Z","owner":"worker-3@app01"}
```

Field names are frozen at 1.0; adding fields is allowed, removing or retyping requires
`messq.v2.*`. JSON Schemas live in `docs/schemas/` and are asserted in tests (§8).

`-o template=` uses Go `text/template` with helpers (`ago`, `bytes`, `pad`, `color`):

```
$ messq dlq ls -o template='{{range .}}{{.msg_id}} {{.reason}} {{ago .dead_at}}{{"\n"}}{{end}}'
```

We deliberately ship **no** embedded jq and **no** jsonpath: two query dialects to learn is a
worse deal than `messq … -o json | jq`, which every operator already knows. The `--help` for
`--output` says exactly that and shows a `jq` example.

### 6.5 stdout/stderr discipline and exit codes

- **stdout carries data only.** Tables, JSON, message bodies.
- **stderr carries narration**: progress, warnings, the "next command" hints, confirmations,
  flow-control notices. `messq peek orders --seq 1042 --raw > body.bin` works, always.
- Exit codes are a documented contract (`messq help exit-codes`):

| Code | Meaning |
|---|---|
| 0 | success |
| 1 | generic runtime failure |
| 2 | usage error (bad flag/arg) |
| 3 | not found (stream/consumer/message) |
| 4 | conflict / precondition failed (exists, revision mismatch, already redriven) |
| 5 | empty / timed out waiting (e.g. `messq sub --count 1 --wait 5s` found nothing) |
| 6 | cannot reach the daemon |
| 7 | permission denied |
| 70 | daemon internal error (bug; prints the report URL) |

Exit code 5 being distinct from 1 is what makes `messq` usable in shell loops and cron without
`grep`-ing output.

### 6.6 `messq sub --exec`: turning any script into a durable worker

The feature that makes messq feel like a Unix tool rather than a broker.

```
$ messq sub orders --as billing --exec ./handle.sh --concurrency 4
```

The message body arrives on the child's **stdin**; metadata arrives as environment variables
(`MESSQ_MSG_ID`, `MESSQ_SUBJECT`, `MESSQ_SEQ`, `MESSQ_ATTEMPT`, `MESSQ_MAX_DELIVER`,
`MESSQ_TRACE_ID`, `MESSQ_DEADLINE`, `MESSQ_HEADER_*`). The child's **exit code is the ack**:

| Exit | Action | Rationale |
|---|---|---|
| 0 | `ack` | success |
| 75 (`EX_TEMPFAIL`) | `nak` with backoff schedule | the sysexits convention for "try later" |
| 65 (`EX_DATAERR`) | `term` → straight to DLQ, no retries | poison payload, retrying is pointless |
| any other non-zero | `nak` with backoff | ordinary failure |
| killed by signal | `nak`, reason `signal:<name>` | crash |

`extend` heartbeats are sent automatically at `ack_wait/2` while the child runs, and the child's
stderr is captured (first 4 KiB) into the `nak` reason so it shows up in `messq trace` and in the
DLQ entry. This closes the loop: **the reason a job failed is visible from the queue, not only
from the worker's own logs.**

`--concurrency N` runs N children; with `ordered=true` consumers, concurrency is silently capped
per subject and the cap is reported once.

### 6.7 Inspect / replay / seek UX

```
$ messq peek orders --subject 'orders.*.created' --last 3
SEQ    ID            SUBJECT              AGE     SIZE   BODY
1040   01J8Z…KQ5     orders.eu.created    6m12s   412 B  {"id":40,"total":19900,"cur…
1041   01J8Z…KQ6     orders.us.created    5m58s   398 B  {"id":41,"total":4500,"curr…
1042   01J8Z…KQ7     orders.eu.created    4m12s   1.2 K  {"id":42,"total":129900,"cu…

peek does not consume. to see one in full:  messq peek orders --seq 1042 --raw
```

```
$ messq seek billing --to time:2026-08-21T13:00:00Z --dry-run
would move consumer "billing" cursor:
  from  seq 1042  (orders.eu.created, 4m12s ago)
  to    seq  907  (orders.us.created, 1h14m ago)
  => 135 messages would be re-delivered (58.4 MiB)
  => 12 messages currently in flight would be left alone (they still need acks)
re-run without --dry-run to apply, or add --yes to skip confirmation
```

```
$ messq replay --from -2h --to -1h --subject 'orders.eu.>' --into consumer:replay-eu --speed max
this will create consumer "replay-eu" and deliver 4,182 messages (18.2 MiB)
estimated 12s at max speed
continue? [y/N]
```

Every destructive or bulk command supports `--dry-run`, prints a precise preview, prompts on a TTY,
and requires `--yes` when not on a TTY. `messq stream rm` additionally requires the stream name to
be typed (`--confirm orders`) — the one place we deliberately add friction.

### 6.8 Errors that teach

Error rendering is a single function over a single struct. Shape: **what happened / why / what to
do next / where to learn more.**

```
$ messq sub orders --as bilingg
Error: consumer "bilingg" not found in stream "orders"

  A consumer holds the cursor and the ack state; messages are delivered through it.

  Did you mean:  billing

  Consumers in "orders":
    billing     1,284 backlog   12 in flight
    audit-tap       0 backlog    0 in flight

  Create it:   messq consumer create orders bilingg --ack-wait 30s
  Learn more:  messq help concepts

exit status 3
```

```
$ messq pub orders.created --file big.bin
Error: message is 4.1 MiB, stream "orders" allows at most 1.0 MiB

  Large bodies make the database and every backup larger, and slow every
  redelivery. Consider storing the payload elsewhere and publishing a reference.

  Raise the limit:   messq stream edit orders --max-msg-size 8MiB
  Check current:     messq stream info orders

exit status 4
```

```
$ messq pub orders.created 'hi'
Error: cannot reach messq at unix:///run/messq/messq.sock

  connect: no such file or directory — the daemon does not appear to be running.

  Start it:      messq serve
  Or as a unit:  systemctl start messq
  Other server:  messq --server http://queue-1:4222 ...  (or: messq context use prod)

exit status 6
```

Rules enforced in code review and by a linter test: no error ends at the verb phrase; every
`UserError` must have at least one `Next` entry; no error text contains a Go type name, a stack
trace, or the word "unexpected" without a bug-report URL.

### 6.9 Shell completion

Completion is where a CLI stops being a CLI and starts being an interface. Ours is dynamic and
descriptive:

```
$ messq sub or<TAB>
orders    -- 214,882 messages, 2 consumers, 1.2 GiB

$ messq pending <TAB>
billing    -- 12 in flight, oldest 4m12s
audit-tap  -- 0 in flight
invoices   -- 2 in flight, 41 dead-lettered

$ messq dlq redrive --reason <TAB>
max_deliver  -- exhausted delivery attempts
terminated   -- explicitly terminated by a consumer
too_large    -- exceeded max message size
```

Implementation: Cobra `ValidArgsFunction` and `RegisterFlagCompletionFunc`, returning
`cobra.CompletionWithDesc(name, description)` with `ShellCompDirectiveNoFileComp`. Two hard rules:

1. **Completion must never hang.** 200 ms budget with its own context; on timeout or connection
   failure it returns `nil, ShellCompDirectiveError` silently — a slow or dead daemon must never
   make the shell feel broken.
2. **Completion must never be expensive for the daemon.** Results cached in
   `$XDG_CACHE_HOME/messq/complete.json` for 5 s.

`messq completion bash|zsh|fish` emits the script; packages install them to
`/usr/share/bash-completion/completions/messq`, `/usr/share/zsh/site-functions/_messq`,
`/usr/share/fish/vendor_completions.d/messq.fish`.

### 6.10 Help design

- Every command: a `Short` (one line, lowercase, verb-first), a `Long` that **teaches the concept**
  and not just the syntax, and an `Example` block with 3–5 real invocations including one
  scripting example. A CI test fails the build if any command lacks Examples or has a `Long`
  shorter than 120 characters.
- **Topical help pages**, `git help everyday`-style: `messq help concepts`, `messq help ack`,
  `messq help redelivery`, `messq help dlq`, `messq help cursors`, `messq help ordering`,
  `messq help flow-control`, `messq help output`, `messq help exit-codes`, `messq help scripting`,
  `messq help lifecycle` (prints the state diagram from §4.2), `messq help durability`.
- `--help` is styled when on a TTY (headings, dimmed defaults, coloured flags) and plain text when
  piped, so `messq sub --help | less` is readable.
- Man pages generated from the same Cobra metadata at release time; `messq docs man --dir ./man`.
- The docs site is generated from the same source: `messq docs markdown` → `docs/cli/`. There is
  exactly one source of truth for command documentation, so drift is structurally impossible.

### 6.11 `messq top`

A live dashboard for the one screen an operator keeps open during an incident. Alt-screen TUI,
250 ms tick, `q` quits, `f` filters, `Enter` drills into a consumer, `d` jumps to its DLQ,
`p` pauses a consumer (with confirmation).

```
 messq top                     unix:///run/messq/messq.sock          up 4d 02:11   ●
 ────────────────────────────────────────────────────────────────────────────────
 publish  1.2k/s ▁▂▄█▆▃▂▁    deliver 1.1k/s ▂▃▅█▇▄▂▁    ack 1.1k/s   dead 0.2/s
 db 1.4 GiB   wal 8 MiB   fsync p99 2.1ms   events 4.2M (72h)

 CONSUMER    BACKLOG  INFLIGHT      SCHED  DLQ  ACK p50/p99   OLDEST UNACKED
 billing       1,284  ████░░ 12/256     3    2  14ms / 210ms  4m12s
 audit-tap         0  ░░░░░░  0/64      0    0   2ms /   9ms  -
 invoices         17  █░░░░░  2/32      0   41  1.2s /  8.9s  2h07m  ● attention

 [q]uit [f]ilter [enter] drill in [d] dlq [p]ause
```

---

## 7. Observability & logging design

Logging is not instrumentation here; it is the feature list. The rule: **the daemon's log and
`messq trace` and `/v1/events` are three renderings of one table.** There is no second logging
path, no `fmt.Println` anywhere in the broker.

### 7.1 The event vocabulary — closed set

Lifecycle verbs (10): `publish`, `deliver`, `ack`, `nak`, `extend`, `timeout`, `redeliver`,
`term`, `dead`, `drop`.
Operator verbs (8): `seek`, `replay`, `purge`, `redrive`, `create`, `delete`, `pause`, `resume`.
System verbs (4): `recovery`, `retention`, `flow`, `error`.

Adding a verb requires a docs change and a schema bump. A closed vocabulary is what makes
`grep`, alerting rules, and human pattern-recognition all work.

### 7.2 Field set — the same on every event

`ts, verb, level, stream, subject, consumer, msg, seq, trace, attempt, of, latency, reason, err`

- `msg` — the ULID. Stable from publish to DLQ. Abbreviated to `01J8Z…KQ7` on screen,
  full in JSON, expandable with `--full-ids`.
- `trace` — taken from the publisher's `Messq-Trace-Id`, or parsed out of a W3C `traceparent`
  header, or defaulted to the message ULID. Passed back to consumers as a header so the
  application's own logs correlate with ours without any configuration.
- `attempt`/`of` — always shown as `2/5`, never as two separate mysteries.

### 7.3 Human log format (default when stderr is a TTY)

Column-stable so that both the eye and `awk` work. Verb padded to 9 characters and colourised by
class (green ack, yellow nak/timeout, red dead/error, blue deliver, dim publish).

```
14:02:11.417  publish    orders.created   01J8Z…KQ7  seq=1042  1.2KiB  from=10.0.0.4
14:02:11.421  deliver    orders.created   01J8Z…KQ7  seq=1042  billing   attempt=1/5  wait=30s
14:02:41.422  timeout    orders.created   01J8Z…KQ7  seq=1042  billing   attempt=1/5  after=30.001s
14:02:41.423  redeliver  orders.created   01J8Z…KQ7  seq=1042  billing   attempt=2/5  backoff=1.1s
14:02:42.550  nak        orders.created   01J8Z…KQ7  seq=1042  billing   attempt=2/5  reason="upstream 503"
14:03:46.902  dead       orders.created   01J8Z…KQ7  seq=1042  billing   attempt=5/5  reason=max_deliver  age=95.4s
```

`--log-format` = `auto|human|json|logfmt`; auto = human on TTY, json otherwise (so systemd and
Loki get structured logs without anyone configuring anything).
`--log-events` = `all|lifecycle|changes|errors` controls which verbs are written to the log stream
(they are always written to the events table, subject to event retention).
Runtime level changes without restart: `messq log-level debug` → `POST /v1/admin/log-level` →
`slog.LevelVar.Set`.

### 7.4 Implementation

`log/slog` from the standard library, with **one custom handler** (`internal/eventlog.HumanHandler`)
implementing `Enabled/Handle/WithAttrs/WithGroup`. No third-party logging library: slog's
`Handler` interface is exactly the extension point we need, `slog.LevelVar` gives runtime level
control, and `HandlerOptions.ReplaceAttr` covers key renaming for the logfmt variant. The JSON
variant is `slog.NewJSONHandler` unmodified.

The broker emits `Event` structs; a fan-out hub delivers each to (a) the events table writer —
**in the same SQLite transaction as the state change it describes**, so an event can never claim
something that did not commit; (b) the slog handler; (c) live `/v1/events` subscribers; (d) the
Prometheus counters.

### 7.5 `messq trace` — the flagship

```
$ messq trace 01J8Z9K3QF7VB2M0RS4KQ7
message  01J8Z9K3QF7VB2M0RS4KQ7
subject  orders.created      stream orders      seq 1042      1.2 KiB
trace    4bf92f3577b34da6a3ce929d0e0e4736
headers  content-type=application/json  x-request-id=req_88fa

  +0.000s   publish     from 10.0.0.4 (pid 4412)
  +0.004s   deliver     -> billing        attempt 1/5   ack-wait 30s   owner=worker-3@app01
 +30.001s   timeout     billing did not ack within 30s
 +30.002s   redeliver   -> billing        attempt 2/5   after 1.1s backoff
 +31.133s   nak         billing: "upstream 503 from payments.internal"
 +36.140s   redeliver   -> billing        attempt 3/5   after 5.0s backoff
 +37.002s   nak         billing: "upstream 503 from payments.internal"
 +67.100s   redeliver   -> billing        attempt 4/5   after 30s backoff
 +68.010s   nak         billing: "upstream 503 from payments.internal"
 +95.400s   redeliver   -> billing        attempt 5/5   after 2m backoff
 +95.900s   nak         billing: "upstream 503 from payments.internal"
 +95.901s   dead        -> dlq  reason=max_deliver  after 5 attempts in 1m35s

  also delivered to:  audit-tap  (acked at +0.009s, attempt 1/1)

currently  DEAD-LETTERED since 4m12s ago
  inspect  messq dlq show 01J8Z9K3QF7VB2M0RS4KQ7
  body     messq peek orders --seq 1042 --raw
  retry    messq dlq redrive --msg 01J8Z9K3QF7VB2M0RS4KQ7
  siblings messq events --trace 4bf92f3577b34da6a3ce929d0e0e4736
```

`--trace <trace-id>` shows every message sharing a trace, so one upstream request that fanned out
into six messages reads as one story. This is the auditable-event-processing use case, delivered by
a single command.

### 7.6 `messq events` — the live tail

```
$ messq events --follow --verb nak,timeout,dead --consumer billing
$ messq events --since -1h --subject 'orders.eu.>' -o ndjson | jq -r 'select(.verb=="dead").msg'
$ messq events export --since -7d --gzip > audit-2026-08.ndjson.gz
```

Event retention: `--event-retention 72h` and `--event-max 5000000`, pruned by the janitor, with a
loud startup line stating the effective window. `messq doctor` warns if the events table exceeds
40% of the database size, and suggests the export-then-shrink command. We are explicit in the docs
that the audit trail costs disk, and we give the knob rather than hiding it.

### 7.7 Metrics

Prometheus on the admin listener (`/metrics`), using a **non-global registry** with
`promauto.With(reg)` and `promhttp.HandlerFor(reg, …)` so tests can instantiate isolated metric
sets and so we never inherit surprise collectors.

```
messq_published_total{stream,subject}          messq_delivered_total{stream,consumer}
messq_acked_total{consumer}                    messq_naked_total{consumer,source=explicit|timeout}
messq_dead_total{consumer,reason}              messq_redriven_total{consumer}
messq_dropped_total{stream,reason=retention}   messq_backlog{consumer}
messq_inflight{consumer}                       messq_inflight_limit{consumer}
messq_oldest_unacked_seconds{consumer}         messq_scheduled{consumer}
messq_ack_latency_seconds{consumer}            (histogram)
messq_publish_latency_seconds{durability}      (histogram, includes fsync)
messq_fsync_seconds                            (histogram)
messq_db_bytes / messq_wal_bytes / messq_events_rows
```

`messq metrics` renders the same registry as a human table so no Prometheus is needed to answer
"how is it doing?".

`messq doctor` — the diagnostic that encodes our operational knowledge:

```
$ messq doctor
[ok]    daemon reachable at unix:///run/messq/messq.sock (messq 1.0.0)
[ok]    durability=group, synchronous=FULL verified on a live connection
[ok]    disk /var/lib/messq: 41 GiB free (db 1.4 GiB, growing ~180 MiB/day)
[warn]  consumer "invoices": ack-wait 5s but observed ack p99 is 8.9s
        -> 31% of deliveries time out and retry. Raise it:
           messq consumer edit orders invoices --ack-wait 30s
[warn]  consumer "audit-tap" has not fetched in 3h14m (backlog 0, may be dead)
[fail]  consumer "invoices": dlq grew from 12 to 41 in the last hour and nothing
        has redriven it. Poison message likely.
        -> messq dlq ls --consumer invoices --group-by reason
[info]  stream "ordres" has 2 messages, 0 consumers, created 4d ago — possible typo?
        -> messq stream rm ordres --confirm ordres
3 checks need attention. exit status 1
```

---

## 8. Testing strategy

### 8.1 The pyramid, with a CLI-shaped top

1. **Pure state machine (unit).** `broker.Apply` is a pure function; every transition in §4.2 is a
   table row. Target: 100% branch coverage of the broker package, enforced in CI.
2. **Invariant / property tests.** A model-based test generates random interleavings of
   `publish / fetch / ack / nak / term / extend / timeout-tick / restart` and checks after every
   step:
   - no message is in `inflight` for a consumer more than once;
   - `deliveries ≤ max_deliver` always;
   - every message eventually reaches `ACKED` or `DEAD` (with a fair scheduler);
   - `pending = inflight ∪ scheduled` and each element is `< cursor.next_seq`;
   - the events table replayed from scratch reconstructs the current state exactly
     (**event/state agreement** — this is what makes the audit trail trustworthy);
   - no message is acked by a consumer whose filter does not match its subject.
3. **Crash tests.** A harness runs a real daemon, drives load, and `SIGKILL`s it at random
   intervals (and at fault-injection points behind a build tag: after insert-before-commit,
   between commit and reply, mid-fetch). After restart it asserts: nothing acked was lost, nothing
   published-and-201'd is missing, every previously in-flight message is redelivered exactly once.
4. **Durability tests.** `doctor --durability` asserts live pragma values. A dedicated test opens
   the DB, asserts `synchronous` reads back as `2` (FULL) on a *pooled* connection (this is the
   regression test for the "driver silently used NORMAL" class of bug). A documented manual
   power-cut procedure with a USB drive lives in `docs/testing/power-cut.md` and is run before each
   minor release.
5. **CLI golden tests — `testscript`.** The centrepiece. Every command has `.txtar` scripts under
   `testdata/script/`, executed by `rogpeppe/go-internal/testscript` with `go test` (coverage
   included). Golden files refresh with `UPDATE_SCRIPTS=true go test ./...`.

```
# testdata/script/dlq_redrive.txtar
messq serve --db $WORK/db --socket $WORK/sock &daemon&
messq pub orders.created '{"n":1}'
messq consumer create orders billing --ack-wait 1s --max-deliver 2
messq sub orders --as billing --count 1 --ack-mode none
sleep 3s
messq dlq ls --consumer billing
stdout 'max_deliver'
! stderr 'panic'
messq dlq redrive --consumer billing --yes
stdout 'redrove 1 message'
messq trace $MSGID -o json
stdout '"verb":"redrive"'

-- expected-dlq-ls.txt --
MSG            SUBJECT          CONSUMER  REASON       ATTEMPTS  DEAD
01J8Z…KQ7      orders.created   billing   max_deliver       2/2  1s ago
```

   Both faces are asserted: the human table (with `TERM=dumb`, deterministic clock via
   `MESSQ_FAKE_TIME`) and `-o json`.
6. **Help & DX linter tests.** Go tests that walk the Cobra command tree and fail if: any command
   lacks `Example`; any `Long` is shorter than 120 chars; any flag lacks a usage string; any
   command name is longer than 12 chars; the alias map has collisions; a documented exit code is
   unused or an undocumented one is returned. **DX is enforced by CI, not by discipline.**
7. **Schema compatibility tests.** Every `-o json` shape is validated against a committed JSON
   Schema in `docs/schemas/`; removing or retyping a field fails the build with a message telling
   you to bump to `messq.v2.X`.
8. **Executable documentation.** Every example line in every `Example:` block and every fenced
   command block in `README.md`, `docs/quickstart.md` and the `messq help <topic>` pages is
   extracted and run by a testscript test. Documentation cannot rot.
9. **Fuzzing.** `FuzzSubjectMatch` (pattern vs subject, cross-checked against a naive reference
   implementation), `FuzzHeaderParse`, `FuzzNDJSONDecode`, `FuzzTemplateOutput`.
10. **Soak & bench.** `messq bench --publishers 8 --consumers 4 --duration 1h --kill-every 5m` in a
    nightly job; asserts zero loss, bounded duplicate rate, no unbounded memory or DB growth, and
    records p50/p99 publish and ack latency into a committed `docs/benchmarks.md` so the numbers in
    our positioning stay honest.
11. `-race` on every test run; `golangci-lint` with `errcheck`, `bodyclose`, `sqlclosecheck`,
    `exhaustive` (for the verb and state enums).

### 8.2 What "done" means for a feature

A feature is done when it has: a state-machine test, a testscript covering the human output, a
testscript covering the JSON output, an entry in the relevant `messq help` topic, an `Example` in
its command, an error path with a `Next` suggestion, and completion support if it takes a resource
name. No exceptions; the checklist is in `CONTRIBUTING.md` and half of it is CI-enforced.

---

## 9. Roadmap: from empty repository to the ideal product

Estimates assume one focused engineer. Each milestone ends with a demo that a stranger can run.

### M0 — The chassis (week 1)

Build the DX machinery *before* the broker, so every later command inherits it for free.

- Repo layout, `go.mod`, `Makefile`, CI (build, test, `-race`, lint, golden-update check).
- Cobra root wrapped by `fang` for styled help/version, with our own error handler.
- `internal/cli/render`: the output framework — `Renderer` interface, `table`/`wide`/`json`/
  `ndjson`/`template` modes, TTY + colour-profile detection, relative-time and byte formatting,
  ID abbreviation.
- `internal/cli/errs`: `UserError{Code, Summary, Because, Next, HelpTopic, Details}`, exit-code
  mapping, JSON error rendering.
- `internal/config`: three-layer precedence + `messq context` + `--context`.
- testscript harness, fake clock, `messq version`, `messq help <topic>` scaffolding, docs
  generation (`messq docs man|markdown`), `goreleaser` config producing a static binary.
- **Exit criteria:** `messq --help` and `messq help concepts` look finished; `go test ./...` runs
  golden tests; a release tarball builds. Zero queue functionality exists.

### M1 — Durable core (weeks 2–3)

- SQLite store, migrations, connection hook asserting pragmas, writer goroutine with group commit.
- Streams, messages, consumers, cursor, inflight tables.
- HTTP server on UDS: `publish`, `fetch` (long-poll), `ack`, `info`, `healthz`.
- `messq serve`, `messq pub`, `messq sub` (print mode), `messq peek`.
- `broker.Apply` pure state machine for transitions 1 and 2; invariant test harness.
- **Exit criteria:** publish 1M messages, kill -9, restart, consume all of them exactly once when
  every fetch is acked. The 60-second quickstart works, minus retries.

### M2 — The full lifecycle (weeks 4–5)

- `nak` (+`--delay`), `extend`, timeout scanner, backoff schedule with jitter, `max_deliver`,
  `term`, DLQ table, redelivery queue, restart orphan sweep.
- `messq pending`, `messq ack/nak/term`, `messq dlq ls|show|redrive|drop`.
- Flow control (`max_ack_pending`, `max_ack_bytes`) with the visible "window full" notice.
- Crash-test harness with fault injection.
- **Exit criteria:** the state machine diagram in §4.2 is implemented and each transition has a
  passing test; a poison message reliably lands in the DLQ after exactly `max_deliver` attempts and
  can be redriven.

### M3 — Observability: the product becomes itself (weeks 6–7)

- Events table written in the same transaction as state changes; closed verb vocabulary.
- `HumanHandler` for slog; `--log-format auto|human|json|logfmt`; runtime level control.
- `messq events` (query + `--follow`), **`messq trace`**, `messq lag`.
- Trace-ID intake (`Messq-Trace-Id` / `traceparent`), propagation to consumers.
- Prometheus registry + `/metrics` + `messq metrics`.
- Event retention/pruning.
- **Exit criteria:** `messq trace <id>` renders §7.5 for a message that timed out, was naked, and
  died. Replaying the events table reconstructs current state (tested).

### M4 — Operations surface (weeks 8–10)

- Full `stream` and `consumer` CRUD with optimistic revisions; pause/resume; `reset`.
- `seek`, `replay`, `purge`, `stream export/import`, `backup`/`restore` (`VACUUM INTO`).
- Retention modes (`limits`, `workqueue`), size/age/count limits, `discard` policy, gap events.
- Publish dedupe window (`Messq-Msg-Id`).
- Per-subject ordering (`ordered=true`) with blocked-subject display.
- Dynamic shell completion with descriptions and the 200 ms budget; `messq completion`.
- `messq doctor` with the check set from §7.7.
- `--dry-run` and confirmation flow for every destructive command.
- **Exit criteria:** an operator can run an entire incident — find, inspect, redrive, replay, seek,
  purge — without reading anything but `--help`.

### M5 — Worker mode & the tutorial (weeks 11–12)

- `messq sub --exec` with the exit-code protocol, env metadata, stderr capture into nak reasons,
  automatic `extend` heartbeats, `--concurrency`.
- `pkg/messq` Go client + `Worker` helper.
- `messq quickstart` (guided, throwaway db, testscript-verified).
- All `messq help <topic>` pages written; every command's `Example` block finalised and executed
  in CI.
- **Exit criteria:** `messq sub jobs --exec ./handle.sh` is a production-viable worker; the
  quickstart from §6.1 is a passing test.

### M6 — 1.0 hardening (weeks 13–15)

- `messq top` (bubbletea), isolated in one package.
- JSON schemas frozen; error-code table frozen; exit codes frozen; compatibility test suite.
- Packaging: deb/rpm/apk/tarball via goreleaser+nfpm, with man pages and completions installed to
  the right paths; a systemd unit with `StateDirectory=messq`, `DynamicUser`, hardening options.
- TCP + token auth + TLS; socket permission model documented.
- Nightly soak, published `docs/benchmarks.md`, `docs/durability.md`, `SECURITY.md`.
- Docs site generated from Cobra.
- **Exit criteria:** `apt install messq && systemctl start messq` on a clean VM, then the
  quickstart, with no other steps. Tag **1.0.0**.

### M7 — Phase 2a: time and priority (weeks 16–19)

- Delayed / scheduled publish (`messq pub --at`, `--after`), reusing the redelivery timer wheel.
- Priority channels: `--priority 0..9` on publish, priority-aware delivery selection, and
  `messq lag --by-priority`.
- Per-consumer rate limiting (`--max-rate 100/s`, token bucket) with a visible "rate limited" state
  in `top` and `consumer info`.
- **Exit criteria:** a delayed message shows its scheduled time in `peek` and `trace`.

### M8 — Phase 2b: groups, size and audit (weeks 20–24)

- Consumer groups with leases: multiple workers share one consumer with an explicit lease id,
  visible in `pending` (`owner` column), lease expiry = redelivery, `messq consumer members`.
- Body compression (zstd) above a threshold, transparent, reported in `stream info`.
- Retention policies per subject; `messq stream policy`.
- Audit trail export/rotation with signing (`messq events export --sign`), tamper-evident hash
  chain over the events table.
- **Exit criteria:** four workers on one consumer, kill one, its leases redeliver within
  `ack_wait`, and `messq pending` shows which host held them.

### M9 — Phase 3: modest replication (weeks 25+, only if demanded)

- Read-only follower: streaming the events+messages log to a second node, `messq follow`, with
  explicit "this is asynchronous, you can lose the tail" documentation and a
  `messq promote` runbook. Never presented as HA.
- **Explicitly not** consensus, not automatic failover.

---

## 10. Risks & open questions

### 10.1 Risks with mitigations

| # | Risk | Mitigation |
|---|---|---|
| R1 | **SQLite single-writer throughput ceiling.** Durable publishes are bounded by fsync rate. | Group commit amortises fsync across concurrent publishers. Publish measured numbers in `docs/benchmarks.md` and state the ceiling in the README: expect ~5–15k durable msg/s on NVMe with concurrency, ~500–2k on cloud network storage. If you need 100k/s, we say "use Kafka" in our own docs. |
| R2 | **Large bodies bloat the DB and every backup, and are re-read on every redelivery.** | `max_msg_size` default 1 MiB, enforced with a teaching error. Compression in M8. Docs recommend the claim-check pattern. Revisit external blob storage only if users ask. |
| R3 | **The events table is the product and also the disk cost.** | `--event-retention 72h` / `--event-max`, janitor pruning, `--log-events` filtering, `doctor` warns at 40% of DB size, `events export --gzip` for archival. |
| R4 | **Poison-message loops** — the classic "retry death spiral" that silently drops money. | DLQ is on by default, `max_deliver` defaults to 5 (never unlimited), `doctor` alerts on a growing un-drained DLQ, `dead` events are ERROR level, `messq_dead_total` is a metric people will alert on. |
| R5 | **Ack-wait shorter than the real job duration** — causes duplicate work at scale, and is the #1 operational mistake in every system with an ack deadline. | `extend` exists; the Go `Worker` and `--exec` mode send heartbeats automatically; `doctor` compares `ack_wait` against observed ack p99 and prints the exact `consumer edit` command. |
| R6 | **Nak storms / thundering herd** when a shared dependency fails. | Exponential backoff by default with ±20% jitter, not linear; explicit nak delays; rate limiting in M7. |
| R7 | **`--exec` exit-code protocol is a new convention** users may not know. | Follows `sysexits.h` (75 tempfail, 65 dataerr); printed in `messq sub --help`; `messq help exit-codes`; the first non-zero exit prints a one-time hint explaining the mapping. |
| R8 | **Auto-provisioning hides typos** (`messq pub ordres.created` creates a stream). | Auto-provision only when no existing stream matches; `--strict` for production; `doctor` flags tiny consumer-less streams; the publish output *says* "created stream" loudly the first time. |
| R9 | **Clock skew / backwards jumps** break ULID sortability and deadline math. | Deadlines use the monotonic clock; wall clock only for display and ULID timestamps. The ULID generator clamps against a backwards jump. A detected jump > 1s logs a WARN and is a `doctor` check. |
| R10 | **`charmbracelet/fang` is explicitly experimental.** | Isolated behind `internal/ui`; a build-tag-free fallback path renders plain Cobra help. If fang breaks, we lose polish, not function. |
| R11 | **Output stability becomes a compatibility burden** once people script against it. | Human output is explicitly *not* a contract (documented in `messq help output` and in `--help`); JSON is, and it is schema-tested and versioned. This split is stated everywhere so nobody scripts against tables. |
| R12 | **Scope creep toward being a small Kafka.** | The positioning section is a gate in `CONTRIBUTING.md`: any feature that requires a new noun in the command tree needs an explicit written justification against "understandable in an evening". |
| R13 | **Security expectations.** People will expose it on TCP. | UDS default with `0660`; TCP refuses to start without a token or `--allow-insecure` (which warns every 60s); systemd unit is hardened; `SECURITY.md` states plainly that per-subject ACLs do not exist in 1.0. |

### 10.2 Open questions (to be decided by M4, tracked as issues)

1. **Should `ack` be allowed cross-consumer by message id alone?** Today `messq ack <id>` must
   resolve to exactly one in-flight delivery; if two consumers hold the same message, we error and
   ask for `--consumer`. Is that friction worth the safety? (Leaning: yes, keep it.)
2. **Should the cursor be per-consumer only, or should we offer a "shared cursor group"** before
   M8's lease-based groups? (Leaning: wait for M8; two overlapping concepts is worse than one late
   one.)
3. **`workqueue` retention interaction with `replay`** — replaying messages that a workqueue stream
   has already deleted is impossible. Do we refuse to create a replay consumer on a workqueue
   stream, or warn? (Leaning: refuse, with a teaching error.)
4. **Event-table hash chaining** (M8): worth the write cost on every event, or an export-time-only
   signature? (Leaning: export-time only; the write path is our scarcest resource.)
5. **Do we ship a `messq sub --exec` supervisor** (restart the child on crash, backoff) or leave
   that to systemd? (Leaning: leave it; `--exec` spawns per message, so there is nothing to
   supervise.)
6. **Multi-tenant single daemon or one daemon per app?** Currently one DB per daemon and no
   namespacing beyond streams. Do we need `--db` multiplexing? (Leaning: no — run two daemons, two
   sockets; that is simpler than a tenancy model.)
7. **Windows/macOS support.** The target is Linux. Do we build the client for macOS so developers
   on laptops can talk to a Linux daemon? (Leaning: yes for the client, from M6, at no cost given
   pure-Go dependencies.)

---

## 11. Library choices, grounded in fetched documentation

Every dependency below was checked against its current docs via context7. The bar for a dependency
in this project: it must either be unavoidable, or it must directly serve the DX thesis.

| Library | Version target | Why, grounded in the docs I read |
|---|---|---|
| **`github.com/spf13/cobra`** (v1.9.x) | CLI framework | The docs confirm exactly the completion machinery this plan depends on: `ValidArgsFunction` for runtime-computed argument completion, `RegisterFlagCompletionFunc` for flag values, the `ShellCompDirective` set (we use `NoFileComp` and `KeepOrder`), and `cobra.CompletionWithDesc` for the *described* completions in §6.9 (`orders -- 214,882 messages, 2 consumers`). It also ships `GenBashCompletion`/`GenZshCompletion`/`GenFishCompletion` and man-page generation from the same command metadata, which is what makes "one source of truth for docs" (§6.10) achievable. `urfave/cli` was rejected because, per current comparisons, its completion customisation is limited to commands rather than arbitrary arguments — and dynamic resource-name completion is a headline feature here, not a nicety. |
| **`charm.land/fang/v2`** | Cobra polish | Docs show `fang.Execute(ctx, cmd)` plus `WithErrorHandler`, `WithColorSchemeFunc` (light/dark adaptive via `lipgloss.LightDarkFunc`), `WithoutCompletions` and `WithoutVersion`. We take the styled help/usage and light/dark theming, supply **our own** `WithErrorHandler` so the teaching-error format in §6.8 is ours, and pass `WithoutCompletions` because we ship a richer `completion` command. Isolated behind `internal/ui` because it is self-described as experimental (R10). |
| **`charmbracelet/lipgloss`** (v2) | Tables, colour | The docs state that Lip Gloss "automatically downsamples colors to the best available profile and strips colors when output is not a TTY", and expose `colorprofile.Detect(os.Stdout, os.Environ())` with `lipgloss.Complete(profile)`. That behaviour is precisely the guarantee §6.4's auto-mode rests on: piping to `grep` yields clean text with no flag. `table.New().Headers(...).Rows(...)` covers the `info` panels; list output is our own aligned writer (borders are noise). |
| **`modernc.org/sqlite`** | Storage driver | Pure Go ⇒ `CGO_ENABLED=0` ⇒ the single static binary we promise, cross-compiled without a toolchain. The docs document the DSN parameters we rely on: `_pragma=journal_mode(WAL)`, `_pragma=synchronous(...)`, `_pragma=busy_timeout(...)`, `_pragma=foreign_keys(1)` and `_txlock=immediate|deferred|exclusive` (we use `immediate` so write transactions take the write lock up front and avoid mid-transaction `SQLITE_BUSY` upgrades). Critically, it documents `RegisterConnectionHook`, "a callback that runs once per newly opened connection, after all DSN parameters are applied" — that is our enforcement point for durability pragmas on every pooled connection (§3.2), which addresses the best-documented SQLite durability trap: a driver defaulting `synchronous` to `NORMAL` in WAL mode, which does not fsync on commit. `mattn/go-sqlite3` was rejected for CGO; `bbolt` was rejected because losing `sqlite3 messq.db` inspectability costs more than it saves. |
| **`log/slog`** (stdlib) | Logging | The docs give the full `Handler` contract (`Enabled`, `Handle`, `WithAttrs`, `WithGroup`, plus the rules about zero `Time`/`PC`, resolved values and empty groups) — enough to write `HumanHandler` correctly in a few hundred lines. `slog.LevelVar` gives us runtime level changes without a restart (`messq log-level debug`), and `HandlerOptions.ReplaceAttr` handles key renaming for the logfmt variant and custom level names. No third-party logger: the extension point we need is already the standard one, and one fewer dependency in the daemon is worth more than any benchmark delta. |
| **`prometheus/client_golang`** | Metrics | Docs show `prometheus.NewRegistry()` + `promauto.With(reg)` + `promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg, MaxRequestsInFlight: N})`, and explicitly recommend a dedicated registry for test isolation. We use a non-global registry so `messq metrics` and `/metrics` render the same object and tests never fight over global state. |
| **ULID** (`oklog/ulid`, spec `/ulid/spec`) | Message IDs | The spec docs give the properties that make it a *DX* choice, not just an ID choice: 26 characters in Crockford Base32 with the alphabet `0123456789ABCDEFGHJKMNPQRSTVWXYZ` (no I, L, O, U — so an operator retyping an ID off a screenshot cannot make the classic confusions), lexicographic sortability matching time order (so `ORDER BY id` equals `ORDER BY time`, and abbreviated IDs still cluster sensibly), and a documented monotonic factory that increments the randomness within a millisecond to preserve ordering. UUIDv4 was rejected (unsortable, 36 chars, hyphenated); UUIDv7 is sortable but hex-encoded, longer, and lacks the ambiguity-free alphabet. |
| **`rogpeppe/go-internal/testscript`** | CLI testing | The docs describe it as "a shell-like test environment optimized for testing Go CLI commands… supports assertions for stderr/stdout, integrates with `go test` including coverage, uses the txtar format for input/output files, allows automatic updating of golden files". That is the exact tool for treating CLI output as a contract (§8.1.5), including the `-update` flow for regenerating goldens when output legitimately changes. |
| **`charmbracelet/bubbletea`** | `messq top` only | Docs confirm `WithAltScreen` (auto-restores on quit), `tea.Tick` for the refresh loop, and `WindowSizeMsg` for resize. Confined to `internal/top`; no other command imports it, so the core CLI stays dependency-light and every non-TUI command works over SSH on a dumb terminal. |
| **`goreleaser` + `nfpm`** | Release | Docs show `nfpms` producing deb/rpm/apk and the `contents` mechanism with per-`packager` filtering — that is how generated man pages and the three completion scripts land in the right paths per distro, so `apt install messq` gives you `man messq-dlq` and working `<TAB>` immediately. |
| stdlib `net/http`, `encoding/json`, `text/template`, `context`, `database/sql` | Core | No web framework, no router library. The endpoint list in §5.2 fits in `http.ServeMux` with Go 1.22+ method+pattern routing. |

**Explicit anti-dependency list** (things we consciously do not take, so nobody "helpfully" adds
them): gRPC/protobuf, `spf13/viper`, `cobra-cli`, any ORM, `zap`/`zerolog`/`logrus`, a YAML
library, an embedded jq/jsonpath engine, a web UI framework, a metrics facade (OpenTelemetry SDK —
we emit trace *ids* and correlate, but we do not take the SDK in 1.0), and any clustering library.
Each of these would either enlarge the mental model or break the single-static-binary promise.

### Repository layout

```
cmd/messq/main.go                 thin: build root command, fang.Execute
internal/cli/                     one file per command; each is ~100 lines of flags + a call
internal/cli/render/              output modes, tables, colours, time/byte formatting
internal/cli/errs/                UserError, exit codes, suggestion (Levenshtein) engine
internal/cli/complete/            dynamic completion funcs + cache
internal/config/                  three-layer config, contexts
internal/broker/                  PURE state machine: Apply(state, cmd, now) -> mutations, events
internal/store/                   sqlite, migrations/*.sql, writer goroutine, group commit
internal/api/                     http handlers, error envelope, NDJSON streaming
internal/eventlog/                Event type, verb vocabulary, HumanHandler (slog), fan-out hub
internal/metrics/                 prometheus registry
internal/top/                     bubbletea dashboard (isolated)
pkg/messq/                        public Go client + Worker helper
docs/                             concepts, errors.md, schemas/, benchmarks.md, durability.md, cli/
testdata/script/*.txtar           the CLI contract
```

---

## Appendix A — Design rules, one line each (pin these above the desk)

1. Human output on a TTY, machine output everywhere else, never a third mode.
2. Data to stdout, narration to stderr, always.
3. Every inspect command ends by printing the next useful command.
4. Every error says what happened, why, and what to type next.
5. Exit codes are a contract; 5 means "empty", not "broken".
6. Anything the daemon can do to a message, `messq` can do by hand.
7. Every destructive command has `--dry-run`, a preview, and `--yes`.
8. Completion never hangs and never costs the daemon anything.
9. The audit trail commits in the same transaction as the state it describes.
10. If a feature needs a new noun in the command tree, justify it in writing first.
