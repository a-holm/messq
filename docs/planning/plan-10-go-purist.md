# messq — Project Plan (Plan 10: the Go idiom purist)

> **Persona stance.** Stdlib first. Two non-stdlib modules in the whole binary. One goroutine owns
> one thing. `context.Context` on every blocking call. Errors are sentinels wrapped with context, never
> strings compared with `strings.Contains`. `log/slog` and nothing else. The codebase must be readable
> front-to-back in an evening, and that constraint is enforced in CI, not in a README promise.

---

## 1. Vision & positioning

### 1.1 The product in one paragraph

messq is a single Go binary that turns a directory on a Linux box into a durable, at-least-once
message queue with explicit ack semantics borrowed from NATS JetStream — ack, nak, term, ack-wait,
max-deliver, dead-letter, max-ack-pending — and a first-class, per-message audit trail in the logs.
You start it with `messq serve --data /var/lib/messq`. You publish with `curl` or `messq pub`. You
consume with `messq sub orders workers --exec ./my-worker`. When something goes wrong at 03:00 you
run `messq trace orders/1284` and get the complete life story of one message: published, delivered,
timed out, redelivered, naked, dead-lettered — with timestamps, attempt numbers and the reason
string the worker gave.

### 1.2 What "Go idiom purist" buys the product

This is not aesthetics. Every purist rule below maps to a product property that the positioning
statement promises:

| Purist rule | Product property it buys |
|---|---|
| One storage engine, no second one | A message and its DLQ copy move in **one transaction**. No cross-engine reconciliation, no "the payload is there but the cursor isn't" bug class. |
| Two dependencies (`go.etcd.io/bbolt`, its own `golang.org/x/sys`) | `go.sum` audit is a 30-second job. No supply-chain surface. `CGO_ENABLED=0` static binary, `scratch` container, no glibc. |
| One goroutine owns one stream | The concurrency story fits on an index card. No lock ordering. `-race` is green because there is almost nothing to race on. |
| `applyTransition` is the only state mutator **and** the only logger | It is structurally impossible to change a message's state without emitting a log line. The audit trail can't drift from reality. |
| `context` everywhere, `signal.NotifyContext` at the root | Clean shutdown is a single cancel, not a pile of `done chan struct{}`. |
| stdlib `net/http` + JSON | `curl`-able. Every language on earth already has a client. No protoc in the build. |
| stdlib `flag` + a 90-line dispatcher | The CLI has no framework opinions to fight; `messq --help` prints what we wrote. |
| `testing/synctest` | Ack-timeout tests run in microseconds and are deterministic. Time-dependent logic gets *more* test coverage, not less. |

### 1.3 Positioning

- **Above** an in-process channel / Redis LIST / a `jobs` table you hand-rolled: real ack semantics,
  real redelivery, real DLQ, real replay, real observability.
- **Below** Kafka / Pulsar / RabbitMQ clusters: no quorum replication, no consensus, no partitions
  across nodes, no exactly-once. One process, one file, one machine.
- **Beside** NATS JetStream: we adopt its vocabulary deliberately so the semantics are familiar, but
  we trade its clustering and performance ceiling for a fraction of the operational and conceptual
  surface, plus an ops/observability story JetStream does not have (`messq trace`, `messq sub --exec`,
  human log format, fenced acks).

### 1.4 Explicit non-goals (v0.x and v1.0)

Clustering. Replication. Exactly-once. Cross-stream transactions. Pluggable storage backends.
A plugin system. A web UI. Anything requiring a code generator in the build.

### 1.5 The "evening" contract, made testable

- `internal/broker/stream.go` contains the **entire** delivery state machine and is capped at
  **500 non-comment lines** (CI check).
- Total non-test Go LOC capped at **6 000** (CI check, `scripts/loc.sh`).
- Non-stdlib module count capped at **2** (CI check, `scripts/deps.sh`).
- Documented reading order in `README.md`: `docs/semantics.md` → `internal/messq/types.go` →
  `internal/broker/stream.go` → `internal/store/schema.go`. Four files, ~1 400 lines, and you know
  how messq works.

---

## 2. Architecture overview

### 2.1 Processes

One. `messq serve`. Everything else is the same binary in client mode talking HTTP over a Unix
socket. No sidecars, no supervisor tree, no embedded database server.

### 2.2 Goroutines — the complete census

For a daemon with `S` streams:

```
1  main / run()          signal.NotifyContext, wiring, shutdown orchestration
1  http.Server.Serve     accept loop  (+ N short-lived handler goroutines, owned by net/http)
1  committer             the ONLY goroutine that opens a bbolt write transaction
1  janitor               global ticker: retention sweep, lag logging, stats
S  stream actors         one per stream; owns ALL mutable state for that stream and its consumers
```

That is `4 + S` long-lived goroutines. Handler goroutines are transient and own nothing; they
marshal a request into a command, hand it to an actor, and wait.

### 2.3 Ownership rules (non-negotiable)

1. **A stream's in-memory state is touched only by its actor goroutine.** No mutex guards queue
   state anywhere in the codebase. `grep -c sync.Mutex internal/broker/` must be 0.
2. **Only the committer opens `db.Update`.** Actors never write to bbolt directly. Read-only
   `db.View` is allowed from actors and handlers, but see rule 3.
3. **No bbolt transaction may be held across a network wait or a channel send.** bbolt's byte slices
   are only valid inside the transaction, and long-lived read transactions prevent page reclamation
   and make the file grow monotonically — this is the single loudest caveat in the bbolt README. The
   `internal/store` package therefore returns only **owned copies**; nothing of bbolt's leaks past a
   `View`.
4. **Anything that can block takes `ctx` as its first parameter.**

### 2.4 Actor protocol

```go
// internal/broker/stream.go

// op is a unit of work executed on the stream's own goroutine.
type op struct {
	ctx  context.Context
	fn   func(*stream)   // runs on the actor goroutine; may only touch s and o
	done chan struct{}
}

// do enqueues fn and waits for it. Once an op is accepted by the actor it always
// runs to completion; callers must not abandon it, because fn writes into memory
// the caller will read. Cancellation is honoured at two points only: while queued,
// and by the actor before it starts fn.
func (s *stream) do(ctx context.Context, fn func(*stream)) error {
	o := op{ctx: ctx, fn: fn, done: make(chan struct{})}
	select {
	case s.ops <- o:
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closed:
		return messq.ErrShuttingDown
	}
	<-o.done
	return nil
}
```

The actor loop, in full shape:

```go
func (s *stream) run() {
	defer close(s.closed)
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		s.armTimer(timer) // reset to the earliest ack deadline / nak visibility
		select {
		case o := <-s.ops:
			if o.ctx.Err() == nil {
				o.fn(s)
			}
			close(o.done)
		case <-timer.C:
			s.expireDeadlines(time.Now())
		case c := <-s.commits: // committer reports a batch durable
			s.releaseWaiters(c)
		case <-s.quit:
			s.drain()
			return
		}
	}
}
```

Three cases. That is the whole concurrency model of messq.

### 2.5 Data flow: publish

```
HTTP handler                 stream actor                  committer                bbolt
  parse + validate
  ──── do(publish) ────────►
                              seq = nextSeq()
                              append mutation to open group
                              register waiter(seq)
                              ──── group (when full or 1ms) ───►
                                                             db.Update{
                                                               apply mutations
                                                             }.Commit   ──► fsync
                              ◄──── commitDone(groupID) ──────
                              durableSeq = seq
                              close(waiter)
  ◄─── 201 {seq, id, trace_id}
```

Two properties fall out of this shape and both are load-bearing:

- **A publisher's `201` means `fsync` returned.** No "durable-ish" acknowledgements.
- **`durableSeq` gates delivery.** A message is not a candidate for any consumer until its commit
  group is on disk. Readers therefore can never observe data that a crash would erase, which removes
  an entire class of "phantom delivery" reasoning.

### 2.6 Data flow: fetch → ack

```
HTTP handler                     stream actor                      committer
  ─ do(fetch{n, maxWait}) ─►
                                 if inflight >= maxAckPending: return 0 (429)
                                 pick redeliverables (heap), then new (cursor)
                                 attempt++, visibleAt = now+ackWait
                                 append pending mutations; register waiter
                                 (if 0 available: park handler in waiters,
                                  wake on next publish or maxWait)
                                                                      commit + fsync
                                 ◄────── commitDone ───────
  ◄─ 200 NDJSON envelopes ──
  ...worker processes...
  ─ do(ack{token}) ────────►
                                 verify token attempt == current attempt   (fence!)
                                 delete pending; record finished; advance ackFloor
                                 append mutations; register waiter
                                                                      commit + fsync
  ◄─ 204 ────────────────────
```

Delivery-state mutations are committed **before** the fetch response is written. That costs one
group-commit round trip per fetch (amortised over the whole batch) and buys guarantee **G6**: a
crash cannot reset attempt counters, so `max_deliver` is honoured across restarts and poison
messages cannot loop forever.

### 2.7 Package layout

```
messq/
├── go.mod                      module github.com/messq/messq   (go 1.26)
├── Makefile
├── README.md
├── docs/
│   ├── semantics.md            the state machine, normative
│   ├── protocol.md             HTTP API reference
│   ├── operations.md           runbook: what each log line means and what to do
│   └── storage-format.md       on-disk layout, versioned
├── cmd/messq/main.go           ~25 lines: os.Exit(cli.Main(...))
└── internal/
    ├── messq/     domain types, sentinel errors, config structs   (imports: stdlib only)
    ├── subject/   token split, "*" and ">" matching                (stdlib only)
    ├── id/        message ids, trace ids, ack tokens               (stdlib only)
    ├── store/     bbolt schema, record codec, committer            (+ bbolt)
    ├── broker/    stream actor, consumer state machine, retention
    ├── wire/      JSON request/response types (shared api⇄client)  (stdlib only)
    ├── api/       net/http handlers, error mapping
    ├── client/    HTTP client used by the CLI (and later exported)
    ├── cli/       flag-based subcommand dispatcher + commands
    ├── logfmt/    human-readable slog.Handler
    └── metric/    ~200-line Prometheus text-exposition metrics
```

Dependency direction is strictly downward and **enforced in CI** by `scripts/layers.sh`, which runs
`go list -deps` per package and fails if e.g. `internal/store` ever imports `internal/api`.

No `pkg/`. No `api/`. No `internal/service/`, `internal/repository/`, `internal/domain/` ceremony.
Packages are named after what they *are*, and `internal/` means we can refactor freely until v1.0,
when `internal/client` graduates to `client/` as the only exported surface.

### 2.8 Error handling discipline

```go
// internal/messq/errors.go
var (
	ErrNotFound      = errors.New("not found")
	ErrConflict      = errors.New("already exists")
	ErrStaleAck      = errors.New("stale ack: message was redelivered")
	ErrTooLarge      = errors.New("message too large")
	ErrStreamFull    = errors.New("stream full")
	ErrFlowControl   = errors.New("max ack pending reached")
	ErrFaulted       = errors.New("stream faulted; restart required")
	ErrShuttingDown  = errors.New("shutting down")
	ErrBadSubject    = errors.New("subject does not match stream")
)
```

- Every classifiable failure is one of these, wrapped: `fmt.Errorf("store: put %s/%d: %w", …)`.
- `internal/api/errors.go` holds **one** table mapping sentinel → (HTTP status, machine code). A test
  iterates a `[]error` of all sentinels and fails if any lacks a mapping. Adding a sentinel without a
  mapping breaks the build.
- The daemon does not `panic` for operational conditions. Two `recover` sites exist, both documented:
  the HTTP middleware (log at ERROR, 500, keep serving) and the actor loop (fault this stream, keep
  the other streams and the process alive).
- `errors.Is` at boundaries, never string matching.

---

## 3. Storage & durability design

### 3.1 Engine decision: `go.etcd.io/bbolt` v1.5.x — and why not the alternatives

**Chosen: bbolt.** A single-file, copy-on-write B+tree with serialisable ACID transactions, written
in pure Go, whose only dependency is `golang.org/x/sys`. It is the storage engine under etcd, and
its crash-safety story is "the meta page is written last; if it doesn't verify, use the other one" —
meaning **messq ships zero write-ahead-log replay code**. Recovery is `bolt.Open` plus rebuilding
in-memory indexes from durable records.

Rejected, with reasons grounded in the docs I read:

- **A hand-rolled append-only segment log + index.** This is the "obvious" queue design and it is the
  wrong choice *for this persona*. It means writing segment rotation, CRC-per-record torn-write
  detection, group-commit framing, index rebuild on startup, and compaction — the exact pattern
  `hashicorp/raft-wal` implements, where the commit frame carries a CRC32C over everything appended
  since the last fsync specifically to detect torn tails. That is 1 500–2 500 lines of the hardest,
  least-testable code in the project, and it *is* the "understand it in an evening" budget. bbolt has
  already written and battle-tested it.
- **SQLite (`modernc.org/sqlite`).** Genuinely tempting: pure Go, no cgo, WAL mode, and the docs show
  the DSN pattern (`file:///data.db?_journal=WAL&_synchronous=NORMAL&_txlock=immediate`). But (a) it
  is a machine-translated port of ~250 kLOC of C — auditing it is not an evening, it is a career;
  (b) it drags `database/sql`, a query planner, and SQL string handling into a component whose entire
  access pattern is "get by u64 key, scan a key range"; (c) `_synchronous=NORMAL` in WAL mode is
  explicitly *not* durable per transaction — the well-documented trap that SQLite's default settings
  do not fsync on commit — so we would end up at `synchronous=FULL` and back to one fsync per commit,
  i.e. exactly bbolt's cost with 100× the surface area; (d) WAL mode still enforces a single writer,
  so the concurrency model would be identical anyway. The SQL introspection benefit is real but is
  better served by `messq` subcommands than by handing operators a `sqlite3` shell on live state.
- **`mattn/go-sqlite3`.** cgo. Kills `CGO_ENABLED=0`, static linking, and cross-compilation. Out on
  the first criterion.
- **Badger / Pebble / LSM trees.** Better write throughput, far more moving parts (compaction,
  value logs, tuning knobs), large dependency trees. We are explicitly not chasing Kafka's throughput.

**The one thing bbolt does not give us** is a checksum on data pages (only meta pages are checksummed).
So each message record carries its own CRC32C, giving us `messq verify` and honest bit-rot detection
for ~10 lines using stdlib `hash/crc32` with the Castagnoli table.

### 3.2 On-disk layout

One file: `<data>/messq.db`. Plus `<data>/messq.pid` and `<data>/LOCK` (bbolt takes an flock itself).

Nested buckets (bbolt buckets hold both keys and sub-buckets):

```
root
├── "meta"                          key "format_version" → u32 (=1)
│                                   key "node_id"        → 16 bytes (random, for log correlation)
│                                   key "created_at"     → i64 unix nanos
└── "streams"                       (bucket)
    └── <streamName>                (bucket)
        ├── key "cfg"               → JSON StreamConfig
        ├── key "state"             → StreamState{FirstSeq, LastSeq, Count, Bytes} (32 bytes, fixed)
        ├── "msgs"                  (bucket)  be64(seq) → msgRecord
        └── "consumers"             (bucket)
            └── <consumerName>      (bucket)
                ├── key "cfg"       → JSON ConsumerConfig
                ├── key "state"     → ConsumerState{DeliveredSeq, AckFloor, Acked, Naked,
                │                                   Timeouts, DeadLettered, CreatedAt} (fixed)
                ├── "pending"       (bucket)  be64(seq) → pendingRecord
                └── "finished"      (bucket)  be64(seq) → u8 outcome (ack|term|dlq|expire)
```

Big-endian u64 keys mean bbolt's B+tree ordering **is** sequence ordering, so a cursor `Seek` from
`delivered_seq+1` is the natural "next messages" scan, and `Cursor.First()` on `msgs` is the
retention head. Bucket `FillPercent` is set to `0.9` on `msgs` — bbolt's documented tuning for
append-only, monotonically-increasing keys — which roughly halves page splits and file growth on the
hot path.

Sequences come from bbolt's per-bucket `NextSequence()`, which is durable inside the same transaction
as the record; we never maintain our own counter that could diverge from the data.

### 3.3 Record encodings

`msgRecord` is hand-rolled binary (it is the hot path; JSON here would be a self-inflicted wound):

```
offset  size  field
0       1     magic 'M'
1       1     version (=1)
2       1     flags   (bit0: gzip payload, bit1..7 reserved)
3       1     reserved
4       8     seq                 (u64 BE, redundant with the key — used by `messq verify`)
12      8     published_at        (i64 unix nanos)
20      2     subject_len         (u16)
22      1     trace_len           (u8, 16 or 0)
23      2     header_count        (u16)
25      4     body_len            (u32)
29      …     subject bytes
        …     trace bytes
        …     headers: repeated { u8 klen, klen bytes, u16 vlen, vlen bytes }
        …     body
end-4   4     crc32c over bytes [0, end-4)
```

Hard limits: subject ≤ 1 KiB, 32 headers, header key ≤ 64 B, header value ≤ 4 KiB, body ≤
`--max-msg-size` (default **1 MiB**, hard ceiling 8 MiB). Oversized publishes get `413` with the
limit in the body. The cap is not arbitrary: bbolt copies pages on write, so multi-megabyte values
turn every publish into a multi-megabyte page copy.

`pendingRecord` is fixed-width, 40 bytes + reason:

```
0   4   attempt          (u32)
4   8   visible_at       (i64 unix nanos)  — ack deadline, or nak-delay expiry
12  8   first_delivered  (i64 unix nanos)
20  8   last_delivered   (i64 unix nanos)
28  1   in_flight        (u8: 1 = handed to a consumer, 0 = waiting out a nak delay)
29  1   reason_len       (u8)
30  …   reason bytes     (last nak/term reason, truncated to 255 B)
```

`StreamConfig` and `ConsumerConfig` are JSON. They are cold, human-editable, and forward-compatible
by construction — exactly what `encoding/json` is good at. Configuration lives **in the database**,
not in a config file: one source of truth, changed through the API, and every change logged.

### 3.4 The committer and the fsync policy

The committer is the only goroutine that opens a write transaction:

```go
type mutation interface{ apply(*bolt.Tx) error }  // ~10 small concrete types

type group struct {
	streamID string
	muts     []mutation
	bytes    int
	id       uint64
}

func (c *committer) run() {
	for {
		g, ok := <-c.in
		if !ok { return }
		batch := []group{g}
		bytes := g.bytes
		// merge everything already queued, plus anything arriving within the window
		win := time.NewTimer(c.window) // default 1ms
		for bytes < maxBatchBytes {    // 8 MiB
			select {
			case g2, ok := <-c.in:
				if !ok { goto commit }
				batch = append(batch, g2); bytes += g2.bytes
			case <-win.C:
				goto commit
			}
		}
	commit:
		win.Stop()
		start := time.Now()
		err := c.db.Update(func(tx *bolt.Tx) error {
			for _, g := range batch {
				for _, m := range g.muts {
					if err := m.apply(tx); err != nil { return err }
				}
			}
			return nil
		})
		c.observe(len(batch), bytes, time.Since(start), err)
		c.notify(batch, err)
	}
}
```

**Why we own group commit instead of using `db.Batch`.** bbolt ships `DB.Batch`, which opportunistically
coalesces concurrent `Update` calls and is configured through `MaxBatchSize` / `MaxBatchDelay`. It
would work. But its contract is that *"the provided function must be idempotent as it may be called
multiple times"* — bbolt re-runs and splits batches when one member fails. That is a subtle,
action-at-a-distance rule sitting on our most correctness-critical path, and a future contributor who
appends to a captured slice inside a batch function introduces a silent duplicate. Owning ~80 lines of
committer removes the footgun, makes the fsync policy explicit and measurable
(`messq_commit_duration_seconds`, `messq_commit_batch_size`), and lets us merge across streams, which
`Batch` on a per-actor basis could not.

**Durability modes** (`--sync`):

| Mode | Behaviour | Loss window | Default |
|---|---|---|---|
| `group` | every commit group ends in an fsync; responses sent after fsync returns | **zero acknowledged writes lost** | ✅ |
| `none` | `db.NoSync = true`, periodic `db.Sync()` | unbounded, **and the file can be left inconsistent** | benchmarks only |

`--sync=none` refuses to start without `--i-accept-data-loss`, logs `WARN` on every startup, and sets
`durable:false` in publish responses. bbolt's `NoSync` is documented as a bulk-loading option, not a
durability tier, and I am not going to pretend otherwise by offering a cosy-sounding `interval` mode
that can corrupt the database. Honest options only.

**Throughput envelope** (to be verified at M5, and published in the README with hardware stated): with
a 1 ms merge window on consumer NVMe (fsync ≈ 100–400 µs), messq should sustain **10 000–25 000
1 KiB messages/s** for publish+ack, at p99 end-to-end latency of a few milliseconds. That is one to
two orders of magnitude below Kafka and roughly two orders of magnitude above what the target
workloads need. If a workload needs more, messq is the wrong tool and the README says so.

**Backpressure.** At most two commit groups are outstanding (one being written, one accumulating).
When both are full, the actor stops reading `s.ops`, handler goroutines block in `do()`, `net/http`
stops reading request bodies, and TCP backpressure reaches the publisher. There is no unbounded
queue anywhere in the write path. This is the entire flow-control implementation for publishers.

**On commit error** (ENOSPC, EIO): the stream is marked `faulted`. In-memory state is now ahead of
disk and cannot be reconciled safely, so the actor rejects everything with `503 ErrFaulted`, logs at
ERROR with the underlying error, sets `messq_stream_faulted{stream}=1`, and stays that way until
restart — at which point recovery loads the last durable truth. Crash-only design: the only repair
path is the one we test on every crash test.

### 3.5 Crash recovery

There is no log replay. Startup is:

1. `bolt.Open(path, 0600, &bolt.Options{Timeout: 5s, FreelistType: bolt.FreelistMapType})`. bbolt
   validates a meta page and uses the previous one if the newest is torn. If both fail, the file is
   unusable and we exit non-zero with instructions to restore from `messq backup`.
2. Check `meta/format_version`. Unknown → refuse to start (never silently migrate).
3. For each stream: read `cfg` and `state`; verify `state.LastSeq` against `msgs` bucket's last key,
   repairing `state` if a commit landed between them (it can't, they're in one tx — but we check and
   log, because "can't happen" is where the bugs live).
4. For each consumer: load `cfg`, `state`, and stream all `pending` records into the in-memory
   min-heap. **Every recovered in-flight entry is made immediately visible** (`visible_at = now`,
   `in_flight = false`), with attempt preserved. This is safe with no thundering herd precisely
   because delivery is *pull*-based: nothing is pushed at anybody; the redelivered messages are
   simply the first candidates the next `fetch` sees, still bounded by `max_ack_pending`.
5. Log one `store.recover` INFO line per stream with counts, and one WARN summarising recovered
   in-flight messages, since those are the ones that will be redelivered and duplicated.
6. `durableSeq = state.LastSeq` — everything on disk is by definition durable.

**Clock discipline.** In-memory deadlines are `time.Time` values carrying Go's monotonic reading, so
NTP steps and DST cannot delay or prematurely fire a redelivery while the process is up. Only the
persisted `visible_at` is wall-clock, and it is only ever used as "was pending at crash time" — never
compared against a live wall clock — because recovery resets visibility to *now*. This makes messq
immune to the classic "clock jumped backwards and the queue went quiet for an hour" incident.

### 3.6 Retention, purge, compaction, backup

- **Deletion has exactly one owner: retention.** Acks advance cursors and clear pending; they never
  free bytes. Consequently there is no reference counting between consumers, replay within the
  retention window is *always* possible, and adding a consumer to a stream later can start from
  `seq 1`. This is a deliberate divergence from work-queue brokers that delete on ack, and it is what
  makes "replay and inspection as core features" actually true.
- `StreamConfig.Retention = {MaxAge, MaxMsgs, MaxBytes, Discard: old|new}`. The janitor ticks every
  10 s, sends a `purgeExpired` op to each actor, and the actor deletes from the head using a cursor,
  emitting one `msg.expire` DEBUG per message and one `stream.purge` INFO summary per sweep.
- **Undelivered-message protection**: retention will not delete a message that is still `pending` for
  any consumer unless `--retention-force`; instead it logs WARN `retention.blocked` with the blocking
  consumer. Silent data loss under retention pressure is a classic broker footgun and we refuse it by
  default.
- `messq stream purge <stream> [--keep N | --before-seq N | --before-time T]` — explicit, logged,
  requires `--yes` when it would delete pending messages.
- **File growth.** bbolt reuses freed pages but never shrinks the file. Steady state is stable; a
  traffic spike leaves a high-water mark. `messq compact` quiesces writes, runs `bolt.Compact` into
  `messq.db.new`, fsyncs, renames, reopens, and logs before/after sizes.
- `messq backup [-o file]` uses `Tx.WriteTo` inside a `View` for a consistent hot copy — the one
  place we knowingly hold a read transaction for a long time, which is why it logs a WARN when it
  runs for more than 10 s and why the docs tell you to run it off-peak.

---

## 4. Delivery semantics & message lifecycle

### 4.1 Guarantees, numbered so logs and docs can cite them

- **G1 — At-least-once.** A published message that received `201` is delivered to each matching
  consumer at least once, until it is acked, termed, dead-lettered, or aged out by retention.
- **G2 — Durable publish.** `201` implies `fsync` returned.
- **G3 — No phantom reads.** A message is never delivered before its publish is durable.
- **G4 — Explicit completion.** Only `ack`, `term`, dead-lettering, or retention completes a message
  for a consumer. Nothing else.
- **G5 — Bounded attempts.** A message is delivered at most `max_deliver` times per consumer, then
  dead-lettered or parked.
- **G6 — Durable attempts.** Attempt counters and ack floors survive crash and restart.
- **G7 — Fenced acks.** An ack naming attempt *n* is rejected once attempt *n+1* has been delivered.
- **G8 — Per-subject ordering (opt-in).** With `ordered_by_subject`, a consumer never has two
  messages with the same subject in flight simultaneously, and redelivery of a subject's message
  blocks later messages on that subject until it completes.
- **G9 — Replayability.** Any message still within retention can be replayed by seeking a consumer.
- **G10 — Explicitly not guaranteed:** exactly-once, global ordering across subjects, ordering
  across consumers, delivery to a consumer created after retention removed the message.

### 4.2 State machine

State is per **(consumer, message)** pair, not per message. It is derived from three durable
structures: `ack_floor`, the `pending` bucket, and the `finished` bucket.

```
                       ┌─────────────┐
   publish  ──────────►│  AVAILABLE  │◄──────────────────────────┐
   (durable)           └──────┬──────┘                           │
                              │ fetch: attempt++,                │ nak delay elapses
                              │ visible_at = now+ack_wait        │ (in_flight=false → true)
                              ▼                                  │
                       ┌─────────────┐  nak(delay)        ┌──────┴──────┐
                       │  IN-FLIGHT  ├───────────────────►│   WAITING   │
                       └──┬───┬───┬──┘                    └─────────────┘
                          │   │   │
             ack ─────────┘   │   └───────── deadline expires (ack_wait)
                              │                       │
                progress ─────┤ (extend deadline,     │  attempt < max_deliver ─► AVAILABLE
                 (heartbeat)  │  no attempt++)        │  attempt >= max_deliver ─┐
                              │                       │                          │
                              │ term(reason)          ▼                          ▼
                              ▼                  ┌─────────┐              ┌──────────────┐
                        ┌──────────┐             │ EXHAUST │─────────────►│ DEAD-LETTERED│
                        │  ACKED   │             └─────────┘   on_exhaust │  or PARKED   │
                        └──────────┘                            = dlq/park└──────────────┘
                              │                                        │
                              └──────────► TERMINATED ◄────────────────┘
                                     (all terminal states are recorded in `finished`
                                      until ack_floor sweeps past them)
```

**Formal state derivation** (this is the definition; the code is a transcription of it):

```
finished(c, s)   ⇔  s ≤ ackFloor(c)  ∨  s ∈ finishedBucket(c)
inflight(c, s)   ⇔  s ∈ pending(c) ∧ pending[s].in_flight
waiting(c, s)    ⇔  s ∈ pending(c) ∧ ¬pending[s].in_flight
available(c, s)  ⇔  s ≤ durableSeq ∧ ¬finished(c,s) ∧ s ∉ pending(c)
                    ∧ matchesFilter(c, subject(s))
redeliverable(c,s) ⇔ waiting(c,s) ∧ pending[s].visible_at ≤ now
```

**Invariants** (asserted by the model test after every operation):

```
I1  ackFloor(c) ≤ deliveredSeq(c) ≤ lastSeq(stream)
I2  ∀ s ∈ pending(c):        s > ackFloor(c) ∧ s ≤ deliveredSeq(c)
I3  ∀ s ∈ finishedBucket(c): s > ackFloor(c) ∧ s ≤ deliveredSeq(c)
I4  pending(c) ∩ finishedBucket(c) = ∅
I5  |{s : inflight(c,s)}| ≤ maxAckPending(c)
I6  ∀ s: attempt(c,s) ≤ maxDeliver(c)
I7  ackFloor is monotonically non-decreasing
I8  ackFloor(c)+1 ∉ pending(c) ∪ finishedBucket(c)   (the floor is fully swept)
I9  ordered_by_subject ⇒ ∀ subj: |{s : inflight(c,s) ∧ subject(s)=subj}| ≤ 1
```

### 4.3 Transitions, exhaustively

| Transition | Trigger | Durable effect | Log event | Level |
|---|---|---|---|---|
| `publish` | `POST …/msgs` | `msgs[seq]`, `state.LastSeq++` | `msg.publish` | DEBUG |
| `deliver` | fetch selects the message | `pending[seq]{attempt+1, in_flight, visible_at}` , `state.DeliveredSeq` | `msg.deliver` | DEBUG |
| `ack` | `POST /v1/ack` | delete `pending[seq]`, `finished[seq]=ack`, sweep floor | `msg.ack` | DEBUG |
| `nak` | `POST /v1/nak` | `pending[seq].in_flight=false`, `visible_at=now+delay` | `msg.nak` | INFO |
| `term` | `POST /v1/term` | delete `pending[seq]`, `finished[seq]=term`, sweep floor | `msg.term` | WARN |
| `progress` | `POST /v1/progress` | `pending[seq].visible_at = now+ack_wait` | `msg.progress` | DEBUG |
| `timeout` | actor timer, `in_flight ∧ visible_at ≤ now` | `pending[seq].in_flight=false` | `msg.timeout` | INFO |
| `exhaust→dlq` | timeout/nak with `attempt ≥ max_deliver` | append to `<stream>.DLQ`, delete `pending`, `finished[seq]=dlq` — **one transaction** | `msg.dlq` | WARN |
| `exhaust→park` | same, `on_exhaust=park` | delete `pending`, `finished[seq]=park` | `msg.park` | WARN |
| `expire` | retention sweep | delete `msgs[seq]`, `state.FirstSeq++` | `msg.expire` | DEBUG |
| `seek` | `POST …/seek` | rewrite `state`, clear `pending` + `finished` | `consumer.seek` | INFO |
| `stale_ack` | ack with wrong attempt | *none* | `msg.stale_ack` | WARN |

### 4.4 Delivery selection algorithm

```
fetch(consumer c, n, maxBytes, maxWait):
  if inflight(c) ≥ c.maxAckPending:            return 429 ErrFlowControl
  if |finishedBucket(c)| ≥ maxFinishedAboveFloor: return 429 (see §10 risk R7)
  out := []
  # 1. redeliveries first — old work before new work, always
  for s in heap.PopWhile(visible_at ≤ now) while len(out) < n:
      if c.orderedBySubject && subjectInFlight(c, subject(s)): continue
      out = append(out, s)
  # 2. then fresh messages
  for s := c.deliveredSeq+1; s ≤ durableSeq && len(out) < n; s++:
      if !matchesFilter(c, subject(s)) { c.deliveredSeq = s; continue }
      if finished(c, s) || pending(c, s) { continue }
      if c.orderedBySubject && subjectInFlight(c, subject(s)) { break }   # note: break, not continue
      out = append(out, s); c.deliveredSeq = s
  # 3. long-poll if empty
  if len(out) == 0 && maxWait > 0:
      park handler as a waiter; wake on publish, on nak-visibility, or at maxWait
  # 4. record attempts durably, then respond
  for s in out: attempt++, in_flight=true, visible_at = now + backoff(c, attempt)
  commit(mutations); wait for durable
  return envelopes(out)
```

Redeliveries are always drained before new messages: a failing message must not be starved behind an
infinite stream of new work, or it never reaches its DLQ and the operator never learns about it.

`backoff(c, attempt)` = `c.Backoff[min(attempt-1, len-1)]` if a backoff list is configured, else
`c.AckWait`. Defaults: `ack_wait = 30s`, `max_deliver = 5`, `backoff = []`, `max_ack_pending = 256`.

### 4.5 Ack fencing (G7) — a deliberate improvement over the prior art

The ack token embeds the attempt number:

```
ack token := "<stream>/<consumer>/<seq>/<attempt>/<epoch>"
             epoch = the consumer's monotonically increasing seek generation
```

If a worker stalls for 40 s with `ack_wait=30s`, the message is redelivered to a second worker as
attempt 2. The first worker's late ack names attempt 1 → `409 stale_ack`, logged at WARN with both
attempts. Without fencing, that ack silently completes a message that another worker is still
processing, and the second worker's later ack silently completes a message that no longer exists —
the exact "duplicate + lost completion" pattern that makes at-least-once systems hard to debug.
The `epoch` component makes acks from before a `seek` equally impossible.

Cost: one `uint32` comparison. Benefit: a whole class of production mystery becomes a WARN line
that names the problem.

### 4.6 Dead-letter handling, with the war stories applied

The DLQ is an ordinary stream, `<stream>.DLQ`, auto-created with `max_age=30d`. Because it lives in
the same bbolt database, the "copy to DLQ + mark original finished" pair is **one transaction** —
no window in which a message is in both places or neither.

DLQ messages carry the original headers plus:

```
Messq-Dlq-Origin-Stream, -Origin-Seq, -Origin-Subject, -Consumer,
Messq-Dlq-Attempts, -Reason (timeout|max_deliver|term), -Last-Error,
Messq-Dlq-First-Delivered, -Dlq-At, and the original Messq-Trace-Id (preserved!)
```

Preserving the trace id means `messq trace <original-id>` shows the DLQ hop and the replayed copy
as one continuous story.

Guard rails, taken directly from the RabbitMQ operational literature:

- `messq dlq replay <stream> [--limit N] [--subject pat] [--yes]` — `--limit` is **mandatory**.
  There is no "replay everything" flag.
- Replayed messages get `Messq-Replay-Of: <origin-id>` and `Messq-Replay-Count: n`. messq **refuses**
  to replay a message with `Replay-Count ≥ 3` without `--force`, and prints why. A message that fails
  after two fix-and-replay cycles is not transient; looping it is how DLQs become infinite.
- Retention on the DLQ never deletes without logging, and `messq dlq ls` shows *growth rate* next to
  depth, because a DLQ that grew 400 messages in the last five minutes is an incident while one that
  grew 400 messages last Tuesday is a chore.
- `messq stream info` warns if the DLQ has no consumer, since an unread DLQ is a silent data sink.

---

## 5. API / protocol

### 5.1 Decision: HTTP/1.1 + JSON on `net/http`

**Chosen:** HTTP/1.1, JSON bodies, `net/http` with Go 1.22+ `ServeMux` method-and-wildcard patterns
(`mux.HandleFunc("POST /v1/streams/{stream}/msgs", h.publish)`, `r.PathValue("stream")`) — a real
router with zero dependencies. Default listener is a Unix socket; TCP is opt-in.

**gRPC rejected**, on evidence from the grpc-go docs: it requires `protoc` plus `protoc-gen-go` and
`protoc-gen-go-grpc` in the build (a code-generation step in a project whose thesis is readability),
pulls in `google.golang.org/protobuf`, `golang.org/x/net`, `golang.org/x/sys`, `golang.org/x/text`
and `genproto` — i.e. it alone would be ~6× our entire dependency budget — and its headline benefit
here, HTTP/2 stream flow control, is something messq structurally does not need: our consumers are
**pull**-based with explicit credits (`max_messages`, `max_bytes`, `max_ack_pending`), so
backpressure is expressed in the protocol semantics rather than in transport windows. The grpc-go
flow-control example — a goroutine watching whether `Send()` blocks for a second to infer that the
window is exhausted — is a good illustration of complexity we get to skip entirely.

The deciding argument is a product one: **`curl` is the universal client**, and "readable ops" is a
feature we are selling. An operator on a box with nothing but `curl` and `jq` can publish, consume,
ack, inspect and replay.

Content negotiation is in place from day one (`Accept: application/x-ndjson`) so a binary framing can
be added at M9 without a v2 API.

### 5.2 Endpoints

```
Streams
  POST   /v1/streams                                  create             201 | 409
  GET    /v1/streams                                  list               200 (NDJSON)
  GET    /v1/streams/{stream}                         info               200 | 404
  PATCH  /v1/streams/{stream}                         update config      200
  DELETE /v1/streams/{stream}?confirm={name}          delete             204
  POST   /v1/streams/{stream}/purge                   purge              200

Messages
  POST   /v1/streams/{stream}/msgs                    publish one        201 | 413 | 429 | 507
  POST   /v1/streams/{stream}/msgs:batch              publish NDJSON     207
  GET    /v1/streams/{stream}/msgs/{seq}              inspect            200 | 404
  GET    /v1/streams/{stream}/msgs/{seq}/data         raw payload        200
  GET    /v1/streams/{stream}/msgs?from=&limit=&subject=   peek          200 (NDJSON)

Consumers
  POST   /v1/streams/{stream}/consumers               create             201 | 409
  GET    /v1/streams/{stream}/consumers               list               200
  GET    /v1/streams/{stream}/consumers/{c}           info + lag         200
  DELETE /v1/streams/{stream}/consumers/{c}           delete             204
  POST   /v1/streams/{stream}/consumers/{c}/fetch     pull               200 (NDJSON)
  POST   /v1/streams/{stream}/consumers/{c}/seek      replay/skip        200
  POST   /v1/streams/{stream}/consumers/{c}/pause     pause delivery     200
  GET    /v1/streams/{stream}/consumers/{c}/pending   in-flight + parked 200 (NDJSON)

Acknowledgement (token-based, batchable)
  POST   /v1/ack        {"tokens":[…]}                                   204 | 409
  POST   /v1/nak        {"tokens":[…],"delay":"5s","reason":"…"}         204 | 409
  POST   /v1/term       {"tokens":[…],"reason":"…"}                      204 | 409
  POST   /v1/progress   {"tokens":[…]}                                   204 | 409
  (curl-friendly aliases: POST /v1/streams/{s}/consumers/{c}/msgs/{seq}/ack?attempt=N)

Operations
  GET    /v1/info                 build, uptime, data dir, sync mode, counts
  GET    /v1/health               200 always-if-serving; 503 if any stream faulted
  GET    /v1/stats                per-stream/consumer counters as JSON
  GET    /metrics                 Prometheus text exposition
  GET    /debug/pprof/…           stdlib, only with --pprof
```

### 5.3 Publish

Raw body, metadata in headers — no base64 on the write path, and `curl --data-binary @file` just
works.

```http
POST /v1/streams/orders/msgs HTTP/1.1
Messq-Subject: orders.eu.created
Messq-Trace-Id: 7f3a91c2b4e5d6a780f1e2d3c4b5a697        (optional; also accepts W3C traceparent)
Messq-Msg-Id: order-88214-created                        (optional; dedupe key, M8)
Messq-Header-Tenant: acme
Content-Type: application/octet-stream
Content-Length: 1284

{"order_id":88214,…}
```

```http
HTTP/1.1 201 Created
Location: /v1/streams/orders/msgs/1284

{"id":"orders/1284","stream":"orders","seq":1284,"subject":"orders.eu.created",
 "trace_id":"7f3a91c2b4e5d6a780f1e2d3c4b5a697","published_at":"2026-08-21T14:02:01.101Z",
 "durable":true}
```

### 5.4 Fetch

```http
POST /v1/streams/orders/consumers/workers/fetch HTTP/1.1
Accept: application/x-ndjson

{"max_messages":10,"max_bytes":1048576,"max_wait":"20s"}
```

Response is NDJSON, one envelope per line, `Flush`ed as each becomes available via
`http.NewResponseController(w).Flush()`; a write deadline is set with `SetWriteDeadline` so a dead
peer cannot pin the handler forever. The connection closes when `max_messages` is reached, at
`max_wait`, or on shutdown (a clean end-of-stream, never a reset).

```json
{"id":"orders/1284","stream":"orders","seq":1284,"subject":"orders.eu.created",
 "trace_id":"7f3a91c2b4e5d6a780f1e2d3c4b5a697","attempt":2,"max_deliver":5,
 "published_at":"2026-08-21T14:02:01.101Z","ack_deadline":"2026-08-21T14:03:01.512Z",
 "headers":{"Tenant":"acme"},"data":"eyJvcmRlcl9pZCI6…",
 "ack_token":"orders/workers/1284/2/1"}
```

Base64 on this path is a deliberate trade: it costs 33 % on the read side and buys a single,
uniformly parseable, human-readable frame. `GET …/msgs/{seq}/data` gives raw bytes when it matters.

### 5.5 Errors

```json
{"error":{"code":"stale_ack","message":"ack for attempt 1, message is on attempt 2",
          "stream":"orders","seq":1284,"trace_id":"7f3a…"}}
```

`code` is a stable machine string; the mapping table is exhaustively tested (§2.8). Statuses:
`400` malformed, `401` bad token, `404` unknown stream/consumer/seq, `409` conflict or stale ack,
`413` too large, `429` flow control or rate limit (with `Retry-After`), `503` faulted or shutting
down, `507` stream full with `discard=new`.

### 5.6 Transport, auth, limits

- Default `unix:///run/messq/messq.sock`, mode `0660`, group `messq`. Filesystem permissions are the
  authorisation model for local use, which is the right amount of security for the 90 % case.
- `--listen tcp://127.0.0.1:4747` adds TCP; TCP **requires** `--token-file` (compared with
  `crypto/subtle.ConstantTimeCompare`) or explicit `--insecure-no-auth`. Optional
  `--tls-cert/--tls-key` via stdlib `crypto/tls`.
- `http.Server{ReadHeaderTimeout: 10s, IdleTimeout: 120s, MaxHeaderBytes: 16 << 10}`. No global
  `WriteTimeout` — it would kill long-polls; per-response `SetWriteDeadline` is used instead.
- `--max-conns` enforced with a buffered-channel semaphore in an `Accept` wrapper.
- Every request gets a `request_id`; it and the `trace_id` come back in response headers.

---

## 6. CLI & developer experience

### 6.1 Structure

One binary, two personalities. `messq serve` runs the daemon; every other subcommand is a client
that talks to `--addr` (default: `$MESSQ_ADDR`, else the standard socket path).

No cobra. The cobra docs show the intended path — install `cobra-cli`, scaffold a `cmd/` package per
command, wire `viper` for config — and that is three dependencies (`cobra`, `pflag`, `viper` and its
tree) plus a generated code layout to onboard onto, in exchange for features we do not use
(`viper`-backed config, dynamic completion, automatic docs generation). Our replacement is
`internal/cli/cli.go`, roughly 90 lines:

```go
type command struct {
	name, usage, short string
	run  func(ctx context.Context, env *Env, args []string) error
}

func Main(ctx context.Context, env *Env, argv []string) int { … }   // dispatch, --help, exit codes
```

`Env` carries `Stdin io.Reader`, `Stdout, Stderr io.Writer`, `Args`, `Getenv func(string) string`.
Nothing in `internal/cli` touches package-level `os` state, which makes every CLI test an in-process
table test with no subprocess and no `t.Setenv` races.

Flags use `flag.NewFlagSet` per subcommand. Completion is a hand-written static script emitted by
`messq completion bash|zsh|fish` (~60 lines of shell, generated at build time from the command table
and checked in, so `messq completion` can't drift from the commands).

### 6.2 Command surface

```
messq serve      --data DIR --listen ADDR[,ADDR] --sync group|none
                 --log-format auto|text|json --log-level debug|info|warn|error
                 --max-msg-size 1MiB --commit-window 1ms --pprof --check-config

messq stream     create NAME --subjects 'orders.>' [--max-age 7d --max-msgs N --max-bytes 10GiB
                                                    --discard old|new]
                 ls | info NAME | rm NAME | purge NAME [--keep N|--before-seq N|--before-time T]
messq consumer   create STREAM NAME [--filter 'orders.eu.>' --ack-wait 30s --max-deliver 5
                                     --backoff 1s,5s,30s --max-ack-pending 256
                                     --on-exhaust dlq|park|drop --ordered-by-subject
                                     --start new|all|seq:N|time:T]
                 ls STREAM | info STREAM NAME | rm STREAM NAME
                 seek STREAM NAME --to start|end|seq:N|time:T
                 pause STREAM NAME | resume STREAM NAME

messq pub        STREAM SUBJECT [-d DATA | -f FILE | -]  [--header k=v]… [--trace-id ID] [--count N]
messq sub        STREAM CONSUMER [-n 10] [--auto-ack] [--exec 'cmd args'] [--concurrency 4]
messq peek       STREAM [--from SEQ] [--last N] [--subject PAT] [--data]
messq inspect    STREAM/SEQ
messq pending    STREAM CONSUMER [--parked]
messq lag        [STREAM]
messq dlq        ls STREAM | inspect STREAM/SEQ | replay STREAM --limit N [--subject PAT] [--yes]
                 | purge STREAM --yes
messq trace      MSG-ID [--log FILE|GLOB|-] [--follow]
messq backup     [-o FILE] | messq compact | messq verify | messq info | messq bench
```

Every command takes `--json` (NDJSON for lists) so it composes with `jq`. Human output is
`text/tabwriter`. Colour only when `stdout` is a character device and `NO_COLOR` is unset — detected
with `os.Stdout.Stat()` and `os.ModeCharDevice`, not a dependency.

Exit codes: `0` ok · `1` error · `2` usage · `4` not found · `5` conflict/stale · `69` daemon
unreachable (`EX_UNAVAILABLE`, so `systemd` and shell scripts can distinguish "broker down" from
"bad request").

### 6.3 The killer feature: `messq sub --exec`

```
$ messq sub orders workers --exec './process-order' --concurrency 4
```

For each delivered message, `messq` runs the command with the payload on **stdin** and metadata in
the environment:

```
MESSQ_MSG_ID=orders/1284      MESSQ_STREAM=orders     MESSQ_SEQ=1284
MESSQ_SUBJECT=orders.eu.created  MESSQ_ATTEMPT=2      MESSQ_MAX_DELIVER=5
MESSQ_TRACE_ID=7f3a91c2…      MESSQ_ACK_DEADLINE=2026-08-21T14:03:01Z
MESSQ_HEADER_TENANT=acme      traceparent=00-7f3a91c2…-…-01
```

Exit-code contract, borrowed from `sysexits.h` so it feels native to anyone who has written a
mail filter:

| Exit | Meaning | Action |
|---|---|---|
| `0` | success | `ack` |
| `75` (`EX_TEMPFAIL`) | transient | `nak` with the consumer's backoff |
| `65` (`EX_DATAERR`) | permanently bad message | `term` (straight to `finished`, no retries) |
| anything else | failure | `nak`; stderr's last line becomes the nak `reason` |
| killed by signal | crash | `nak`; reason `"killed by SIGxxx"` |

`--concurrency N` runs up to N children; the ack deadline is heartbeated with `progress` at
`ack_wait/3` while a child is alive, so long jobs never get redelivered under a running worker.

This makes messq a complete background job system with **zero client library and zero code**, which
is the fastest possible path from `apt install` to value, and it is the single strongest reason a
small team would pick messq over a `jobs` table.

### 6.4 First five minutes

```
$ messq serve --data ./data &
$ messq stream create orders --subjects 'orders.>' --max-age 7d
$ messq consumer create orders workers --ack-wait 30s --max-deliver 3
$ echo '{"id":1}' | messq pub orders orders.eu.created -
$ messq sub orders workers --exec 'jq .id'
$ messq lag
STREAM  CONSUMER  BACKLOG  IN-FLIGHT  ACK-FLOOR  OLDEST-PENDING  DLQ
orders  workers         0          0       1284               -    0
```

### 6.5 Go client

`internal/client` is a plain `*http.Client` wrapper with no magic: `Publish`, `Fetch`, `Ack`, `Nak`,
`Term`, `Progress`, plus one convenience `Consume(ctx, stream, consumer, func(context.Context, Msg) error)`
that implements the fetch/heartbeat/ack loop. It takes an `*http.Client` so callers control
timeouts and transports. It graduates from `internal/` to `client/` at v1.0 (§9, M9) — until then we
are free to change it, and users can talk HTTP, which is stable from M5.

---

## 7. Observability & logging design

### 7.1 The structural commitment

`applyTransition` is the only function that mutates message state, and it is the only function that
logs message events:

```go
// internal/broker/transition.go
func (s *stream) applyTransition(t transition) {
	s.mutate(t)                       // in-memory
	s.enqueue(t.mutations()...)       // durable
	s.metrics(t)                      // counters
	s.log(t)                          // slog — same call site, always
}
```

You cannot change a message's state without emitting its event, because there is no other code path.
This is enforced by `TestEveryTransitionLogs`, which enumerates the `transitionKind` values and
asserts each produces exactly one record with a distinct `event` attribute.

### 7.2 Handlers

`log/slog` only. Two handlers:

- `--log-format=json` → stdlib `slog.NewJSONHandler`. Ships to Loki/ELK/Vector unchanged.
- `--log-format=text` (default when stderr is a TTY) → `internal/logfmt.Handler`, a ~180-line
  implementation of the four-method `slog.Handler` interface (`Enabled`, `Handle`, `WithAttrs`,
  `WithGroup`), observing the documented rules (resolve values, skip zero `Attr`s, inline
  empty-keyed groups). It renders the message-event vocabulary as aligned, colourised lines:

```
14:02:01.101 PUBLISH  orders/1284             orders.eu.created  1.2KiB  trace=7f3a91c2
14:02:01.203 DELIVER  orders/1284 → workers   attempt=1/3  deadline=+30s  trace=7f3a91c2
14:02:31.444 TIMEOUT  orders/1284 → workers   attempt=1/3  waited=30.2s   trace=7f3a91c2
14:02:31.512 DELIVER  orders/1284 → workers   attempt=2/3  deadline=+30s  trace=7f3a91c2
14:02:33.001 NAK      orders/1284 → workers   attempt=2/3  retry_in=5s  reason="db timeout"
14:05:12.884 DLQ      orders/1284 → orders.DLQ/17   attempts=3/3  reason=max_deliver
```

Domain types implement `slog.LogValuer` so a `Message` or `pendingRecord` always logs with the same
field names, and expensive rendering (hex-encoding trace ids, formatting deadlines) is deferred until
the record is actually enabled.

### 7.3 Event taxonomy

Every record carries `event` (stable), plus `node_id` and `request_id` where applicable. Message
events additionally always carry: `stream`, `subject`, `seq`, `msg_id`, `trace_id`, `consumer`,
`attempt`, `max_deliver`.

```
msg.publish  msg.deliver  msg.ack  msg.nak  msg.term  msg.progress
msg.timeout  msg.dlq      msg.park msg.expire  msg.stale_ack
consumer.create  consumer.delete  consumer.seek  consumer.pause  consumer.lag
stream.create  stream.delete  stream.purge  stream.full  stream.faulted  retention.blocked
store.recover  store.commit  store.slow_commit  store.error  store.compact  store.backup
server.start  server.ready  server.shutdown  api.request  api.error
```

Levels are chosen so that **INFO is readable at production volume**:

| Level | Events | Rationale |
|---|---|---|
| DEBUG | `publish`, `deliver`, `ack`, `progress`, `expire`, `api.request`, `store.commit` | the happy path is a firehose |
| INFO | `nak`, `timeout`, `seek`, `purge`, lifecycle, `consumer.lag` (30 s, only when backlog > 0) | things a human cares about |
| WARN | `dlq`, `park`, `term`, `stale_ack`, `stream.full`, flow-control saturation, `slow_commit`, `retention.blocked` | something is wrong |
| ERROR | `store.error`, `stream.faulted` | wake someone |

### 7.4 Targeted verbosity — the feature that makes the firehose usable

Three mechanisms, all cheap:

1. **`--trace-subjects 'orders.eu.>'`** — force DEBUG for messages whose subject matches, regardless
   of global level. Debug one tenant in production without debugging everything.
2. **`--trace-msg orders/1284`, or `messq trace --follow orders/1284`** — force DEBUG for one message
   for the rest of its life.
3. **Per-message sampling: `--log-sample 1/100`** — DEBUG happy-path events are sampled by
   `fnv32(trace_id) % N == 0`, i.e. **by message, not by event**. A sampled message has its *entire*
   lifecycle logged; an unsampled one has none. Sampling per-event, which is what most systems do,
   produces exactly the useless artefact of half a story. This detail is the difference between a log
   you can reason from and a log you can only count.

`X-Messq-Debug: 1` on any request forces full logging for that request's whole path, which makes
support requests reproducible on demand.

### 7.5 `messq trace`

`messq trace` is a log *reader*, not a second storage system. It consumes the JSON log (a file, a
glob of rotated files, or stdin from `journalctl -u messq -o cat`), filters on `msg_id` or
`trace_id`, and renders the timeline:

```
$ journalctl -u messq -o cat | messq trace orders/1284

orders/1284   subject=orders.eu.created   trace=7f3a91c2b4e5d6a7   1.2 KiB   tenant=acme

  14:02:01.101  publish                                    from 10.0.0.7  durable in 0.9ms
  14:02:01.203  deliver   → workers    attempt 1/3         deadline 14:02:31.203
  14:02:31.444  timeout   → workers    attempt 1/3         waited 30.24s
  14:02:31.512  deliver   → workers    attempt 2/3         deadline 14:03:01.512
  14:02:33.001  nak       → workers    attempt 2/3         reason "db timeout"  retry in 5s
  14:02:38.010  deliver   → workers    attempt 3/3         deadline 14:03:08.010
  14:03:08.221  timeout   → workers    attempt 3/3         waited 30.21s
  14:03:08.223  dlq       → orders.DLQ/17                  reason max_deliver
  14:41:02.110  replay    → orders/1602                    by johan (replay_count 1)

  lifetime 38m 61s · 3 attempts · 1 consumer · ended: dead-lettered, then replayed
```

Zero extra storage, zero extra write path, and it works on logs shipped to a laptop. A durable event
journal (for `messq export`, audit compliance) arrives at M8 as a *separate*, opt-in bucket — not as
a prerequisite for tracing.

### 7.6 Metrics

`GET /metrics` in Prometheus text exposition format, served by `internal/metric` (~200 lines:
`Counter`, `Gauge`, `Histogram` with fixed buckets, pre-created label children, a `Registry` that
writes `# HELP`/`# TYPE`/samples with correct escaping).

**Why not `prometheus/client_golang`.** Its docs describe exactly what we would be buying: registries,
`Collector`/`Desc` machinery, `promauto`, `promhttp.HandlerFor` with `HandlerOpts`, summaries with
objectives, native histograms — plus a module tree including `google.golang.org/protobuf`,
`prometheus/common`, `prometheus/procfs` and `golang.org/x/sys`. We export **17 metrics** with fixed
label sets. Text exposition is a stable, trivial format. Trading a 4× increase in our dependency
budget for machinery we would use 5 % of contradicts the project's central premise. Documented
limitations: no exemplars, no OpenMetrics, no native histograms, no Go runtime collector beyond the
five values we export by hand from `runtime` and `runtime/metrics`. If users demand exemplars, we
revisit at v1.0 — as a single decision, recorded in an ADR, not as a build tag that forks the code.

```
messq_build_info{version,commit,go_version}
messq_messages_published_total{stream}
messq_messages_delivered_total{stream,consumer}
messq_messages_acked_total{stream,consumer}
messq_messages_naked_total{stream,consumer}
messq_messages_termed_total{stream,consumer}
messq_ack_timeouts_total{stream,consumer}
messq_stale_acks_total{stream,consumer}
messq_dead_lettered_total{stream,consumer}
messq_messages_expired_total{stream}
messq_consumer_inflight{stream,consumer}          gauge
messq_consumer_backlog{stream,consumer}           gauge   last_seq - delivered_seq
messq_consumer_ack_floor_lag{stream,consumer}     gauge   last_seq - ack_floor  ← the real lag
messq_consumer_oldest_pending_seconds{stream,consumer}
messq_stream_messages{stream} / messq_stream_bytes{stream}
messq_delivery_latency_seconds{stream,consumer}   histogram  publish → first deliver
messq_processing_seconds{stream,consumer}         histogram  deliver → ack
messq_commit_duration_seconds                     histogram  the fsync truth
messq_commit_batch_size                           histogram
messq_store_errors_total / messq_stream_faulted{stream}
```

`ack_floor_lag` is deliberately promoted over `backlog`: a consumer whose `delivered_seq` races
ahead while one message sticks at the floor looks perfectly healthy on backlog alone. Ack-floor lag
is what actually tells you whether work is *completing*.

### 7.7 Tracing interop without an SDK

We accept and emit W3C `traceparent`, and expose `trace_id` on every event and in `--exec`'s
environment. No OpenTelemetry SDK dependency (it is larger than messq). Users who want spans run a
collector that reads our logs, or correlate by `trace_id` — which works, because our trace ids are
16-byte hex and W3C-compatible by construction.

---

## 8. Testing strategy

### 8.1 No storage interface, on purpose

There is no `type Store interface` and no mock. Tests open a real bbolt file in `t.TempDir()` —
open+close is around a millisecond, and every test exercises the encoding, the transaction boundary
and the recovery path for free. Mock-driven design would let the schema and the state machine drift
apart, and would add an abstraction whose only consumer is the test suite. This is a deliberate
stance and it is written in `docs/testing.md` so nobody "helpfully" adds the interface later.

### 8.2 Layers

**1. Unit (table-driven).** `internal/subject` matching, `internal/id` token parse/format,
record codec round-trips, ack-floor sweep, metric rendering. Fast, hermetic, boring.

**2. Time (`testing/synctest`).** Every deadline behaviour is tested with virtualised time — Go 1.25
graduated `testing/synctest` from experiment to stable, and it is the reason messq can afford
detailed timing tests. Inside a bubble the clock starts at 2000-01-01 and advances instantly once all
goroutines are durably blocked:

```go
func TestAckTimeoutRedelivers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newTestBroker(t, consumerCfg{AckWait: 30 * time.Second, MaxDeliver: 3})
		b.publish("orders.eu.created", []byte("x"))

		m := b.fetchOne(t)
		if m.Attempt != 1 { t.Fatalf("attempt = %d, want 1", m.Attempt) }

		time.Sleep(29 * time.Second)
		synctest.Wait()
		if got := b.fetchN(t, 1); len(got) != 0 {
			t.Fatal("redelivered before ack_wait elapsed")
		}

		time.Sleep(2 * time.Second)
		synctest.Wait()
		m2 := b.fetchOne(t)
		if m2.Seq != m.Seq || m2.Attempt != 2 {
			t.Fatalf("got %v/%d, want %v/2", m2.Seq, m2.Attempt, m.Seq)
		}
	})
}
```

Covered this way: ack-wait, backoff sequences, nak delay, `progress` heartbeats, long-poll
`max_wait`, retention sweeps, janitor cadence, shutdown deadlines. Total runtime: milliseconds.

**3. Model-based property test.** `internal/broker/model_test.go` holds a ~250-line reference
implementation of §4.2 using plain maps, with no persistence and no concurrency. A generator
(`math/rand/v2`, seeded from `-seed` and printed on failure) produces random operation sequences —
publish, fetch, ack, nak, term, timeout-advance, seek, purge, restart — applies each to both the real
broker and the model, and after **every** operation asserts observable equality plus invariants
I1–I9. This is where the real confidence comes from: it finds the out-of-order-ack floor-sweep bugs
and the ordered-by-subject starvation bugs that hand-written tests miss.

**4. Crash tests.** `crashtest` spawns `messq serve` as a subprocess against `t.TempDir()`, drives
concurrent publishers and consumers, and `SIGKILL`s it at a chosen point. Crash points are injected
by an env var read once at startup (`MESSQ_CRASH_AFTER_COMMITS=n`, `MESSQ_CRASH_BEFORE_FSYNC=1` —
five lines in the committer, no build tags). After restart we assert:
  - every publish that received `201` is present with identical bytes and CRC;
  - `seq` has no gaps and `state` matches the `msgs` bucket;
  - attempt counters are ≥ their pre-crash values (never reset) — this is G6;
  - no message below `ack_floor` reappears as pending;
  - `messq verify` passes.
  The crash matrix runs at every commit point across a 60-second workload, ~40 scenarios, in CI.

**5. HTTP contract tests.** `httptest.NewServer` + golden files under `testdata/http/` covering every
endpoint's success and error shapes. A test asserts every sentinel error has a status mapping, and
another asserts every documented endpoint in `docs/protocol.md` is registered on the mux (the docs
cannot silently rot).

**6. CLI tests.** In-process, via `cli.Main(ctx, env, argv)` with buffers for stdin/stdout/stderr.
No subprocess, no `os.Args`, no global state — which is why `Env` exists. Golden output for human
tables and `--json`.

**7. Fuzz (stdlib).** `FuzzDecodeMsgRecord` (never panics, round-trips or errors cleanly),
`FuzzSubjectMatch` (never panics; `*`/`>` semantics hold), `FuzzParseAckToken`,
`FuzzPublishHeaders`. Seed corpora checked in; CI runs 60 s each, nightly runs 30 min.

**8. Race and static analysis.** All tests under `-race` in CI. `go vet`, `staticcheck`,
`govulncheck` (tool dependencies, not module dependencies — they never enter `go.mod`).

**9. Benchmarks.** `BenchmarkPublish1KiB`, `BenchmarkFetchAckBatch100`, `BenchmarkCommitGroup`,
`BenchmarkRecoverPending100k`. `benchstat` gates merges: a >10 % regression on the publish path
fails the build.

**10. Soak (`make soak`, nightly).** One hour: 3 producers at 2 kHz, 5 consumer processes, 10 % nak
rate, 2 % deliberate ack-timeouts, a `SIGKILL`+restart every 5 minutes, retention active. Final
assertion is **conservation**: `published == acked + termed + dead_lettered + expired + still_pending`,
plus flat memory and a bounded database file.

**11. Documentation tests.** `TestREADMECommandsExist` extracts every `messq …` invocation from
`README.md` and `docs/*.md` and asserts the subcommand and flags exist. Docs that lie fail CI.

### 8.3 Coverage and CI gates

| Gate | Threshold |
|---|---|
| Coverage `internal/broker`, `internal/store` | ≥ 85 % |
| Coverage overall | ≥ 75 % |
| `scripts/deps.sh` — non-stdlib modules | ≤ 2 |
| `scripts/loc.sh` — non-test Go lines | ≤ 6 000 |
| `scripts/loc.sh` — `internal/broker/stream.go` | ≤ 500 |
| `scripts/layers.sh` — import direction | must hold |
| `go vet`, `staticcheck`, `govulncheck`, `-race` | clean |

---

## 9. Roadmap: from empty repository to the ideal product

Estimates assume one focused engineer. Each milestone ends with something runnable and demoable.

### M0 — Skeleton and contracts · ~3 days
`go.mod` (`go 1.26`), `cmd/messq/main.go` with the `run()` pattern (25 lines, everything testable),
`internal/cli` dispatcher, `internal/messq` types and sentinels, slog wiring with both handlers,
`--check-config`, Makefile (`build`, `test`, `race`, `lint`, `soak`, `cover`), CI workflow with all
gates from §8.3 active **from the first commit**, `docs/` skeletons, `messq serve` answering
`/v1/health` and `/v1/info`.
**Done when:** `make ci` is green and `messq serve` + `messq info` work over the Unix socket.

### M1 — Storage and the write path · ~4 days
bbolt schema, record codec + CRC, `internal/store` with copy-out discipline, the committer with
group commit and `--sync` modes, stream create/list/info/delete, publish (single + batch),
`durableSeq` gating, `messq pub`, `messq peek`, `messq inspect`, `messq verify`, recovery of stream
state, first crash test.
**Done when:** you can publish a million messages, `kill -9`, restart, and `messq verify` passes.

### M2 — Consumers and the state machine · ~7 days · **this is the MVP**
Consumer CRUD, the actor's timer wheel and deadline heap, `fetch` with long-poll and flow control,
`ack`/`nak`/`term`/`progress`, ack fencing, `pending`/`finished` buckets, ack-floor sweep,
`max_deliver`, DLQ streams, `on_exhaust` policies, `messq sub` including `--exec`. The full
`testing/synctest` suite and the model-based property test land here, alongside the code.
**Done when:** the MVP list from the brief — publish, consume, ack/nak, ack timeout, redelivery,
max deliveries, DLQ — is implemented and property-tested.

### M3 — Operations surface · ~5 days
`seek` (start/end/seq/time) with epoch bump, purge, the retention janitor with `retention.blocked`
protection, `messq lag`, `messq pending`, `messq dlq` with the replay guard rails, `backup`,
`compact`, pause/resume, `messq bench`.
**Done when:** an operator can answer "what is stuck, why, and how do I safely retry it" using only
the CLI.

### M4 — Observability · ~5 days
`internal/logfmt` handler, the complete event taxonomy with `TestEveryTransitionLogs`, `LogValuer`
implementations, per-message sampling, `--trace-subjects` / `--trace-msg` / `X-Messq-Debug`,
`messq trace`, `internal/metric` and `/metrics`, `docs/operations.md` documenting every log event
and what to do about it.
**Done when:** `messq trace` reconstructs a full lifecycle from shipped logs, and a Grafana panel
built from `/metrics` shows backlog, ack-floor lag and DLQ growth rate.

### M5 — Hardening and v0.1.0 · ~7 days
Crash matrix, soak in nightly CI, fuzzers, auth/TLS/socket permissions, connection limits, graceful
shutdown ordering with deadlines, systemd unit + `logrotate` snippet, `docs/semantics.md` (normative,
with the state machine and invariants), `docs/protocol.md`, `docs/storage-format.md`, man page,
reproducible `-trimpath` static builds for linux/amd64 and linux/arm64, published benchmark numbers
with hardware stated.
**Done when:** **v0.1.0 is tagged and is honestly deployable for internal production workflows.**

### M6 — Phase 2a: ordering, delay, rate control · ~5 days
`ordered_by_subject` (in-flight subject set, starvation-safe), delayed delivery via
`Messq-Deliver-At` (reuses the visibility heap — nearly free), per-consumer rate limiting (token
bucket inside the actor, `429` + `Retry-After`), priority as ordered `filter_subjects` drained
high-to-low on fetch (no new storage, no new state).

### M7 — Phase 2b: worker groups · ~4 days
Multiple workers on one consumer already work correctly because acks are fenced; this milestone adds
the observability and safety around it: `worker_id` on fetch, per-worker in-flight caps, `messq
workers` listing with last-seen and in-flight, per-worker metrics labels, and lease semantics
documented as "the ack deadline *is* the lease".

### M8 — Phase 2c: retention policies, compression, audit export, client package · ~7 days
Full retention matrix, `compress: none|gzip` using stdlib `compress/gzip` above a size threshold
(flag bit already reserved in the record header), producer dedupe via `Messq-Msg-Id` with a TTL'd
`dedupe` bucket, an opt-in durable event journal + `messq export --since --format ndjson` for audit,
and the Go client's API frozen in preparation for export.

### M9 — v1.0 · ~5 days
API freeze, on-disk `format_version: 1` freeze plus a migration tool skeleton, a compatibility corpus
(`testdata/compat/v1.db` opened and validated by every future build), `internal/client` → `client/`,
optional binary content type on the fetch path, semantic-versioning policy, and an ADR log recording
every decision in this document that anyone might later want to reverse.

### Explicitly deferred beyond v1.0
Clustering and replication. The architectural seam already exists — the committer is the single
point where every durable mutation passes, so a replicator would sit between the actors and the
committer — and the record header reserves an `epoch` byte. **We do not build any of it now**, and
we do not add abstractions "in preparation". Speculative generality is the fastest way to lose the
evening.

---

## 10. Risks & open questions

**R1 — bbolt's write throughput ceiling.** Copy-on-write B+trees have real write amplification, and
every commit is an fsync. *Mitigation:* group commit across all streams; measured gate at M5
(≥ 10 000 1 KiB msg/s publish+ack on NVMe). *If we miss it:* the seam is `internal/store`, and the
fallback is segment files for payloads with bbolt as the index — but we will not build both
speculatively, and we will not chase the number past the point where the codebase stops fitting in
an evening. The positioning explicitly says "not a Kafka replacement at scale".

**R2 — Database file growth from long read transactions.** bbolt cannot reclaim pages while an old
read transaction is open. *Mitigation:* rule 3 in §2.3 (never hold a transaction across a wait),
enforced by review and by a `messq stats` warning when `db.Stats().OpenTxN` stays above zero for more
than 5 s; `messq backup` is the one long reader and it warns about itself.

**R3 — The whole database has one write lock.** One enormous stream can delay another's commits.
*Mitigation:* the committer merges rather than serialises, and caps a batch at 8 MiB;
`messq_commit_duration_seconds` makes the symptom visible before it becomes an incident.

**R4 — Duplicates are guaranteed, not hypothetical.** Every at-least-once system produces them, and
NSQ's and Redis Streams' operational literature is unanimous that consumers must be idempotent.
*Mitigation:* say so on the first screen of the README, expose `attempt` in every envelope and
`--exec` environment so workers can detect retries, and ship producer-side dedupe (M8) without ever
claiming exactly-once.

**R5 — Crash-loops burn delivery attempts.** Attempts increment on recovery redelivery and on
deliveries whose response never reached the client. This is honest but can push a message to the DLQ
faster than intended during an outage. *Mitigation:* recovery redeliveries are logged with a distinct
reason, `max_deliver` defaults to 5 rather than 3, `on_exhaust=park` keeps messages inspectable
instead of moving them, and `messq redeliver --reset-attempts` exists for the operator. *Rejected
alternative:* a separate "recovery attempts" counter excluded from `max_deliver` — it adds a durable
field and a subtle rule to the one state machine everybody must understand.

**R6 — Base64 and JSON overhead on the read path.** ~33 % bandwidth plus parsing cost.
*Mitigation:* batch fetches amortise it, raw payload endpoint exists, binary content type is
scheduled for M9. *Accepted*, because `curl`-ability is a stated product feature.

**R7 — Unbounded `finished`-above-floor growth.** One stuck message at `ack_floor+1` while a consumer
keeps acking later messages makes `finished` grow. It is bounded in practice by `max_deliver ×
ack_wait` (the stuck message eventually dead-letters and the floor sweeps), but a consumer with
`max_deliver: 0` (unlimited) and a permanently poisoned message would grow it forever.
*Mitigation:* an explicit cap `--max-finished-above-floor` (default 100 000); on breach, fetches
return `429`, a WARN names the blocking sequence, and `messq pending` shows it at the top. `max_deliver: 0`
prints a warning at consumer creation.

**R8 — Long-poll connection count.** Each parked fetch costs a goroutine and a waiter registration.
*Mitigation:* `--max-conns`, `IdleTimeout`, and a documented recommendation of `max_wait ≤ 30s`.
At 10 000 idle long-polls this is ~100 MB of stacks — acceptable, and measured in the soak test.

**R9 — Single node, no replication.** The machine dying loses everything since the last backup.
*Mitigation:* say it plainly in the positioning; ship `messq backup` and a documented cron/systemd
timer recipe; recommend a filesystem or block-level replica for anyone who needs more. Do **not**
half-build clustering.

**R10 — Security model is coarse.** One shared token, no per-stream ACLs, no audit of who did what.
*Mitigation for v0.x:* Unix socket + filesystem permissions is the recommended deployment; TCP
requires a token; `api.request` logs carry `request_id` and peer address. *Open question below.*

### Open questions (to be resolved by the milestone named)

1. **Per-stream ACLs and multi-token auth** — needed for shared internal brokers. Decide by M5; my
   inclination is a `tokens` bucket with `{name, hash, streams[], perms[]}` and `Authorization:
   Bearer`, which is ~150 lines. *(Owner decision required.)*
2. **Should `messq sub` support push-style server-sent delivery for very low latency?** Long-poll
   with `max_wait=30s` already gives sub-millisecond wake-up on publish. Current answer: **no**;
   revisit only with a measured requirement.
3. **Subject wildcard syntax** — committed to NATS semantics (`.` separator, `*` = one token, `>` =
   one-or-more trailing tokens) for familiarity. Open only in the sense that we should confirm no
   one wants `#`/AMQP style before M2 freezes it.
4. **Does the durable event journal (M8) belong in the same bbolt file?** It doubles write volume on
   the hot path. Leaning towards a separate append-only file with its own retention, since it needs
   no transactional relationship with message state — the one place where a second storage mechanism
   might genuinely pay for itself. Decide at M8 with measurements.
5. **Windows/macOS support.** Not a target. bbolt and the code are portable; the socket defaults and
   systemd integration are not. Answer for v1.0: Linux only, `GOOS=darwin` builds may work and are
   untested.
6. **`max_bytes` per fetch vs. per message accounting** when a single message exceeds `max_bytes` —
   current answer: always return at least one message, even if oversized, so a large message can
   never wedge a consumer. Confirm at M2.

---

## 11. Library choices, justified against the docs I fetched

**Budget: two non-stdlib modules total.** `go.etcd.io/bbolt` and its own dependency
`golang.org/x/sys`. `scripts/deps.sh` fails the build at three.

### 11.1 Accepted

**`go.etcd.io/bbolt` (v1.5.x) — the only direct dependency.**
The docs I read establish exactly what we need and what to avoid. `DB.Batch` exists and coalesces
concurrent read-write transactions, configured by `MaxBatchSize`/`MaxBatchDelay` — but its contract
is *"the provided function must be idempotent as it may be called multiple times"*, so we implement
our own 80-line committer instead of putting that footgun on the correctness-critical path (§3.4).
`Options.NoSync` is documented as a bulk-loading switch, which is why our only non-durable mode is
named `none` and is gated behind `--i-accept-data-loss` rather than dressed up as a tuning knob.
`Bucket.FillPercent = 0.9` is the documented optimisation for append-only, monotonically increasing
keys, which is precisely our `msgs` bucket. `Cursor.Seek(min)` with a bounded scan is exactly our
"deliver from `delivered_seq+1`" loop. `Tx.WriteTo` inside a `View` gives us `messq backup` for free.
And the README's caveats section is the source of two hard rules in this plan: long-running read
transactions prevent page reclamation, and returned byte slices are invalid outside the transaction —
hence `internal/store` returns owned copies only. It is pure Go (so `CGO_ENABLED=0`), transactional
(so the DLQ move is atomic), needs no recovery replay code (so we ship none), and is exercised
continuously as etcd's storage engine.

**Go 1.26 standard library**, load-bearing packages, all verified against current docs:
`net/http` (`ServeMux` method+wildcard patterns with `r.PathValue`, `NewResponseController` for
`Flush`/`SetWriteDeadline`/`EnableFullDuplex` on the long-poll path, `Server.Shutdown` returning
`ErrServerClosed`) · `log/slog` (the four-method `Handler` interface, `LogValuer` for deferred
expensive rendering, `WithGroup`/`WithAttrs` semantics) · `testing/synctest` (stable since Go 1.25;
`Test` bubbles with a fake clock starting at 2000-01-01 that advances when all goroutines are durably
blocked, and `Wait` for quiescence) · `encoding/json` for config and wire types · `flag` for the CLI ·
`text/tabwriter` for human output · `hash/crc32` (Castagnoli) for record checksums · `crypto/rand`,
`crypto/tls`, `crypto/subtle` for ids, TLS and constant-time token comparison · `os/signal.NotifyContext`
for shutdown · `os/exec` for `--exec` workers · `compress/gzip` for M8 · `net/http/pprof` behind a
flag · `math/rand/v2` for test generators · `container/heap` for the deadline queue.

### 11.2 Rejected, with the evidence

**`spf13/cobra`.** The current docs push `cobra-cli` scaffolding and a `viper`-backed configuration
pattern — the canonical example wires `viper.BindPFlag`, config-file search paths and
`cobra.OnInitialize` before a single command runs. That is three modules plus a generated `cmd/`
package layout for a CLI whose whole dispatch logic is a map lookup and a `flag.FlagSet`. We also
want completion scripts we wrote and can read, not generated ones. Cost of rejection: ~90 lines and
hand-maintained completions. Worth it.

**`grpc/grpc-go`.** The docs make the cost concrete: a `protoc` + `protoc-gen-go` +
`protoc-gen-go-grpc` build step, generated servers that must embed `Unimplemented*Server`, and a
dependency tree several times the size of messq. Its main technical draw — HTTP/2 flow control, which
the official example demonstrates by timing how long `Send()` blocks — solves a push-model problem we
do not have, because our consumers pull with explicit credits. Rejected on dependency budget, build
complexity, and loss of `curl`-ability.

**`modernc.org/sqlite`.** Genuinely close. Pure Go, no cgo, Go 1.18+, currently tracking SQLite
3.53.x, with a clean DSN pragma story (`_journal=WAL`, `_synchronous=NORMAL`, `_txlock=immediate`).
Rejected because: it is a mechanical translation of a very large C codebase, so "read the storage
engine" stops being possible; `database/sql` plus SQL strings is a heavy interface for `get(u64)` and
`scan(range)`; the `_synchronous=NORMAL` default that makes SQLite fast is precisely the setting that
makes it non-durable per commit (well documented, frequently discovered the hard way), so a durable
messq would run `FULL` and pay bbolt's fsync cost anyway; and WAL mode is still single-writer, so the
concurrency model would be identical. The SQL-introspection upside is better delivered by
`messq peek`/`inspect`/`trace` than by an operator with a `sqlite3` shell on live queue state.

**`mattn/go-sqlite3`.** cgo. Fails the static-binary and cross-compilation requirement immediately.

**`prometheus/client_golang`.** Docs confirm the shape of what we would import: registries,
`Collector`/`Desc`, `promauto`, `promhttp.HandlerFor` with `HandlerOpts`, histograms with
`ExponentialBuckets`, summaries with objectives — backed by `protobuf`, `prometheus/common`,
`procfs` and `x/sys`. We export 17 metrics with static label sets into a stable text format.
~200 hand-written lines beats quadrupling the dependency budget. Documented limitations and a
single, recorded revisit point at v1.0.

**OpenTelemetry Go SDK.** Larger than the product. We emit W3C-compatible trace ids and correlate in
logs; anyone who needs spans can bridge from the JSON log.

**Any logging library (`zap`, `zerolog`, `logrus`).** `log/slog` is the standard, is fast enough for
our volume, and its `Handler` interface is small enough that our human-readable format is 180 lines.
Using anything else would also force a logging dependency on library consumers, which the slog design
explicitly avoids.

**Any router (`chi`, `gorilla/mux`, `httprouter`).** Since Go 1.22, `ServeMux` matches methods, hosts
and wildcards, and `r.PathValue` extracts parameters. Nothing left to buy.

**`uuid` libraries.** `crypto/rand` + `hex` is four lines and our ids are `stream/seq` anyway.

**A test framework (`testify`, `gomega`).** Table tests plus `t.Fatalf` with a "got X, want Y"
convention. `-race`, `synctest`, fuzzing and the model test carry the real weight.

### 11.3 Tools (never in `go.mod`)

`staticcheck`, `govulncheck`, `benchstat` — installed in CI via `go run <tool>@<version>` so they
never become module dependencies of the shipped binary.

---

## Appendix A — Line budget (design target, enforced at 6 000 total)

| Package | Target LOC | Why it is that size |
|---|---|---|
| `internal/messq` | 250 | types, config structs, sentinels — mostly declarations |
| `internal/subject` | 120 | tokenise + `*`/`>` match |
| `internal/id` | 120 | message ids, trace ids, ack tokens |
| `internal/store` | 900 | bbolt schema, codec, committer, recovery |
| `internal/broker` | 1 400 | actor (≤500), transitions, consumers, retention, waiters |
| `internal/wire` | 250 | JSON request/response types |
| `internal/api` | 700 | handlers, error mapping, auth, NDJSON streaming |
| `internal/client` | 350 | HTTP client + `Consume` helper |
| `internal/cli` | 1 100 | dispatcher + ~25 subcommands + output formatting |
| `internal/logfmt` | 180 | human `slog.Handler` |
| `internal/metric` | 200 | counters/gauges/histograms + text exposition |
| `cmd/messq` | 30 | `run()` pattern |
| **Total** | **~5 600** | headroom of 400 against the 6 000 cap |

## Appendix B — Reading order (the evening)

1. `README.md` — what it is, what it guarantees, what it refuses to do (10 min)
2. `docs/semantics.md` — §4 of this plan, normative (20 min)
3. `internal/messq/types.go` — the whole vocabulary in one file (15 min)
4. `internal/broker/stream.go` — the actor loop and the state machine, ≤500 lines (60 min)
5. `internal/store/schema.go` + `committer.go` — the durability story (45 min)
6. `internal/api/handlers.go` — the thin translation layer (30 min)

Three hours, and you can review a pull request against any part of it.

---

## Sources consulted

- [NATS Docs — Consumers](https://docs.nats.io/nats-concepts/jetstream/consumers) · [Consumer Details](https://docs.nats.io/using-nats/developer/develop_jetstream/consumers) · [JetStream Model Deep Dive](https://docs.nats.io/using-nats/developer/develop_jetstream/model_deep_dive)
- [NSQ Design](https://nsq.io/overview/design.html) · [nsqio/nsq](https://github.com/nsqio/nsq) · [NSQ for Sensu Go — operational report](https://c-kruse.com/posts/nsqforsensugo/)
- [Migrating Channels webhooks from Beanstalkd — Pusher](https://pusher.com/blog/migrating-channels-webhooks-from-beanstalkd-to-kinesis-sqs/) · [beanstalkd(1)](https://manpages.debian.org/testing/beanstalkd/beanstalkd.1.en.html)
- [Redis Streams — durable message queues](https://redis.io/docs/latest/develop/use-cases/job-queue/) · [At-least-once with Redis Streams](https://oneuptime.com/blog/post/2026-03-31-redis-at-least-once-processing-streams/view)
- [Dead-Letter Queues & Poison-Message Handling](https://www.task-queues.com/queue-fundamentals-architecture/dead-letter-queues-poison-messages/) · [RabbitMQ DLQ design and monitoring](https://qarote.io/blog/rabbitmq-dead-letter-queue/) · [Avoiding requeue/redelivery loops](https://groups.google.com/g/rabbitmq-users/c/qxWfKH1JIO4)
- [SQLite commits are not durable under default settings](https://avi.im/blag/2025/sqlite-fsync/) · [SQLite's durability settings are a mess](https://www.agwa.name/blog/post/sqlite_durability) · [Building a queue based on SQLite — SQLite forum](https://sqlite.org/forum/info/b047f5ef5b76edff)
- [hashicorp/raft-wal — torn-write detection and commit frames](https://github.com/hashicorp/raft-wal/blob/main/README.md) · [fsync, group commit & durability modes](https://shipthatcode.com/courses/build-lsm-tree/lessons/fsync-durability)
- [etcd-io/bbolt](https://github.com/etcd-io/bbolt) · [bbolt v1.5.0 release](https://github.com/etcd-io/bbolt/releases/tag/v1.5.0) · bbolt docs via context7 (`/etcd-io/bbolt`)
- [Structured Logging with slog — go.dev](https://go.dev/blog/slog) · `log/slog`, `net/http`, `testing/synctest` docs via context7 (`/websites/pkg_go_dev_go1_25_3`)
- [Go 1.25 release notes — testing/synctest](https://go.dev/doc/go1.25) · [Go 1.25 is released](https://go.dev/blog/go1.25)
- cobra docs via context7 (`/spf13/cobra`) · grpc-go docs via context7 (`/grpc/grpc-go`) · modernc SQLite docs via context7 (`/gitlab_cznic/sqlite`) · prometheus/client_golang docs via context7 (`/prometheus/client_golang`)
- [Kafka data observability best practices](https://factorhouse.io/articles/best-practices-kafka-data-observability/) · [How to monitor Kafka consumer lag](https://factorhouse.io/articles/how-to-monitor-kafka-consumer-lag/)
- [No-nonsense guide to Go project layout](https://laurentsv.com/blog/2024/10/19/no-nonsense-go-package-layout.html) · [How to structure Go projects](https://daveamit.com/posts/2026-02-13-go-folder-structure/)
