# messq — Project Plan

**Author lens:** distributed-systems correctness purist.
**Scope:** empty repository → production-usable single-node broker.
**Target:** Linux, Go 1.25+, one static binary, no cgo.

---

## 1. Vision & positioning

### 1.1 The thesis

Most small teams do not need Kafka. They need a broker whose **promises are written down precisely enough to be tested**, and whose **failure behaviour is predictable when the machine loses power in the middle of a publish**.

The market gap is not throughput. It is *epistemic*: operators of small brokers usually cannot answer

- "Was this message durable when the publisher got its ack?"
- "This message was processed twice — was that the broker or my code?"
- "The box lost power at 03:12. Exactly which messages could have been lost, and which could have been duplicated?"

messq's differentiator is that all three questions have a **short, provable answer**, and the binary itself will tell you (`messq trace`, `messq verify`).

### 1.2 The contract, stated up front

messq promises exactly this, and nothing more:

> **P1 — No acknowledged loss.** If a publish returned `PubAck{durable: true}`, the message is present after any crash of the messq process or the machine, provided the storage device does not lie about `fdatasync`.
>
> **P2 — At-least-once delivery.** Every non-retired message matching a consumer's filter is delivered to that consumer at least once, until it is acked, terminated, dead-lettered, purged, or expired by retention.
>
> **P3 — Retirement is terminal.** Once a `(consumer, seq)` pair is retired, messq will never redeliver it — except through an explicit, loudly logged operator action (`seek`, `dlq redeliver`, `replay`).
>
> **P4 — Bounded poison.** A message is dispatched at most `max_deliver` times per consumer *epoch*, as counted by durably recorded failures.
>
> **P5 — Deterministic recovery.** Post-restart state is a pure function of the durable byte prefix of the journal. `messq verify --state-hash` prints a hash of that state; two recoveries of identical bytes print identical hashes.

And, equally important, what messq **does not** promise:

- **Not exactly-once.** Duplicates are inevitable (Two Generals). messq narrows the duplicate window and gives you a *stable dedup key*; it does not eliminate duplicates.
- **Not durable across disk loss.** Single node, no replication. Losing the filesystem loses the data. Back up the data directory or accept it.
- **`max_deliver` bounds durable failures, not physical deliveries.** A crash during in-flight processing can produce one extra delivery beyond the counter (§4.7). This is documented, logged (`lease.orphaned`), and counted.
- **No cross-stream atomicity** other than the dead-letter forwarding protocol in §4.9, which is at-least-once with a dedup key.

Everything in this plan is downstream of those five promises. Where a design choice would make a promise weaker but the system faster, the promise wins, and where a promise is weakened deliberately the plan says so out loud and makes the weakening *visible to the client* (e.g. `PubAck.durable = false`).

### 1.3 Prior art, and what we take from each

| System | What we adopt | What we reject |
|---|---|---|
| NATS JetStream | ack / nak / term / in-progress vocabulary; `AckWait`; `MaxDeliver`; `BackOff` schedule; per-consumer pending set; publish dedup via a message-id header + bounded window | Clustering, RAFT, subject-hierarchy complexity, in-memory dedup maps sized by wall-clock window alone |
| Kafka | Append-only segmented log, sparse per-segment offset index, recovery-point checkpoint file, segment unlink as the retention primitive | Consumer-group rebalancing, partitions-as-a-user-concept, offsets-only (no per-message ack) |
| Redis Streams | The Pending-Entries-List mental model; explicit "claim by idle time" as an *operator* action rather than a hidden mechanism | Unbounded PEL growth (we bound it — see I6), consumer-owned PELs |
| RabbitMQ quorum queues | Two distinct counters (`delivery-count` vs an acquire count) so operator-initiated returns don't burn the poison budget; `x-delivery-count` exposed to the consumer; delivery-limit + DLX as the default-on pattern | Channel-closing on ack timeout; classic-queue semantics |
| NSQ | Single-binary operational simplicity; "clients must be idempotent" honesty | Memory-first storage where disk is an overflow buffer, not a durability mechanism |
| beanstalkd | `reserve` / `release` / `bury` — "bury" is the ancestor of our in-place DLQ | Job-centric protocol without streams or replay |

The key inheritance from RabbitMQ 4.3 is worth spelling out because it is a *correctness* refinement most brokers got wrong first: **a redelivery caused by the operator, or by a consumer politely returning work it cannot do right now, must not count toward the poison budget.** messq therefore keeps `dispatch_count` (observability) and `fail_count` (the budget) as separate durable fields from day one.

---

## 2. Architecture overview

### 2.1 Guiding structural rule

> **All mutable state of a stream is owned by exactly one goroutine.**

There is no lock protecting the message-lifecycle state. State mutations arrive as commands on a channel and are applied sequentially by `StreamCore`. This buys three things a purist cares about:

1. The state machine is **single-threaded and therefore directly comparable to an executable reference model** in tests (§8.3).
2. Lock-ordering bugs are structurally impossible.
3. The order of transitions is the order of log events, so the human-readable log *is* the serialization order.

Concurrency lives at stream granularity and in the I/O and codec layers, not in the semantics.

### 2.2 Process & goroutine map

One process. One data directory, held under `flock` (`messq.lock`) so a second instance cannot corrupt it.

```
                            messq process
 ┌───────────────────────────────────────────────────────────────────────┐
 │  main                                                                 │
 │   ├─ config load  → flock(data/messq.lock)  → Recovery (§3.6)         │
 │   └─ after recovery completes: sd_notify(READY=1), then serve         │
 │                                                                       │
 │  net layer (one goroutine per connection pair, no semantic state)     │
 │   ┌──────────────┐   commands    ┌──────────────────────────────┐     │
 │   │ session.recv │──────────────▶│                              │     │
 │   │ session.send │◀──────────────│      StreamCore["orders"]    │     │
 │   └──────────────┘   deliveries  │      (single goroutine)      │     │
 │        (× N clients)             │                              │     │
 │                                  │  owns: cursor, ack_floor,    │     │
 │  ┌────────────────┐   expiries   │  tracked map, credit, epoch, │     │
 │  │ TimerWheel     │─────────────▶│  dedup index, subject index  │     │
 │  │ (100 ms tick)  │◀─────────────│                              │     │
 │  └────────────────┘   schedule   └───────┬──────────┬───────────┘     │
 │                                          │ append   │ snapshot        │
 │                                          ▼          ▼                 │
 │                              ┌─────────────────┐  ┌─────────────────┐ │
 │                              │ Committer       │  │ Maintainer      │ │
 │                              │ (1 per stream)  │  │ (1 per stream)  │ │
 │                              │ owns the fd     │  │ checkpoint,     │ │
 │                              │ group commit +  │  │ segment roll,   │ │
 │                              │ fdatasync       │  │ retention, idx  │ │
 │                              └────────┬────────┘  └────────┬────────┘ │
 │                                       ▼                    ▼          │
 │                          data/streams/<s>/journal/*.jnl, checkpoints/ │
 │                                                                       │
 │  obs: slog JSONHandler → stderr;  promhttp on admin listener          │
 └───────────────────────────────────────────────────────────────────────┘
```

Goroutine budget per stream: `StreamCore` (1) + `Committer` (1) + `TimerWheel` (1) + `Maintainer` (1) = 4, plus 2 per active consumer session. A 20-stream / 50-consumer deployment runs ~180 goroutines. This is deliberately boring.

### 2.3 Data flow: publish

```
client ──Publish{subject, body, msg_id, headers}──▶ session.recv
   └─▶ StreamCore.cmd chan (capacity 1024; full ⇒ CodeResourceExhausted, never blocks)
        ├─ validate size/subject/stream limits
        ├─ dedup index lookup on msg_id  ─── hit ⇒ reply PubAck{seq: orig, duplicate: true}
        ├─ assign seq (monotonic, gapless per stream)
        ├─ build PUB record, hand to Committer with a reply channel
        │      Committer: append to segment buffer, join current commit batch
        │                 batch closes on 1 ms linger OR 4 MiB OR explicit flush
        │                 append COMMIT frame → fdatasync(fd) → durableLSN := lsn
        │                 close all reply channels in the batch
        ├─ on reply: update in-memory index, tracked maps, wake idle consumers
        └─ reply PubAck{seq, durable: true} ──▶ session.send ──▶ client

log: publish.accepted {stream, seq, msg_id, subject, bytes, trace_id, commit_ms}
```

The publisher's ack is emitted **after** `fdatasync` returns, never before. This is the single most important line in the architecture.

### 2.4 Data flow: consume

Credit-based, client-driven, bidirectional.

```
client ──Consume(open bidi stream)──▶ session
client ──Request{credit: 64}──▶ StreamCore: credit[session] += 64

StreamCore dispatch loop (runs after every state change):
   while credit > 0
     && unretired_tracked < max_ack_pending
     && (cursor - ack_floor) < max_ack_gap
     && ordering_key_free
     && next_candidate() != nil:
        seq := next_candidate()            # redelivery queue first, then cursor
        attempt++ ; dispatch_count++
        state := INFLIGHT ; lease := now_mono + ack_wait
        TimerWheel.schedule(lease, seq)
        credit--
        emit Delivery{token, seq, subject, body, headers, attempt, fail_count, num_pending}

client ──Ack{token} / Nak{token, delay} / Term{token} / Progress{token}──▶ StreamCore
```

`num_pending` (backlog for this consumer) rides on every delivery so a consumer library can expose lag without polling.

### 2.5 Backpressure policy

> **No unbounded queue exists anywhere in messq.** Every channel has a fixed capacity and a written-down overflow policy.

| Queue | Capacity | Overflow policy |
|---|---|---|
| `StreamCore.cmd` | 1024 | Reject with `CodeResourceExhausted`, log `flow.stalled`, increment `messq_publish_rejected_total{reason="core_busy"}` |
| Committer batch | 4 MiB / 1 ms linger | Backpressure onto `StreamCore.cmd` (natural) |
| Session send buffer | 64 deliveries | Stop dispatching to that session; log `flow.stalled{session}` |
| Per-consumer in-flight | `max_ack_pending` | Stop dispatching (this *is* flow control) |
| Per-consumer ack gap | `max_ack_gap` | Stop dispatching, log `consumer.gap_stalled` at WARN every 30 s |
| TimerWheel | bounded by `max_ack_pending × consumers` | n/a — structurally bounded |

The last two rows are the reason messq needs no general-purpose embedded database (§3.2).

---

## 3. Storage & durability design

### 3.1 Engine decision

**Decision: a purpose-built, segmented, append-only journal per stream, plus atomically-renamed checkpoint files. No SQLite. No bbolt. No embedded KV store.**

The README sketch suggested "SQLite/WAL". This plan deliberately overrides that, and owes an argument.

**Why not SQLite** (`modernc.org/sqlite`, docs fetched):
- Durability in the pure-Go driver is configured through the DSN and pragmas — `_journal=WAL&_synchronous=NORMAL&_txlock=immediate&_timeout=5000`. That makes the **durability of P1 a connection-string property**. Any operator, any config-management drift, any copy-pasted DSN can silently downgrade `synchronous=FULL` to `NORMAL` and P1 evaporates with no visible symptom until a power cut. A correctness-first broker cannot have its central guarantee live in a query string.
- The workload is log-shaped: append at the head, scan sequentially, delete the tail wholesale. A B-tree gives us index maintenance, page splits, `DELETE` churn and `VACUUM` in exchange for query flexibility we will never use. Retention becomes a large `DELETE` + free-list churn instead of `unlink()`.
- Replay from offset becomes an index range scan instead of a `sendfile`-shaped sequential read.
- Group commit must be hand-rolled anyway (batching transactions), so SQLite's transaction machinery is not saving us the hard part.
- `mattn/go-sqlite3` additionally requires cgo, which breaks the single-static-binary goal.

**Why not bbolt** (`etcd-io/bbolt`, docs fetched):
- bbolt's own documentation is clear that durability is "fsyncing to disk after every commit", using a two-phase write (dirty pages + fsync, then a new meta page + fsync). That is **two fsyncs per commit** — for a 40-byte `ACK` record this is enormous write amplification against a copy-on-write B+tree.
- The documented escape hatches are `NoSync` and `DB.Batch`. `NoSync` throws away the guarantee entirely with no per-operation visibility. `DB.Batch`'s contract is that the supplied function **"must be idempotent as it may be called multiple times"** — a live footgun when the function increments `fail_count`.
- bbolt is a single-writer store, so it does not relieve us of the single-writer discipline we already have.
- Most decisively: we need a durable append log for message bodies regardless. Adding bbolt means **two independent durability domains** and a cross-domain crash-consistency protocol between them. One durability mechanism is provably easier to reason about than two.

**What we give up, and how we buy it back.** SQLite's crash recovery is among the most-tested code in existence; ours will not be. We buy the assurance back with (a) a record format simple enough that the recovery function is ~250 lines, (b) a crash-point enumerator that mechanically drives recovery from *every* fsync/torn-write boundary (§8.4), and (c) a shipped `messq verify` fsck. **Revisit trigger:** if the crash-test suite is still finding recovery bugs after milestone M7, that is evidence the hand-rolled approach was wrong, and we port the metadata (not the bodies) onto SQLite with `synchronous=FULL` pinned in code, not in a DSN.

### 3.2 Why no KV store is needed at all

The classical argument for an embedded DB is "the pending set could be huge". In messq it cannot be, by construction:

- **I5** bounds unretired tracked entries by `max_ack_pending` (default 1000).
- **I6** bounds `cursor − ack_floor` by `max_ack_gap` (default 100 000). This is the crucial one: without it, one stuck message at seq 100 while seqs 101…10⁷ are acked would force us to remember 10⁷ sparse acks above the floor. With it, dispatch simply stops and screams.
- The retired-above-floor set is stored as an **interval set** (run-length), because acks are overwhelmingly contiguous; worst case `max_ack_gap/2` intervals, i.e. a few MB.

So per-consumer live state is O(hundreds of KB) and per-stream index state is a sparse offset index. It all fits in memory, it is all reconstructible, and a KV store would be dead weight.

### 3.3 On-disk layout

```
$MESSQ_DATA/
  messq.lock                            flock'd, contains pid + boot id
  FORMAT                                {"format_version":1}, atomic-renamed
  streams/
    orders/
      stream.json                       stream config; atomic write + dir fsync
      consumers/
        billing.json                    consumer config (durable config only)
      journal/
        0000000000000000.jnl            segment; filename = first LSN, 16 hex
        0000000004000000.jnl
        0000000004000000.idx            sparse index; derived, rebuildable
      checkpoints/
        ckpt-0000000003ff8120.bin       state snapshot at LSN; newest wins
      dlq/
        index.bin                       dead-letter index (see §4.9)
```

**Everything under `journal/` is ground truth. Everything else is derived and may be deleted without data loss** (`.idx` and `checkpoints/` are rebuilt by replay; `stream.json`/`consumers/*.json` are mirrors of `CFG` records in the journal). This rule is testable: `messq verify --rebuild-derived` deletes them, rebuilds, and asserts an identical state hash.

### 3.4 Record frame format

Fixed 26-byte header, little-endian, frames 8-byte aligned (padding is outside `len` and outside the CRC):

```
 offset size field
 ------ ---- ------------------------------------------------------------
   0     4   len        uint32  bytes of [type..body], excludes pad & crc
   4     4   crc32c     uint32  Castagnoli over bytes [8, 8+len)
   8     1   rec_type   uint8
   9     1   rec_version uint8
  10     8   lsn        uint64  byte offset of this frame in the logical journal
  18     8   wall_nanos int64   UTC wall clock at append (advisory only)
  26   len-18 body               protobuf, type-specific
       0..7  pad        zeroes to the next 8-byte boundary
```

`len` first, then CRC over the payload: a torn write that lands `len` but not the payload fails CRC; a torn write that lands nothing leaves zeroes, so `len == 0` fails the parse. `crc32.Castagnoli` from `hash/crc32` is SSE4.2-accelerated on every target CPU.

**Record types**

| Type | Class | Body | Emitted by |
|---|---|---|---|
| `SEGHDR` | — | magic, format version, stream id, first LSN, first seq | segment creation |
| `PUB` | **sync** | seq, subject, headers, msg_id, body | publish |
| `COMMIT` | **sync** | durable_lsn, crc32c over all bytes since previous COMMIT, batch record count | Committer, after every batch |
| `ACK` | batch | consumer id, seq | ack |
| `NAK` | batch | consumer id, seq, new fail_count, redeliver_at | nak |
| `EXP` | batch | consumer id, seq, new fail_count | lease expiry |
| `TERM` | batch | consumer id, seq, reason | term / operator drop |
| `DLQ` | batch | consumer id, seq, fail_count, reason | poison bound reached |
| `CURSOR` | batch | consumer id, cursor, ack_floor | dispatch (coalesced) |
| `EPOCH` | **sync** | consumer id, new epoch, reason | seek / reset / filter change |
| `CFG` | **sync** | stream or consumer config blob | admin ops |
| `PURGE` | **sync** | scope, up-to seq | `messq stream purge` |
| `RECLAIM` | batch | segment first LSN, reason, message count | retention |
| `CKPT` | batch | checkpoint file name, covered LSN | Maintainer |

### 3.5 fsync policy — deliberately asymmetric

The whole policy follows from one observation:

> **Losing a `PUB` record loses data. Losing an `ACK`, `NAK`, `EXP`, or `CURSOR` record only causes a redelivery.**

Since at-least-once already permits redelivery, the ack-class records may be synced lazily; the publish class may not.

| Class | Records | Sync rule | Failure if lost |
|---|---|---|---|
| **sync** | `PUB`, `COMMIT`, `EPOCH`, `CFG`, `PURGE` | Group commit: batch closes after **1 ms linger** or **4 MiB**; append `COMMIT`; `fdatasync`; only then release waiters | Would violate P1 — not permitted |
| **batch** | `ACK`, `NAK`, `EXP`, `TERM`, `DLQ`, `CURSOR`, `RECLAIM`, `CKPT` | Written into the same file immediately; flushed to disk **at most `ack_sync_interval` (default 200 ms) later**, or piggybacked on the next sync-class commit | Redelivery of ≤ 200 ms of acks. Never loss. |
| **never synced** | in-flight leases, credit, session state | Not written at all | Redelivery. By design (§4.7) |

Implementation notes:

- Use `unix.Fdatasync`, not `os.File.Sync()`. `Sync()` is `fsync(2)` and additionally flushes inode metadata (atime/mtime) we do not care about. `fdatasync` still flushes the size change needed to read the appended bytes back — which is exactly the metadata we *do* need.
- **Directory fsync on file creation.** When a new segment or checkpoint appears, `fdatasync` on the file is not enough — the directory entry may not survive. Every `create` and every `rename` is followed by an `fsync` of the parent directory *before* the new file is considered usable. This is the ext4/PostgreSQL lesson and it is a one-line mistake that costs entire files.
- **Preallocate** each segment with `unix.Fallocate` at creation (64 MiB default) to avoid per-append extent metadata churn.
- `TERM` and `DLQ` are batch-class even though they are terminal: losing them only means the message gets delivered again and the consumer terms/DLQs it again. Idempotent by construction.
- The Committer records `fdatasync` latency and logs `commit.slow` at WARN when it exceeds `commit_slow_threshold` (default 250 ms). Slow fsync is the number-one silent killer of small brokers on shared/virtualised disks, and it should be a *log line*, not something you infer from a latency graph.
- `sync_mode=interval:<d>` is offered as an explicit per-stream downgrade for people who genuinely want it — and when it is set, **`PubAck.durable` is `false`**. The client can see the weakened guarantee in the response. Never a silent downgrade.

### 3.6 Crash recovery

**Recovery rule.** Let *D* be the byte prefix of the journal ending at the last frame that (a) parses, (b) has a valid CRC32C, and (c) is a `COMMIT` frame whose covering CRC over the bytes since the previous `COMMIT` matches. Recovery truncates the journal to `|D|` and defines post-restart state as `fold(D)`.

**Recovery theorem (P1).** No acknowledged publish is lost.
*Proof sketch.* A `PubAck{durable:true}` is emitted only after the Committer appended a `COMMIT` frame covering that `PUB` and `fdatasync` returned. So the `PUB` and its `COMMIT` are both in the durable prefix, hence in *D*, hence in `fold(D)`. Records after the last valid `COMMIT` were, by construction, never acknowledged to any client, so discarding them cannot lose an acknowledged fact. ∎

This is why `COMMIT` frames exist even though per-record CRCs already detect a torn tail: they turn "which bytes are trustworthy?" from a heuristic into a one-line rule that mirrors exactly the point at which we made a promise.

**Recovery algorithm**

1. `flock` the data dir; read `FORMAT`; refuse to start on an unknown format version (never silently upgrade).
2. For each stream, load the newest valid `checkpoints/ckpt-<lsn>.bin` (CRC-checked; fall back to the previous one, then to LSN 0).
3. Open segments in order; skip whole segments below the checkpoint LSN except for the one containing it.
4. Scan frames forward from the checkpoint LSN, tracking the last valid `COMMIT`. Apply `fold` to every record.
5. On the first frame that fails to parse or fails CRC: stop. Truncate the active segment to the offset just past the last valid `COMMIT`. Log `journal.truncated {stream, from_lsn, to_lsn, dropped_bytes, dropped_records}` at **WARN**, and record `messq_journal_truncated_bytes_total`. A truncation is never silent.
6. Rebuild the sparse `.idx` for any segment whose index is missing or stale.
7. **Materialise volatile state:** for every consumer, for every seq in `(ack_floor, cursor)` matching the filter and not retired, create a tracked entry in state `WAITING` with `redeliver_at = now`, `attempt` from the durable record, `fail_count` from the durable record. Emit `lease.orphaned` at INFO per entry (aggregated above 100).
8. **Bump `run_id`** for the whole process (persisted in the next checkpoint) — this fences lease-mutating operations from clients that survived the restart (§4.6).
9. Rebuild the dedup index by scanning `PUB` records within `dedup_window` of now.
10. Write a fresh checkpoint. Then `sd_notify(READY=1)` and open the listeners.

**Recovery is offline.** Listeners open only after step 10 for every stream. Serving traffic against a half-recovered stream is the kind of shortcut that produces unexplainable duplicate reports; `/readyz` returns 503 until recovery finishes, and recovery progress is logged (`journal.recovering {stream, pct, lsn}`) every second so a 40 GB recovery is not a silent hang.

### 3.7 Checkpoints, segment rolling, retention

**Checkpoint** = a serialized snapshot of all consumer state (cursor, ack_floor, interval set of retired-above-floor, tracked entries with `attempt`/`fail_count`, epoch, run_id) plus stream head seq and dedup digest, at a specific LSN. Written every 30 s or every 64 MiB of journal, whichever first. Written by `Maintainer` from an immutable snapshot handed over by `StreamCore`, so it never blocks the hot path.

Atomic write sequence (this exact order):
`write ckpt.tmp` → `fdatasync(ckpt.tmp)` → `rename(ckpt.tmp, ckpt-<lsn>.bin)` → `fsync(dir)` → append `CKPT` record → unlink older checkpoints (keep 2).

**Segment roll** at 64 MiB or 24 h. New segment: `open(O_CREAT|O_WRONLY)` → `fallocate` → write `SEGHDR` → `fdatasync(file)` → `fsync(dir)` → only then may the Committer append to it.

**Retention policies** (per stream, mirroring familiar names):

| Policy | Segment `S` may be unlinked when |
|---|---|
| `limits` (default) | age/bytes/count limits exceeded. **May discard undelivered messages** — this is loud: `retention.discard_undelivered {stream, seq_from, seq_to, consumers}` at WARN plus a counter |
| `workqueue` | every message in `S` is retired for **every** bound consumer |
| `interest` | every message in `S` is retired for every consumer that ever expressed interest in its subject |

**Invariant I10** gates all three: a segment is unlinked only if additionally (a) no dead-letter index entry references a message in it, and (b) the newest checkpoint's LSN ≥ the segment's end LSN (so the consumer-state records inside it are already folded into a checkpoint). Violating (b) would make recovery replay a segment that no longer exists.

---

## 4. Delivery semantics & message lifecycle

### 4.1 The unit of state

**The unit of lifecycle state is the pair `(consumer, seq)`, not the message.** A message with three consumers has three independent lifecycles. This is the single most common modelling error in small brokers and it is worth being explicit about: "the message is acked" is a meaningless sentence in messq.

### 4.2 Consumer state, precisely

```
Consumer C := {
  id, stream, filter (subject pattern),
  epoch          uint32   // semantic generation; bumped by seek/reset/filter change
  cursor         uint64   // next never-yet-dispatched seq
  ack_floor      uint64   // all matching seqs ≤ ack_floor are retired
  retired_above  IntervalSet  // retired seqs in (ack_floor, cursor)
  tracked        map[uint64]Entry  // non-retired seqs in (ack_floor, cursor)
  ack_wait, max_deliver, backoff[], max_ack_pending, max_ack_gap,
  ordered_by     enum{none, subject, header:<k>}
  on_poison      enum{dead_letter, block, drop}
}

Entry := { state, attempt uint32, dispatch_count uint32, fail_count uint32,
           lease_deadline mono, redeliver_at mono, session *Session }
```

Note `ack_floor ≤ cursor` always, `tracked ∪ retired_above` exactly covers the matching seqs in `(ack_floor, cursor)`, and advancing `ack_floor` absorbs the leading run of `retired_above`.

### 4.3 States

```
                          seq >= cursor
                        ┌───────────────┐
                        │    UNSEEN     │  (implicit; not stored)
                        └───────┬───────┘
                                │ dispatch
                                ▼
        ┌──────────────┐  ack   ┌───────────────┐
        │   WAITING    │◀──────┐│   INFLIGHT    │───ack───▶┌──────────┐
        │ redeliver_at │  nak / ││  lease armed  │         │ RETIRED  │
        │              │  expiry││  progress ⟲   │───term──▶│ (ack|term│
        └──────┬───────┘◀───────┘└───────────────┘         │  |drop)  │
               │ dispatch (when now >= redeliver_at)        └──────────┘
               │                                                  ▲
               │ fail_count >= max_deliver                        │ operator drop
               ▼                                                  │
        ┌──────────────┐   dlq redeliver (fail_count := 0)         │
        │ DEAD_LETTERED│───────────────────────────────────────────┘
        └──────────────┘   dlq drop ──▶ RETIRED
```

`RETIRED` is terminal (P3). `DEAD_LETTERED` is terminal *for automatic delivery* but is not retired — the message still pins its segment (I10) and still appears in `messq dlq ls`.

### 4.4 Transition table

Every transition names its guard, its durable record, and its log event. This table is the specification; the implementation is a `switch` over it and the reference model in `internal/model` is a second, independent implementation of it.

| # | From | Trigger | Guard | Durable record | Effects | To | Log event |
|---|---|---|---|---|---|---|---|
| T1 | UNSEEN | dispatch | credit>0 ∧ \|unretired\|<max_ack_pending ∧ cursor−ack_floor<max_ack_gap ∧ key free (I9) | `CURSOR` (coalesced, batch) | attempt=1, dispatch_count=1, lease=now+ack_wait, cursor++ | INFLIGHT | `delivery.sent` |
| T2 | INFLIGHT | `Ack` | token.epoch = epoch ∧ not retired | `ACK` (batch) | credit++, absorb into ack_floor if leading | RETIRED | `delivery.ack` |
| T3 | INFLIGHT | `Nak{delay}` | token fully fenced (I4b) | `NAK` (batch) | fail_count++, redeliver_at = now + (delay ?? backoff[min(fail_count-1, len-1)]), credit++ | WAITING | `delivery.nak` |
| T4 | INFLIGHT | `Nak{delay, no_fail:true}` | token fully fenced | `NAK` (batch, fail unchanged) | redeliver_at set, **fail_count unchanged** | WAITING | `delivery.returned` |
| T5 | INFLIGHT | lease expiry | now ≥ lease_deadline | `EXP` (batch) | fail_count++, redeliver_at = now + backoff[…] | WAITING | `lease.expired` |
| T6 | INFLIGHT | `Progress` | token fully fenced | none | lease_deadline = now + ack_wait | INFLIGHT | `delivery.progress` (DEBUG) |
| T7 | INFLIGHT | `Term{reason}` | token fully fenced | `TERM` (batch) | credit++ | RETIRED | `delivery.term` |
| T8 | WAITING | dispatch | now ≥ redeliver_at ∧ T1's guards | none (attempt is volatile) | attempt++, dispatch_count++, lease armed | INFLIGHT | `delivery.sent{redelivery:true}` |
| T9 | WAITING | poison bound, `on_poison=dead_letter` | fail_count ≥ max_deliver | `DLQ` (batch) | remove from tracked, add to DLQ index | DEAD_LETTERED | `message.dead_lettered` (WARN) |
| T10 | WAITING | poison bound, `on_poison=block` | fail_count ≥ max_deliver | none | stop dispatch for this ordering key (or whole consumer if `ordered_by=none`) | WAITING | `consumer.blocked` (ERROR, every 30 s) |
| T11 | WAITING | poison bound, `on_poison=drop` | fail_count ≥ max_deliver | `TERM{reason:poison}` | — | RETIRED | `message.dropped` (WARN) |
| T12 | DEAD_LETTERED | `dlq redeliver` | operator | `NAK{fail_count:0}` | fail_count=0, redeliver_at=now | WAITING | `dlq.redelivered` |
| T13 | DEAD_LETTERED | `dlq drop` | operator | `TERM{reason:dlq_drop}` | — | RETIRED | `dlq.dropped` |
| T14 | INFLIGHT | broker restart | — | none | attempt/fail_count preserved from durable records, redeliver_at=now | WAITING | `lease.orphaned` |
| T15 | any | `seek` | operator, `--yes` | `EPOCH` (sync) + `CURSOR` | epoch++, cursor/ack_floor reset, tracked cleared, all outstanding tokens invalid | UNSEEN/WAITING | `consumer.seek` (WARN) |
| T16 | any | `purge` | operator, `--yes` | `PURGE` (sync) | messages removed; affected tracked entries dropped | — | `stream.purge` (WARN) |
| T17 | INFLIGHT/WAITING | retention expiry (`limits`) | age/size limit | `RECLAIM` | entry dropped without delivery | — | `retention.discard_undelivered` (WARN) |
| T18 | INFLIGHT | session closes | — | none | redeliver_at = now (immediate), fail_count **unchanged** | WAITING | `delivery.returned{reason:disconnect}` |

T4 and T18 are the RabbitMQ-4.3 lesson made concrete: a polite return and a disconnect are not consumer failures, so they must not burn the poison budget. T5 and T3 are failures, and they do.

### 4.5 Publish deduplication

`Publish` may carry `msg_id`. Within the stream's `dedup_window` (default 2 min, also bounded by `dedup_max_entries`, default 100 000 — a *time-only* window is a memory leak waiting for a traffic spike), a second publish with the same `msg_id` is not stored; the `PubAck` returns the original `seq` with `duplicate: true`.

This gives **idempotent publish across a lost PubAck** — the single most common source of duplicates in practice — and nothing more. The plain statement for the docs: *messq offers exactly-once publish within the dedup window; end-to-end exactly-once processing does not exist and messq will not pretend otherwise.*

### 4.6 Token fencing — two counters, not one

A delivery token is 28 bytes, opaque to clients:
`consumer_id(8) ‖ epoch(4) ‖ run_id(4) ‖ seq(8) ‖ attempt(4)`.
Forgery across consumers is impossible because a token is validated against the consumer bound to the session that presents it.

The subtlety that most implementations get wrong: **acks and nak-class operations need different fencing.**

- **`Ack` is fenced by `epoch` only, and is idempotent.** A late ack from attempt 1, arriving while attempt 2 is in flight, still retires the message — the work *was* done, and accepting it removes a duplicate rather than creating one. Attempt 2's later ack is a no-op; attempt 2's nak is ignored because the entry is already retired. Acks are *not* fenced by `run_id`, so a client that survives a broker restart can still retire its work.
- **`Nak`, `Term`, `Progress` are fenced by `epoch` ∧ `run_id` ∧ `attempt`.** These mutate counters or extend leases; applying a stale one would corrupt `fail_count` or resurrect a lease on an attempt that no longer exists. A stale one returns `CodeFailedPrecondition{"stale token"}`.
- **`epoch`** bumps only on `seek`, `reset`, or a filter change — events where the *meaning* of a seq changed. A stale ack across an epoch must not retire a message the operator deliberately rewound.
- **`run_id`** bumps on every broker start, purely to invalidate lease-mutating operations from pre-restart clients.

### 4.7 What at-least-once really costs across a crash

Stated in full, because this is the question operators actually ask:

- A message in `INFLIGHT` when the process dies is redelivered (T14). If the consumer had already completed the side effect but not yet acked, **the side effect happens twice.** No broker can prevent this; only an idempotent consumer can.
- `fail_count` and `attempt` for that message are read from the last durable `NAK`/`EXP` record, which may be up to `ack_sync_interval` (200 ms) stale. So a message can, in the worst case, be dispatched **`max_deliver + k` times** where `k` is the number of crashes it lived through. This is exactly **P4's caveat**, and it is why the shipped Grafana panel plots `dispatch_count` and `fail_count` as separate series.
- Acks lost in the last ≤ 200 ms cause redelivery of those messages. Never loss.
- A `PubAck{durable:false}` publish (opt-in weak mode) may be lost. The client was told.

Written as an operator-facing table in `docs/guarantees.md`, generated from the same table the tests use.

### 4.8 Ordering

**Ordering is a property of concurrency, not of storage.** The journal is totally ordered by construction; that buys nothing unless dispatch is constrained.

`ordered_by = none` (default): no ordering guarantee. Messages may be processed concurrently and out of order.

`ordered_by = subject` or `header:<k>`: for each key *k*, at most one message with key *k* may be non-retired at a time. Concretely: dispatch of key *k* is blocked while any message of key *k* is `INFLIGHT`, `WAITING`, or `DEAD_LETTERED` with a lower seq. Consequences, stated plainly:

- A nak head-of-line-blocks its key. That is the point.
- **For ordered consumers, `on_poison` defaults to `block`, not `dead_letter`.** Silently dead-lettering message 5 and proceeding to message 6 breaks the very guarantee the operator asked for. messq refuses to do that by default: it stops the key, logs `consumer.blocked` at ERROR every 30 s, and waits for a human. An operator who prefers the gap can set `on_poison=dead_letter` explicitly.
- Throughput for an ordered consumer is bounded by (number of distinct live keys) × (1 / processing latency).

### 4.9 Dead-letter design

**Decision: dead-lettering is a terminal state plus an index entry, not a copy into another stream.**

The alternative — republish the body into a `<stream>.dlq` stream and term the original — requires atomicity across two journals. It is solvable (append to the DLQ stream with a dedup key of `origin_stream/seq/consumer`, then `TERM` the original; a crash in between replays and dedups), but it doubles storage, doubles the message's identity, and adds a cross-log protocol to the correctness surface for no user benefit. In-place dead-lettering has none of those costs:

- `DLQ` record + entry in `dlq/index.bin` (`consumer, seq, fail_count, first_failure, last_failure, reason, last_error`).
- The body is read from the origin journal; nothing is copied.
- The entry pins the segment (I10), so `messq dlq ls` can always show the payload.
- `messq dlq redeliver` is T12: `fail_count := 0`, back to `WAITING`. No republish, no new seq, **the original seq is preserved** — which matters enormously, because the consumer's dedup key (§6.4) stays valid.
- DLQ retention (`dlq_max_age`, default 30 d; `dlq_max_entries`, default 10 000) prevents a dead-letter entry pinning a segment forever. Eviction logs `dlq.evicted` at WARN.

For teams that genuinely want a separate stream (e.g. a different service handles failures), `dlq_forward_stream` enables the two-step protocol above, explicitly documented as at-least-once with a dedup key. Off by default.

### 4.10 Invariants

Numbered, because §8 tests them by number.

| # | Invariant |
|---|---|
| **I1** | Every `PUB` that produced `PubAck{durable:true}` is present in `fold(D)` after any crash. |
| **I2** | Every message returned by any read path corresponds to a `PUB` frame inside a valid `COMMIT` boundary. |
| **I3** | Once `(C,S)` is `RETIRED`, `S` is never dispatched to `C` again, except after `seek`, `replay`, or `dlq redeliver`. |
| **I4a** | `Ack(t)` retires `(C,S)` iff `t.consumer = C ∧ t.epoch = epoch(C) ∧ (C,S)` not retired. Idempotent. |
| **I4b** | `Nak`/`Term`/`Progress(t)` take effect iff `t.epoch = epoch(C) ∧ t.run_id = run_id ∧ t.attempt = attempt(C,S) ∧ state(C,S) = INFLIGHT`. |
| **I5** | `\|{S : state(C,S) = INFLIGHT}\| ≤ min(max_ack_pending(C), outstanding_credit(C))`. |
| **I6** | `cursor(C) − ack_floor(C) ≤ max_ack_gap(C)`. |
| **I7** | `ack_floor(C)` is non-decreasing except across a `seek`. |
| **I8** | `fail_count(C,S) ≤ max_deliver(C)` for any non-terminal entry; dispatch stops at the bound. |
| **I9** | If `ordered_by ≠ none`: for each key *k*, at most one `(C,S)` with key *k* is `INFLIGHT`, and no `S'` of key *k* with `S' > S` is dispatched while `(C,S)` is non-retired. |
| **I10** | A segment is unlinked only if all its messages are retired-or-retention-expired for all bound consumers, no DLQ index entry references it, and `checkpoint_lsn ≥ segment_end_lsn`. |
| **I11** | Within `dedup_window`, two `PUB` requests with equal non-empty `msg_id` yield exactly one stored message. |
| **I12** | `fold` is pure: `state_hash(fold(D))` is identical across repeated recoveries of identical bytes, on any machine. |
| **I13** | No unbounded queue: every channel and every in-memory collection has a static or config-derived bound. |

---

## 5. API / protocol

### 5.1 Decision

**ConnectRPC (`connectrpc.com/connect`) over HTTP/2, one Protobuf service definition, served on a Unix socket by default and optionally on TCP.**

The purist argument for Connect over "gRPC" or "a hand-rolled binary protocol" is *one semantic surface*. The lifecycle rules of §4 are hard; implementing them twice — once for the streaming binary protocol and once for a convenience REST API — is the fastest route to two subtly different brokers. Connect gives three wire encodings from a single handler implementation: the gRPC protocol (for gRPC clients), gRPC-Web, and the Connect protocol (plain HTTP with JSON bodies, directly curl-able). The CLI, the Go client and `curl` all traverse the same code path.

From the fetched Connect docs, the constraints we must design around:

- **Bidirectional streaming requires end-to-end HTTP/2.** Unary, client-streaming and server-streaming work over HTTP/1.1; bidi does not. `Consume` is bidi, so `Consume` requires HTTP/2.
- Without TLS this means h2c, configured with `http.Protocols` — server side `p.SetHTTP1(true); p.SetUnencryptedHTTP2(true)`, client side `p.SetUnencryptedHTTP2(true)` on the `http.Transport`. The docs warn that the deprecated `Upgrade: h2c` mechanism is not supported, so a client that only speaks HTTP/1 cannot reach the bidi endpoint.
- Errors carry a `connect.Code` plus optional Protobuf details (`connect.NewError`, `connect.CodeOf`, `Error.Details()`), which is how messq returns structured, machine-readable failures (§5.4).

Consequence and mitigation: the default listener is a **Unix socket with h2c**, where HTTP/2 is guaranteed because both ends are ours. TCP listeners default to TLS. A `--allow-h2c-tcp` flag exists for trusted networks and logs a warning at startup. HTTP/1.1-only environments still get every non-`Consume` RPC, plus a `FetchBatch` unary fallback (§5.3) that provides pull-based consumption without streaming — degraded throughput, identical semantics.

### 5.2 Service sketch

`proto/messq/v1/messq.proto`, compiled with `buf` + `protoc-gen-go` + `protoc-gen-connect-go`.

```proto
service MessqService {
  // --- data plane ---
  rpc Publish(PublishRequest) returns (PublishResponse);                 // unary
  rpc PublishBatch(stream PublishRequest) returns (PublishBatchAck);     // client-stream
  rpc Consume(stream ConsumeClientMsg) returns (stream ConsumeServerMsg);// BIDI, needs h2c
  rpc FetchBatch(FetchBatchRequest) returns (FetchBatchResponse);        // unary fallback
  rpc AckBatch(AckBatchRequest) returns (AckBatchResponse);              // unary fallback

  // --- inspection (read-only, never mutates lifecycle) ---
  rpc Peek(PeekRequest) returns (stream PeekResponse);                   // server-stream
  rpc ListPending(ListPendingRequest) returns (ListPendingResponse);
  rpc TraceMessage(TraceMessageRequest) returns (TraceMessageResponse);
  rpc StreamInfo(StreamInfoRequest) returns (StreamInfoResponse);
  rpc ConsumerInfo(ConsumerInfoRequest) returns (ConsumerInfoResponse);
  rpc Replay(ReplayRequest) returns (stream PeekResponse);

  // --- control plane (mutating; each logs at WARN) ---
  rpc CreateStream(...) returns (...);   rpc UpdateStream(...) returns (...);
  rpc DeleteStream(...) returns (...);   rpc PurgeStream(...) returns (...);
  rpc CreateConsumer(...) returns (...); rpc UpdateConsumer(...) returns (...);
  rpc DeleteConsumer(...) returns (...);
  rpc SeekConsumer(SeekConsumerRequest) returns (SeekConsumerResponse);
  rpc ListDeadLetters(...) returns (stream DeadLetterEntry);
  rpc RedeliverDeadLetter(...) returns (...);
  rpc DropDeadLetter(...) returns (...);
}
```

Key messages:

```proto
message PublishRequest {
  string stream = 1;  string subject = 2;  bytes body = 3;
  map<string,string> headers = 4;
  optional string msg_id = 5;             // publish dedup key
  optional string traceparent = 6;        // W3C; generated if absent
  optional uint64 expect_last_seq = 7;    // optimistic concurrency; CodeAborted on mismatch
}
message PublishResponse {
  uint64 seq = 1; string msg_id = 2;
  bool duplicate = 3;                     // dedup hit; seq is the original
  bool durable = 4;                       // false only under sync_mode=interval
  string trace_id = 5;
}

message ConsumeClientMsg {
  oneof kind {
    ConsumeInit init = 1;   // stream, consumer, optional ephemeral config
    Credit      credit = 2; // additive credit grant
    Ack         ack = 3;    // token
    Nak         nak = 4;    // token, optional delay, bool no_fail
    Term        term = 5;   // token, reason
    Progress    progress=6; // token
    Drain       drain = 7;  // stop new deliveries, finish outstanding
  }
}
message ConsumeServerMsg {
  oneof kind {
    Delivery    delivery = 1;
    Heartbeat   heartbeat = 2;   // liveness + lag; every 5 s when idle
    FlowStalled stalled = 3;     // why we stopped: no_credit|ack_pending|ack_gap|ordering
    Closing     closing = 4;
  }
}
message Delivery {
  bytes token = 1;                  // 28-byte opaque fence
  uint64 seq = 2; string subject = 3; bytes body = 4;
  map<string,string> headers = 5;
  uint32 attempt = 6;               // physical dispatch number, this run
  uint32 dispatch_count = 7;        // durable total
  uint32 fail_count = 8;            // the poison budget consumed
  uint32 max_deliver = 9;
  google.protobuf.Timestamp published_at = 10;
  int64  lease_ms = 11;             // ack_wait remaining
  uint64 num_pending = 12;          // backlog for this consumer
  string trace_id = 13;
}
```

`expect_last_seq` gives publishers optimistic concurrency (a cheap "only if nothing else was published since" primitive), useful for single-writer event-sourcing patterns and costing one comparison.

### 5.3 The `FetchBatch` fallback

`FetchBatch{consumer, max_messages, max_bytes, max_wait}` performs a long-poll dispatch of up to N messages under the same guards as T1 (it *is* T1, with credit = N). `AckBatch{tokens[]}` applies T2/T3/T7 per token and returns per-token results. Semantically identical, just chattier. Its existence means "HTTP/1.1 only" is a performance limitation, not a semantic one — and it makes `curl`-driven consumption possible for debugging.

### 5.4 Error model

| Condition | Connect code | Detail |
|---|---|---|
| Unknown stream/consumer | `CodeNotFound` | — |
| Body over `max_msg_size` | `CodeInvalidArgument` | limit |
| Stale token | `CodeFailedPrecondition` | `StaleToken{expected_attempt, expected_epoch, expected_run_id}` |
| `expect_last_seq` mismatch | `CodeAborted` | `SeqMismatch{actual_last_seq}` |
| Core queue full / disk backpressure | `CodeResourceExhausted` | `retry_after_ms` |
| Recovery not finished | `CodeUnavailable` | `Recovering{stream, pct}` |
| Disk write failure | `CodeInternal` | plus broker enters read-only mode and logs `storage.fatal` at ERROR |

**Read-only mode** is worth calling out: on any `EIO` from write or `fdatasync`, messq stops accepting publishes for that stream, keeps serving reads and acks, logs `storage.fatal`, and flips `/readyz` to 503. Continuing to accept writes after a failed sync is how brokers lose data quietly.

### 5.5 Transport defaults

- Default listener: `unix:///run/messq/messq.sock`, mode `0660`, group-owned. No TCP by default.
- TCP requires either TLS (`--tls-cert/--tls-key`, optional mTLS via `--tls-client-ca`) or the explicit `--allow-h2c-tcp`.
- AuthN: mTLS client CN, or a bearer token file (`--auth-tokens`). AuthZ: per-token allow-lists of `stream:verb`. Deliberately minimal — this is an internal broker, not an API gateway.
- Admin listener (metrics, pprof, health) is separate and defaults to `127.0.0.1:9469`.

---

## 6. CLI & developer experience

### 6.1 Shape

Single binary, `spf13/cobra`. From the fetched Cobra docs, the conventions we adopt: `RunE` everywhere (so errors bubble to a single exit-code mapper), `SilenceUsage`/`SilenceErrors` set on the root so a runtime failure prints an error rather than a usage wall, `MarkPersistentFlagRequired` where appropriate, and `GenBashCompletion`/`CompletionOptions` for shipped completions (bash/zsh/fish).

```
messq serve [--config FILE] [--data DIR] [--dev]
messq stream    add|ls|info|update|purge|rm
messq consumer  add|ls|info|update|rm|seek|pending
messq pub       <stream> <subject> [--body -|@file|STR] [--msg-id ID] [-H k=v] [--expect-last-seq N]
messq sub       <stream> <consumer> [--credit N] [--auto-ack] [--exec CMD]
messq peek      <stream> [--seq N|--from N --count K|--subject PAT] [--body]
messq trace     <msg-id | stream/seq>
messq replay    <stream> --from <seq|ts|start> [--to ...] [--into CONSUMER | --stdout]
messq dlq       ls|show|redeliver|drop
messq verify    [--deep] [--state-hash] [--rebuild-derived]
messq journal   dump [--from-lsn N] [--types PUB,ACK]
messq bench     publish|consume
messq config    check
messq completion bash|zsh|fish
```

Global flags: `--addr` (default `unix:///run/messq/messq.sock`), `--json`, `--timeout`, `--log-level`.

### 6.2 The flagship command: `messq trace`

```
$ messq trace orders/8814
message  orders/8814   msg_id=ord-9f31c2   subject=orders.eu.created
         published 2026-08-21T09:14:02.118Z  1,284 bytes  trace_id=4bf92f3577b34da6a3ce929d0e0e4736

consumer billing            RETIRED(ack)      2 dispatches, 1 failure
  09:14:02.119  delivery.sent      attempt=1 lease=30s   session=10.0.3.11:52114
  09:14:32.119  lease.expired      fail_count=1  next in 1s
  09:14:33.121  delivery.sent      attempt=2 lease=30s   session=10.0.3.11:52114
  09:14:33.402  delivery.ack                              282ms

consumer analytics          DEAD_LETTERED     5 dispatches, 5 failures
  09:14:02.119  delivery.sent      attempt=1
  09:14:04.550  delivery.nak       fail_count=1  err="schema v3 unsupported"
  ... 3 more ...
  09:16:41.008  message.dead_lettered  fail_count=5  reason=max_deliver
```

Two properties make this more than pretty output:

1. **It is reconstructed from the journal, not from log files.** Log files rotate, get dropped by the shipper, or were never enabled. The journal is the durable record of every transition, so `messq trace` works on a box where logging was misconfigured for a month.
2. It is per-consumer, matching §4.1's model, so the question "did *billing* process this?" has a direct answer.

`--json` emits the same data as an array of events for piping into `jq`.

### 6.3 Operational ergonomics

- `messq serve --dev` — ephemeral data dir under `$TMPDIR`, human-readable colourised logs, auto-created streams, prints a ready-to-paste `messq pub` line. Zero-config first five minutes.
- Every mutating command requires `--yes` when non-interactive and the operation can lose or duplicate work (`purge`, `seek`, `rm`, `dlq drop`). `seek` additionally prints exactly how many messages will be redelivered before asking.
- `messq consumer pending <stream> <consumer>` is the PEL view — seq, state, attempt, fail_count, age, session — sorted by age. This is the first thing you run when a consumer is "stuck".
- Exit codes are documented and stable: `0` ok, `1` generic, `2` usage, `3` not found, `4` precondition failed, `5` unavailable/recovering, `6` verification failed.
- `messq verify --deep` walks every frame, checks every CRC, cross-checks the sparse indexes and the checkpoint against a full replay, and prints a state hash. Designed to be a cron job.
- systemd unit shipped: `Type=notify` with `sd_notify(READY=1)` after recovery (plain `net` write to `$NOTIFY_SOCKET`, no dependency), `WatchdogSec` wired to a StreamCore liveness ping, `LimitNOFILE`, `ProtectSystem=strict`, `ReadWritePaths=/var/lib/messq`.

### 6.4 Client library and idempotency guidance

`github.com/messq/messq/client` ships in the same module.

```go
c, _ := client.Dial(ctx, "unix:///run/messq/messq.sock")
sub, _ := c.Consume(ctx, "orders", "billing", client.WithCredit(64))
for msg := range sub.Messages() {
    if err := handle(ctx, msg); err != nil {
        msg.Nak(client.Delay(5*time.Second))   // counts as a failure
        continue
    }
    msg.Ack()
}
```

- `msg.Ack()`, `msg.Nak(...)`, `msg.Term(reason)`, `msg.InProgress()`, `msg.Return()` (T4 — no fail count).
- `client.Handler` wrapper: converts a `panic` into `Term` (a panicking handler will panic again — retrying is pointless and burns the budget), a returned error into `Nak`, and runs `InProgress` on a ticker at `ack_wait/3` for long jobs.
- **The dedup key the docs push is `(stream, seq)`, not `msg_id`.** `seq` is stable across every redelivery, across DLQ redelivery, and across broker restarts; `msg_id` is publisher-supplied and is meant for publish-side dedup. Getting this backwards is the classic consumer bug, so the client exposes `msg.DedupKey() string` returning `"orders/8814"` and `docs/idempotency.md` shows the canonical pattern:

  ```sql
  INSERT INTO processed(key) VALUES ($1) ON CONFLICT DO NOTHING;  -- same txn as the side effect
  ```

  with the explicit warning that this only works if the dedup insert and the side effect share a transaction; otherwise you have moved the race, not removed it.
- `docs/guarantees.md` is generated from the invariant table and the transition table, so the documentation cannot drift from the tests.

---

## 7. Observability & logging design

Logging is a product feature here, not instrumentation. The design rule:

> **Every state transition in the table in §4.4 emits exactly one log event, with a stable name and a stable field set.**

### 7.1 Implementation

`log/slog` from the standard library (fetched docs: `slog.NewJSONHandler(w, *HandlerOptions)`, `HandlerOptions.ReplaceAttr`, `AddSource`, and the `LogValuer` interface). No zap, no zerolog: `slog` is fast enough at our event rate, it is the ecosystem default, and a `Handler` is a 5-method interface so the human-readable renderer is a small internal package rather than a dependency.

- `--log-format=json` (default) → `JSONHandler` on stderr.
- `--log-format=human` → internal handler with aligned columns and colour when the terminal is a TTY. Auto-selected by `--dev`.
- Message context is attached via a `LogValuer` on an internal `msgctx` type, so the expensive field expansion happens only if the record is actually emitted.
- `ReplaceAttr` redacts message bodies unconditionally; `--log-bodies` (dev only) opts in, and logs a warning that it does so.

### 7.2 Event taxonomy

Stable `event` names, alphabetical, each with a fixed field set. Renaming one is a breaking change requiring a major version.

| Event | Level | Notes |
|---|---|---|
| `publish.accepted` | INFO | `commit_ms` included — this is your fsync health signal |
| `publish.duplicate` | INFO | dedup hit; `original_seq` |
| `publish.rejected` | WARN | `reason` |
| `delivery.sent` | DEBUG/INFO | **sampleable** |
| `delivery.ack` | INFO | `process_ms` |
| `delivery.nak` | INFO | `fail_count`, `next_delivery_in_ms`, consumer-supplied `error` header |
| `delivery.returned` | INFO | T4/T18 — no fail count |
| `delivery.term` | INFO | `reason` |
| `delivery.progress` | DEBUG | |
| `lease.expired` | WARN | the "consumer is too slow or dead" signal |
| `lease.orphaned` | INFO | post-restart; aggregated above 100 |
| `message.dead_lettered` | WARN | `fail_count`, `reason`, `last_error` |
| `message.dropped` | WARN | `on_poison=drop` |
| `consumer.blocked` | ERROR | repeated every 30 s until cleared |
| `consumer.gap_stalled` | WARN | I6 hit |
| `consumer.lag` | INFO | every 30 s per consumer: `num_pending`, `ack_pending`, `oldest_unacked_age_ms` |
| `consumer.seek` | WARN | `from`, `to`, `messages_affected`, `actor` |
| `stream.purge` | WARN | `up_to_seq`, `messages_removed`, `actor` |
| `retention.reclaimed` | INFO | `segment`, `messages`, `bytes` |
| `retention.discard_undelivered` | WARN | the one retention event that can lose work |
| `flow.stalled` | INFO | `reason` ∈ {no_credit, ack_pending, ack_gap, ordering, session_slow} |
| `commit.slow` | WARN | `fdatasync_ms` over threshold |
| `storage.fatal` | ERROR | broker went read-only |
| `journal.recovering` | INFO | progress, once per second |
| `journal.recovered` | INFO | `records`, `duration_ms`, `state_hash` |
| `journal.truncated` | WARN | `dropped_bytes`, `dropped_records` |
| `dlq.redelivered` / `dlq.dropped` / `dlq.evicted` | WARN | `actor` |

### 7.3 Universal fields

Every event carries, where applicable: `ts`, `level`, `event`, `stream`, `seq`, `msg_id`, `consumer`, `attempt`, `dispatch_count`, `fail_count`, `trace_id`, `span_id`, `bytes`, `session`, `actor`.

`trace_id`/`span_id` follow W3C Trace Context: if a publisher supplies `traceparent`, messq propagates it onto every subsequent event for that message *and onto the delivery headers*, so a consumer's own traces link back to the publish. If absent, messq mints a 16-byte trace id from `crypto/rand` at publish. **The trace id is stored in the `PUB` record**, so it survives restarts and appears in `messq trace`.

### 7.4 Sampling, honestly

At 50k msg/s, `delivery.sent` at one line per message is not operable. Policy:

- `--log-sample delivery.sent=1/100` (and `delivery.ack`, `delivery.progress`) is supported.
- **`delivery.nak`, `lease.expired`, `message.dead_lettered`, `consumer.blocked`, `retention.discard_undelivered`, `journal.truncated`, `storage.fatal` are never sampleable.** The code enforces this: the sampler consults an allow-list, and a config naming a non-sampleable event fails `messq config check`.
- Sampled-away events are still counted in metrics, and are still in the journal — `messq trace` remains complete regardless of sampling. This is exactly why the journal, not the log, is the audit source of truth.

### 7.5 Metrics

`prometheus/client_golang` with an explicit `prometheus.NewRegistry()` and `promauto.With(reg)` (fetched docs) — **no use of the default global registry**, so what messq exports is exactly what messq declares. Served via `promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg, MaxRequestsInFlight: 4})` on the admin listener.

```
messq_published_total{stream,subject_prefix}
messq_publish_duplicate_total{stream}
messq_publish_rejected_total{stream,reason}
messq_commit_duration_seconds{stream}            # NATIVE histogram
messq_fdatasync_duration_seconds{stream}         # NATIVE histogram
messq_delivered_total{stream,consumer,redelivery="true|false"}
messq_acked_total / messq_nakked_total / messq_termed_total{stream,consumer}
messq_lease_expired_total{stream,consumer}
messq_dead_lettered_total{stream,consumer,reason}
messq_consumer_pending{stream,consumer}          # backlog (lag)
messq_consumer_ack_pending{stream,consumer}      # in-flight
messq_consumer_ack_gap{stream,consumer}          # cursor - ack_floor  (I6 headroom)
messq_consumer_oldest_unacked_seconds{stream,consumer}
messq_stream_head_seq / messq_stream_bytes{stream}
messq_journal_truncated_bytes_total{stream}
messq_recovery_duration_seconds{stream}
messq_readonly{stream}                           # 1 = storage.fatal latched
```

Native histograms for the two latency families: the fetched client_golang source shows native histograms encode buckets as sparse spans inside a single metric rather than one series per bucket, which is what makes a wide-range fsync-latency histogram (µs to seconds) affordable.

A shipped Grafana dashboard and a shipped `messq-alerts.yaml` with four alerts that actually matter: `oldest_unacked_seconds` rising, `ack_gap` near `max_ack_gap`, `dead_lettered_total` rate > 0, `fdatasync p99 > 250 ms`.

### 7.6 Tracing

OpenTelemetry spans behind a build tag and off by default (`--otlp-endpoint` to enable), using `connectrpc/otelconnect-go` for RPC spans plus manual spans for `publish→commit` and `dispatch→ack`. Optional because a correctness-first broker should not require a collector to be operable — the logs and `messq trace` are the primary path.

---

## 8. Testing strategy

The claim "correctness purist" is only credible if the invariants in §4.10 are mechanically checked. Every invariant has a named test; `docs/guarantees.md` is generated from that mapping so an untested invariant is a build failure.

### 8.1 Layer 0 — unit & fuzz on the frame codec

- Round-trip property tests for every record type.
- `FuzzParseFrame` (native Go fuzzing) over arbitrary bytes: must never panic, never allocate unboundedly, never return a record for corrupt input. Seed corpus checked in; CI runs `-fuzztime=60s` per PR and 30 min nightly.
- Golden-file tests pinning the byte layout of every record type. Changing the layout requires deliberately regenerating goldens and bumping `rec_version`.

### 8.2 Layer 1 — the crash model and the crash-point enumerator

All storage code goes through a narrow interface; nothing in `internal/journal` touches `os` directly.

```go
type FS interface {
    Create(name string) (File, error)
    Open(name string) (File, error)
    Rename(old, new string) error
    Remove(name string) error
    SyncDir(name string) error
    List(dir string) ([]string, error)
}
type File interface {
    io.Writer; io.ReaderAt
    Sync() error            // fdatasync
    Truncate(int64) error
    Fallocate(int64) error
    Size() (int64, error)
    Close() error
}
```

**The messq crash model**, stated explicitly (a test suite without a stated crash model tests nothing in particular):

> On a simulated crash, every byte written since the last successful `Sync()` on that file may independently be present, absent, or garbage; the most recent write may be *torn* at an arbitrary offset; a `Rename` not followed by a successful `SyncDir` may be absent; and a file created without a successful `SyncDir` may be absent even if its contents were synced.

This is deliberately more adversarial than real filesystems, and it is the model under which the Recovery Theorem is provable.

`test/crash` implements an enumerator: run a workload against `FaultFS` recording every write/sync boundary, then for each boundary *b* replay the workload up to *b*, apply the crash model (including a sweep of torn offsets), run recovery, and assert:

- I1: every message whose `PubAck` was observed before *b* is present.
- I2: no message is present that was never committed.
- I12: recovery is idempotent and the state hash is stable.
- No panic, no infinite loop, and `messq verify --deep` passes on the resulting directory.

Additional injected faults: `ENOSPC` on write, `EIO` on `Sync` (must latch read-only mode, not continue), short writes, `Rename` losing the directory entry.

Nightly: `dm-log-writes` / `dm-flakey` under a VM to validate the model against a real kernel and a real power-cut simulation. Stretch goal for M7, not a gate.

### 8.3 Layer 2 — model-based testing of the lifecycle

`internal/model` contains a second, independent, deliberately naive implementation of §4.4 — a map from `(consumer, seq)` to a state, ~200 lines, optimised for obviousness rather than speed. It is written by reading the transition table, not by reading `internal/stream`.

`flyingmutant/rapid` drives both (fetched docs: `t.Repeat(map[string]func(*rapid.T))` with the `""` key acting as an after-every-action invariant check):

```go
func TestLifecycleAgainstModel(t *testing.T) {
    rapid.Check(t, func(t *rapid.T) {
        real, model := newRealBroker(t), newModel()
        t.Repeat(map[string]func(*rapid.T){
            "publish":   ..., "dispatch": ..., "ack": ..., "nak": ...,
            "term":      ..., "expire":   ..., "restart": ..., "seek": ...,
            "stale_ack": ...,   // T2 with an old attempt — must still retire (I4a)
            "stale_nak": ...,   // T3 with an old attempt — must be rejected (I4b)
            "":          func(t *rapid.T) { assertInvariantsI3toI9(t, real, model) },
        })
    })
}
```

Failures reproduce via `-rapid.seed=N` and shrink automatically; every discovered failure gets its `.rapid/…/*.fail` file checked in as a permanent regression case, run with `-rapid.failfile`.

The `restart` action is the important one: it serialises the real broker's journal, recovers from it, and asserts the model's post-restart projection (which knows the volatile-lease rule of T14) matches.

### 8.4 Layer 3 — deterministic time

Every timing-dependent test — `ack_wait` expiry, backoff schedules, `ack_sync_interval`, heartbeats, `consumer.lag` emission, DLQ retention — runs inside `testing/synctest` (stable in Go 1.25). Inside a bubble the `time` package uses a fake clock that only advances when every goroutine in the bubble is durably blocked, so a 10-minute backoff schedule is tested in microseconds with zero flakiness and zero `time.Sleep`.

Rule enforced by a lint check: **no `time.Sleep` in `_test.go` files outside `test/soak`.** A sleeping test is a flaky test.

Leases themselves use a monotonic base (`time.Since(start)`), never wall clock, so an NTP step cannot expire leases early. A dedicated test steps the wall clock ±1 h mid-lease and asserts nothing expires.

### 8.5 Layer 4 — history checking

Randomised multi-consumer runs record the full event history, then an offline checker asserts the global properties that no single-step invariant catches:

- Every published message reaches a terminal state per consumer, or is still legitimately in-flight.
- No message is retired twice.
- Under `ordered_by`, the ack order per key is the seq order (I9).
- The union of delivered messages ⊇ the set of published messages minus retention-discarded (P2).
- `dispatch_count` per message ≤ `max_deliver + crashes` (P4's caveat, tested as an equality bound).

### 8.6 Layer 5 — integration, soak, performance

- `test/integration`: real binary, real Unix socket, real client, `testcontainers`-free (no Docker requirement).
- **Soak**: 24 h nightly, 10k msg/s, 20 % nak rate, 5 % consumer kills, 3 broker restarts, random `seek`/`purge`, ending with `messq verify --deep` and a full history check. Failure blocks the release.
- **Performance gates** (regression detection, not marketing): p99 publish latency with `sync=always` on NVMe; sustained throughput; recovery time for a 10 GB journal. Recorded per commit with `benchstat` comparison against the previous release.
- **Upgrade tests**: a corpus of data directories written by each released version, recovered by `main`. Format compatibility is a promise from v1.0.

### 8.7 CI gates

`go vet`, `staticcheck`, `golangci-lint`, `-race` on everything, `-msan`-equivalent not applicable, `go test ./... -race -count=2`, fuzz smoke, crash enumerator (fast subset), rapid at `-rapid.checks=1000`, and a coverage floor on `internal/journal` and `internal/stream` of 90 % branch coverage. Not a vanity number — those two packages are where a bug loses data.

---

## 9. Roadmap

Each milestone has a scope and an exit criterion. **The ordering is deliberately storage-first: no network code exists until recovery is proven.** Building the protocol first is how you end up retrofitting durability, and retrofitted durability is never quite right.

### M0 — Specification and skeleton (week 1)
Repo layout, module `github.com/messq/messq`, Go 1.25 toolchain pin, CI, licence.
`docs/spec/` containing: the record format (§3.4), the transition table (§4.4), the invariant list (§4.10), the crash model (§8.2). These are written *before* code and reviewed as documents.
**Exit:** `go build ./...` succeeds on an empty skeleton; the spec documents are merged and a `docs/guarantees.md` generator stub exists.

### M1 — Journal, recovery, fsck (weeks 2–4)
`internal/vfs` (FS interface + real impl + FaultFS). `internal/journal`: frames, CRC, segments, `SEGHDR`, Committer with group commit, `COMMIT` frames, `fdatasync` + directory fsync discipline, `fallocate`, segment rolling, sparse index, checkpoints with atomic rename, the recovery fold, state hashing. `messq journal dump` and `messq verify`.
**Exit:** the crash-point enumerator runs a publish-only workload across every boundary with torn-write sweeps and passes I1, I2, I12. Fuzz corpus green. **No network code merged yet.**

### M2 — Lifecycle engine (weeks 5–7)
`internal/stream`: StreamCore single-goroutine loop, consumer state, interval set, timer wheel, the full transition table T1–T18, token fencing (epoch + run_id + attempt), credit accounting, `max_ack_pending`, `max_ack_gap`, backoff schedules, dedup index, in-place DLQ, ordering keys. `internal/model` reference implementation.
**Exit:** rapid model-based tests pass at 100k checks including `restart`, `seek`, `stale_ack`, `stale_nak`. All of I3–I9, I11 have named passing tests. Still no network code.

### M3 — Protocol and consume path (weeks 8–10)
`proto/messq/v1`, buf codegen, Connect handlers, UDS listener with h2c, `Publish`, `PublishBatch`, `Consume` bidi with credit, `FetchBatch`/`AckBatch` fallback, error model, read-only mode on `EIO`, `sd_notify` readiness. Go client library with `Ack`/`Nak`/`Term`/`InProgress`/`Return` and the `Handler` wrapper.
**Exit:** integration tests publish and consume 1M messages across a broker restart with zero loss and a bounded, measured duplicate count. `curl` can drive `Publish` and `FetchBatch`.

### M4 — CLI and DX (weeks 11–12)
Cobra command tree, `--json` on everything, `serve --dev`, `stream`/`consumer` CRUD, `pub`/`sub`, `peek`, `pending`, `dlq`, confirmation prompts, exit codes, shell completions, systemd unit, `config check`.
**Exit:** a new engineer installs the binary and goes from zero to publish/consume/inspect/DLQ-redeliver in under five minutes following `docs/quickstart.md`, verified by an actual dry run with someone outside the team.

### M5 — Observability (weeks 13–14)
slog event taxonomy with the full field set, human handler, sampling with the non-sampleable allow-list, W3C trace propagation and storage, the Prometheus registry and metric set, `commit.slow` detection, `consumer.lag` emission, `/healthz` and `/readyz`, `messq trace` reconstructing history from the journal, Grafana dashboard, alert rules.
**Exit:** the "message stuck" runbook in `docs/runbooks/` is executable end-to-end using only `messq` commands and the dashboard, demonstrated on a deliberately broken consumer.

### M6 — Replay, retention, purge, seek (weeks 15–16)
`replay` (ephemeral consumer or stdout), `seek` with epoch bump and impact preview, `purge`, the three retention policies, segment reclamation under I10, DLQ retention and eviction, `expect_last_seq`.
**Exit:** I10 has a passing test including the "DLQ entry pins a segment" case; `retention.discard_undelivered` fires exactly when expected and never otherwise; replay of a 10 GB stream sustains sequential-read throughput.

### M7 — Hardening and the 1.0 contract (weeks 17–20)
24 h soak green three nights running. `verify --deep` and `--rebuild-derived`. Performance gates and `benchstat` baselines. `dm-log-writes` validation of the crash model on a real kernel. Format-version freeze and upgrade-test corpus. Security pass (UDS permissions, TLS, token authz, fuzzing the protocol layer). `docs/guarantees.md` generated and reviewed line by line against the implementation.
**Exit:** **v1.0.0.** The five promises in §1.2 are each backed by a named passing test, and the document saying so is generated from the test names.

### M8 — Phase 2, correctness-compatible subset (post-1.0)
Ordered in the sequence that adds least risk to the invariants:
1. **Delayed delivery** (`publish --deliver-at`) — a second timer wheel; no lifecycle change.
2. **Rate limiting** per consumer — a dispatch guard; strictly narrows T1, cannot break an invariant.
3. **Consumer groups with lease** — multiple sessions on one consumer; the pending set is already per-consumer, so this is a dispatch-target change plus session-death handling (already T18).
4. **Priority channels** — `priority` header selects among N sub-queues at dispatch; interacts with ordering, so gated behind `ordered_by=none`.
5. **Compression** (zstd per-record, `rec_version` bump) — touches the frame format, hence after the format has been stable for a release.
6. **Audit-trail export** — `messq journal export --since` producing NDJSON for a SIEM; trivially safe, it is a read path.
7. **Metrics endpoint hardening / OTLP** — already scaffolded in M5.

### M9 — Cluster mode (explicitly not planned)
A follow-on design document, not a feature. The honest position: adding replication means adding consensus, and adding consensus to a system whose selling point is "understandable in an evening" is a different product. If it happens, it is Raft over the existing journal (which is already a totally ordered log with LSNs, so the shape fits) with `PubAck{durable:true}` redefined as "committed to a quorum". Until then, the README says "single node" in the first paragraph.

---

## 10. Risks & open questions

### 10.1 Risks with mitigations

| # | Risk | Impact | Mitigation |
|---|---|---|---|
| R1 | **Storage lies about `fdatasync`** (consumer SSD with volatile cache, virtualised disk, `nobarrier`) | P1 silently false | `messq verify --disk-check` measures sync latency at startup and logs `storage.suspicious` at WARN when p50 `fdatasync` of a 4 KiB write is under ~50 µs on rotational or unbatched writes — a strong hint the cache is volatile. Document the `dm-log-writes` procedure. We cannot fix hardware; we can refuse to be quiet about it. |
| R2 | **Hand-rolled recovery has a bug SQLite would not have had** | Data loss | The entire §8.2 apparatus exists for this. Explicit revisit trigger at M7 (§3.1). |
| R3 | **Single node = single point of failure** | Availability, and total loss on disk failure | Stated in the first paragraph of the README; `messq backup` (M6 stretch: consistent snapshot via checkpoint + segment hardlinks) documented as the answer. |
| R4 | **`max_ack_gap` stalls a consumer in production** and the operator does not know why | Perceived outage | `consumer.gap_stalled` at WARN every 30 s, a dedicated metric, `FlowStalled{reason}` pushed to the client so the client library can log it too, and the runbook. The alternative — unbounded memory — is worse and less debuggable. |
| R5 | **ConnectRPC bidi requires end-to-end HTTP/2**; a proxy or an HTTP/1-only environment breaks `Consume` | Integration friction | UDS default (HTTP/2 guaranteed), `FetchBatch` fallback with identical semantics, and a startup log line stating which protocols each listener supports. |
| R6 | **Ordered consumers block on poison by default**, surprising operators expecting throughput | Perceived hang | Loud `consumer.blocked` at ERROR, a dedicated alert, and the `on_poison` setting printed in `messq consumer info`. The surprise is preferable to a silent order violation. |
| R7 | **Group commit latency under `sync=always` on slow disks** makes publish p99 unacceptable | Adoption | 1 ms linger amortises fsync across concurrent publishers; `PublishBatch` for bulk; `commit.slow` makes the cause visible; `sync_mode=interval` exists as an explicit, client-visible downgrade. |
| R8 | **Log volume at high throughput** | Cost, and log loss | Sampling with a non-sampleable allow-list, plus the fact that the journal — not the log — is the audit source. |
| R9 | **In-place DLQ pins segments**, so a forgotten dead letter blocks retention | Disk growth | `dlq_max_age` / `dlq_max_entries` with `dlq.evicted` at WARN, plus `messq_dead_lettered_total` alerting. |
| R10 | **Scope creep toward clustering** | Loses the product | M9 is explicitly a design doc, not a milestone. |
| R11 | **Recovery time on a large journal** (10 GB+) delays startup | Availability | Checkpoint every 30 s / 64 MiB bounds the replay tail; recovery progress logged per second; parallel per-stream recovery; `messq_recovery_duration_seconds` tracked and gated in M7. |
| R12 | **Two implementations of the state machine drift** (`internal/stream` vs `internal/model`) | False confidence | The model is intentionally naive and is never optimised; any change to the transition table must land in the spec doc, the model, and the implementation in the same PR — enforced by a CODEOWNERS-style checklist. |

### 10.2 Open questions

1. **Should `ack_sync_interval` default to 200 ms or 0?** 200 ms trades a bounded redelivery window for a large reduction in fsync load. A `sync=always-acks` mode should exist; the question is whether *some* workloads (very low rate, high duplicate cost) should get it by default. **Leaning:** keep 200 ms, expose the setting, measure real deployments before changing.
2. **Interval-set representation for `retired_above`.** A sorted slice of intervals is simple and almost always tiny; a pathological interleaving (ack every odd seq) degrades it. Bounded by `max_ack_gap`, so worst case is 50k intervals ≈ 800 KB — acceptable, but worth a benchmark in M2.
3. **Should `Peek` count as a delivery?** Currently no: `peek` never leases, never increments counters, never affects the cursor. This makes inspection safe but means a `peek`-heavy operator sees no trace of their own reads. **Leaning:** keep it non-mutating, but log `inspect.peek` with the actor so the audit trail records who looked.
4. **Subject matching syntax.** NATS-style `a.b.*` / `a.>` is familiar and cheap; globs are more familiar to shell users. Committing to NATS-style for wire compatibility of intuition, but the matcher must be a sealed, fuzz-tested package because subject matching bugs silently misroute.
5. **Per-stream vs per-process journal.** Per-stream (chosen) gives independent recovery, independent retention and stream-granular parallelism, at the cost of one fsync per stream per commit window. A process with 200 low-rate streams pays 200× the fsyncs. **Mitigation to evaluate in M7:** a shared journal for streams marked `low_rate`, or a cross-stream commit coalescer. Not in the 1.0 scope; the limit (`recommended max ~50 active streams per instance`) goes in the docs until measured.
6. **`expect_last_seq` scope** — per stream or per subject? Per subject is more useful for event-sourcing (one aggregate per subject) but requires a per-subject last-seq index. **Leaning:** ship per-stream in M6, add per-subject in phase 2 if asked for.
7. **Do we need `Progress` at all**, or should long jobs simply configure a longer `ack_wait`? `Progress` costs a round trip and a fenced token check; a long `ack_wait` costs slow failure detection. Both ship; the docs recommend `ack_wait` sized to p99 processing time and `Progress` only for genuinely unbounded jobs.

---

## 11. Library choices

Dependency policy: **every direct dependency must justify its existence against the cost of an audit.** The target is fewer than ten direct non-test dependencies. Each entry below cites what the fetched documentation established.

### Adopted

**Go 1.25+ standard library, `testing/synctest`.**
`synctest` graduated from experiment to stable in Go 1.25. It runs a test function in an isolated bubble where the `time` package uses a fake clock (starting midnight UTC 2000-01-01) that advances only when every goroutine in the bubble is durably blocked, and `synctest.Test` waits for all bubble goroutines to exit. This is decisive for messq: `ack_wait`, backoff schedules, `ack_sync_interval`, heartbeats and DLQ retention are all timing behaviour, and testing them with real sleeps would produce a slow, flaky suite. It also removes any temptation to inject a clock interface through the entire codebase. *Cost:* pins the minimum Go version to 1.25 — acceptable for a new project.

**`log/slog` (stdlib).**
Fetched docs confirm `slog.NewJSONHandler(w, *HandlerOptions)` for line-delimited JSON, `HandlerOptions.AddSource`, `ReplaceAttr` for transforming or redacting attributes, `(*JSONHandler).WithAttrs` for pre-bound context, and the `LogValuer` interface for deferring expensive value expansion until a record is actually emitted. All four map directly onto requirements: JSON output, body redaction via `ReplaceAttr`, per-message context via `WithAttrs`, and lazy field expansion via `LogValuer` so sampled-away events cost almost nothing. A `Handler` is a small interface, so the human-readable renderer is ~150 internal lines rather than a dependency. Rejecting zap/zerolog costs a little throughput and saves an ecosystem-splitting dependency.

**`connectrpc.com/connect` + `google.golang.org/protobuf` + `buf`.**
Fetched docs establish exactly what the design needs and what it must work around:
- `connect.NewBidiStreamHandler[Req, Res](procedure, func(ctx, *BidiStreamForHandler[Req,Res]) error, ...)` with `stream.Receive()` / `stream.Msg()` / `stream.Send()` / `stream.Err()` — the natural shape for the credit-based `Consume` loop.
- The protocol reference states plainly that bidirectional streaming requires HTTP/2 while unary, client- and server-streaming work over HTTP/1.1, and that Connect uses no HTTP trailers. This is why `Consume` is HTTP/2-only and why `FetchBatch` exists (§5.3).
- h2c is configured with `http.Protocols` — `p.SetHTTP1(true); p.SetUnencryptedHTTP2(true)` on the server, `p.SetUnencryptedHTTP2(true)` in the client's `http.Transport` — and the docs warn the deprecated `Upgrade: h2c` path is unsupported. Our listener setup and the startup log line are written directly from this.
- `connect.NewError(code, err)`, `connect.CodeOf(err)`, `(*connect.Error).Details()/Meta()` and `connect.NewErrorDetail(protoMsg)` give the structured error model of §5.4 without hand-rolling one.

The decisive property is that one handler implementation serves gRPC clients, browser clients and `curl` alike. A hand-written binary protocol would be more auditable in isolation but would force a second implementation for the ops/REST surface, and two implementations of §4's rules is a correctness risk far larger than the framing code it saves.

**`spf13/cobra` + `spf13/pflag`.**
Fetched docs cover the pieces we rely on: the `RunE`/`PersistentPreRunE` execution chain (so every command returns an error to a single exit-code mapper), `SilenceErrors`/`SilenceUsage` (so a runtime failure prints an error, not a usage wall — important for an ops tool), `MarkPersistentFlagRequired`, `SetFlagErrorFunc` for uniform flag-error formatting, `GenBashCompletion(w)` plus `CompletionOptions` for shipped completions, and `ValidArgsFunction` for dynamic completion of stream and consumer names (which is a genuine ergonomic win: tab-completing consumer names against a live broker). Cobra is a large-ish dependency but it is the de facto standard for Go ops binaries and the alternative is reimplementing subcommand routing and completion.

**Explicitly *not* `spf13/viper`.** Configuration is a single TOML file plus flags, parsed by a small internal package with an explicit, documented precedence (`flag > env > file > default`). Viper's implicit precedence and key-case behaviour is a category of production surprise a correctness-first project should not import — and `messq config check` must be able to print the exact resolved value and its source for every setting.

**`prometheus/client_golang`.**
Fetched docs give the pattern we adopt verbatim: `prometheus.NewRegistry()` with `promauto.With(reg)` bound into a `Metrics` struct rather than package-level globals, and `promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg, MaxRequestsInFlight: N})` for exposition. Avoiding the default global registry means messq exports exactly its declared metrics and nothing a transitive dependency happens to register. The native-histogram support (buckets encoded as sparse spans inside a single `dto.Histogram` rather than one series per bucket) is what makes `messq_fdatasync_duration_seconds` affordable across a µs-to-seconds range — precisely the metric that diagnoses R1 and R7.

**`flyingmutant/rapid` (test-only).**
Fetched docs confirm the two features the strategy depends on: `t.Repeat(map[string]func(*rapid.T))` for stateful/model-based testing, where the `""` key action runs after every other action as an invariant check and `t.Skip` marks an action inapplicable in the current state; and reproducible failures via `-rapid.seed`, saved failure files replayed with `-rapid.failfile`, plus `-rapid.checks` and `-rapid.shrinktime` for CI tuning. Automatic shrinking is what makes a 400-step counterexample into a 6-step one you can read. This maps one-to-one onto §8.3.

**`golang.org/x/sys/unix` (test and runtime).**
Needed for `Fdatasync` (Go's `os.File.Sync` is `fsync`, which additionally flushes inode metadata we do not need), `Fallocate` (segment preallocation), and `Flock` (single-instance guard). Three syscalls, one well-maintained dependency, no alternative in the standard library.

**`google/go-cmp` (test-only)** for structural diffs in model comparisons.

### Rejected, with reasons

**`etcd-io/bbolt`.** The fetched documentation states bbolt's durability is "fsyncing to disk after every commit" via a two-phase write (dirty pages + `fsync`, then a new meta page + `fsync`). Two syncs per commit against a copy-on-write B+tree is severe write amplification for a 40-byte `ACK`. The documented mitigations make it worse for us: `NoSync` discards the guarantee wholesale with no per-operation visibility, and `DB.Batch`'s contract — "the provided function must be idempotent as it may be called multiple times" — is a live footgun for a function that increments `fail_count`. bbolt is also single-writer, so it does not relieve the single-writer discipline we already impose. Most importantly, we need a durable append log for bodies regardless; adding bbolt creates a second durability domain and a cross-domain crash-consistency protocol, which is precisely the complexity this plan exists to avoid.

**`modernc.org/sqlite` / `mattn/go-sqlite3`.** The fetched docs show durability is configured through the DSN and pragmas: `_journal=WAL`, `_synchronous=NORMAL`, `_txlock=immediate`, `_timeout=5000`, `_pragma=...`. That places P1 — the central promise — inside a connection string, where config drift can silently downgrade it. Add the log-shaped-workload mismatch (row-delete churn and `VACUUM` instead of `unlink()`, index range scans instead of sequential reads), the single-writer serialization, and the fact that group commit must be hand-rolled anyway, and the case does not close. `mattn/go-sqlite3` additionally requires cgo, breaking the static-binary goal. Recorded as a real alternative with a named revisit trigger (§3.1), not dismissed.

**`grpc-go` directly.** Would give bidi streaming with a mature ecosystem, but no HTTP/JSON surface without also running grpc-gateway — a second codegen path and a second server. Connect delivers both from one handler.

**`google/uuid` / `oklog/ulid`.** Message identity is `(stream, seq)`, which is already unique, ordered and meaningful; `msg_id` is publisher-supplied; trace IDs are 16 random bytes from `crypto/rand` formatted as hex per W3C Trace Context. No dependency needed.

**`cespare/xxhash` and friends.** `hash/crc32` with `crc32.MakeTable(crc32.Castagnoli)` is hardware-accelerated on every target CPU and is the standard choice for WAL frame checksums. Stdlib wins.

**`testify`.** Standard library `testing` plus `go-cmp` covers it. `assert`-style APIs that continue after a failed assertion produce confusing cascades in state-machine tests where the first failure invalidates everything after it.

---

## Appendix A — Repository layout

```
cmd/messq/                 main + cobra wiring
internal/vfs/              FS/File interfaces, real impl, faultfs
internal/journal/          frames, segments, committer, checkpoints, recovery, index
internal/stream/           StreamCore, consumer state, timer wheel, transition table
internal/model/            independent reference implementation of §4.4 (test-only build tag)
internal/dedup/            bounded publish-dedup index
internal/subject/          subject matcher (sealed, fuzzed)
internal/rpc/              connect handlers, listeners, auth
internal/obs/              slog events, human handler, metrics registry, tracing
internal/cli/              command implementations
internal/config/           TOML + flags, explicit precedence, `config check`
client/                    public Go client library
proto/messq/v1/            .proto sources; buf.yaml, buf.gen.yaml
test/crash/                crash-point enumerator
test/integration/
test/soak/
docs/spec/                 record format, transition table, invariants, crash model
docs/runbooks/
docs/guarantees.md         GENERATED from spec + test names
```

## Appendix B — Default configuration

```toml
[server]
listen        = "unix:///run/messq/messq.sock"
admin_listen  = "127.0.0.1:9469"
data_dir      = "/var/lib/messq"
log_format    = "json"
log_level     = "info"

[storage]
segment_bytes           = "64MiB"
segment_max_age         = "24h"
checkpoint_interval     = "30s"
checkpoint_bytes        = "64MiB"
sync_mode               = "always"     # or "interval:20ms" (PubAck.durable = false)
ack_sync_interval       = "200ms"
commit_linger           = "1ms"
commit_batch_max        = "4MiB"
commit_slow_threshold   = "250ms"

[stream_defaults]
max_msg_size    = "1MiB"
retention       = "limits"
max_age         = "168h"
max_bytes       = "10GiB"
dedup_window    = "2m"
dedup_max_entries = 100000

[consumer_defaults]
ack_wait        = "30s"
max_deliver     = 5
backoff         = ["1s", "5s", "30s", "2m", "10m"]
max_ack_pending = 1000
max_ack_gap     = 100000
ordered_by      = "none"
on_poison       = "dead_letter"        # forced to "block" when ordered_by != "none"
dlq_max_age     = "720h"
dlq_max_entries = 10000
```

---

## Sources

- [Reliable Message Delivery in NATS JetStream: Acks, Retries, Dead Letters, and Replay — Synadia](https://www.synadia.com/blog/jetstream-reliable-delivery-dlq-replay)
- [JetStream Model Deep Dive — NATS Docs](https://docs.nats.io/using-nats/developer/develop_jetstream/model_deep_dive)
- [Consumers — NATS Docs](https://docs.nats.io/nats-concepts/jetstream/consumers)
- [Infinite message deduplication in JetStream — NATS blog](https://nats.io/blog/new-per-subject-discard-policy/)
- [NATS Large Deduplication Window: Causes, Diagnosis, and Fixes — Synadia](https://www.synadia.com/insights/checks/nats-large-deduplication-window)
- [Quorum Queues — RabbitMQ](https://www.rabbitmq.com/docs/quorum-queues)
- [RabbitMQ 4.3 Highlights](https://www.rabbitmq.com/blog/2026/04/23/rabbitmq-4.3-release)
- [RabbitMQ consumer timeout — Netdata](https://www.netdata.cloud/guides/rabbitmq/rabbitmq-consumer-timeout/)
- [NSQ Features & Guarantees](https://nsq.io/overview/features_and_guarantees.html)
- [NSQ Design](https://nsq.io/overview/design.html)
- [XAUTOCLAIM — Redis Docs](https://redis.io/docs/latest/commands/xautoclaim/)
- [Streams Consumer Group Patterns — antirez](https://redis.antirez.com/fundamental/streams-consumer-patterns.html)
- [Message Delivery Guarantees for Apache Kafka — Confluent](https://docs.confluent.io/kafka/design/delivery-semantics.html)
- [Kafka Internals: Segments, Segment Size & Indexes — Conduktor](https://www.conduktor.io/kafka/kafka-topics-internals-segments-and-indexes)
- [KAFKA-3359 Parallel log-recovery of un-flushed segments on startup](https://issues.apache.org/jira/browse/KAFKA-3359)
- [hashicorp/raft-wal — design README](https://github.com/hashicorp/raft-wal/blob/main/README.md)
- [Torn Write Detection and Protection — transactional.blog](https://transactional.blog/blog/2025-torn-writes)
- [Building a Corruption-Proof Write-Ahead Log in Go — UnisonDB](https://unisondb.io/blog/building-corruption-proof-write-ahead-log-in-go/)
- [Everything You Always Wanted To Know About fsync()](https://blog.httrack.com/blog/2013/11/15/everything-you-always-wanted-to-know-about-fsync/)
- [Durability: Linux File APIs — Evan Jones](https://www.evanjones.ca/durability-filesystem.html)
- [pgsql: Avoid unlikely data-loss scenarios due to rename() without fsync](https://www.postgresql.org/message-id/E1adrE0-0001Os-CD%40gemulon.postgresql.org)
- [The Write Stuff: Concurrent Write Transactions in SQLite — Oldmoe](https://oldmoe.blog/2024/07/08/the-write-stuff-concurrent-write-transactions-in-sqlite/)
- [Testing concurrent code with testing/synctest — The Go Blog](https://go.dev/blog/synctest)
- [synctest package — pkg.go.dev](https://pkg.go.dev/testing/synctest)
- [(Mostly) Deterministic Simulation Testing in Go — Polar Signals](https://www.polarsignals.com/blog/posts/2024/05/28/mostly-dst-in-go)
- [Deterministic simulation testing — Antithesis Docs](https://antithesis.com/docs/resources/deterministic_simulation_testing/)
- [Scalable and Accurate Application-Level Crash-Consistency Testing](https://arxiv.org/pdf/2503.01390)
- [Connect protocol reference](https://connectrpc.com/docs/protocol)
- [Connect Go — streaming](https://connectrpc.com/docs/go/streaming)
- [Connect Go — deployment / h2c](https://connectrpc.com/docs/go/deployment)
- [bbolt — etcd-io/bbolt](https://github.com/etcd-io/bbolt)
- [modernc.org/sqlite — configuration](https://gitlab.com/cznic/sqlite/-/blob/master/_autodocs/configuration.md)
- [log/slog — pkg.go.dev (go1.25.3)](https://pkg.go.dev/log/slog@go1.25.3)
- [Cobra — pkg.go.dev](https://pkg.go.dev/github.com/spf13/cobra)
- [prometheus/client_golang](https://github.com/prometheus/client_golang)
- [flyingmutant/rapid](https://github.com/flyingmutant/rapid)
