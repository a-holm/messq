# messq — Project Plan (Persona: SRE / Day-2 Operator)

**Author lens:** the person who gets paged at 03:00, has never read the source, has `ssh`, `journalctl`, `curl`, `df` and the `messq` binary, and needs to know *what happened to message X* and *what is safe to do next*.

**Thesis:** For a broker of this size, operability is not a feature layer on top of the queue — it *is* the product. Throughput is a commodity; a queue you can reason about at 3am is not. Every design decision below is resolved in favour of the operator, and where that costs performance the cost is stated and bounded.

---

## 1. Vision & positioning

### 1.1 What messq is

A single Go binary that stores messages in append-only per-stream logs on local disk and hands them to consumers under an explicit at-least-once contract (deliver → ack | nak | term | timeout → redeliver → dead-letter). Semantics are deliberately copied from NATS JetStream — `ack`, `nak`, `term`, `ack_wait`, `max_deliver`, `max_ack_pending` — because that vocabulary is already correct, already understood, and already documented for a generation of engineers. messq's contribution is not new semantics; it is *making the state of those semantics visible and safe to manipulate*.

### 1.2 The 3am test (the product requirement)

An operator who has never seen the codebase must answer these four questions in under 60 seconds, from the CLI, without a dashboard, with the daemon under load:

| # | Question | Answer surface |
|---|---|---|
| Q1 | Where is message `01J8…` now, and everything that ever happened to it? | `messq msg trace <mid>` |
| Q2 | Which consumer is behind, by how many messages and by how much wall-clock time? | `messq consumer ls --sort=lag` |
| Q3 | Why is the disk growing, and what will happen when it is full? | `messq disk` |
| Q4 | What is in-flight right now, to which client, and when does it expire? | `messq inflight <stream>/<consumer>` |

Every architectural choice in §3–§7 is traceable to one of these four questions or to a failure mode in §10.4. If a feature does not serve those, it is Phase 2 or it is out (§1.5).

### 1.3 Trust is multiplicative

messq is trusted only if **all** of these hold; any single zero makes the product zero, regardless of feature count:

```
trust = bounded_resources × legible_state × safe_control_actions × predictable_lifecycle × recoverable_failure
```

- **bounded_resources** — no unbounded quantity anywhere: disk bytes, pending entries, in-flight per consumer, connections, goroutines, *and log volume*. Each has a ceiling and a documented action at the ceiling (§8.2).
- **legible_state** — the stable message ID appears in every log line and every API response; the pending set is a queryable table, not an internal map (§7).
- **safe_control_actions** — every destructive verb has `--dry-run`, name-confirmation, and an audit record (§6.4).
- **predictable_lifecycle** — start/reload/drain/stop/upgrade each have exactly one meaning and a bounded worst case (§8).
- **recoverable_failure** — worst case after `kill -9` is "lose at most the un-fsynced tail, *detect it*, log exactly which sequences were lost, continue" (§3.6).

### 1.4 Positioning

| | messq | NATS JetStream | Kafka | RabbitMQ | beanstalkd / NSQ |
|---|---|---|---|---|---|
| Deploy unit | 1 binary + 1 dir | cluster-capable | JVM + coordination | Erlang + plugins | 1 binary |
| HA | **none, by design** | R3/R5 RAFT | ISR replication | quorum queues | none/limited |
| Replay | first-class CLI | yes | yes | no | no |
| Per-message audit trail | **first-class, default on** | logs+advisories | no | no | no |
| Backpressure signal | **typed error code** | flow control msgs | producer throttle | TCP block (opaque) | limited |
| Understandable in an evening | **yes** | mostly | no | no | yes |

messq wins on: *a team of 3 running 4 internal services needs retries that survive a reboot, and needs to explain to an auditor what happened to invoice #4711 last Tuesday.* messq loses on: *anything requiring survival of a node loss.* That losing case is written into the README, not hidden.

### 1.5 Negative space (deliberate non-features, permanently)

Each omission below is a stated guarantee that the operator does *not* have to reason about it:

| Not built | Failure it prevents | If you need it |
|---|---|---|
| Replication / consensus | split-brain, quorum-loss debugging, ISR mysteries | NATS JetStream, Kafka, Redpanda |
| Exactly-once | distributed-transaction reasoning at 3am | idempotent consumers + `mid` dedupe key |
| Cross-stream transactions | deadlock, partial-commit forensics | redesign the stream boundary |
| Plugin/extension system | "which plugin broke the broker?" | fork; the code is small |
| Auth backends (LDAP/OIDC) | credential-path outages | mTLS + static token file |
| Query language | unbounded-cost queries against a broker | export + DuckDB/`jq` |
| Clustering in v1 | 90% of the failure-mode catalogue | run one per service; see §11 exit path |

**Exit ramp is a feature.** `messq export` produces a documented newline-delimited JSON stream (`{mid, seq, subject, headers, body_b64, ts}`) that can be replayed into NATS/Kafka with 30 lines of glue. Leaving messq must be cheap, or teams cannot afford to adopt it.

---

## 2. Architecture overview

### 2.1 Process model

**One process. No supervisor tree, no helper daemons, no sidecars.** `messq serve` under systemd (`Type=notify`). The same binary is the CLI; `messq <verb>` talks to the daemon over a Unix socket by default.

Rationale (operator): one PID to find, one cgroup to limit, one journal unit to read, one binary to roll back.

### 2.2 Goroutine topology

Every goroutine is owned by a supervisor that holds its `context.Context` and a `sync.WaitGroup` entry, is registered in a global registry with a name, and is visible via `messq debug goroutines`. NSQ's stated lesson — *"an orphaned goroutine is a memory leak, and memory leaks in long-running daemons are bad, especially when the expectation is that your process will be stable when all else fails"* — is enforced structurally: `go` statements outside `supervisor.Spawn(name, fn)` fail CI via a `go vet`-style custom analyzer.

```
main
├─ supervisor (owns ctx, WaitGroup, goroutine registry)
├─ lifecycle          1  — state machine, signal handling, sd_notify, watchdog ping
├─ resourceMonitor    1  — statfs/fd/mem/inode sampling, watermark FSM, degraded flags
├─ stateCheckpointer  1  — batches bbolt writes (ack-state group commit)
├─ journalSink        1  — slog JSON writer + per-class token buckets
├─ metricsRegistry    0  — passive (prom collectors read under RLock)
├─ opsHTTP            1 + N per request  — /livez /readyz /healthz /metrics /api/v1/* /debug/pprof
├─ grpcServer         1 + 2 per active stream (grpc-go internal send/recv)
├─ per stream S:
│   ├─ writer[S]      1  — SOLE owner of the active segment fd; group-commit + fsync
│   ├─ retention[S]   1  — ticker: seal/trim/delete segments, enforce limits
│   └─ readers        pooled, read-only fds, page-cache reads, no locks on writer
└─ per consumer C:
    └─ coordinator[C] 1  — cursor, pending set, credit, deadline min-heap, backoff, DLQ routing
```

**Concurrency invariants (enforced by design, asserted in tests):**

1. `writer[S]` is the only goroutine that writes stream S's log. Publishers hand off via a bounded channel; a full channel *is* the backpressure signal.
2. `coordinator[C]` is the only goroutine that mutates consumer C's cursor/pending set. No lock is needed on the hot path; therefore no lock ordering to get wrong.
3. bbolt is written only by `stateCheckpointer`. All other components enqueue mutation intents. One writer ⇒ no bbolt write-lock contention, no long-running read transaction stalling the freelist.
4. No goroutine is spawned per message. Per connection: 2 (grpc). Per consumer: 1. This makes goroutine count a *predictable function of configuration*, which is exactly what an operator needs when reading `messq_goroutines`.

### 2.3 Data flow — publish

```
client --gRPC Publish--> handler
   ↳ admission: size limit? disk watermark? stream limits? conn rate?  ── reject → RESOURCE_EXHAUSTED{reason}
   ↳ assign mid (ULID) + trace_id (from header, else = mid)
   ↳ send to writer[S] bounded chan (cap = publish_queue_depth, default 4096)
        ── chan full → RESOURCE_EXHAUSTED{reason="publish_queue_full"}  (never block indefinitely)
writer[S]:
   ↳ append record to active segment (buffered, 8-byte aligned)
   ↳ accumulate group: until group_commit_bytes (1 MiB) OR group_commit_interval (2 ms) OR chan drained
   ↳ fdatasync(segment)                      ← THE durability line
        ── error → FATAL (see §3.5, fsyncgate)
   ↳ assign seq numbers, update in-memory last_seq
   ↳ reply PublishAck{mid, seq, ts} to every waiter in the group
   ↳ emit journal event=accepted (one line per message)
   ↳ notify subscribed coordinators (non-blocking signal, not a fan-out copy)
```

Publish latency = group-commit window + fsync. Documented as: *p50 ≈ 2–4 ms on ext4/NVMe with `group_commit_interval=2ms`.* Throughput scales with batch size, not with fsync count.

### 2.4 Data flow — consume

```
client opens ONE bidirectional stream: Consume(stream ConsumeRequest) returns (stream Delivery)
client → Attach{stream, consumer}
client → Credit{n}                       ← the ONLY flow-control mechanism
coordinator[C] loop, wakes on: new message | credit granted | deadline fires | backoff timer:
   while credit > 0 && len(pending) < max_ack_pending && !paused:
       pick next:
          1. redelivery due (backoff/nak-delay expired) — earliest deadline first
          2. else next unseen seq > delivered_seq matching subject filter
          3. else block
       if ordering=subject && subject already in-flight: skip (head-of-line, counted)
       read body from segment (page cache), build Delivery{delivery_id, mid, seq, attempt, deadline_at}
       insert pending entry + deadline; enqueue state mutation; credit--
       emit journal event=delivered
client → Ack{delivery_id} | Nak{delivery_id, delay} | Term{delivery_id} | Extend{delivery_id, by}
```

**Push vs pull is not a mode.** Push is `Credit(large)`; pull is `Credit(batch)` per iteration. One mechanism ⇒ one thing to explain, one thing to tune, one metric to alert on. This is the direct answer to the JetStream operational trap surfaced in research: *"When max_ack_pending is reached, the server stops delivering… this is an abrupt stop, not a gradual slowdown."* messq makes the stop legible: `messq consumer info` prints `delivery_blocked_by: max_ack_pending | zero_credit | paused | ordering` as a first-class field, and exports it as a metric.

---

## 3. Storage & durability design

### 3.1 The central decision: two engines, not one

The payload path and the state path have **opposite** access patterns. Forcing them into one engine is where both common designs fail:

| | Bodies | Consumer/ack state |
|---|---|---|
| Write pattern | append-only, write-once | high-churn insert/delete |
| Read pattern | sequential, page-cache friendly | point + range-by-deadline |
| Deletion | bulk, by age/size | individual, immediate |
| Size | GB | MB (bounded — see §3.4) |
| Needs transactions | no (CRC + torn-tail rule) | yes (with the log's committed offset) |
| Right engine | **segmented append-only log** | **embedded transactional KV** |

- SQLite-for-everything dies on delete amplification, page churn and `VACUUM` pauses when bodies live in rows.
- Log-for-everything dies because the pending set is not queryable and ack state recovery becomes O(entire log).

**Decision: custom segmented log for bodies + bbolt for state.** Both are ~500 LOC of integration each, both are boring, and each is optimal for its half.

### 3.2 On-disk layout

```
/var/lib/messq/                      (mode 0750, owned by messq:messq)
├── MANIFEST                         64B: magic, format_version u32, node_id (ULID), created_at, crc32c
├── LOCK                             flock(LOCK_EX|LOCK_NB); contains pid + boot_id
├── state.db                         bbolt
├── state.db.compacting              transient, only during offline compaction
├── audit.log                        append-only admin audit (JSONL, fsync per record, rotated by size)
└── streams/
    └── <stream-name>/
        ├── meta.json                human-readable MIRROR of stream config (never authoritative)
        ├── 0000000000000001.seg     64 MiB, fallocate'd at creation
        ├── 0000000000000001.idx     sparse index, fully rebuildable
        ├── 0000000000000002.seg
        └── ...
```

Segment filename = zero-padded `base_seq`. `ls` therefore sorts correctly and an operator can read the sequence range off the directory listing. Deliberate: `ls -la` is a debugging tool.

### 3.3 Segment file format

**Header (64 bytes, written and fsync'd at creation):**

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | magic `"MESSQSEG"` |
| 8 | 4 | `format_version` u32 |
| 12 | 8 | `stream_id` u64 (xxhash64 of name — name also in path) |
| 20 | 8 | `base_seq` u64 |
| 28 | 8 | `created_unix_nanos` u64 |
| 36 | 4 | `flags` u32 (bit0 = sealed) |
| 40 | 20 | reserved (zero) |
| 60 | 4 | `header_crc32c` |

**Record (8-byte aligned, never spans a segment):**

```
u32  total_len        (bytes after this field, including padding)
u32  crc32c           (Castagnoli, hardware-accelerated; covers everything after this field)
u64  seq
u64  ts_unix_nanos
[16] mid              (ULID, binary form)
u16  subject_len
u16  headers_len
u32  body_len
...  subject bytes
...  headers bytes    (canonical: sorted, len-prefixed key/value pairs)
...  body bytes
...  zero padding to 8-byte alignment
```

**Seal trailer** (appended when a segment is closed, then `flags|=sealed` rewritten and fsync'd):

```
magic "MESSQEND", record_count u32, last_seq u64, min_ts u64, max_ts u64,
subject_digest: u16 count + count × u64 xxhash64(subject),   ← segment skip-set
crc32c
```

The `subject_digest` is the cheap answer to filtered consumption: a consumer with a narrow subject filter can skip an entire sealed segment without reading it, bounding the cost of "one consumer cares about 0.1% of a 40 GB stream". Bounded size: capped at 4096 distinct hashes per segment; overflow sets a "dense" flag meaning "do not skip".

**Index (`.idx`)**: fixed 16-byte entries `{seq u64, offset u32, _pad u32}`, one per 4 KiB of segment. Purely an accelerator — deletable at any time; `messq store verify --rebuild-index` regenerates by scan. Never fsync'd on the hot path.

### 3.4 bbolt schema (state.db)

| Bucket | Key | Value | Bounded by |
|---|---|---|---|
| `meta` | `format_version`, `node_id` | — | O(1) |
| `streams` | name | StreamConfig (proto) | `max_streams` (default 256) |
| `stream_state` | name | `{first_seq,last_seq,bytes,msgs,active_segment,sealed_upto}` | `max_streams` |
| `consumers` | `<stream>\x00<consumer>` | ConsumerConfig (proto) | `max_consumers` (default 1024) |
| `cursor` | `<stream>\x00<consumer>` | `{ack_floor_seq, delivered_seq, last_active_unix}` | `max_consumers` |
| `pending` | `<stream>\x00<consumer>\x00<seq BE u64>` | `{mid,attempts,deadline,first_delivered,delivery_id,last_reason}` | **Σ `max_ack_pending`** |
| `deadline` | `<stream>\x00<consumer>\x00<deadline BE u64>\x00<seq BE u64>` | ∅ | = `pending` |
| `redeliver` | `<deadline BE u64>\x00<stream>\x00<consumer>\x00<seq BE u64>` | ∅ | = `pending` |
| `dlq_index` | `<stream>\x00<dlq_seq BE u64>` | `{orig_mid,orig_seq,reason,attempts}` | DLQ retention |
| `audit` | `<seq BE u64>` | admin action record (mirror of audit.log) | rotation |

**The bbolt caveat, handled explicitly.** bbolt never shrinks its file; freed pages go to a freelist and are reused, and long-running read transactions can bloat the freelist. This is a real, documented operational trap (etcd's `defrag` exists for exactly this reason). messq neutralises it three ways:

1. **Bound what goes in.** Every bucket above is bounded by *configuration*, not by throughput. Worst case with defaults (1024 consumers × 1024 `max_ack_pending` × ~120 B) ≈ 125 MB, and typical deployments are 100× smaller. Message *bodies never enter bbolt.*
2. **Measure it.** `messq_state_db_bytes` and `messq_state_db_free_page_ratio` are exported; an alert fires at ratio > 0.6 for 1 h.
3. **Fix it without an outage window you didn't plan.** `messq store compact` (offline, `bolt.Compact` into `state.db.compacting`, fsync, atomic rename) plus automatic compaction *at startup only* when ratio > 0.5 and size > 64 MiB — logged with before/after bytes. Never online, never surprising.

**bbolt tuning (grounded in the bbolt docs fetched via context7):** `FreelistType: FreelistMapType` (better with many free pages), `InitialMmapSize` = 128 MiB (avoids remap stalls during growth), `AllocSize` default (amortises truncate+fsync on growth), `NoSync: false`, `NoFreelistSync: false` (the docs are explicit that `NoFreelistSync` forces a full re-sync during recovery — unacceptable for startup-time predictability), `StrictMode: false` in production but **`true` in the crash-injection test suite**. Ack-state writes use `DB.Batch` with `MaxBatchSize`/`MaxBatchDelay` tuned to the checkpoint policy in §3.5.

### 3.5 fsync policy — the durability asymmetry

**This is the single biggest performance lever that does not weaken the user-visible guarantee, and it must be stated in the docs so operators know why duplicates appear after a crash.**

| Path | Policy | Worst-case loss | Why it's OK |
|---|---|---|---|
| Message body (segment) | `fdatasync` on group commit **before** `PublishAck` | none acknowledged | at-least-once requires it |
| Segment creation | `fallocate` + `fsync` + `fsync(dir)` | none | ENOSPC surfaces here, not mid-record |
| Segment seal | `fsync` | re-seal on recovery | idempotent |
| Ack/cursor state (bbolt) | batched checkpoint: **every 200 ms or 4096 mutations, whichever first** | ≤200 ms of acks | losing an ack ⇒ *duplicate delivery*, which at-least-once already permits |
| Deadline/pending inserts | same batch | ≤200 ms | expired-early redelivery = duplicate |
| Consumer create/delete, stream config, purge, seek | **synchronous, own transaction, fsync** | none | control-plane actions must never be lost |
| audit.log | `fsync` per record | none | it is evidence |

`durability.ack_checkpoint_interval` is configurable down to `0` (fsync every ack, for people who genuinely want it) with the throughput cost printed in the docs.

**fsync failure = fatal. No retry.** Linux clears the page error flag after the first failed `fsync`, so a retry can return success while the data never reached disk — the PostgreSQL "fsyncgate" bug that took 20 years to find, whose accepted fix was *panic instead of retry*. messq's policy:

```
fsync/fdatasync returns error
  → journal FATAL: event=fsync_failed, path, errno, stream, seq_range, "data may not be durable"
  → mark process UNSAFE; refuse all further writes
  → flush journal, sd_notify STOPPING=1
  → exit code 74 (EX_IOERR)
  → systemd Restart=on-failure re-runs recovery, which re-validates CRCs and truncates the torn tail
```

Never "log a warning and continue". The runbook entry `RB-04: fsync failure` says: check `dmesg` for media errors, check `df`/quota, check for a full or read-only filesystem, and **do not restart into the same disk without checking**, because a restart loop on a dying disk destroys evidence.

### 3.6 Crash recovery — exact algorithm

```
1. flock(LOCK, EX|NB)          → held? print holder pid+boot_id, exit 75 (EX_TEMPFAIL)
2. read MANIFEST
     format_version > binary   → exit 78 (EX_CONFIG) "data dir written by messq >= vX; upgrade binary or restore backup"
     format_version < binary   → exit 78 with "run: messq store migrate"   (NEVER auto-migrate, §8.5)
3. open state.db (bbolt); if free_page_ratio>0.5 && size>64MiB → compact now (logged)
4. state ← RECOVERING; /readyz = 503; sd_notify STATUS="recovering"
5. per stream, concurrently (bounded by GOMAXPROCS):
     a. list segments; verify header CRC of each; sealed segments are TRUSTED (not rescanned)
     b. open the highest (unsealed) segment
     c. seek to the last .idx checkpoint ≤ stream_state.last_seq (or header if no idx)
     d. scan forward record by record:
          - total_len sane (≤ max_record_size, fits in file)   else STOP
          - crc32c matches                                     else STOP
          - seq == expected                                    else STOP
        on STOP at offset O:
          - journal WARN event=torn_tail, stream, first_bad_seq, discarded_bytes, offset=O
          - ftruncate(segment, O); fsync; fsync(dir)
          - metric messq_recovery_torn_tail_bytes_total += (size - O)
     e. rebuild .idx for the scanned region
     f. stream_state.last_seq ← last good seq
6. reconcile: for each pending entry with seq > stream last_seq (ack-state ahead of a truncated log):
     drop the pending entry, journal WARN event=pending_orphaned  (crash between fsync policies)
7. rebuild in-memory pending sets + deadline heaps from bbolt
8. any pending entry with deadline in the past → deadline = now + jitter(0..redelivery_jitter)
9. state ← READY; sd_notify READY=1; /readyz = 200
```

**Recovery time is bounded and reported.** Worst case = `segment_size × num_streams` of sequential CRC scan (64 MiB × 16 streams ≈ 1 GB ≈ 2–4 s on NVMe). Progress is logged every 250 ms; `messq_recovery_duration_seconds` is a gauge that survives into steady state so you can see how long the last recovery took. `--verify-all` forces a full scan of sealed segments (minutes to hours; for post-incident forensics only).

**Restart-induced redelivery counts as an attempt.** A broker restart burns one retry from the consumer's budget. The alternative — not counting it — means a crash-looping broker can redeliver a poison message forever without ever reaching the DLQ, i.e. an unbounded failure mode. Bounded beats polite. The delivery carries header `Messq-Redelivery-Reason: broker_restart` and the journal line says so, so nobody has to guess.

### 3.7 Disk-full behaviour (the headline failure mode)

Three independent ceilings, checked in this order:

1. **Per-stream limits** — `max_bytes`, `max_msgs`, `max_age`, with `discard = old | new` (§4.6).
2. **Data-dir watermarks** — from `statfs`, sampled every 10 s, and **every 500 ms once free space is within 2× of the high watermark** (RabbitMQ samples up to 10×/s near the limit for the same reason: the last 5% goes fast).
3. **Absolute reserve** — `disk.reserve_bytes` (default `max(1 GiB, 3 × segment_size)`) is never allocated into, so there is always room to seal a segment, checkpoint state, and write the last log lines.

| Free space | State | Publish | Consume/Ack | Signals |
|---|---|---|---|---|
| > `low_watermark` (default 20%) | `OK` | accept | accept | — |
| ≤ low | `DEGRADED_DISK_LOW` | accept | accept | WARN every 60 s, `messq_disk_state=1`, alert |
| ≤ `high_watermark` (default 5% or `reserve_bytes`) | `READ_ONLY` | **reject** `RESOURCE_EXHAUSTED{reason="disk_high_watermark"}` | accept | ERROR, `messq_disk_state=2`, page |
| `fallocate` returns ENOSPC | `READ_ONLY` | reject | accept | ERROR event=segment_alloc_failed |
| `fsync` returns ENOSPC/EIO | `UNSAFE` | reject | reject | FATAL, exit 74 |

**Consumers keep running in `READ_ONLY`.** The whole point is that the backlog can drain its way out of the problem. Blocking consumption during a disk alarm is how a recoverable incident becomes an outage.

**`/readyz` stays 200 during `READ_ONLY`.** Learned directly from RabbitMQ-on-Kubernetes operational history: wiring disk alarms into the readiness probe causes the orchestrator to kill the node that is trying to drain itself, turning a disk alarm into a crash loop. Disk pressure is exported as `degraded`, not as `not ready` (§7.4).

**Preallocation makes ENOSPC survivable.** Because every segment is `fallocate`d in full at creation, the filesystem commits the space up front. ENOSPC is therefore observed at *segment rotation* — a clean, atomic, recoverable point — and never in the middle of appending a record.

---

## 4. Delivery semantics & message lifecycle

### 4.1 Identifiers

| Field | Form | Purpose |
|---|---|---|
| `mid` | ULID, 26-char Crockford base32 (48-bit ms timestamp + 80-bit monotonic entropy) | **stable, global, time-sortable, copy-pasteable** message identity; survives DLQ and replay |
| `seq` | uint64, per stream, gapless, monotonic | address within the stream; what you seek to |
| `delivery_id` | uint64, per (consumer, delivery attempt) | disambiguates a late ack from attempt N against attempt N+1 |
| `trace_id` | client header `Messq-Trace-Id`, else = `mid` | correlates across services |
| `attempt` | uint32, starts at 1 | redelivery counter, compared against `max_deliver` |

ULID over UUIDv4 because an operator reading a log can tell *when* a message was published by looking at it, and `sort` works. ULID over raw `seq` because `seq` is not unique across streams, DLQ copies, or purges — and the operator's question is "what happened to *this* message", not "what happened to slot 4711".

`delivery_id` is essential and frequently forgotten: without it, an ack that arrives after the ack-timeout has already redelivered the message will silently ack the *new* attempt, causing a message that was never processed to be marked done. messq rejects such an ack with `FAILED_PRECONDITION{reason="stale_delivery"}` and journals `event=stale_ack` — a metric worth alerting on, because it means `ack_wait` is too short.

### 4.2 Per-(message, consumer) state machine

```
                         ┌──────────────┐
      seq > delivered ───▶│ UNDELIVERED  │
                         └──────┬───────┘
              filter miss ──────┤ (subject filter / ordering block)
                    │           │ deliver  [credit>0 ∧ pending<max_ack_pending ∧ !paused]
                    ▼           ▼
              ┌──────────┐  ┌──────────┐
              │ SKIPPED  │  │IN_FLIGHT │◀────────────┐
              └──────────┘  └────┬─────┘             │
                                 │                   │ due
        Ack ─────────────────────┤              ┌────┴──────┐
                                 │              │ SCHEDULED │
        Extend(by) ──────────────┤ (deadline+=) └────▲──────┘
                                 │                   │
        Nak(delay) ──────────────┼───────────────────┤ attempt++
        deadline expired ────────┼───────────────────┤ attempt++, backoff[attempt]±jitter
                                 │                   │
                                 │  attempt ≥ max_deliver
                                 ├───────────────────┴──▶ ┌──────┐
        Term ────────────────────┼───────────────────────▶│ DEAD │──▶ $DLQ.<stream>
                                 ▼                        └──────┘
                            ┌────────┐
                            │ ACKED  │──▶ ack_floor advances when contiguous
                            └────────┘
```

**Transition table (this is the contract — implement exactly this):**

| From | Trigger | To | attempt | Journal event | Metric |
|---|---|---|---|---|---|
| UNDELIVERED | subject filter miss | SKIPPED | — | *(none — would be unbounded)* | `filtered_total` |
| UNDELIVERED | `ordering=subject` and subject busy | UNDELIVERED (blocked) | — | `head_of_line_blocked` (rate-limited) | `hol_blocked` gauge |
| UNDELIVERED | credit & capacity available | IN_FLIGHT | =1 | `delivered` | `delivered_total` |
| SCHEDULED | timer due, credit available | IN_FLIGHT | (already incremented) | `delivered` (with `redelivery_reason`) | `redelivered_total` |
| IN_FLIGHT | `Ack` | ACKED | — | `acked` | `acked_total`, `processing_seconds` |
| IN_FLIGHT | `Extend(by)` | IN_FLIGHT | unchanged | `extended` | `extends_total` |
| IN_FLIGHT | `Nak(delay)` | SCHEDULED (due = now+delay) | +1 | `naked` (+`reason`) | `naked_total` |
| IN_FLIGHT | `Nak`, attempt ≥ max_deliver | DEAD | +1 | `dead_lettered{cause=max_deliver}` | `dlq_total` |
| IN_FLIGHT | deadline expired | SCHEDULED (due = now+backoff±jitter) | +1 | `ack_timeout` | `ack_timeouts_total` |
| IN_FLIGHT | deadline expired, attempt ≥ max_deliver | DEAD | +1 | `dead_lettered{cause=ack_timeout}` | `dlq_total` |
| IN_FLIGHT | `Term(reason)` | DEAD | — | `dead_lettered{cause=terminated}` | `dlq_total` |
| IN_FLIGHT | client disconnects | SCHEDULED (due = now) | +1 | `delivery_abandoned` | `abandoned_total` |
| IN_FLIGHT | broker restart | SCHEDULED (due = now+jitter) | +1 | `delivery_abandoned{cause=restart}` | `abandoned_total` |
| any | `messq consumer seek` | UNDELIVERED/SKIPPED per new cursor | reset | `seek` (admin, audited) | — |
| any | `messq stream purge` | gone | — | `purged` (admin, audited) | — |
| IN_FLIGHT | ack with stale `delivery_id` | IN_FLIGHT (rejected) | — | `stale_ack` | `stale_acks_total` |

**`ack_floor` advances only over a contiguous acked prefix.** This is what makes `messq consumer info` truthful about "everything before X is definitely done" and what makes retention safe in `all_acked` mode (§4.6).

### 4.3 Redelivery backoff

Per-consumer `backoff = ["1s","5s","30s","2m","10m"]` (default), consumed by attempt index; the last entry repeats until `max_deliver`. **±20% jitter is applied always and is not configurable off** — a synchronised retry wave from a recovering downstream is a self-inflicted second outage, and there is no legitimate reason to want zero jitter.

`max_deliver` must be sized against the backoff schedule length; `messq consumer create` prints the computed total retry horizon (`"5 attempts over ~12m40s ±20%"`) and refuses `max_deliver` > 1 with `backoff=[]` unless `--ack-wait` is set, because "retry forever every 30 s" is a footgun someone must opt into visibly.

### 4.4 Dead-letter handling

A DLQ is a **real stream** named `$DLQ.<stream>`, created implicitly, with its own retention (default `max_age=30d`). Dead-lettering copies the body and adds headers:

```
Messq-Dlq-Origin-Stream, Messq-Dlq-Origin-Seq, Messq-Dlq-Origin-Mid,
Messq-Dlq-Consumer, Messq-Dlq-Attempts, Messq-Dlq-Cause,       (max_deliver|ack_timeout|terminated)
Messq-Dlq-Last-Reason, Messq-Dlq-First-Delivered, Messq-Dlq-Dead-At
```

The `mid` is **preserved**, so `messq msg trace <mid>` shows the whole story across both streams. That single property is what makes the DLQ useful rather than a graveyard.

Operational treatment follows the standard DLQ lifecycle: *alert on depth > 0*, triage before redrive, and never redrive into a still-broken consumer.

```
messq dlq ls <stream> --group-by=cause,subject   # what kinds of failure
messq dlq show <mid>                             # full record + headers + trace
messq dlq redrive <stream> --filter 'cause=ack_timeout' --limit 10 --rate 5/s --dry-run
messq dlq redrive <stream> --filter '...' --limit 10 --rate 5/s --confirm <stream>
messq dlq purge <stream> --before 2026-01-01 --confirm <stream>
```

`redrive` is **rate-limited and defaults to `--limit 100`**. It republishes to the origin stream with `Messq-Redrive-Of: <orig-mid>` and `Messq-Redrive-Count`, and refuses to redrive a message already redriven 3 times without `--force` — the "re-DLQ loop" is the most common way a redrive turns into a second incident.

### 4.5 Ordering

| `ordering` | Guarantee | Cost |
|---|---|---|
| `none` (default) | none across subjects; per-stream FIFO for first delivery only | max throughput |
| `subject` | at most one in-flight-or-scheduled message per subject; strict per-subject order | head-of-line blocking on nak; `hol_blocked` metric |
| `strict` | `max_ack_pending` forced to 1 | serial throughput; use for tiny critical streams |

`ordering=subject` is honest about its cost: a nak'd message blocks its subject until it succeeds or dead-letters. `messq consumer info` shows `blocked_subjects: 3` and `messq inflight --blocked` lists them.

### 4.6 Retention — where a queue quietly loses your data

The Kafka lesson from the research is unambiguous: a consumer goes offline, retention deletes the segments containing its position, and the offset becomes unrecoverable. That must not be the *default*.

| Mode | Deletion rule | Data loss risk | Default? |
|---|---|---|---|
| `all_acked` | segment deleted only when **every non-orphaned consumer's `ack_floor` is past it** *and* a limit is exceeded | none | **yes** |
| `limits` | segment deleted when a limit is exceeded, regardless of consumers (Kafka-like) | yes, loudly logged | opt-in |
| `interest` | segment deleted as soon as all consumers acked past it (no minimum retention) | none | opt-in |

In `all_acked`, when limits are exceeded but a consumer is behind, the stream enters **`BLOCKED_BY_CONSUMER`**: publishes are rejected with `RESOURCE_EXHAUSTED{reason="retention_blocked_by_consumer", consumer="…"}`. Refusing new data is strictly better than silently destroying accepted data. This is loud, specific, and immediately actionable — the error names the guilty consumer.

**Orphan protection** closes the obvious hole (a dead consumer blocking the stream forever): a consumer with no attach and no ack for `inactive_threshold` (default 7 d) is marked `ORPHANED`, excluded from the retention calculation, and screamed about hourly (`event=consumer_orphaned`, metric, and it shows in red in `messq consumer ls`). It is *never auto-deleted* — deleting consumer state is destructive and belongs to a human (`messq consumer rm`).

In `limits` mode, deletion that outruns a consumer emits `event=trim_ahead_of_consumer` with the exact lost seq range and increments `messq_msgs_dropped_total{reason="retention"}`. There is a default alert on that counter being non-zero.

---

## 5. API / protocol

### 5.1 Decision: gRPC data plane + read-only HTTP ops plane

Both are needed and they serve different people:

- **gRPC (protobuf, HTTP/2)** — the data plane. Gives bidirectional streaming, deadlines, typed status codes, per-stream flow control, keepalive with server-side enforcement, and generated clients in every language. Its weakness is that it is opaque to `curl`.
- **HTTP/JSON** — the ops plane, **read-only**, so that a 3am operator with nothing but `curl` and `jq` can answer Q1–Q4, and so that a leaked ops port cannot purge a queue.

Destructive administration lives in a **separate gRPC `Admin` service bound by default to the Unix socket only** (`/run/messq/messq.sock`, dir mode 0750, group `messq`). Exposing it over TCP requires explicitly setting `admin.listen` *and* configuring mTLS or a token file; the daemon refuses to start with `admin.listen` set on a non-loopback address without authentication configured.

`grpcurl` with server reflection (enabled by default on the Unix socket, off on TCP) is the documented fallback for poking the data plane by hand.

### 5.2 Listeners

| Listener | Default bind | Purpose | Auth |
|---|---|---|---|
| `data.listen` | `127.0.0.1:4390` | gRPC Publish/Consume | optional token / mTLS |
| `data.socket` | `/run/messq/messq.sock` | same, local | filesystem perms |
| `ops.listen` | `127.0.0.1:4391` | HTTP: health, metrics, read-only inspection | none by default (read-only) |
| `admin.listen` | *(unset)* | gRPC Admin | **required** if set |

Binding to `127.0.0.1` by default is deliberate: a broker that becomes reachable from the internet the moment it is installed is an operational liability. `messq serve --listen 0.0.0.0:4390` is a conscious act and logs a WARN if auth is not configured.

### 5.3 Proto sketch

```protobuf
syntax = "proto3";
package messq.v1;

// ---------- data plane ----------
service Publisher {
  rpc Publish(PublishRequest) returns (PublishAck);
  rpc PublishStream(stream PublishRequest) returns (stream PublishAck);   // pipelined, group-committed
}

service Consumer {
  // ONE bidirectional stream carries attach, credit and all acknowledgements.
  rpc Consume(stream ConsumeRequest) returns (stream Delivery);
}

message PublishRequest {
  string stream = 1;
  string subject = 2;
  bytes  body = 3;
  map<string,string> headers = 4;
  string trace_id = 5;                 // else server uses mid
  optional string dedupe_key = 6;      // best-effort window dedupe (Phase 2)
  optional uint64 expect_last_seq = 7; // optimistic concurrency; FAILED_PRECONDITION on mismatch
}
message PublishAck { string mid = 1; uint64 seq = 2; int64 ts_unix_nanos = 3; bool duplicate = 4; }

message ConsumeRequest {
  oneof op {
    Attach attach = 1;   // {stream, consumer, optional ephemeral_config}
    Credit credit = 2;   // {uint32 n}
    Ack    ack    = 3;   // {uint64 delivery_id}
    Nak    nak    = 4;   // {uint64 delivery_id, uint32 delay_ms, string reason}
    Term   term   = 5;   // {uint64 delivery_id, string reason}
    Extend extend = 6;   // {uint64 delivery_id, uint32 by_ms}
    Drain  drain  = 7;   // stop new deliveries, keep acking, then EOF
  }
}

message Delivery {
  uint64 delivery_id = 1;
  string mid = 2;
  uint64 seq = 3;
  string subject = 4;
  bytes  body = 5;
  map<string,string> headers = 6;
  uint32 attempt = 7;
  int64  first_delivered_at = 8;
  int64  deadline_at = 9;              // absolute — client can compute its own budget
  string redelivery_reason = 10;       // "", "ack_timeout", "nak", "broker_restart", "abandoned"
  uint64 stream_pending = 11;          // backlog after this message — free lag signal to the client
}

// ---------- control plane ----------
service Admin {
  rpc CreateStream(...)   returns (...);
  rpc UpdateStream(...)   returns (...);   // only mutable fields; rejects narrowing that would drop data
  rpc DeleteStream(...)   returns (...);   // requires confirm_name
  rpc PurgeStream(...)    returns (PurgeResult);      // supports dry_run
  rpc CreateConsumer(...) returns (...);
  rpc DeleteConsumer(...) returns (...);   // requires confirm_name
  rpc SeekConsumer(...)   returns (SeekResult);       // supports dry_run
  rpc PauseConsumer(...)  returns (...);   // stop delivery, keep accepting acks
  rpc RedriveDlq(...)     returns (stream RedriveProgress);
  rpc Drain(...)          returns (...);   // node-level drain
}

// ---------- read-only inspection (also exposed as HTTP/JSON) ----------
service Inspect {
  rpc Status(...)         returns (NodeStatus);
  rpc ListStreams(...)    returns (...);
  rpc StreamInfo(...)     returns (...);
  rpc ListConsumers(...)  returns (...);
  rpc ConsumerInfo(...)   returns (ConsumerInfo);   // includes delivery_blocked_by
  rpc ListPending(...)    returns (stream PendingEntry);
  rpc GetMessage(...)     returns (Message);        // by mid or stream+seq
  rpc TraceMessage(...)   returns (stream TraceEvent);
  rpc Peek(...)           returns (stream Message); // read WITHOUT affecting any cursor
  rpc TailJournal(...)    returns (stream JournalEvent);
}
```

`Peek` deserves emphasis: **inspection must never mutate delivery state.** A tool that advances a cursor when you look at a message is a tool operators refuse to use during an incident.

### 5.4 Error model — every rejection is typed and greppable

| gRPC code | `reason` | Client should | Operator meaning |
|---|---|---|---|
| `RESOURCE_EXHAUSTED` | `disk_high_watermark` | retry w/ backoff | free disk NOW |
| `RESOURCE_EXHAUSTED` | `stream_max_bytes` / `stream_max_msgs` | retry / drop | limits hit, `discard=new` |
| `RESOURCE_EXHAUSTED` | `retention_blocked_by_consumer` | retry w/ backoff | named consumer is stuck |
| `RESOURCE_EXHAUSTED` | `publish_queue_full` | retry immediately-ish | broker write path saturated |
| `RESOURCE_EXHAUSTED` | `max_connections` / `max_consumers` | fail | raise limit or fix leak |
| `FAILED_PRECONDITION` | `stale_delivery` | do not retry | `ack_wait` too short |
| `FAILED_PRECONDITION` | `expect_last_seq_mismatch` | re-read | optimistic concurrency |
| `UNAVAILABLE` | `draining` | reconnect elsewhere/later | rolling restart in progress |
| `UNAVAILABLE` | `recovering` | retry | startup scan in progress |
| `INVALID_ARGUMENT` | `message_too_large` etc. | fix | client bug |
| `NOT_FOUND` | `no_such_stream` / `no_such_consumer` | fix | config drift |
| `INTERNAL` | `unsafe_state` | stop | broker is dying, see RB-04 |

Every rejection carries `google.rpc.ErrorInfo{reason, domain="messq", metadata{...}}` **and** is journalled once per (reason, stream) per second with a count. The operator greps one word.

### 5.5 Ops HTTP surface (read-only)

```
GET /livez                      200 unless process is wedged/UNSAFE       (restart signal)
GET /readyz                     200 only in READY                          (traffic signal)
GET /healthz                    full JSON: state, degraded[], disk, streams, consumers, oldest_unacked
GET /metrics                    Prometheus text (or OpenMetrics)
GET /api/v1/status
GET /api/v1/streams             /api/v1/streams/{s}
GET /api/v1/consumers           /api/v1/consumers/{s}/{c}
GET /api/v1/consumers/{s}/{c}/pending?limit=100
GET /api/v1/messages/{mid}      /api/v1/streams/{s}/messages/{seq}
GET /api/v1/messages/{mid}/trace
GET /api/v1/journal?follow=1&stream=…&event=…      (SSE)
GET /debug/pprof/*              (config-gated, default on but localhost-only)
GET /debug/goroutines           named goroutine registry + counts
```

---

## 6. CLI & developer experience

### 6.1 Shape

One binary, `messq`. `messq serve` is the daemon; every other verb is a client that defaults to the Unix socket. Built on **Cobra** for POSIX flags, nested commands, generated help, and — importantly for ops — dynamic shell completion of *live* stream and consumer names via `RegisterFlagCompletionFunc`/`ValidArgsFunction`, so `messq consumer info <TAB>` completes against the running daemon. Fewer typos at 3am is a real reliability feature.

`RunE` everywhere; exit codes are meaningful (§6.6). `--output=table|json|yaml` on every read command; `table` is default for humans, `json` is stable and versioned for scripts.

### 6.2 Command map

```
messq serve [--config /etc/messq/messq.toml]
messq version [--json]                     # version, commit, go version, format_version, build flags
messq status                               # node: state, uptime, disk, degraded reasons, counts
messq disk                                 # Q3: per-stream bytes, growth rate, watermarks, ETA-to-full
messq config show [--defaults] [--diff]    # effective config; --diff vs shipped defaults
messq config check <file>                  # validate WITHOUT touching the daemon (use in CI + preStop)

messq stream ls | info <s> | create <s> [...] | update <s> [...] | rm <s> --confirm <s>
messq stream purge <s> [--subject …] [--before …] [--keep-last N] --dry-run | --confirm <s>

messq consumer ls [--sort=lag|age|dlq] | info <s>/<c> | create <s>/<c> [...] | rm <s>/<c> --confirm
messq consumer pause <s>/<c> | resume <s>/<c>
messq consumer seek <s>/<c> --to start|end|seq:N|time:RFC3339|mid:… --dry-run | --confirm

messq pub <s> <subject> [--file -|path] [--header k=v] [--count N] [--rate R]
messq sub <s>/<c> [--ack auto|manual] [--credit N] [--max N] [--output json]
messq peek <s> [--from seq:N|time:…] [--subject …] [--limit N]        # never mutates cursors
messq inflight <s>/<c> [--sort=deadline] [--blocked]                  # Q4
messq msg get <mid> | messq msg trace <mid>                            # Q1
messq tail [--stream …] [--consumer …] [--event …] [--mid …]           # live journal, filtered

messq dlq ls <s> | show <mid> | redrive <s> [...] | purge <s> [...]
messq replay <s> --from … --to … --into <s2>|--to-consumer <c>  --rate R --dry-run

messq store verify [--all] [--rebuild-index]     # offline-safe; distinguishes "our bug" from "bad disk"
messq store compact                              # offline bbolt compaction
messq store migrate [--to N] [--backup DIR]      # explicit format migration
messq store info                                 # format_version, segments, sizes, node_id
messq export <s> [--from …] [--to …] > x.ndjson
messq import <s> < x.ndjson

messq drain [--timeout 60s]                      # graceful node drain, then exit
messq selftest [--dir /tmp/x]                    # spins an ephemeral instance, runs the acceptance suite
messq debug goroutines | locks | timers
messq completion bash|zsh|fish
```

### 6.3 Output that answers the four questions

`messq consumer ls --sort=lag` (Q2):

```
STREAM    CONSUMER    PENDING  UNACKED  OLDEST-UNACKED  LAG    ATTEMPTS>1  DLQ   BLOCKED-BY      STATE
orders    billing        1204       31          4m12s   1204          7      0   -               active
orders    search       184921      512         51m03s 184921        498     12   max_ack_pending active
orders    archive           0        0              -      0          0      0   -               ORPHANED(9d)
```

`messq msg trace 01J8ZC…` (Q1) — reads the journal index, not a live tail:

```
mid=01J8ZCQ7K3N2R5V8XH4M1PQ0DW  stream=orders  seq=88214  subject=orders.eu.created  trace=req-9f2c
  2026-08-21T02:11:04.120Z  accepted        size=812B  publisher=10.0.3.7:51422
  2026-08-21T02:11:04.310Z  delivered       consumer=billing  attempt=1  deadline=02:11:34.310Z
  2026-08-21T02:11:34.311Z  ack_timeout     consumer=billing  attempt=1  next_in=1s
  2026-08-21T02:11:35.402Z  delivered       consumer=billing  attempt=2  reason=ack_timeout
  2026-08-21T02:11:36.019Z  naked           consumer=billing  attempt=2  reason="downstream 503"
  ... (3 more)
  2026-08-21T02:24:11.882Z  dead_lettered   consumer=billing  attempt=5  cause=max_deliver
  2026-08-21T02:24:11.883Z  accepted        stream=$DLQ.orders  seq=17  (same mid)
```

`messq disk` (Q3):

```
data_dir=/var/lib/messq  fs=ext4  total=200G  used=141G  free=59G (29.5%)  state=OK
watermarks: low=20% (40G)  high=5% (10G)  reserve=1G
growth: +2.1 GiB/h over 6h   →  low watermark in ~9h04m   high watermark in ~23h11m
STREAM        BYTES    MSGS     SEGMENTS  OLDEST        RETENTION            TRIMMABLE-NOW
orders        118.2G   41.2M         1892  2026-07-02    all_acked/max_age=90d  0 B  (blocked: archive)
$DLQ.orders     1.1G    0.4M           18  2026-08-01    limits/max_age=30d     412 M
```

`TRIMMABLE-NOW: 0 B (blocked: archive)` is the entire incident, printed in one line.

### 6.4 Destructive-action discipline

Every destructive verb (`purge`, `seek`, `stream rm`, `consumer rm`, `dlq purge`, `dlq redrive`, `import`, `store migrate`) obeys the same four rules:

1. **`--dry-run` is free and exact** — prints the count, the byte size, the seq range and the first/last `mid` that *would* be affected. `seek --dry-run` prints "would make 184,921 messages eligible for redelivery to consumer `search`".
2. **Confirmation is by name, never `y/n`** — `--confirm orders`. Reflex-typing `y` at 3am is how outages are made.
3. **It is audited** — an fsync'd record in `audit.log` and bbolt with `{ts, actor, action, target, params, dry_run, before_counts, after_counts, cli_version, peer}`. Actor = SO_PEERCRED uid/gid on the Unix socket, or token subject / mTLS CN over TCP.
4. **Reversibility is stated in `--help`** — e.g. `seek` says *"REVERSIBLE: the old cursor is printed and stored in the audit log; restore with `messq consumer seek … --to seq:N`"*; `purge` says *"IRREVERSIBLE: data is deleted."*

`--dry-run` output and the real output are the *same renderer*, so the preview cannot drift from reality.

### 6.5 Client library DX

`github.com/<org>/messq/client` ships in-repo, with defaults that make the common mistakes impossible:

- `Ack` is explicit; there is no `AutoAck` option in the library (only in the `messq sub` CLI, where it prints a warning).
- The consumer helper runs handlers in a worker pool sized `min(max_ack_pending, workers)` and automatically sends `Extend` at 60% of the remaining deadline while a handler is still running — the single most common at-least-once bug (long handler → ack timeout → duplicate) is solved in the library, not left to each user.
- Handler returning `error` ⇒ `Nak`; returning `messq.ErrPermanent` ⇒ `Term`; panic ⇒ `Nak` + logged, never a crashed consumer.
- Reconnect with exponential backoff **and jitter**, and a `Drain()` for graceful consumer shutdown.

### 6.6 Exit codes (sysexits-flavoured, documented in `messq --help`)

| Code | Meaning |
|---|---|
| 0 | success |
| 1 | generic failure |
| 2 | usage / bad flags |
| 3 | command rejected because confirmation was missing/incorrect |
| 4 | daemon reachable but unhealthy (`status` returns degraded, for cron checks) |
| 64 | `EX_USAGE` |
| 69 | `EX_UNAVAILABLE` — daemon not reachable |
| 74 | `EX_IOERR` — fsync/IO fatal |
| 75 | `EX_TEMPFAIL` — lock held by another process |
| 77 | `EX_NOPERM` — socket permission / auth |
| 78 | `EX_CONFIG` — bad config or incompatible data-dir format version |

---

## 7. Observability & logging design

### 7.1 The journal is an API

Log lines are a public interface with a compatibility promise. Field names and `event` values are versioned; removing or renaming one is a breaking change requiring a major version and a `docs/journal-schema.md` entry. Golden-file tests in CI assert the exact shape of every event (§8.6). Operators build alerts on these; treating them as casual `printf` output is how observability rots.

**Two sinks, one taxonomy:**

- **Operational log** (`stderr` → journald): daemon lifecycle, degradation, errors, admin actions. Human-readable `text` when a TTY, `json` otherwise.
- **Message journal**: per-message transitions. Default sink is the same stream (tagged `chan=journal`), but can be split to its own file (`journal.sink = "file:/var/log/messq/journal.ndjson"`) with size-based rotation, because *audit volume must not be able to evict your daemon's error messages*.

Built on stdlib **`log/slog`** with a `JSONHandler` and a `slog.LevelVar` so `messq debug level set debug --for 5m` changes verbosity atomically at runtime and reverts automatically — a temporary debug level that you forget to turn off is itself a disk-full incident.

### 7.2 Event taxonomy

```jsonc
{"time":"2026-08-21T02:11:35.402Z","level":"INFO","chan":"journal","event":"delivered",
 "mid":"01J8ZCQ7K3N2R5V8XH4M1PQ0DW","trace_id":"req-9f2c","stream":"orders",
 "subject":"orders.eu.created","seq":88214,"consumer":"billing","delivery_id":40217,
 "attempt":2,"redelivery_reason":"ack_timeout","deadline_at":"2026-08-21T02:12:05.402Z",
 "size":812,"peer":"10.0.3.7:51422"}
```

| `event` | Level | Emitted per | Rate-limited |
|---|---|---|---|
| `accepted` | INFO | message | yes (sampled ≥ threshold) |
| `delivered` | INFO | delivery attempt | yes |
| `acked` | INFO | ack | yes (sampled) |
| `naked` | INFO | nak | no (failures are always interesting) |
| `ack_timeout` | WARN | timeout | no |
| `dead_lettered` | ERROR | DLQ | **never** |
| `terminated` | WARN | term | no |
| `stale_ack` | WARN | occurrence | yes |
| `delivery_abandoned` | WARN | occurrence | yes |
| `purged`, `seek`, `redrive`, `stream_created`, `consumer_deleted` | WARN | admin action | **never** |
| `trim_ahead_of_consumer`, `consumer_orphaned`, `retention_blocked` | ERROR | occurrence | 1/min per target |
| `disk_state_changed`, `torn_tail`, `fsync_failed`, `recovery_*`, `state_changed` | WARN/FATAL | occurrence | never |

**Every message event carries `mid` and `trace_id`.** That is the promise: paste one ID into `messq tail --mid` or `grep` and get the whole story.

### 7.3 Bounding the observability subsystem

The rule "no unbounded quantity" applies recursively to logging — a redelivery storm must not fill the disk with its own logs.

- Per-`event`-class token bucket (`journal.rate_limit.<event> = N/s`, default 5000/s for `accepted`/`delivered`/`acked`). When a bucket drops lines it emits, at most once per second, `{"event":"journal_suppressed","class":"delivered","dropped":41233}` and increments `messq_journal_dropped_total`. **Silent sampling is worse than no logging**; the operator must know the record is incomplete.
- `dead_lettered`, admin actions and all FATAL/ERROR lifecycle events are exempt from suppression — they are low-volume by construction and are the ones you need.
- If the journal sink is a file and the filesystem hits the high watermark, the journal switches to *errors-only* mode and says so.

### 7.4 Health endpoints — three distinct meanings

| Endpoint | Question | 200 when | Never affected by |
|---|---|---|---|
| `/livez` | *Should the supervisor kill me?* | process responsive, supervisor heartbeat < 10 s old, not `UNSAFE` | disk, backlog, consumer health |
| `/readyz` | *Should clients connect?* | state == `READY` | disk pressure, DLQ depth, lag |
| `/healthz` | *What is actually going on?* | always 200 (or 503 if not READY) with a full JSON body | — |

**Disk pressure and backlog never affect `/livez` or `/readyz`.** They set `degraded[]` in `/healthz` and drive metrics/alerts. This is the RabbitMQ-on-Kubernetes lesson made structural.

```json
{
  "state": "READY", "since": "2026-08-20T21:04:11Z", "version": "1.4.2", "format_version": 3,
  "degraded": [
    {"kind":"disk_low_watermark","since":"2026-08-21T01:40:00Z","detail":"free 17.2% < 20%"},
    {"kind":"consumer_orphaned","target":"orders/archive","since":"2026-08-12T00:00:00Z"}
  ],
  "disk": {"free_bytes": 63316000000, "free_ratio": 0.172, "state":"DEGRADED_DISK_LOW",
           "growth_bytes_per_hour": 2254857830, "eta_high_watermark": "2026-08-22T01:15:00Z"},
  "streams": 4, "consumers": 11, "goroutines": 47, "open_fds": 63,
  "worst_consumer": {"name":"orders/search","lag":184921,"oldest_unacked_seconds":3063}
}
```

### 7.5 Metrics

Prometheus via `prometheus/client_golang` with an **explicit custom registry** (never `DefaultRegisterer` — accidental global metrics from a transitive dependency are how cardinality explodes), `promauto.With(reg)` for construction, `promhttp.HandlerFor(reg, HandlerOpts{MaxRequestsInFlight: 4, ErrorLog: slogAdapter})`, plus `collectors.NewGoCollector()` and `NewProcessCollector()`.

**Cardinality rule: labels are `stream` and `consumer` only. Never `subject`, never `mid`, never `peer`.** Subject cardinality is unbounded by design; top-N subject breakdowns live in the ops API (`/api/v1/streams/{s}/subjects?top=20`), where they cost nothing when nobody asks.

| Metric | Type | Labels |
|---|---|---|
| `messq_build_info` | gauge=1 | version, commit, go_version, format_version |
| `messq_node_state` | gauge | state (enum as value: 0 starting…5 unsafe) |
| `messq_degraded` | gauge | kind |
| `messq_published_total` | counter | stream |
| `messq_publish_rejected_total` | counter | stream, reason |
| `messq_publish_duration_seconds` | histogram | stream |
| `messq_fsync_duration_seconds` | histogram | — |
| `messq_group_commit_size` | histogram | — |
| `messq_delivered_total` | counter | stream, consumer |
| `messq_redelivered_total` | counter | stream, consumer, reason |
| `messq_acked_total` | counter | stream, consumer |
| `messq_naked_total` | counter | stream, consumer |
| `messq_ack_timeouts_total` | counter | stream, consumer |
| `messq_stale_acks_total` | counter | stream, consumer |
| `messq_dlq_total` | counter | stream, consumer, cause |
| `messq_processing_duration_seconds` | histogram | stream, consumer |
| `messq_consumer_pending` | gauge | stream, consumer |
| `messq_consumer_unacked` | gauge | stream, consumer |
| `messq_consumer_oldest_unacked_seconds` | gauge | stream, consumer |
| `messq_consumer_delivery_blocked` | gauge | stream, consumer, cause |
| `messq_consumer_credit` | gauge | stream, consumer |
| `messq_stream_bytes` / `messq_stream_messages` | gauge | stream |
| `messq_stream_first_seq` / `_last_seq` | gauge | stream |
| `messq_msgs_dropped_total` | counter | stream, reason |
| `messq_disk_free_bytes` / `_free_ratio` / `_state` | gauge | — |
| `messq_disk_growth_bytes_per_hour` | gauge | — |
| `messq_state_db_bytes` / `_free_page_ratio` | gauge | — |
| `messq_recovery_duration_seconds` | gauge | — |
| `messq_recovery_torn_tail_bytes_total` | counter | stream |
| `messq_journal_dropped_total` | counter | class |
| `messq_goroutines` / `messq_open_fds` / `messq_connections` | gauge | — |

`messq_consumer_oldest_unacked_seconds` is the single most valuable metric in the system: it is the true user-facing latency SLI, it is immune to throughput fluctuations, and it goes up when *anything* is wrong.

### 7.6 Shipped alert rules (`deploy/alerts.yaml`, with runbook links)

```yaml
- alert: MessqDlqNonEmpty                      # even one message means investigate
  expr: increase(messq_dlq_total[15m]) > 0
  for: 0m
  labels: {severity: warning, runbook: RB-02}

- alert: MessqConsumerStalled
  expr: messq_consumer_oldest_unacked_seconds > 900
  for: 5m
  labels: {severity: critical, runbook: RB-01}

- alert: MessqDiskWillFill                     # predictive, not reactive
  expr: messq_disk_free_bytes / clamp_min(messq_disk_growth_bytes_per_hour, 1) < 6
  for: 30m
  labels: {severity: warning, runbook: RB-03}

- alert: MessqReadOnly
  expr: messq_disk_state >= 2
  for: 1m
  labels: {severity: critical, runbook: RB-03}

- alert: MessqRetentionBlocked
  expr: increase(messq_publish_rejected_total{reason="retention_blocked_by_consumer"}[10m]) > 0
  labels: {severity: critical, runbook: RB-05}

- alert: MessqDataDropped                      # should ALWAYS be zero
  expr: increase(messq_msgs_dropped_total{reason="retention"}[1h]) > 0
  labels: {severity: critical, runbook: RB-06}

- alert: MessqAckWaitTooShort
  expr: rate(messq_stale_acks_total[10m]) > 0.1
  for: 15m
  labels: {severity: warning, runbook: RB-07}

- alert: MessqRedeliveryStorm
  expr: rate(messq_redelivered_total[5m]) > 5 * rate(messq_acked_total[5m])
  for: 10m
  labels: {severity: warning, runbook: RB-08}

- alert: MessqTornTailOnRecovery
  expr: increase(messq_recovery_torn_tail_bytes_total[1h]) > 0
  labels: {severity: warning, runbook: RB-09}

- alert: MessqStateDbBloat
  expr: messq_state_db_free_page_ratio > 0.6
  for: 1h
  labels: {severity: info, runbook: RB-10}

- alert: MessqJournalSuppressed
  expr: increase(messq_journal_dropped_total[10m]) > 0
  labels: {severity: warning, runbook: RB-11}
```

### 7.7 Grafana dashboard (`deploy/grafana/messq.json`, one screen)

Row 1 (is it alive): node state, uptime, degraded list, disk free % + ETA-to-full.
Row 2 (is it working): publish rate & rejects by reason, deliver/ack rate, ack-latency heatmap.
Row 3 (is anyone suffering): `oldest_unacked_seconds` per consumer (top 10), pending per consumer, blocked-by breakdown.
Row 4 (is anything lost): DLQ rate by cause, dropped-by-retention, stale acks, torn-tail bytes.

---

## 8. Operational design (the persona's core section)

### 8.1 Configuration

**TOML, not YAML.** No significant whitespace, no type-coercion surprises (`no` → boolean, `1.20` → number), unambiguous strings. Config bugs found at 3am are the worst kind.

Loading uses **koanf** with explicit precedence — defaults → `/etc/messq/messq.toml` → `/etc/messq/conf.d/*.toml` (lexical) → `MESSQ_*` env → flags. Every value's *origin* is retained and printed by `messq config show --origins`, because "why is this setting this value?" is a real 3am question.

> koanf's own docs note the file watcher is **not goroutine-safe during concurrent `Load` calls**. messq therefore does **not** use `file.Watch` for auto-reload. Reload is explicit (SIGHUP / `messq reload`), performed under a mutex into a fresh `koanf` instance, validated, and then published via `atomic.Pointer[Config]`. Config that changes because someone's editor wrote a temp file is a genuine incident source.

```toml
[node]
name       = "messq-1"
data_dir   = "/var/lib/messq"

[data]
listen           = "127.0.0.1:4390"
socket           = "/run/messq/messq.sock"
max_connections  = 1024
max_message_size = "1MiB"          # hard ceiling 32MiB

[ops]
listen = "127.0.0.1:4391"
pprof  = true

[admin]
# listen = "0.0.0.0:4392"          # requires auth below; refuses to start otherwise
# token_file = "/etc/messq/tokens"
# tls = { cert = "...", key = "...", client_ca = "..." }

[durability]
group_commit_interval    = "2ms"
group_commit_bytes       = "1MiB"
ack_checkpoint_interval  = "200ms"   # 0 = fsync every ack
ack_checkpoint_mutations = 4096
segment_size             = "64MiB"
preallocate              = true

[disk]
low_watermark  = 0.20
high_watermark = 0.05
reserve_bytes  = "1GiB"
poll_interval  = "10s"

[limits]
max_streams              = 256
max_consumers            = 1024
max_ack_pending_default  = 1024
max_ack_pending_ceiling  = 65536
publish_queue_depth      = 4096
max_credit               = 65536

[log]
level          = "info"
format         = "json"            # auto-detects "text" on a TTY
[log.journal]
sink           = "stderr"          # or "file:/var/log/messq/journal.ndjson"
rate_limit     = { accepted = 5000, delivered = 5000, acked = 5000 }

[defaults.consumer]
ack_wait        = "30s"
max_deliver     = 5
backoff         = ["1s", "5s", "30s", "2m", "10m"]
max_ack_pending = 1024
dlq             = true
ordering        = "none"

[defaults.stream]
retention          = "all_acked"
max_age            = "168h"
max_bytes          = "50GiB"
discard            = "new"
inactive_threshold = "168h"
```

`messq config check` validates *without* a running daemon (usable in CI and in `ExecStartPre`) and refuses configurations that are individually valid but collectively dangerous:

- `max_consumers × max_ack_pending_ceiling` state exceeding a sane bbolt size → error with the computed number.
- `high_watermark ≥ low_watermark` → error.
- `Σ stream.max_bytes` > filesystem size × (1 − high_watermark) → **warning**, printed with both numbers. Over-committing disk is the most common way to reach 3am.
- `max_deliver > 1` with an empty backoff → error unless `--allow-hot-retry`.

**Hot-reloadable set (SIGHUP)**: log level/format, journal rate limits, disk watermarks & poll interval, per-consumer `ack_wait`/`max_deliver`/`backoff`/`max_ack_pending`, stream retention limits (widening only; narrowing that would delete data requires `messq stream update`), rate limits.
**Requires restart**: listeners, `data_dir`, `segment_size`, durability intervals, `max_connections`.

Reload logs a diff and is explicit about what it ignored:

```
INFO event=config_reloaded changed=["log.level: info→debug","disk.low_watermark: 0.20→0.15"]
     ignored_requires_restart=["data.listen: 127.0.0.1:4390→0.0.0.0:4390"]
```

### 8.2 Operational limits (all enforced, all metered, all documented)

| Limit | Default | Ceiling | Action at limit |
|---|---|---|---|
| `max_message_size` | 1 MiB | 32 MiB | `INVALID_ARGUMENT` |
| `max_streams` | 256 | 4096 | `RESOURCE_EXHAUSTED` on create |
| `max_consumers` | 1024 | 16384 | `RESOURCE_EXHAUSTED` on create |
| `max_ack_pending` (per consumer) | 1024 | 65536 | stop delivering (reported via `delivery_blocked`) |
| `max_credit` (per attach) | 65536 | — | clamp + WARN once |
| `publish_queue_depth` | 4096 | — | `RESOURCE_EXHAUSTED{publish_queue_full}` |
| `max_connections` | 1024 | — | refuse accept, WARN |
| `stream.max_bytes` / `max_msgs` / `max_age` | 50 GiB / ∞ / 7 d | — | retention policy (§4.6) |
| `max_subject_len` | 256 B | — | `INVALID_ARGUMENT` |
| `max_headers_bytes` | 4 KiB | — | `INVALID_ARGUMENT` |
| journal rate | 5000/s per class | — | drop + `journal_suppressed` |
| `GOMEMLIMIT` | set from cgroup if present | — | GC pressure instead of OOM |

Memory is bounded by construction: bodies are read from the page cache into a per-delivery buffer that is returned to a `sync.Pool`; the only per-consumer state is the pending map (≤ `max_ack_pending` entries × ~120 B) and the deadline heap. Peak RSS ≈ `base(30 MiB) + Σ(max_ack_pending × 150 B) + connections × 64 KiB`. This formula is printed by `messq config show --capacity` — an operator can size a VM without running a benchmark.

### 8.3 Lifecycle state machine

```
                ┌──────────┐
   exec ───────▶│ STARTING │  config load, flock, manifest check
                └────┬─────┘
                     ▼
                ┌────────────┐  /readyz 503, /livez 200
                │ RECOVERING │  segment scan, torn-tail truncation, state rebuild
                └────┬───────┘
                     ▼
                ┌────────┐  sd_notify READY=1
     ┌─────────▶│ READY  │◀────────────┐
     │          └───┬────┘             │ disk recovers
     │  disk ok     │ disk high wm     │
     │          ┌───▼────────┐         │
     │          │ READ_ONLY  │─────────┘   publishes rejected, consumption continues
     │          └───┬────────┘
     │              │ SIGTERM / messq drain
     │          ┌───▼──────┐
     └──────────│ DRAINING │  reject publish (UNAVAILABLE{draining}),
                └───┬──────┘  stop new deliveries, accept acks, wait grace
                    ▼
                ┌─────────┐   sd_notify STOPPING=1, exit 0
                │ STOPPED │
                └─────────┘
   any state + fsync error ──▶ UNSAFE ──▶ exit 74
```

### 8.4 Graceful shutdown & drain (exact sequence)

`SIGTERM`, `SIGINT`, or `messq drain`:

```
t+0      state ← DRAINING; sd_notify STOPPING=1, STATUS="draining"
         /readyz → 503                       (clients/LBs stop sending new work here)
t+0      publishes → UNAVAILABLE{reason="draining"}   (typed, retryable, not a hang)
t+0      stop issuing NEW deliveries; existing in-flight deliveries keep their deadlines
t+0      send Drain signal on every open consume stream so clients can finish cleanly
t+0..G   accept acks/naks; log progress every 2s: "draining: 412 in-flight, 38s remaining"
t+G      (G = drain_grace, default 30s, or until in-flight == 0, whichever first)
         remaining in-flight entries stay in bbolt as pending → redelivered after restart
t+G      grpcServer.GracefulStop()  (blocks until RPCs finish; hard Stop() after 5s)
t+G      flush ack-state checkpoint (synchronous fsync)
t+G      seal nothing; leave active segment open-but-durable (recovery handles it)
t+G      release flock, close bbolt, flush journal
t+G      exit 0
```

`TimeoutStopSec` in the unit is set to `drain_grace + 15 s`. If systemd `SIGKILL`s us anyway, recovery in §3.6 handles it — **drain is an optimisation, never a correctness requirement.** That is the property that makes `kill -9` safe, and it is asserted by the crash-injection suite.

### 8.5 Upgrade & schema migration

**Single-binary means the on-disk format *is* the API between versions.** Handled as a first-class failure mode:

1. `MANIFEST.format_version` (uint32). The binary declares `min_readable` and `writes` versions.
2. **Newer data than binary → refuse to start**, exit 78, print both versions and "downgrade path: restore backup or install ≥ vX". Never guess, never partially parse.
3. **Older data than binary → refuse to start**, exit 78, print `messq store migrate` and the estimated duration/space requirement. **Never auto-migrate at boot** — an auto-migration turns a routine package upgrade into an irreversible one, and rollback at 3am must remain possible.
4. `messq store migrate` is offline (requires the flock), writes a `MIGRATION` journal file for resumability, refuses to run with < 2× the state-db size free, and prints the exact rollback statement *before* starting:
   `"ROLLBACK: stop messq, restore /var/lib/messq from your backup, reinstall messq 1.3.x."`
5. **Compatibility promise**: format changes are additive within a major version; every release migrates from at least the previous **two** minor versions; segment records are append-only with a version in the header so a new binary reads old segments without rewriting them. Most upgrades therefore need no migration at all.
6. **Downgrade** is supported iff `format_version` is unchanged, and `messq version --json` prints `format_version` so a deploy pipeline can check *before* it upgrades.
7. **Wire compatibility** is protobuf's problem (fields added, never renumbered), and CI runs the previous release's client against the new server and vice versa.

Documented upgrade procedure (in `docs/runbooks/RB-12-upgrade.md`):

```
1. messq status                                  # expect READY, degraded=[]
2. messq version --json | jq .format_version     # note it
3. <new> messq version --json | jq .format_version
4. if equal:  systemctl stop messq && install && systemctl start messq   # ~5s, no migration
   else:      messq drain --timeout 60s
              cp -a --reflink=auto /var/lib/messq /var/lib/messq.bak-$(date +%F)
              install && messq store migrate && systemctl start messq
5. messq status && messq consumer ls             # lag must recover within 2 minutes
6. rollback: systemctl stop; reinstall old; (if migrated) restore the backup dir
```

### 8.6 systemd integration

`Type=notify` with `sd_notify` (`READY=1` after recovery, `STATUS=` continuously updated with state + lag summary, `WATCHDOG=1` pinged by the lifecycle goroutine, `STOPPING=1` on drain). The watchdog is the real deadlock detector: if the lifecycle goroutine cannot run, systemd restarts us — which is exactly right, because a wedged broker is worse than a restarting one.

```ini
[Unit]
Description=messq queue daemon
Documentation=man:messq(8) https://…/docs/runbooks/
After=network-online.target
Wants=network-online.target

[Service]
Type=notify
NotifyAccess=main
WatchdogSec=30s
ExecStartPre=/usr/bin/messq config check /etc/messq/messq.toml
ExecStart=/usr/bin/messq serve --config /etc/messq/messq.toml
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
RestartSec=2s
StartLimitIntervalSec=300
StartLimitBurst=5
TimeoutStopSec=45s
KillSignal=SIGTERM
FinalKillSignal=SIGKILL

User=messq
Group=messq
StateDirectory=messq
RuntimeDirectory=messq
RuntimeDirectoryMode=0750
LimitNOFILE=65536
OOMScoreAdjust=-500
OOMPolicy=stop

NoNewPrivileges=yes
PrivateTmp=yes
PrivateDevices=yes
ProtectSystem=strict
ProtectHome=yes
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectControlGroups=yes
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6
RestrictNamespaces=yes
LockPersonality=yes
MemoryDenyWriteExecute=yes
SystemCallFilter=@system-service
SystemCallArchitectures=native
CapabilityBoundingSet=

[Install]
WantedBy=multi-user.target
```

`StartLimitBurst=5` matters: a crash-looping broker on a dying disk must stop and stay stopped so a human sees it, rather than grinding the disk and rotating the evidence out of the journal.

`OOMScoreAdjust=-500` + `OOMPolicy=stop`: prefer that the kernel kills something else; if it does kill us, stop rather than loop.

### 8.7 Failure-mode catalogue

Each row is a test case (§9) *and* a runbook section. This table is the acceptance criterion for "production ready".

| # | Failure | Detection | Broker behaviour | Data outcome | Runbook |
|---|---|---|---|---|---|
| F01 | `kill -9` mid-publish | recovery log `torn_tail` | truncate torn tail, continue | un-acked publishes lost; acked ones present | RB-09 |
| F02 | `kill -9` between body fsync and ack checkpoint | — | pending rebuilt from last checkpoint | duplicate deliveries ≤ 200 ms window | RB-13 |
| F03 | Disk fills (`ENOSPC` at rotation) | `disk_state_changed`, alert | READ_ONLY; publishes rejected; consumption continues | none | RB-03 |
| F04 | `fsync` returns EIO | `fsync_failed` FATAL | exit 74; systemd stops after 5 tries | possible tail loss, *detected* | RB-04 |
| F05 | Filesystem remounts read-only | segment write EROFS | UNSAFE, exit 74 | none beyond tail | RB-04 |
| F06 | Consumer crashes holding 1000 in-flight | `delivery_abandoned` ×1000 | immediate redelivery on reconnect | duplicates | RB-01 |
| F07 | Consumer hangs (attached, never acks) | `oldest_unacked_seconds` ↑, `ack_timeout` | redelivery with backoff; DLQ at max_deliver | none | RB-01 |
| F08 | Consumer is offline for days | `consumer_orphaned` | excluded from retention; stream unblocked | none | RB-05 |
| F09 | Poison message | `dead_lettered{cause=max_deliver}` | DLQ after N attempts | preserved in DLQ | RB-02 |
| F10 | Redelivery storm (downstream down) | `MessqRedeliveryStorm` | backoff + jitter absorbs it | none | RB-08 |
| F11 | Slow consumer, fast producer | `messq_consumer_pending` ↑ | delivery stops at `max_ack_pending`, backlog grows on disk | none until retention | RB-01 |
| F12 | Retention would delete un-acked data (`all_acked`) | `retention_blocked_by_consumer` | reject publishes, name the consumer | none | RB-05 |
| F13 | Retention deletes un-acked data (`limits`) | `trim_ahead_of_consumer`, `msgs_dropped_total` | continue, log exact lost range | **data loss, by policy** | RB-06 |
| F14 | Clock jumps backwards (NTP step) | monotonic clock used for all deadlines | unaffected | none | RB-14 |
| F15 | Clock jumps forwards | `ack_timeout` burst | mass redelivery, absorbed by jitter | duplicates | RB-14 |
| F16 | Two daemons, same data dir | `flock` fails | second exits 75 with holder pid | none | RB-15 |
| F17 | Corrupt sealed segment (bit rot) | CRC error on read | that read fails `DATA_LOSS`; consumer skips with loud ERROR; `store verify` locates it | that message | RB-16 |
| F18 | bbolt file bloat | `state_db_free_page_ratio` | auto-compact at next start; `store compact` on demand | none | RB-10 |
| F19 | Ack arrives after timeout | `stale_ack` | rejected `FAILED_PRECONDITION`; work will be done twice | duplicate | RB-07 |
| F20 | Client sends huge `Credit` | clamped, WARN once | clamp to `max_credit` | none | — |
| F21 | Goroutine/connection leak | `messq_goroutines` vs formula | `max_connections` refuses new | none | RB-17 |
| F22 | Data dir on NFS | startup probe (`flock` semantics + `O_DIRECT` check) | **refuse to start**, exit 78 | none | RB-18 |
| F23 | Upgrade with newer on-disk format | manifest check | refuse to start, exit 78, print both versions | none | RB-12 |
| F24 | Operator purges the wrong stream | audit log | done; audit record names actor & counts | **irreversible** — hence name-confirm + dry-run | RB-19 |

### 8.8 Runbooks (`docs/runbooks/`, shipped in the repo and linked from every alert)

Fixed template — *Symptom · Confirm (exact commands) · Impact · Immediate mitigation · Root cause hunt · Prevention · Escalation*. Milestone-gated: **a milestone is not done until its runbooks exist.**

RB-01 consumer lag / stalled consumer · RB-02 DLQ non-empty · RB-03 disk filling / READ_ONLY · RB-04 fsync failure / IO error · RB-05 retention blocked by consumer · RB-06 data dropped by retention · RB-07 ack_wait tuning & stale acks · RB-08 redelivery storm · RB-09 torn tail after crash · RB-10 state.db bloat · RB-11 journal suppression · RB-12 upgrade & rollback · RB-13 duplicate deliveries after crash · RB-14 clock anomalies · RB-15 lock held / split data dir · RB-16 corrupt segment · RB-17 connection/goroutine leak · RB-18 unsupported filesystem · RB-19 accidental purge / recovery options · RB-20 capacity planning & sizing.

Example (abridged) — **RB-03 disk filling**:

```
SYMPTOM     MessqDiskWillFill or MessqReadOnly; publishers see RESOURCE_EXHAUSTED{disk_high_watermark}
CONFIRM     messq disk
            messq consumer ls --sort=lag
IMPACT      READ_ONLY: publishes rejected. Consumption UNAFFECTED. No data lost.
MITIGATE    1. Is a consumer blocking trim?  →  "TRIMMABLE-NOW: 0 B (blocked: <name>)"  → RB-05
            2. Free space fast, in order of safety:
               a. messq dlq purge <stream> --before <date> --dry-run   (usually the cheapest win)
               b. messq stream update <s> --max-age 48h                (trims immediately)
               c. grow the filesystem / attach a disk
               d. LAST RESORT, DESTRUCTIVE: messq stream purge <s> --keep-last 100000 --confirm <s>
            3. Do NOT delete .seg files by hand — recovery will detect the gap and refuse.
ROOT CAUSE  messq disk shows growth_bytes_per_hour; compare to the sizing in RB-20.
PREVENT     Set stream.max_bytes so that Σ max_bytes ≤ 70% of the filesystem (config check warns).
ESCALATE    If READ_ONLY persists after freeing space, check `messq status` for a stale watermark
            sample and `journalctl -u messq -e | grep disk_state_changed`.
```

### 8.9 Backup & restore

`messq backup /path/dest` = flock-aware, consistent snapshot: `bolt.Tx.WriteTo` for `state.db` (consistent without stopping writes), hard-link (or `--reflink`) sealed segments, copy the active segment up to its last fsync'd offset, copy MANIFEST. Output is a directory that is a valid `data_dir`, verified by `messq store verify` before `backup` returns 0.

Restore = stop, replace `data_dir`, start. Consumers replay from their stored cursors; messages published after the backup are gone, and `messq status` reports `restored_from` so nobody is confused about why the tail is missing.

---

## 9. Testing strategy

Ordered by what actually decides whether an operator can trust this. Throughput benchmarks come last, and exist mainly to produce honest capacity numbers for RB-20.

### 9.1 Crash-injection (highest value)

- A `Syncer`/`File` interface wraps every write path; the `faultfs` implementation can inject `ENOSPC`, `EIO`, short writes, torn writes (write first N bytes of a record then stop), and reorderings on demand.
- `crashtest` harness: runs a real `messq serve` subprocess with `MESSQ_CRASH_AFTER_FSYNC=N`, drives a workload, the child `SIGKILL`s *itself* at fsync #N, the parent restarts it and asserts the invariants below. Sweeps N over the whole run, in parallel, in CI nightly (fast subset per PR).
- **Invariants asserted after every crash:**
  - I1: every `mid` that received a `PublishAck` is readable after recovery (**no acknowledged loss**).
  - I2: no message is delivered to a consumer after that consumer acked it *and the ack was checkpointed*.
  - I3: `ack_floor ≤ delivered_seq ≤ last_seq` for every consumer.
  - I4: no pending entry references a seq beyond the recovered log.
  - I5: recovery is idempotent — recovering twice yields byte-identical state.
  - I6: the set of delivered `mid`s ⊇ the set of published `mid`s, given enough time (**at-least-once**).

### 9.2 Resource exhaustion (second highest)

Real filesystems, not mocks — `ENOSPC` behaviour differs between ext4/xfs/btrfs and between `fallocate` and buffered writes.

- CI job creates a 256 MiB loopback ext4 (and xfs) image, mounts it, runs messq with tuned watermarks, and publishes until it fills. Asserts: state reaches `READ_ONLY`, publishes get `RESOURCE_EXHAUSTED{disk_high_watermark}`, **consumption keeps working**, no panic, no corrupted segment, and after freeing space the node returns to `READY` without a restart.
- Same for: inode exhaustion, `RLIMIT_NOFILE` exhaustion, `GOMEMLIMIT` pressure with 10 000 in-flight, and 2 000 concurrent connections.

### 9.3 Deterministic simulation of the delivery state machine

The delivery engine takes a `Clock` interface and is driven by a single-goroutine event loop in tests. A seeded simulator generates random schedules of publish/ack/nak/term/timeout/disconnect/restart and compares against a **reference model** (a naive in-memory at-least-once queue, ~200 LOC). Any divergence prints the seed for exact replay. This is where ack-timeout races get caught — in CI, deterministically, not at 3am.

Property tests (`rapid`/`quick`): `ack_floor` monotonicity; `attempt ≤ max_deliver`; every message ends in exactly one of {acked, dlq, retained-pending, purged}; deadline heap ordering matches the bbolt `deadline` index after any operation sequence.

### 9.4 Upgrade / migration tests

`testdata/datadirs/v1/`, `v2/`, `v3/` — real data directories, committed, produced by the actual released binaries. CI asserts: current binary refuses `v_future`; migrates `v_current-1` and `v_current-2` successfully; post-migration `store verify` passes; consumer cursors and pending sets survive; a downgrade at equal `format_version` works. Plus cross-version wire tests (old client ↔ new server, new client ↔ old server).

### 9.5 Log & metric golden tests

Because §7.1 declares the journal an API: for a scripted scenario, the emitted events are compared field-by-field against a golden NDJSON file (timestamps/IDs normalised). Renaming a field fails CI. Same for `/metrics` — a golden list of metric names and label sets prevents accidental cardinality growth (a test asserts *no metric has a label whose value came from a subject or mid*).

### 9.6 Runbook executability tests

Each runbook's `CONFIRM` and `MITIGATE` command blocks are extracted and executed against a fixture instance that has been driven into the corresponding failure state (F01–F24). A runbook whose commands do not run is a lie; CI treats it as a build break.

### 9.7 Soak & chaos

Nightly 8 h (weekly 72 h) soak: mixed publish/consume, 1% nak rate, 0.1% poison, consumers randomly killed, broker `SIGKILL`ed every 20 min, disk deliberately filled once per hour and freed. Asserts: no goroutine growth, RSS within the §8.2 formula ±20%, zero I1 violations, DLQ contents exactly the injected poison set.

### 9.8 Benchmarks (last, and honest)

`messq bench` publishes the capacity table for RB-20: publish throughput/latency vs `group_commit_interval`, on NVMe / SATA SSD / EBS gp3, with fsync on. Numbers are published **with the fsync policy stated next to them**; a "1M msg/s" number obtained with `NoSync` is a lie an operator will pay for.

### 9.9 CI gates

Per PR: unit + property + race detector + short simulation + short crash sweep + golden logs/metrics + `go vet` + custom `spawnlint` (no bare `go` statements) + `staticcheck` + `govulncheck`. Nightly: full crash sweep, ENOSPC matrix, migration matrix, 8 h soak, runbook execution. Release: reproducible build, SBOM, signed artifacts, `messq selftest` on a clean VM for each supported distro.

---

## 10. Roadmap — empty repo → ideal product

Each milestone has a **verifiable exit criterion**. Definition of done for every milestone: *code + tests + metrics + journal events + runbook section + `docs/` update*. A milestone with no runbook is not done.

| M | Name | Scope | Exit criterion |
|---|---|---|---|
| **M0** | Skeleton | repo layout, `go.mod` (Go 1.26), Makefile, CI, licence, `messq version`, config loader (koanf/TOML) + `config check`, slog setup w/ LevelVar, exit-code table, supervisor + goroutine registry + `spawnlint` | `messq version --json`, `messq config check` work; CI green on an empty binary |
| **M1** | Segment log | record format, CRC32C, `fallocate`, rotation, sealing, sparse index, reader, torn-tail recovery, `store verify` | crash-injection sweep passes I1/I5 on a log-only workload |
| **M2** | State store | bbolt schema, checkpointer, stream/consumer CRUD, cursors, `store info`/`compact`, MANIFEST + flock | kill -9 sweep passes I3/I4; `store compact` shrinks a bloated db |
| **M3** | Delivery engine | full §4.2 state machine, deadlines, backoff+jitter, credit, `max_ack_pending`, ordering modes — **no network** | deterministic simulator matches the reference model over 10⁶ random ops, 100 seeds |
| **M4** | gRPC data plane | proto, Publisher, Consumer bidi stream, typed errors (§5.4), keepalive/enforcement, `MaxRecvMsgSize`, Unix socket + TCP, Go client library w/ auto-`Extend` | two processes exchange 1M messages at-least-once with acks; client survives broker restart |
| **M5** | CLI v1 | cobra tree, `pub`/`sub`/`peek`, `stream`/`consumer` ls+info+create, `--output json`, completion | Q2 and Q4 answerable from the CLI |
| **M6** | Observability | journal taxonomy + rate limiting, `msg trace`, `tail`, metrics registry, `/livez` `/readyz` `/healthz` `/metrics`, ops HTTP read-only API, alert rules + Grafana JSON | Q1 and Q3 answerable; golden log/metric tests green |
| **M7** | Lifecycle & ops | systemd unit + sd_notify + watchdog, drain, SIGHUP reload w/ diff, `status`, `disk`, `drain`, audit log, actor identity | `systemctl reload/stop` behave per §8.3–§8.4; drain test asserts zero in-flight loss beyond grace |
| **M8** | DLQ & retries | DLQ streams, causes, headers, `dlq ls/show/redrive/purge` w/ rate limit + re-redrive guard | F09/F10 pass; DLQ preserves `mid` through `msg trace` |
| **M9** | Limits & backpressure | disk watermarks + READ_ONLY, retention modes incl. `all_acked` + `BLOCKED_BY_CONSUMER`, orphan detection, all §8.2 limits | ENOSPC matrix green; F03/F08/F11/F12/F13 pass |
| **M10** | Replay & safety | `seek`, `purge`, `replay`, `export`/`import`, universal `--dry-run` + name-confirm + audit | F24 test: dry-run counts exactly match the real run |
| **M11** | Hardening → **1.0** | full crash sweep + soak in CI, migration harness + committed golden data dirs, capacity benchmarks, all 20 runbooks + executability tests, `selftest`, packaging (deb/rpm/apk/container/`go install`), man pages | 72 h soak clean; every F01–F24 has a passing test and a runbook; **1.0 tag** |
| **M12** | Security | mTLS, token file + subject-scoped permissions (`publish:orders.*`, `consume:orders/billing`, `admin:*`), audit of authn failures, admin-over-TCP | admin over TCP refuses to start without auth; permission matrix tested |
| **M13** | Phase 2a | delayed publish (`Messq-Deliver-At`), priority channels (N ordered sub-queues per consumer), per-consumer rate limiting | delayed messages land within ±1 s at 10⁵ scheduled |
| **M14** | Phase 2b | consumer groups with leases (multiple clients share one consumer; lease + fencing token; rebalance on disconnect) | killing 1 of 3 group members redelivers only its in-flight set, within lease TTL |
| **M15** | Phase 2c | zstd body compression (per-stream, transparent), retention policies by subject, audit-trail export, OpenTelemetry traces/OTLP alongside Prometheus | 3× space saving on JSON payloads; trace spans link publish→ack across services |
| **M16** | Phase 3 (only if demanded) | async read-only follower for backup/read-scale (log shipping, explicitly **not** consensus, explicitly **not** automatic failover) | follower lag metric; documented manual promotion procedure with its data-loss window stated |

**Sequencing rationale:** M1–M3 build the trustworthy core with zero network surface, so crash and simulation testing exist *before* there is an API to be compatible with. M6–M7 (observability and lifecycle) come *before* DLQ and retention, because a feature you cannot observe is a feature you cannot debug, and this plan's thesis is that the observability is the product. M12 (security) after 1.0 is deliberate: the default posture is localhost-only, which is safe, and shipping half-built auth is worse than shipping none.

---

## 11. Risks & open questions

### 11.1 Risks

| # | Risk | Severity | Mitigation | Trigger to revisit |
|---|---|---|---|---|
| R1 | **Single node = single point of failure.** Someone will run it for something that cannot tolerate an hour of downtime. | high | README states it in the first paragraph; `messq status` prints `replication: none`; RB-20 includes "when NOT to use messq"; export path keeps exit cheap | any user asks for HA → point at M16 or at JetStream |
| R2 | **Two engines = two recovery paths that can disagree.** Log ahead of state, or state ahead of log. | high | §3.6 step 6 reconciles explicitly in one direction only (log is authoritative for existence, state is authoritative for progress); I4 asserts it under crash injection | any I4 failure |
| R3 | **bbolt file bloat** under sustained high `max_ack_pending` with long-running reads. | medium | bounded key space (§3.4), `free_page_ratio` metric + alert, startup auto-compaction, `store compact` | ratio > 0.6 sustained in soak |
| R4 | **Ack-checkpoint lag creates duplicates** that surprise users who did not read the docs. | medium | documented as a number; `messq consumer info` shows the window; client library ships a `mid`-based dedupe helper; `ack_checkpoint_interval=0` available | duplicate complaints |
| R5 | **`all_acked` default surprises people** by rejecting publishes when a consumer is stuck. | medium | the error names the consumer; orphan detection auto-unblocks after 7 d; `messq stream update --retention limits` is one command | if support load is high, consider `all_acked` + hard cap that degrades to `limits` with a 24 h grace |
| R6 | **Journal volume** at 50k msg/s makes logging the bottleneck. | medium | per-class token buckets, separate sink, `journal_dropped_total`, benchmark measures with journal on *and* off | journal cost > 15% of publish latency |
| R7 | **gRPC is a heavy dependency** and opaque to `curl`. | low | read-only HTTP ops plane covers 3am; reflection + `grpcurl` documented; `messq` CLI is always the primary interface | if binary size or client-language friction becomes real, add a documented line-protocol gateway — not a replacement |
| R8 | **`fallocate` is not universal** (tmpfs, some network FS, older btrfs behaviour). | low | startup probe; fall back to explicit zero-fill with a WARN; refuse NFS outright (F22) | user reports on an exotic FS |
| R9 | **Scope creep toward "small Kafka".** | high | §1.5 negative-space list is a governance document; adding to it requires deleting something else | every feature request |
| R10 | **Log-line compatibility** ossifies the code. | medium | schema versioned in `docs/journal-schema.md`; additive-only within a major; golden tests make breakage visible rather than silent | major version |
| R11 | **Recovery time on very large streams** (many streams × 64 MiB tail) exceeds an acceptable restart window. | low | only the *unsealed* tail is scanned; progress logged; `segment_size` tunable down | recovery > 30 s in soak |
| R12 | **Operator deletes segment files by hand** to free disk. | medium | recovery detects the seq gap and refuses to start with a specific error and instructions; RB-03 says "do not"; `messq stream purge` is the supported path | any occurrence |

### 11.2 Open questions (with a default decision so nothing blocks)

1. **Should restart-induced redelivery consume a retry attempt?** *Default: yes* (§3.6), because the alternative is an unbounded failure mode. Revisit if operators find broker restarts pushing messages to the DLQ unfairly — possible refinement: a separate `max_restart_redeliveries` budget that does not count against `max_deliver`.
2. **`ack_checkpoint_interval` default of 200 ms** — is that the right duplicate window? *Default: 200 ms.* Measure real-world duplicate rates in soak; consider making it adaptive (tighten when nak rate is high).
3. **Subject filtering cost at scale.** The sealed-segment skip-set bounds it, but a consumer matching 0.01% of an unsealed 64 MiB tail still scans. *Default: accept and document.* Revisit with a per-subject seq index only if a real workload demands it — and only with a bounded-size design.
4. **Ephemeral consumers.** Useful for `messq sub` and for tailing, but they create state that nothing cleans up. *Default: supported, TTL 5 min after disconnect, never counted in retention, hard cap of 64.*
5. **Multi-tenant isolation.** Currently one flat namespace with per-stream limits. *Default: out of scope for 1.0.* If needed, a `tenant` prefix with per-tenant byte quotas is the smallest change — do not build accounts/RBAC.
6. **`max_message_size` of 1 MiB.** Bodies larger than this belong in object storage with a pointer in the message. *Default: keep the 1 MiB default and the 32 MiB ceiling*, and document the claim-check pattern rather than raising it.
7. **Do we need a `Progress`/heartbeat distinct from `Extend`?** *Default: no* — `Extend(by)` covers it and there is one fewer concept to explain.
8. **Windows/macOS support.** *Default: Linux only*, stated in the README. `fallocate`, `flock`, `statfs` and systemd integration all assume Linux; a partially-working port is an operational liability. macOS builds are provided for the *client/CLI* only.

---

## 12. Library choices

All versions checked against current documentation via context7 during planning. Bias: **stdlib first; every dependency must earn its place by removing a failure mode, not by saving typing.** Total non-test direct dependencies: 9.

### 12.1 Chosen

**Go 1.26 standard library** — `log/slog` (structured logging with `JSONHandler` and `slog.LevelVar`, which is documented as safe for concurrent read/write and is exactly the mechanism behind runtime log-level changes in §7.1), `net/http`, `hash/crc32` with the Castagnoli table (hardware CRC32C on amd64/arm64), `context`, `sync`, `os/signal`. Using stdlib slog rather than zap/zerolog means the log schema has no third-party lifecycle attached to it — and §7.1 declares that schema a public API.

**`go.etcd.io/bbolt`** — consumer/ack state. Chosen for: a single file, a real B+tree with ordered range scans (needed for the `deadline`/`redeliver` indexes), serialisable write transactions, and no background compaction threads to surprise anyone. From the fetched docs, the settings that matter here are explicit: `Tx.Commit` writes and fsyncs (with the docs' own warning to always commit or roll back so page reclamation is not blocked — hence "one writer goroutine, no long-lived read transactions"); `NoSync` is documented as unsafe and for bulk loading only, so it is **off**, and `NoFreelistSync` is documented as requiring a *full database re-sync during recovery*, so it is also **off** because startup-time predictability is worth more than write throughput here; `FreelistType: FreelistMapType` and `InitialMmapSize` are the documented knobs for large/high-churn databases; `MaxBatchSize`/`MaxBatchDelay` back the ack-checkpoint batching; `AllocSize` amortises the truncate+fsync of file growth; `StrictMode` (documented as a per-commit consistency check that significantly impacts performance) is enabled only in the crash-injection suite. The known weakness — the file never shrinks and freed pages go to a freelist — is handled head-on in §3.4 rather than discovered in production.

**`google.golang.org/grpc` + `google.golang.org/protobuf`** — data plane. The fetched docs supply exactly the operational primitives this design depends on: `keepalive.ServerParameters` (`MaxConnectionIdle`, `MaxConnectionAge` + `MaxConnectionAgeGrace` for bounded connection lifetime with graceful drain, `Time`/`Timeout` for dead-peer detection) and `keepalive.EnforcementPolicy` (`MinTime`, `PermitWithoutStream`) which lets the server reject abusive clients with `GOAWAY ENHANCE_YOUR_CALM` instead of quietly degrading — a real defence for a broker; `grpc.MaxRecvMsgSize`/`MaxSendMsgSize` for the message-size ceiling; `Server.GracefulStop()` which blocks until in-flight RPCs complete, the backbone of §8.4; `health.NewServer()` + `grpc_health_v1.RegisterHealthServer` + `SetServingStatus` so gRPC-native clients get the same READY/DRAINING signal as `/readyz`; and the documented per-stream HTTP/2 flow-control behaviour (the flow-control example shows `Send` blocking when the peer's window is exhausted) which is why messq layers its **own** application-level credit scheme on top — HTTP/2 windows bound bytes in flight, not *messages awaiting ack*, and only the latter is the operator's concept.

**`github.com/spf13/cobra`** — CLI. From the docs: `PersistentFlags` on the root for `--config`/`--output`/`--socket`; `RunE` for real exit codes; `GenBashCompletion`/`GenZshCompletion`/`GenFishCompletion` for `messq completion`; and `RegisterFlagCompletionFunc` returning `[]cobra.Completion` with `ShellCompDirective` — used to complete live stream and consumer names from the running daemon, with `ShellCompDirectiveNoFileComp` so a mistyped name never silently falls back to filenames. Chosen over `urfave/cli` and hand-rolled flags because the completion machinery is a genuine 3am reliability feature, not decoration.

**`github.com/knadh/koanf/v2`** (+ `parsers/toml`, `providers/file`, `providers/env`, `providers/posflag`) — config. The docs demonstrate exactly the layered-precedence pattern §8.1 needs (defaults → file → env → flags, each `Load` merging over the last) and struct unmarshalling with `koanf:"…"` tags including nested structs and `time.Duration`. Critically, the docs also state that the file watcher is *not goroutine-safe during concurrent `Load` calls* — which is why messq deliberately does **not** use `file.Watch` and instead reloads explicitly on SIGHUP under a mutex into a fresh instance, then swaps an `atomic.Pointer[Config]`. Chosen over Viper for the same reason: no package-level global state, no implicit magic, and a small dependency tree.

**`github.com/prometheus/client_golang`** — metrics. From the docs: `prometheus.NewRegistry()` + `promauto.With(reg)` so metrics are constructed against an explicit registry rather than the process-global default (which is how transitive dependencies silently add metrics and cardinality); `promhttp.HandlerFor(reg, promhttp.HandlerOpts{MaxRequestsInFlight: 4, ErrorLog: …})` to bound scrape concurrency and route collector errors into slog instead of the default logger; `ExponentialBuckets` for latency histograms; native histograms available behind a config flag for latency metrics where bucket choice is hard. Explicit registry + the label-cardinality rule in §7.5 are what keep a broker's metrics from becoming the monitoring system's incident.

**`github.com/oklog/ulid/v2`** — message IDs. Per the ULID spec fetched via context7: 128-bit, 26-character Crockford base32, 48-bit millisecond timestamp plus 80 bits of entropy, lexicographically sortable, case-insensitive, URL-safe. Every one of those properties is an operator property — sortable means `sort` works on logs, base32 means it survives copy-paste from a terminal and a chat message, and the embedded timestamp means an operator can date a message from its ID alone. Monotonic entropy is used within a millisecond so IDs from one publisher never collide or reorder.

**`github.com/cespare/xxhash/v2`** — subject digests in segment seal trailers and stream-id hashing. Non-cryptographic, extremely fast, stable across versions. (Record integrity uses CRC32C from stdlib, not xxhash — CRC is the right tool for torn-write detection and is hardware-accelerated.)

**`golang.org/x/sys/unix`** — `Fallocate`, `Fdatasync`, `Statfs`, `Flock`, `Ftruncate`. Direct syscalls, no cgo. These are the primitives §3 is built on; wrapping them in a third-party abstraction would hide exactly the errors that matter.

**`github.com/coreos/go-systemd/v22/daemon`** — `sd_notify` (`READY=1`, `STATUS=`, `WATCHDOG=1`, `STOPPING=1`) and `SdWatchdogEnabled`. Tiny, zero-dependency, and it is what makes `Type=notify` + `WatchdogSec` in §8.6 work — the difference between systemd knowing we are recovered and systemd guessing. The systemd documentation confirms the protocol contract (`NotifyAccess=main`, watchdog interval delivered via `$WATCHDOG_USEC`, ping at half the interval).

### 12.2 Considered and rejected

**`modernc.org/sqlite`** (cgo-free) / `mattn/go-sqlite3` — the obvious "single-file store" answer, and rejected as the *primary* engine on operational grounds. From the fetched driver docs, the tuning surface is DSN pragmas (`_journal=WAL`, `_synchronous`, `_busy_timeout`/`_timeout`, `_txlock=immediate`) with documented `SQLITE_BUSY` retry guidance — a single-writer model no better than bbolt's, plus SQL parse/plan overhead on a hot ack path. Two harder problems decide it: (a) storing bodies in rows means delete amplification and `VACUUM`, and incremental vacuum still churns pages under a queue's write-once/delete-soon pattern; (b) **the durability default is a trap** — the popular Go drivers set `synchronous=NORMAL` in WAL mode, and SQLite's own documentation is explicit that a transaction committed in WAL mode with `synchronous=NORMAL` *can roll back after a power loss*. A queue whose durability silently depends on a driver's DSN default is precisely the kind of surprise this plan exists to eliminate. Keeping a hand-written segment format means the durability line is one visible `fdatasync` call in messq's own code. SQLite remains a good answer for a *future* offline analysis/export tool over exported NDJSON.

**`dgraph-io/badger`** — LSM with value log; excellent write throughput, but adds a background GC whose pauses and space amplification are another thing to explain at 3am, and higher baseline memory. Rejected: for ≤125 MB of bounded state, B+tree simplicity wins.

**`spf13/viper`** — package-level global state, implicit type coercion, larger dependency tree, and a file watcher with the same concurrency caveat. koanf does the same job with less to reason about.

**`uber-go/zap` / `rs/zerolog`** — faster than slog, but §7.1 makes the log schema a public API and stdlib is the right owner for a public API. If journal throughput ever becomes the bottleneck (R6), the fix is a custom `slog.Handler` that writes pre-serialised bytes, not a new logging library.

**`hashicorp/raft` + `raft-wal`** — would give replication. Explicitly out of scope (§1.5); `raft-wal`'s segment design (length + CRC32C + commit-frame CRC over everything since the last fsync, 8-byte-aligned headers so a frame header lands in one sector, accept-a-torn-tail on recovery) is nonetheless the reference for messq's own segment format in §3.3.

**`nats-io/nats-server` embedded** — would deliver most of the feature list immediately, and is the honest recommendation for anyone who needs HA. Rejected because it defeats the entire premise: the product here is *a broker small enough to understand in an evening, whose every state transition is visible*, and embedding a full JetStream server delivers neither the smallness nor the ownership of the observability surface.

**OpenTelemetry SDK (at 1.0)** — deferred to M15. Prometheus plus a `trace_id` on every journal event covers the 3am case; the OTel SDK's dependency surface and configuration complexity are not worth carrying before there is a user asking for spans.

---

## Appendix A — Definition of Done (applies to every milestone)

1. Code + unit tests + property tests where state is involved.
2. Crash-injection coverage if the change touches the write path.
3. Metrics for every new bounded quantity and every new rejection reason.
4. Journal events for every new state transition, added to `docs/journal-schema.md`.
5. Golden log/metric test updated.
6. CLI surface for anything an operator might need at 3am, with `--output json`.
7. Runbook section, with executable `CONFIRM`/`MITIGATE` blocks.
8. An entry in the failure-mode catalogue (§8.7) if a new failure mode was introduced.
9. Config keys documented in `messq config show --defaults` with an origin.
10. Upgrade note: does this change `format_version`? If yes, a migration and a golden data dir.

## Appendix B — The one-page cheat sheet (shipped as `messq help ops`)

```
IS IT ALIVE?        messq status              curl -s localhost:4391/healthz | jq
WHO IS BEHIND?      messq consumer ls --sort=lag
WHERE IS MSG X?     messq msg trace <mid>
WHAT'S IN FLIGHT?   messq inflight <stream>/<consumer> --sort=deadline
WHY IS DISK FULL?   messq disk
WHAT'S IN THE DLQ?  messq dlq ls <stream> --group-by=cause
WATCH IT LIVE       messq tail --stream orders --event dead_lettered
SAFE TO RETRY?      messq dlq redrive <s> --limit 10 --rate 5/s --dry-run
STOP A CONSUMER     messq consumer pause <s>/<c>          (reversible)
RESTART SAFELY      messq drain --timeout 60s ; systemctl restart messq
DID WE LOSE DATA?   messq store verify        (and: messq_msgs_dropped_total should be 0)
```
