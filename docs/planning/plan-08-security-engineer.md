# messq — Project Plan (Security Engineer lens)

> Plan author persona: **the security engineer**. Threat model first; every design choice below is
> justified by what it does to the attack surface, and every control is checked against the
> single-binary, one-command promise. A control that makes `messq serve` harder than `redis-server`
> gets turned off in the real world, and a control that is turned off is not a control.

---

## 0. Reading guide

| Section | What it settles |
|---|---|
| 1 | Vision, positioning, threat model, non-goals |
| 2 | Process/goroutine architecture and data flow |
| 3 | Storage engine, schema, fsync policy, crash recovery |
| 4 | Delivery state machine, precise transitions and guards |
| 5 | Wire protocol and endpoints |
| 6 | CLI and developer experience |
| 7 | Logging, audit chain, metrics |
| 8 | Testing strategy (security tests are first-class) |
| 9 | Milestones M0→M9 from an empty repo |
| 10 | Risks and open questions |
| 11 | Library choices, grounded in fetched docs |

---

## 1. Vision & positioning

### 1.1 The thesis

Every small broker in this niche made the same bet and lost it. beanstalkd has **no authentication
at all** and its own documentation tells you to firewall the port. NSQ assumes a trusted network
between `nsqd` and `nsqlookupd`, and its auth-server traffic is expected to be unencrypted on a
trusted network. RabbitMQ ships a `guest` account with full administrative rights, and in July 2026
CVE-2026-57219 let anyone who could reach the management port retrieve the broker's OAuth client
secret without authenticating — game over on any deployment where that port drifted onto an
untrusted network. Kafka, meanwhile, has a genuinely good authorization model that almost nobody
configures correctly because doing so takes a week.

The gap in the market is not "a queue with security features". It is **a queue whose secure
configuration is the one you get by typing `messq serve`**, and where the record of what happened
to each message is strong enough to hand to an auditor.

So messq is positioned as:

> *The broker you can put in a regulated internal workflow without writing a compensating-control
> memo. At-least-once semantics borrowed from NATS JetStream, an authorization model borrowed from
> capability systems, a tamper-evident audit trail borrowed from transparency logs, and an
> operational surface small enough that a reviewer can read all of it in an evening.*

### 1.2 Six invariants the product is judged against

1. **No unauthenticated path exists, ever.** There is no "trusted network" mode, no `guest` user, no
   flag that disables authn. The cheapest credential (kernel peer credentials on a unix socket) is
   free to use and cannot be forged, so there is no excuse for an escape hatch.
2. **The default listener is not a network listener.** `messq serve` with no config binds
   `/run/messq/messq.sock` mode `0660` and nothing else. Reaching the network is an explicit,
   TLS-mandatory act.
3. **Authorization is per subject pattern per verb, deny-by-default.** Subjects are the only tenant
   boundary a broker without namespaces has, so they must be the unit of authorization.
4. **The audit trail is hash-chained and separately signable.** Deleting a row must be detectable by
   an outsider who holds only a checkpoint.
5. **Payload bytes never enter the log stream.** Payloads are opaque to the broker; the broker never
   parses them; the logger cannot print them without a config-file opt-in that itself gets audited.
6. **The binary is reproducible, signed, SBOM'd, and built from vendored, budget-capped
   dependencies.** Whose code runs as the broker user is an authorization decision.

### 1.3 Threat model

**Assets, in priority order**

| # | Asset | Why an attacker wants it |
|---|---|---|
| A1 | Message payloads | The business data itself (orders, PII, webhooks, credentials in transit) |
| A2 | Message metadata (subjects, IDs, timing) | Traffic analysis, business intelligence, targeting |
| A3 | Consumer cursors and ack state | Replay to cause duplicate side effects; skip-ahead to cause silent data loss |
| A4 | The audit trail | Cover tracks after any of the above |
| A5 | Tokens, TLS keys, audit signing key | Everything else, transitively |
| A6 | Availability | Ransom / disruption of the workflows that depend on the queue |

**Trust boundaries**

```
 ┌────────────────┐  B1 unix socket (peercred)      ┌───────────────────┐
 │ local process  │────────────────────────────────▶│                   │
 └────────────────┘                                 │                   │
 ┌────────────────┐  B2 TCP + TLS (token or mTLS)   │   messq daemon    │
 │ remote service │────────────────────────────────▶│   (one process,   │
 └────────────────┘                                 │    one uid)       │
 ┌────────────────┐  B3 CLI over B1/B2              │                   │
 │    operator    │────────────────────────────────▶│                   │
 └────────────────┘                                 └─────────┬─────────┘
                                                              │ B4 filesystem
                                                    ┌─────────▼─────────┐
 ┌────────────────┐  B5 build & release             │  /var/lib/messq   │
 │  CI / registry │────────────────────────────────▶│  messq.db  0600   │
 └────────────────┘                                 └───────────────────┘
```

**Adversaries and the controls that stop them**

| ID | Adversary | Capability | Primary controls |
|---|---|---|---|
| **T1** | Unauthenticated network attacker | Can reach the TCP port | TCP listener refuses to start without TLS; every route requires a credential; `/healthz` returns a constant with no version or topology; connection-level rate limit and `ReadHeaderTimeout` |
| **T2** | Authorized tenant exceeding scope | Holds a valid token for `orders.eu.>` | Deny-by-default grants; **pattern-coverage check** on consumer binding (§5.4); stream subject-prefix ownership prevents subject squatting; per-token quotas |
| **T3** | Local unprivileged user on the broker host | Shell as another uid | Socket `0660` owned by `messq:messq`; peercred uid/gid mapping; state dir `0700`; no secrets in argv; `messq.db` `0600` |
| **T4** | Compromised / malicious consumer | Holds a consume token | Single-use delivery leases prevent acking someone else's in-flight message; `max_ack_pending` and lease-extension caps prevent message hostage-taking; `replay`/`purge`/`dlq_redrive` are separate capabilities it does not have |
| **T5** | Insider operator covering tracks | Write access to `messq.db` | Hash-chained audit; Ed25519-signed checkpoints; external checkpoint shipping; broker refuses to start on a broken chain; `messq audit verify` in cron with a staleness metric |
| **T6** | Supply-chain attacker | Can publish a malicious module version or CI action | Vendored deps in-tree (upgrades are reviewable diffs); hard cap of 6 direct deps; `govulncheck` on PRs *and* nightly against released tags; SHA-pinned Actions; reproducible builds verified by double-build diff; cosign + SLSA provenance |
| **T7** | Offline disk thief / backup leak | Has the data file, not the process | File permissions; documented full-disk encryption; Phase-2 envelope encryption with a KEK the broker loads from `systemd LoadCredential=` and never writes next to the DB; **client-side encryption is the recommended answer** |
| **T8** | Sloppy operator (the most common one) | Good intentions, no time | Secure defaults; `messq doctor`; refusal to load a key file with permissions broader than `0600`; refusal to bind non-loopback plaintext |

**Explicit non-goals — stated so nobody assumes them**

- messq does **not** defend against root on the broker host. Root can read the DB, the keys, and the
  chain. Detection (external checkpoints) is what we offer, not prevention.
- messq does **not** hide payloads from the broker operator. If your threat model includes the
  broker operator, encrypt client-side; payloads are opaque bytes and we will never parse them, so
  this works perfectly and we document the pattern.
- No quorum replication, no consensus, no cross-node consistency. Availability is a
  single-node story: process supervision plus fast crash recovery.
- No anti-traffic-analysis. Subjects and sizes are visible to anyone who can read the DB or the logs.
- No protection against an authorized principal DoSing itself beyond configured quotas.

---

## 2. Architecture overview

### 2.1 Process model

One process. One uid. No forking, no helper daemons, no embedded scripting, no plugin loading. The
`messq` binary is both the daemon and the CLI; the CLI never touches the data files directly, it
always goes through the API (so every operator action passes the same authz and audit path as an
application action — no back door, no "offline mode" that skips the audit chain).

```
messq serve
├── listener/unix        (default; SO_PEERCRED authn)
├── listener/tcp+tls     (opt-in; token or mTLS authn)
├── listener/metrics     (opt-in; separate addr, loopback default, own authn setting)
│
├── goroutine per connection  →  net/http handlers
│      auth → authz → validate → enqueue op → wait for result
│
├── writer goroutine (single)         serialises ALL mutations; group-commits to SQLite
├── delivery planner goroutine        picks next messages per consumer, respects flow control
├── ack-timeout sweeper goroutine     ticks every 250 ms, expires leases
├── retention goroutine               ticks every 30 s, prunes below the retention floor
├── audit checkpoint goroutine        ticks every 60 s / 10k events, signs a checkpoint
└── read pool (N conns, N = GOMAXPROCS)   read-only SQLite connections for fetch/peek/inspect
```

**Why a single writer goroutine.** SQLite in WAL mode still allows exactly one writer at a time. By
funnelling all writes through one goroutine with an internal batching window we (a) never see
`SQLITE_BUSY` on the write path at all, (b) get free group commit so one `fsync` covers many
publishes, and (c) get a single, auditable place where every state transition is written and the
matching audit row is appended in the *same transaction*. That last point is a security property:
**it is impossible for a state change to be committed without its audit row**, because they share a
transaction.

### 2.2 Layering

```
cmd/messq              cobra command tree, no business logic
internal/api           HTTP handlers, request/response types, size limits
internal/auth          principals, credential verification (peercred/token/mTLS)
internal/authz         subject grammar, grants, coverage decision procedure  ← security-critical
internal/broker        delivery state machine, planner, sweeper, flow control ← correctness-critical
internal/store         SQLite schema, migrations, writer, read pool, recovery
internal/audit         canonical encoding, hash chain, checkpoints, verify
internal/obs           slog setup, redaction, event taxonomy, prometheus metrics
internal/config        TOML + env + flags, effective-config dump with redaction
internal/simtest       clock/rand/fault injection seams used by tests
```

`internal/authz` and `internal/audit` are marked in CODEOWNERS as requiring a second reviewer. They
are the two packages where a subtle bug is silently exploitable rather than loudly broken.

### 2.3 Data flow — publish

```
client ──POST /v1/streams/orders/publish──▶ handler
   │  body ≤ max_message_size (io.LimitReader, hard)
   │  authn: peercred | token | client cert       → principal
   │  authz: principal has publish on subject      → allow/deny (deny ⇒ audit + 403)
   │  validate: subject grammar, headers allowlist, msg_id length
   ▼
writeOp{publish} ──▶ writer goroutine
   │  batches ops arriving within 2 ms (max 256 ops / 8 MiB)
   │  BEGIN IMMEDIATE
   │    dedup check on (stream_id, msg_id) within the dedup window
   │    INSERT INTO messages (...) seq = next_seq(stream)
   │    INSERT INTO audit (...) event='publish.accepted', hash = H(prev||event)
   │  COMMIT                      ← SQLite fsyncs the WAL here (synchronous=FULL)
   ▼
handler returns 200 {stream, seq, msg_id, trace_id, durability:"fsync"}
   │
   └─▶ delivery planner is woken (channel signal, no polling)
```

### 2.4 Data flow — fetch/ack

```
client ──POST /v1/consumers/jobs-w1/fetch {max_messages:32, expires:"20s"} ──▶ handler
   │  authz: consume + the consumer's filter must be COVERED by the token's grants
   │  ask planner for up to N messages honouring max_ack_pending / max_pending_bytes
   │  planner: reserve rows → writer op {deliver} → pending rows written + audited
   ▼
NDJSON stream, one delivery per line:
   {"seq":941,"subject":"orders.eu.new","msg_id":"01JB...","attempt":1,
    "lease":"lz_9f2c...","lease_expires":"2026-08-21T10:00:30Z","trace_id":"...",
    "headers":{...},"payload_b64":"..."}
   (long-polls until `expires` if nothing is available; returns an empty stream, not an error)

client ──POST /v1/consumers/jobs-w1/ack {acks:[{lease:"lz_9f2c...","action":"ack"}]}──▶
   │  lease looked up by primary key, compared in constant time, checked not expired
   │  wrong/expired ⇒ 409 lease_expired + audit event (never a silent success)
   ▼
writer: DELETE pending row, advance ack_floor, INSERT audit 'delivery.ack'
```

---

## 3. Storage & durability design

### 3.1 Engine decision: SQLite via `modernc.org/sqlite`

**Decision: one SQLite database file, `$STATE/messq.db`, mode `0600`, WAL mode, accessed through the
pure-Go `modernc.org/sqlite` driver.**

Security-lens justification:

- **No cgo.** `CGO_ENABLED=0` gives a fully static binary, cross-compilable, `-trimpath`-clean, and
  bit-for-bit reproducible. That is a prerequisite for the SLSA/double-build verification story in
  §11.6. `mattn/go-sqlite3` would force a C toolchain into the release path, tie us to a libc
  version, and make reproducibility materially harder.
- **One file to protect.** Exactly one file to `chmod`, one file to back up consistently, one file
  to encrypt at the filesystem layer. Segment-file designs multiply the number of things an operator
  can get wrong. (Phase 3 may add external segments for very large payloads — with its own threat
  review.)
- **The inspection story is the product.** `messq peek`, `messq trace`, replay and DLQ triage are
  core features. SQL over a proper schema makes these ~20 lines each and makes ad-hoc forensics
  possible with the `sqlite3` shell during an incident. bbolt would push all of that into
  hand-rolled index maintenance — more code, more places to have an off-by-one that leaks another
  tenant's message.
- **Payloads inline.** Payloads are stored as BLOBs in `messages`. Default `max_message_size` is
  1 MiB (configurable up to 32 MiB). We never memory-map payload data into a shared region and we
  never hand out a `[]byte` that aliases another message's buffer.

Rejected: **bbolt** — its own README documents an exclusive write lock (no second process can even
read for forensics), mmap memory growth on large DBs, and poor page utilisation / slow bulk inserts
into a new bucket, which is exactly our publish pattern. Rejected: **custom log segments** — best
raw performance, worst review surface, and the thing we are selling is review surface.

### 3.2 Connection configuration

Writer connection (exactly one, `sql.DB` with `SetMaxOpenConns(1)`):

```
file:/var/lib/messq/messq.db?_txlock=immediate&_journal=WAL&_timeout=5000
  &_pragma=synchronous(FULL)&_pragma=foreign_keys(1)
  &_pragma=journal_size_limit(67108864)&_pragma=wal_autocheckpoint(2000)
```

Reader pool (`GOMAXPROCS` connections, read-only):

```
file:/var/lib/messq/messq.db?mode=ro&_journal=WAL&_timeout=5000
  &_pragma=synchronous(NORMAL)&_pragma=query_only(1)
```

`_txlock=immediate` matters: it takes the write lock at `BEGIN` rather than on first write, which
eliminates the deferred-to-write lock-upgrade deadlock class. `_timeout` (busy timeout) is set as a
belt-and-braces measure even though the single-writer design means we should never contend; if we
ever *do* see `SQLITE_BUSY` it is a bug and it is logged at ERROR with a dedicated event so it
cannot hide.

The DB file is created with `0600` by opening it ourselves with `os.OpenFile(..., 0600)` before
handing the path to the driver, and the state directory is created `0700`. On startup we `Stat` both
and **refuse to run** if the modes are broader, unless `--allow-insecure-perms` is set (which emits a
`config.insecure` audit event on every start).

### 3.3 Schema

```sql
-- v1 schema. Every migration runs inside one transaction and bumps meta.schema_version.

CREATE TABLE meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
) STRICT;                 -- schema_version, instance_id, epoch, audit_chain_head, created_at

CREATE TABLE streams (
  id              INTEGER PRIMARY KEY,
  name            TEXT NOT NULL UNIQUE,
  subject_prefix  TEXT NOT NULL,          -- owned namespace, e.g. "orders." ; enforced disjoint
  next_seq        INTEGER NOT NULL DEFAULT 1,
  max_msgs        INTEGER NOT NULL DEFAULT 0,   -- 0 = unlimited
  max_bytes       INTEGER NOT NULL DEFAULT 0,
  max_age_ns      INTEGER NOT NULL DEFAULT 0,
  dedup_window_ns INTEGER NOT NULL DEFAULT 120000000000,  -- 2 min
  created_at_ns   INTEGER NOT NULL,
  created_by      TEXT NOT NULL
) STRICT;

CREATE TABLE messages (
  stream_id     INTEGER NOT NULL REFERENCES streams(id),
  seq           INTEGER NOT NULL,
  subject       TEXT    NOT NULL,
  msg_id        TEXT,                       -- broker-assigned ULID; also the idempotency key
  headers       BLOB,                       -- canonical CBOR-ish TLV; size-capped
  payload       BLOB    NOT NULL,
  payload_len   INTEGER NOT NULL,
  payload_sha256 BLOB   NOT NULL,           -- integrity check + redaction bookkeeping
  publisher     TEXT    NOT NULL,           -- principal name
  trace_id      TEXT,
  published_at_ns INTEGER NOT NULL,
  expires_at_ns INTEGER NOT NULL DEFAULT 0, -- per-message TTL, 0 = stream policy
  redacted_at_ns INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (stream_id, seq)
) STRICT;
CREATE INDEX messages_subject ON messages(stream_id, subject, seq);
CREATE UNIQUE INDEX messages_msgid ON messages(stream_id, msg_id) WHERE msg_id IS NOT NULL;

CREATE TABLE consumers (
  id               INTEGER PRIMARY KEY,
  name             TEXT NOT NULL,
  stream_id        INTEGER NOT NULL REFERENCES streams(id),
  filter           TEXT NOT NULL,            -- subject pattern, e.g. "orders.eu.>"
  ack_wait_ns      INTEGER NOT NULL DEFAULT 30000000000,
  max_deliver      INTEGER NOT NULL DEFAULT 5,
  backoff_ns       TEXT NOT NULL DEFAULT '[1e9,5e9,3e10,1.2e11,6e11]',
  max_ack_pending  INTEGER NOT NULL DEFAULT 256,
  max_pending_bytes INTEGER NOT NULL DEFAULT 67108864,
  max_ack_extension_ns INTEGER NOT NULL DEFAULT 150000000000,  -- 5 × ack_wait
  ordered          INTEGER NOT NULL DEFAULT 0,  -- per-subject-key ordering
  partition_header TEXT,                        -- header used as ordering key, default: subject
  deliver_policy   TEXT NOT NULL DEFAULT 'all', -- all | new | by_seq | by_time
  start_seq        INTEGER NOT NULL DEFAULT 0,
  dlq_policy       TEXT NOT NULL DEFAULT 'dlq', -- dlq | hold
  owner            TEXT NOT NULL,
  created_at_ns    INTEGER NOT NULL,
  UNIQUE(stream_id, name)
) STRICT;

CREATE TABLE cursors (
  consumer_id   INTEGER PRIMARY KEY REFERENCES consumers(id),
  ack_floor     INTEGER NOT NULL DEFAULT 0,   -- all seq ≤ ack_floor are done
  delivered_seq INTEGER NOT NULL DEFAULT 0,
  acked_above   BLOB    NOT NULL DEFAULT x'', -- sparse bitmap of acks above the floor
  updated_at_ns INTEGER NOT NULL
) STRICT;

CREATE TABLE pending (
  consumer_id    INTEGER NOT NULL REFERENCES consumers(id),
  seq            INTEGER NOT NULL,
  attempt        INTEGER NOT NULL,
  lease          BLOB    NOT NULL,           -- 16 random bytes, single use
  lease_expires_ns INTEGER NOT NULL,
  extended_by_ns INTEGER NOT NULL DEFAULT 0, -- capped by max_ack_extension_ns
  delivered_at_ns INTEGER NOT NULL,
  delivered_to   TEXT NOT NULL,              -- principal that received it
  PRIMARY KEY (consumer_id, seq)
) STRICT;
CREATE INDEX pending_expiry ON pending(lease_expires_ns);
CREATE UNIQUE INDEX pending_lease ON pending(lease);

CREATE TABLE redeliver (                     -- naks with a delay / backoff schedule
  consumer_id  INTEGER NOT NULL REFERENCES consumers(id),
  seq          INTEGER NOT NULL,
  attempt      INTEGER NOT NULL,
  ready_at_ns  INTEGER NOT NULL,
  PRIMARY KEY (consumer_id, seq)
) STRICT;
CREATE INDEX redeliver_ready ON redeliver(ready_at_ns);

CREATE TABLE dlq (
  consumer_id  INTEGER NOT NULL REFERENCES consumers(id),
  seq          INTEGER NOT NULL,
  reason       TEXT NOT NULL,        -- max_deliver | terminated | poison | redrive_failed
  attempts     INTEGER NOT NULL,
  last_error   TEXT,
  moved_at_ns  INTEGER NOT NULL,
  PRIMARY KEY (consumer_id, seq)
) STRICT;

CREATE TABLE principals (
  name         TEXT PRIMARY KEY,
  kind         TEXT NOT NULL,        -- peer | token | cert
  uid          INTEGER,              -- kind=peer
  gid          INTEGER,              -- kind=peer
  cert_subject TEXT,                 -- kind=cert: SPIFFE URI SAN or exact RFC4514 subject
  grants       TEXT NOT NULL,        -- canonical JSON array of {verb, pattern, effect}
  disabled_at_ns INTEGER NOT NULL DEFAULT 0,
  created_at_ns INTEGER NOT NULL,
  created_by   TEXT NOT NULL
) STRICT;

CREATE TABLE tokens (
  id            TEXT PRIMARY KEY,     -- public token id, appears in logs
  principal     TEXT NOT NULL REFERENCES principals(name),
  secret_sha256 BLOB NOT NULL,        -- SHA-256 of the 256-bit random secret. Never the secret.
  label         TEXT NOT NULL,
  created_at_ns INTEGER NOT NULL,
  created_by    TEXT NOT NULL,
  expires_at_ns INTEGER NOT NULL DEFAULT 0,
  revoked_at_ns INTEGER NOT NULL DEFAULT 0,
  last_used_at_ns INTEGER NOT NULL DEFAULT 0,
  last_used_from TEXT
) STRICT;
CREATE UNIQUE INDEX tokens_secret ON tokens(secret_sha256);

CREATE TABLE audit (
  id          INTEGER PRIMARY KEY,     -- monotone, gapless
  ts_ns       INTEGER NOT NULL,
  event       TEXT NOT NULL,
  actor       TEXT NOT NULL,
  auth_method TEXT NOT NULL,
  remote      TEXT,
  stream      TEXT, consumer TEXT, subject TEXT,
  seq         INTEGER, attempt INTEGER,
  msg_id      TEXT, trace_id TEXT,
  outcome     TEXT NOT NULL,           -- ok | denied | failed
  reason      TEXT,
  meta        TEXT,                    -- canonical JSON, never payload bytes
  prev_hash   BLOB NOT NULL,
  hash        BLOB NOT NULL
) STRICT;
CREATE INDEX audit_msgid ON audit(msg_id);
CREATE INDEX audit_trace ON audit(trace_id);
CREATE INDEX audit_ts    ON audit(ts_ns);

CREATE TABLE audit_checkpoints (
  audit_id   INTEGER PRIMARY KEY REFERENCES audit(id),
  ts_ns      INTEGER NOT NULL,
  chain_size INTEGER NOT NULL,
  head_hash  BLOB NOT NULL,
  key_id     TEXT NOT NULL,
  signature  BLOB NOT NULL             -- Ed25519 over the canonical checkpoint encoding
) STRICT;
```

### 3.4 fsync policy — a per-publish contract, not a global switch

Most brokers make durability a startup flag, which means the person who set it and the person who
depends on it are different people who never spoke. messq makes it part of the publish request and
part of the response, so the contract is visible at the call site and in the audit trail.

| `durability` | Broker acks after | Survives | Default |
|---|---|---|---|
| `fsync` | WAL commit with `synchronous=FULL` — data is on stable storage | process crash **and** power loss | ✅ yes |
| `buffered` | commit returns, no fsync guarantee (`synchronous=NORMAL` semantics) | process crash only | opt-in |

- Default is `fsync`. `buffered` must be requested explicitly per publish, and the choice is written
  into the `publish.accepted` audit event so nobody can retroactively claim they had durability.
- **Group commit makes `fsync` affordable.** The writer goroutine accumulates operations for up to
  `write_batch_window` (default 2 ms, max 256 ops or 8 MiB) and commits them in one transaction, so
  one fsync covers the whole batch. On a datacenter SSD (~1–3 k fsync/s) this yields tens of
  thousands of messages/s with real power-loss durability. Measuring this is an **M1 exit gate**,
  not an assumption (§10).
- `wal_autocheckpoint(2000)` plus an explicit `wal_checkpoint(TRUNCATE)` every 5 minutes bounds WAL
  growth so a crash never has to replay an unbounded WAL.
- `min_free_disk` (default 256 MiB): below it, publishes are rejected with `507` and a
  `limit.exceeded` event, while acks/naks/DLQ writes continue to be accepted. **A full disk must
  never wedge the ack path**, because a wedged ack path turns into mass redelivery.

### 3.5 Crash recovery

On every start, in this order, before any listener binds:

1. **Permission audit.** Verify state dir `0700`, DB `0600`, key files `0600`. Refuse otherwise.
2. **Open + schema check.** `PRAGMA integrity_check` on first start after an unclean shutdown
   (detected by a `clean_shutdown` flag in `meta`). Run migrations in one transaction.
3. **Epoch bump.** `meta.epoch += 1`. The epoch appears in every log and audit event, so an operator
   can see at a glance which events belong to which broker lifetime.
4. **Lease invalidation.** `UPDATE pending SET lease = randomblob(16), lease_expires_ns = 0`. Every
   lease handed out before the crash is now cryptographically dead: a client that survived the crash
   and tries to ack gets `409 lease_expired` rather than silently retiring a message that is about
   to be redelivered to someone else. All pending rows become immediately redeliverable, subject to
   the backoff schedule for their attempt number.
5. **Audit chain verification.** Recompute the hash chain from the most recent signed checkpoint to
   the head (bounded; `--audit-verify-window` default 100 000 events, `full` for everything). If the
   chain does not verify, **the broker refuses to start.** Recovery is an explicit operator act:
   `messq audit repair --confirm` writes a signed `audit.chain_break` event recording the last good
   id, the head hash it found, and the operator identity, then starts a new chain segment.
6. **Retention floor recompute.** `min(ack_floor over consumers, min seq referenced by pending,
   redeliver, dlq)`. Nothing below this may be deleted by retention.
7. Emit `recovery.completed` with counts: messages, pending reset, DLQ size, chain events verified.

A crash between the WAL commit and the HTTP response is indistinguishable from a lost response, so
publishers must retry with the same `msg_id`; the dedup window (default 2 min, configurable per
stream) turns that retry into an idempotent no-op that returns the original `seq`. This is the same
bargain JetStream makes and it is honest at-least-once.

---

## 4. Delivery semantics & message lifecycle

### 4.1 The state machine

State is per `(consumer, seq)`. There is exactly one authority: the `pending` / `redeliver` / `dlq`
tables plus the cursor. A client can only *propose* transitions, and every proposal must present a
valid lease.

```
                     ┌──────────────────────────────────────────────┐
                     │                                              │
                     ▼                                              │
   published ──▶ AVAILABLE ──deliver──▶ IN_FLIGHT ──ack────▶ ACKED (terminal)
                     ▲                     │
                     │                     ├──nak(delay)──▶ WAITING ──ready──┐
                     │                     │                                 │
                     │                     ├──timeout─────▶ WAITING ─────────┤
                     │                     │                                 │
                     │                     ├──extend──────▶ IN_FLIGHT (same attempt)
                     │                     │
                     │                     └──term────────▶ DEAD ──▶ DLQ
                     │
                     └──redrive (privileged)── DLQ
                                               │
                     attempt ≥ max_deliver ────┘
```

### 4.2 Transitions, guards, effects

| Transition | Trigger | Guards | Effects | Audit event |
|---|---|---|---|---|
| `deliver` | planner | `attempt < max_deliver` ∧ `pending_count < max_ack_pending` ∧ `pending_bytes < max_pending_bytes` ∧ (if `ordered`) no other in-flight message with the same partition key | insert `pending` with `attempt = prev+1`, fresh 16-byte lease, `lease_expires = now + backoff[attempt]` (falling back to `ack_wait`); `delivered_seq = max(delivered_seq, seq)` | `delivery.sent` |
| `ack` | client | lease matches (constant-time) ∧ not expired | delete `pending`; set bit in `acked_above`; advance `ack_floor` past contiguous acked run | `delivery.ack` |
| `nak` | client | lease valid | delete `pending`; insert `redeliver` with `ready_at = now + (delay ?? backoff[attempt])`; if `attempt ≥ max_deliver` → DLQ instead | `delivery.nak` |
| `extend` | client | lease valid ∧ `extended_by + Δ ≤ max_ack_extension` | `lease_expires += Δ`; **attempt unchanged** | `delivery.extend` |
| `term` | client | lease valid | delete `pending`; insert `dlq` with `reason='terminated'` | `delivery.term` |
| `timeout` | sweeper | `lease_expires ≤ now` | delete `pending`; if `attempt ≥ max_deliver` → DLQ (`reason='max_deliver'`) else insert `redeliver` with backoff | `delivery.timeout` (+ `delivery.dlq`) |
| `ready` | sweeper | `ready_at ≤ now` | delete `redeliver`; message becomes AVAILABLE | `delivery.redeliver` |
| `redrive` | operator | principal holds `dlq_redrive` on the subject | delete `dlq`; reset `attempt = 0`; make AVAILABLE | `dlq.redrive` |
| `seek` | operator | principal holds `replay` on the consumer's filter | set `ack_floor`, clear `acked_above`, clear `pending`/`redeliver` for the consumer | `cursor.seek` |

Backoff default: `[1s, 5s, 30s, 2m, 10m]`, truncated to `max_deliver`, with ±20 % jitter to avoid
thundering herds after a downstream outage. A `nak` with an explicit delay overrides the schedule.
This mirrors JetStream, where `Backoff` overrides `AckWait` and the first backoff value sets the
initial ack-wait — we adopt the same rule so that anyone who knows JetStream already knows messq.

### 4.3 The lease — why it exists

A naive broker acks by `(consumer, seq)`. That is a bug waiting for a slow consumer: worker A gets
seq 941, stalls past `ack_wait`, the broker redelivers 941 to worker B, then worker A wakes up and
acks 941 — retiring work that B is still doing, or worse, retiring B's in-flight delivery so the
message is lost if B then crashes.

messq gives each delivery attempt a **single-use 16-byte lease from `crypto/rand`**, stored in the
`pending` row with a unique index. An ack must present the lease. Consequences:

- A stale ack from a previous attempt fails with `409 lease_expired` and produces a
  `delivery.ack_rejected` audit event with both the presented lease id prefix and the current
  attempt. That event is a *signal*: it means your `ack_wait` is too short for your workload, and
  it shows up in `messq doctor` and as a metric.
- A malicious consumer cannot ack a message it was never given, even within its own consumer, since
  it cannot guess a 128-bit value.
- Revocation is exact and free: deleting the row invalidates the lease. No key management, no
  clock-skew window, no HMAC to get wrong.
- After a crash all leases are re-randomised (§3.5), so nothing from a previous broker lifetime can
  apply.

### 4.4 Ordering

`ordered = true` on a consumer means: at most one in-flight message per **partition key**, where the
key is the full subject by default or the value of `partition_header` if set. The planner keeps an
in-memory set of locked keys; a key is unlocked on ack, term, or DLQ. This gives per-subject FIFO
processing without partitions, at the cost of head-of-line blocking within a key — which is exactly
the trade the user asked for by setting the flag. Unordered consumers (the default) may have many
in-flight messages per subject.

### 4.5 Flow control

Three independent limits, all server-enforced (a client cannot opt out):

- `max_ack_pending` — count of in-flight messages per consumer (default 256).
- `max_pending_bytes` — bytes in flight per consumer (default 64 MiB). Protects broker memory
  against a consumer that fetches large messages and never acks.
- Per-token token-bucket rate limits on `publish` (msgs/s and bytes/s) and on `fetch`, configured on
  the principal. A rate-limited request gets `429` with `Retry-After` and a `limit.exceeded` event.

Because the API is pull-based, backpressure is inherent — the broker never pushes into a socket that
is not being read, so there is no unbounded server-side buffer to exhaust. This is a deliberate
security decision as much as a design one: push-based delivery with server-side buffering is a
memory-exhaustion primitive granted to every consumer.

---

## 5. API / protocol

### 5.1 Decision: HTTP over stdlib `net/http`, JSON control plane, NDJSON data plane

**Not gRPC.** Justification in security terms:

- `grpc-go` plus protobuf is a large dependency tree parsing attacker-controlled bytes inside our
  address space, with its own CVE stream. The gRPC ecosystem has a documented decompression-bomb
  class where `max_receive_message_length` is checked only *after* decompression, so a 200 KB frame
  expands to 200 MB before any limit applies. We would inherit that surface for no benefit at our
  scale.
- `net/http` and `crypto/tls` are the two Go packages that get the most security attention and are
  patched on the Go release train, which we track with a minimum-toolchain gate in CI.
- It runs identically over a unix socket, is `curl`-debuggable during an incident, and any HTTP
  proxy or WAF an organisation already runs understands it.
- Trade-off accepted: ~200–400 bytes of framing overhead per request versus gRPC. At the target
  scale (tens of thousands of messages/s, batched fetch/ack) this is not the bottleneck; fsync is.

Concretely: HTTP/1.1 and HTTP/2 (h2 via ALPN on TLS; h2c is **not** enabled — prior-knowledge h2c
upgrade paths are a known request-smuggling surface and we gain nothing from them).

### 5.2 Transport and server hardening

```go
srv := &http.Server{
    Handler:           mux,                       // wrapped: authn → authz → limits → handler
    ReadHeaderTimeout: 5 * time.Second,           // slowloris
    ReadTimeout:       60 * time.Second,
    WriteTimeout:      0,                         // 0: long-poll fetch controls its own deadline
    IdleTimeout:       90 * time.Second,
    MaxHeaderBytes:    16 << 10,
    ErrorLog:          slogErrorLog,              // never the default stdlib logger
    TLSConfig: &tls.Config{
        MinVersion:       tls.VersionTLS12,       // TLS 1.3 preferred by negotiation
        ClientAuth:       clientAuthMode,         // RequireAndVerifyClientCert when mTLS is on
        ClientCAs:        clientCAPool,
        GetCertificate:   certReloader.Get,       // SIGHUP hot reload, no restart
        VerifyConnection: verifyPeerIdentity,     // ← NOT VerifyPeerCertificate
        NextProtos:       []string{"h2", "http/1.1"},
    },
}
```

Two details that are easy to get wrong and are therefore written down:

1. **Identity checks go in `VerifyConnection`, never `VerifyPeerCertificate`.** The Go documentation
   states plainly that `VerifyPeerCertificate` "is not invoked on resumed connections … including
   connections resumed across Configs returned by `Config.Clone` or `Config.GetConfigForClient`",
   whereas `VerifyConnection` "will run for all connections, including resumptions". This is not
   theoretical: CVE-2025-68121 (CVSS 10.0, fixed in Go 1.24.13 / 1.25.7 / 1.26.0-rc.3) is exactly the
   case where mutating `ClientCAs` between handshakes lets a resumed handshake succeed when it
   should have failed. We (a) require Go ≥ 1.26.0 in `go.mod` and enforce it in CI, (b) put all
   identity logic in `VerifyConnection`, and (c) carry a regression test that mutates the CA pool
   between an initial and a resumed handshake and asserts rejection (§8.5).
2. **Body limits before parsing.** Every handler wraps the body in
   `http.MaxBytesReader(w, r.Body, limit)`. If `Content-Encoding: gzip` is accepted (Phase 2), the
   decompressed stream is additionally wrapped in `io.LimitReader` at
   `max_message_size × max_decompress_ratio` (default 20) and a violation is a `400` plus a
   `limit.exceeded` event — never an OOM.

### 5.3 Endpoints

All under `/v1`. All responses are JSON except fetch (NDJSON). All errors use one shape:
`{"error":{"code":"lease_expired","message":"...","trace_id":"..."}}`. Codes are a closed enum
documented in `docs/api.md`; messages never echo request payloads back.

| Method | Path | Required capability | Notes |
|---|---|---|---|
| `POST` | `/v1/streams` | `manage_stream` on `subject_prefix` | 409 if prefix overlaps an existing stream |
| `GET` | `/v1/streams` | any grant | returns only streams the principal has *any* grant on |
| `GET` | `/v1/streams/{s}` | any grant on the prefix | counts, bytes, first/last seq, retention |
| `DELETE` | `/v1/streams/{s}` | `manage_stream` + `purge` | requires `?confirm={name}` |
| `POST` | `/v1/streams/{s}/publish` | `publish` on the subject | body: `{subject, payload_b64|payload, headers, msg_id, durability, trace_id}` or a batch under `messages[]`; also accepts `Content-Type: application/octet-stream` with `Messq-Subject` / `Messq-Msg-Id` headers |
| `POST` | `/v1/streams/{s}/purge` | `purge` on the prefix | optional `{subject, keep, before_seq}` |
| `POST` | `/v1/streams/{s}/redact` | `redact` on the subject | zeroes payload bytes, keeps metadata; GDPR erasure escape hatch (§10) |
| `GET` | `/v1/streams/{s}/messages/{seq}` | `peek` on the subject | inspection; does not affect any cursor |
| `POST` | `/v1/consumers` | `manage_consumer` **and** `consume` covering the filter | create/update |
| `GET` | `/v1/consumers/{c}` | `consume` covering filter | cursor, pending count, lag, DLQ size |
| `POST` | `/v1/consumers/{c}/fetch` | `consume` covering filter | `{max_messages, max_bytes, expires}` → NDJSON deliveries |
| `POST` | `/v1/consumers/{c}/ack` | `consume` covering filter | `{acks:[{lease, action: ack\|nak\|term\|extend, delay_ms}]}` batch |
| `POST` | `/v1/consumers/{c}/cursor` | `replay` covering filter | `{to: "start"\|"end"\|seq\|timestamp}` |
| `GET` | `/v1/consumers/{c}/dlq` | `dlq_read` | list with reason and attempts |
| `POST` | `/v1/consumers/{c}/dlq/redrive` | `dlq_redrive` | `{seqs:[...]}` or `{all:true, limit:N}` |
| `GET` | `/v1/messages/{msg_id}/trace` | `audit_read` on the subject | every audit row for one message |
| `POST` | `/v1/tokens` / `GET` / `DELETE` | `admin` | secret returned exactly once |
| `GET` | `/v1/audit` | `audit_read` | JSONL export with `?from_id=&to_id=` |
| `GET` | `/v1/audit/checkpoints` | `audit_read` | signed checkpoints, for offsite anchoring |
| `POST` | `/v1/audit/verify` | `audit_read` | recompute the chain, return first bad id if any |
| `GET` | `/healthz`, `/readyz` | none | constant body `ok` / `ready`; no version, no counts |
| `GET` | `/metrics` | separate listener | own bind address and own auth setting |

### 5.4 Authorization model

**Subject grammar** (deliberately identical to NATS so the mental model transfers): dot-separated
tokens; `*` matches exactly one token; `>` matches one or more trailing tokens and may appear only
as the last token. Tokens are `[A-Za-z0-9_-]{1,64}`, max 16 tokens, max 255 bytes. The parser is
fuzzed (§8.2).

**A grant** is `{verb, pattern, effect}` where `effect ∈ {allow, deny}` and verb is one of:

```
publish  consume  peek  replay  purge  redact
manage_stream  manage_consumer  dlq_read  dlq_redrive  audit_read  admin
```

Evaluation: collect matching grants; **any `deny` wins**; otherwise at least one `allow` is
required; otherwise deny. There is no implicit grant, no owner-implies-all, no wildcard shortcut.
`admin` does **not** imply `consume` — an administrator who wants to read messages must be granted
`consume` or `peek`, and that grant appears in the audit trail. This is deliberate: it makes "the
admin read the payloads" a visible, separately-authorised act.

**The coverage rule (the subtle part).** When a principal binds to a consumer, it is not enough for
its `consume` grants to *intersect* the consumer's filter — they must **cover** it. A token granted
`consume orders.eu.>` binding to a consumer whose filter is `orders.>` would receive `orders.us.*`
messages. So the check is:

> `authorized(principal, consumer)` ⟺ every subject matched by `consumer.filter` is matched by at
> least one `allow` grant of the principal **and** by no `deny` grant.

This is decided syntactically by a decision procedure over the two patterns (no enumeration of the
infinite subject space), implemented in `internal/authz.Covers(grantPattern, filter)`. Because a
bug here is silently exploitable, it is verified two ways in CI: an exhaustive differential test
against a brute-force oracle over a 3-token alphabet up to depth 4, and a fuzz target (§8.2).

**Stream prefix ownership.** A stream declares `subject_prefix`. Creating a stream whose prefix
overlaps an existing stream's prefix is rejected. Without this, tenant B creates a stream claiming
`orders.>` and starts capturing tenant A's traffic — the confused-deputy version of subject
squatting.

### 5.5 Authentication

Three methods, in the order they are attempted:

1. **Peer credentials (unix socket).** `unix.GetsockoptUcred(fd, SOL_SOCKET, SO_PEERCRED)` via
   `golang.org/x/sys/unix` gives `Ucred{Pid, Uid, Gid}` as they were at `connect(2)` time —
   kernel-supplied and unforgeable. Config maps uid/gid to principals:
   `[[principal]] name="jobs-worker" kind="peer" uid=1101`, or by gid so a whole group maps to one
   principal. No secret exists on disk for local clients, which removes an entire class of leak.
   Honest limitation: this authenticates a *uid*, not an application; any process with that uid
   inherits the access (§10).
2. **Bearer token.** `Authorization: Bearer msq_v1_<id>_<secret>`.
   - `secret` is 256 bits from `crypto/rand`, base32-Crockford encoded. `id` is a short public
     identifier that appears in logs so you can trace usage without knowing the secret.
   - The server stores only `SHA-256(secret)` and compares with `crypto/subtle.ConstantTimeCompare`.
   - **Deliberately not argon2/bcrypt.** Password KDFs exist to make *low-entropy* secrets expensive
     to guess. A 256-bit random value is not guessable; a slow KDF on the hot path would only buy a
     DoS vector. Documenting this reasoning in `docs/security.md` prevents a well-meaning future
     contributor from "fixing" it.
   - The `msq_v1_` prefix is a fixed, greppable pattern so that GitHub secret scanning and
     `gitleaks` rules can be written for it. Publishing that rule is an M3 deliverable.
   - Optional `expires_at`. `messq token ls` flags tokens unused for > 90 days.
3. **Client certificate (mTLS).** `ClientAuth: RequireAndVerifyClientCert` against a dedicated
   client CA (separate from the server CA so they rotate and revoke independently). The principal is
   derived in `VerifyConnection` from a URI SAN (`spiffe://…` or `messq://principal/<name>`),
   falling back to the exact RFC 4514 subject if configured. Revocation: prefer short-lived certs
   (hours) from an existing issuer; if that is impossible, a CRL file may be configured and is
   checked inside `VerifyConnection` (Go's chain verification consults neither CRL nor OCSP by
   itself — this must be plumbed explicitly, and we say so in the docs).

**Listener rules, enforced at startup, no override flag:**

| Listener | TLS | Credential | Allowed? |
|---|---|---|---|
| unix socket | n/a | peercred | ✅ default |
| `127.0.0.1:port` | none | token | ⚠️ allowed, warns loudly, `config.insecure` audit event |
| non-loopback | none | any | ❌ **fatal error at startup** |
| non-loopback | TLS | token and/or mTLS | ✅ |

If you terminate TLS in a sidecar, terminate it *into the unix socket* — nginx, haproxy and Envoy
all proxy to unix sockets. This removes the "we'll add TLS later" deployment forever.

---

## 6. CLI & developer experience

Built with `spf13/cobra`. One binary, `messq`, whose subcommands are the daemon, the client and the
forensics toolkit.

```
messq init [--dir /var/lib/messq]        create state dir + DB + audit key + first admin principal
messq serve [--config /etc/messq/messq.toml]
messq doctor                             security & config self-assessment (exit 0/1)

messq stream add|ls|info|purge|redact|rm
messq consumer add|ls|info|seek|reset

messq pub <subject> [-f file|-] [-H k=v] [--msg-id ID] [--durability fsync|buffered] [-n N]
messq sub <consumer> [--ack auto|manual] [--batch 32] [--json]
messq ack -                              read {lease,action} lines from stdin
messq peek <stream> --seq N | --subject "orders.eu.*" --last 10
messq trace <msg-id|trace-id>            end-to-end timeline of one message
messq lag [consumer]                     backlog, in-flight, oldest unacked age

messq dlq ls|show|redrive|drop
messq token create|ls|revoke|show
messq principal add|ls|grant|revoke
messq audit verify|export|checkpoint|repair
messq keys rotate-audit|show
messq bench [--publishers N --rate R]
messq completion bash|zsh|fish
```

### 6.1 The three commands that define the product

**`messq trace 01JBQ7…`** — the signature feature. Pulls every audit row for one message ID and
renders a timeline:

```
msg 01JBQ7X4M2VN3K  stream=orders subject=orders.eu.new  trace=4bf92f35…
  10:00:00.412  publish.accepted   actor=api-gateway  seq=941  durability=fsync  1.2 KiB
  10:00:00.418  delivery.sent      consumer=billing   attempt=1  lease=lz_9f2c…  to=billing-w3
  10:00:30.418  delivery.timeout   consumer=billing   attempt=1  ack_wait=30s
  10:00:31.402  delivery.sent      consumer=billing   attempt=2  lease=lz_11ae…  to=billing-w1
  10:00:31.960  delivery.nak       consumer=billing   attempt=2  delay=5s  reason="upstream 503"
  10:00:36.981  delivery.sent      consumer=billing   attempt=3  lease=lz_77c0…  to=billing-w1
  10:00:37.204  delivery.ack       consumer=billing   attempt=3  latency=223ms
  chain: ids 88123..88129 verified against checkpoint #412 (signed 10:01:00 by key ak_2f9)
```

That last line is the point. The timeline is not just a log; it is a cryptographically verified
extract of an append-only chain.

**`messq doctor`** — turns the threat model into a runnable check:

```
$ messq doctor
✔ state dir /var/lib/messq  mode 0700  owner messq:messq
✔ messq.db  mode 0600
✔ listeners: unix /run/messq/messq.sock (0660 messq:messq)
✔ tcp 0.0.0.0:4711  TLS1.3  mTLS required  cert expires in 71d
✖ token "ci-deploy" (msq_v1_k3f…) unused for 214 days — revoke it
⚠ principal "etl" holds `consume >` — the broadest grant in this deployment
⚠ audit chain last verified 9d ago (recommend: daily `messq audit verify`)
✔ systemd sandbox: systemd-analyze security = 1.4 (OK)
✔ payload logging: disabled
2 warnings, 1 finding.  exit 1
```

`messq doctor` in CI or a cron job is how the secure defaults stay secure a year after installation.

**`messq sub … | … | messq ack -`** — makes shell-pipeline consumers real:

```bash
messq sub billing --ack manual --json \
  | jq -c 'select(.subject|startswith("orders.eu")) | {lease, action:"ack"}' \
  | messq ack -
```

### 6.2 Credential handling in the CLI

- **Never on the command line.** No `--token VALUE` flag exists; `/proc/<pid>/cmdline` is world
  readable. Sources, in order: `MESSQ_TOKEN`, `--token-file PATH`, then
  `$XDG_CONFIG_HOME/messq/credentials` (INI-style, one section per profile). Any credentials file
  with mode broader than `0600` is refused, in the style of OpenSSH.
- `messq token create` prints the secret exactly once; `--out FILE` writes it `0600`. Re-showing it
  is impossible because only the hash is stored, and the CLI says so in the output.
- Best-effort zeroing of secret buffers after use, with an honest note in `docs/security.md` that Go's
  garbage collector may have copied them and this is mitigation, not a guarantee.

### 6.3 Ergonomics

- Human tables by default; `--json` on every read command; `--quiet` for scripts.
- Documented exit codes: `0` ok, `1` runtime error, `2` usage, `3` authz denied, `4` not found,
  `5` conflict/lease-expired, `6` rate-limited. Scriptable retry logic depends on this.
- Shell completions via cobra's generators, with dynamic `ValidArgsFunction` completion of stream,
  consumer and token names by querying the daemon over the unix socket (falls back silently when the
  daemon is unreachable).
- `messq serve --print-config` dumps the effective configuration with every secret replaced by
  `<redacted:sha256:ab12…>` so two hosts can be diffed without leaking anything.

---

## 7. Observability & logging design

Two sinks with different jobs, and the difference is the whole point:

| | Operational log | Audit trail |
|---|---|---|
| Where | stdout → journald / collector | `audit` table in `messq.db`, exportable as JSONL |
| Format | slog JSON (text when stdout is a TTY) | canonical JSON, hash-chained |
| Retention | whatever your collector does | `audit_retention` (default 90 d), floor-protected |
| Can be lost | yes | no — same transaction as the state change |
| Integrity | none | SHA-256 chain + Ed25519 checkpoints |

### 7.1 Event taxonomy

Stable, dotted names — these are an API and are covered by semver:

```
publish.accepted  publish.rejected  publish.deduped
delivery.sent  delivery.ack  delivery.ack_rejected  delivery.nak  delivery.extend
delivery.timeout  delivery.redeliver  delivery.term  delivery.dlq
dlq.redrive  dlq.drop  cursor.seek  stream.purge  stream.redact
consumer.created  consumer.updated  consumer.deleted  stream.created  stream.deleted
authn.ok  authn.failed  authz.denied  token.created  token.revoked  principal.updated
limit.exceeded  tls.handshake_failed  config.insecure  config.loaded
recovery.started  recovery.completed  audit.checkpoint  audit.chain_break  audit.verified
```

### 7.2 Every event carries the same skeleton

```json
{"time":"2026-08-21T10:00:31.402Z","level":"INFO","event":"delivery.sent",
 "msg":"delivered orders.eu.new seq=941 attempt=2 to billing-w1",
 "epoch":7,"stream":"orders","subject":"orders.eu.new","seq":941,
 "msg_id":"01JBQ7X4M2VN3K","trace_id":"4bf92f3577b34da6a3ce929d0e0e4736",
 "consumer":"billing","attempt":2,"lease_id":"lz_11ae",
 "actor":"billing-w1","auth_method":"mtls","remote":"10.2.0.9",
 "outcome":"ok","audit_id":88126}
```

- `msg_id` is a broker-assigned ULID: monotone, sortable, no collision coordination needed.
- `trace_id` is taken from a W3C `traceparent` header if the client supplies one, else generated.
  A message keeps its trace_id across every redelivery, which is what makes `messq trace` work
  across service boundaries.
- `audit_id` links the operational log line to the durable audit row.
- The human-readable `msg` field is present *and* every field is structured — human-friendly logs
  were a product requirement, not a nice-to-have.

### 7.3 Payload and header redaction

- Payloads are typed `obs.Redacted[[]byte]` implementing `slog.LogValuer`. Its `LogValue()` returns
  `<redacted len=1234 sha256=9f2c…>`. This is the pattern the `log/slog` documentation itself
  demonstrates for secrets (`token=REDACTED_TOKEN`), and it means a payload cannot be logged by
  accident even if someone writes `slog.Any("payload", msg.Payload)`.
- A `ReplaceAttr` hook on the JSON handler is the second line of defence: it drops any attribute
  whose key matches a denylist (`payload`, `body`, `secret`, `token`, `authorization`, `password`,
  `key`) unless the value is already a redaction marker.
- Message headers are logged only if their names appear in `log.header_allowlist`; everything else
  becomes `<redacted>`. Headers are attacker-controlled and frequently carry tenant data.
- `--unsafe-log-payloads` exists for debugging but (a) can only be set in the config file, never as a
  flag, (b) emits `config.insecure` on start and a WARN every 60 s while active, and (c) is written
  into the audit chain, so "we had payload logging on for three weeks" is a discoverable fact.
- A CI test publishes a canary string through the full integration suite and greps every log line and
  the audit export for it (§8.5).

### 7.4 Audit chain

```
hash_0 = SHA-256("messq.audit.v1" || instance_id)
hash_i = SHA-256(0x01 || hash_{i-1} || canonical_json(event_i))
```

Domain-separated with a `0x01` prefix so no other hash in the system can be confused with a chain
link. `canonical_json` is a deterministic encoder (sorted keys, no insignificant whitespace, integers
only) with its own fuzz target, because a canonicalisation bug is a chain-forgery bug.

**Checkpoints.** Every 60 s or 10 000 events, whichever comes first:
`{chain_size, head_hash, ts, instance_id}` signed with Ed25519 (`crypto/ed25519`).

**Anchoring — the pitfall we refuse to fall into.** A chain whose roots live in the same file with
the same write permissions as the events is theatre: an attacker who can rewrite rows can rewrite
roots. Mitigations, in increasing strength, all documented in `docs/audit.md`:

1. Default: the audit key lives at `$STATE/audit.key` (`0600`). Stops nothing against a broker-uid
   compromise, but detects accidental corruption and non-root tampering.
2. Recommended: a systemd timer runs `messq audit checkpoint --out -` and ships checkpoints off the
   box (journald→remote syslog, an object store with object-lock, or a git repo). Anyone holding an
   old checkpoint can later prove a deletion.
3. Strongest: `audit.key_mode = "external"` — the broker holds only the *public* key and emits
   unsigned checkpoints; a separate process running as a different uid (or on another host) signs
   them. The broker uid then cannot forge a checkpoint at all.

**Verification.** `messq audit verify [--from-checkpoint N]` recomputes the chain and reports the
first divergent id. The broker refuses to start on a broken chain (§3.5). A
`messq_audit_last_verified_seconds` gauge means "nobody has verified in a month" is alertable —
because the research is unambiguous that the real failure mode of tamper-evident logs is that nobody
ever checks them.

**Design choice: hash chain, not a Merkle history tree.** History trees give O(log n) inclusion and
consistency proofs versus O(n) chain verification. At messq's expected audit volume (millions of
rows, verifiable in seconds) that is not the binding constraint; ~60 lines of reviewable code is
worth more than asymptotics. If inclusion proofs for third parties are ever needed, upgrading to a
history tree is a Phase-3 item with a documented migration (`audit.chain_break` marks the boundary).

### 7.5 Metrics

`prometheus/client_golang` on a **separate listener**, default `127.0.0.1:9711`, off unless
configured, with its own `auth` setting (`none` | `token`). Using a custom registry via
`promauto.With(reg)` rather than the default one, so nothing a dependency registers globally leaks
into our exposition. `promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg,
MaxRequestsInFlight: 4})` bounds scrape concurrency — an unbounded `/metrics` is a cheap DoS. The Go
collector is added explicitly with only the runtime metrics we want (the library enables just three
by default: GC goal percent, memory limit, GOMAXPROCS).

Core series:

```
messq_published_total{stream,subject_prefix,durability}
messq_delivered_total{consumer,attempt_bucket}
messq_acked_total{consumer}            messq_nak_total{consumer}
messq_ack_timeout_total{consumer}      messq_dlq_total{consumer,reason}
messq_pending{consumer}                messq_pending_bytes{consumer}
messq_backlog{consumer}                messq_oldest_unacked_seconds{consumer}
messq_publish_fsync_seconds_bucket     messq_write_batch_size_bucket
messq_authn_failed_total{method,reason}   messq_authz_denied_total{verb}
messq_lease_rejected_total{consumer}   messq_rate_limited_total{principal}
messq_audit_chain_size                 messq_audit_last_verified_seconds
messq_disk_free_bytes                  messq_db_size_bytes
```

`messq_authn_failed_total` and `messq_authz_denied_total` are security telemetry: a spike is either
a misconfiguration or someone probing, and both deserve an alert. Sample alert rules ship in
`deploy/alerts.yml`.

---

## 8. Testing strategy

### 8.1 Unit and table tests

Subject parser/matcher, grant evaluation, backoff schedule, cursor/ack-floor arithmetic (including
sparse out-of-order acks), canonical JSON encoder, token encoding/decoding, config precedence.
`go test -race` everywhere; `go vet` and `staticcheck` gate the build; `gosec` runs advisory-only
(too noisy to gate, useful to read).

### 8.2 Fuzzing (Go native, corpora committed under `testdata/fuzz`)

| Target | Property |
|---|---|
| `FuzzSubjectParse` | never panics; accepted subjects round-trip |
| `FuzzSubjectMatch` | matcher agrees with a brute-force reference implementation |
| `FuzzGrantCovers` | **differential**: `Covers(a,b)` ⟹ every subject sampled from `b`'s language matches `a`; `!Covers(a,b)` ⟹ a witness exists |
| `FuzzPublishBody` | JSON handler never panics, always respects the size cap |
| `FuzzHeaderCodec` | header TLV decode/encode round-trip, no OOM on hostile lengths |
| `FuzzCanonicalJSON` | deterministic output; distinct inputs give distinct bytes |
| `FuzzConfigParse` | TOML config decode never panics; unknown keys are rejected |
| `FuzzAckBatch` | no panic; lease values are never partially applied |

Every fuzz target runs 60 s per PR and 30 min nightly; crashers are auto-filed and their inputs
committed as regression seeds.

### 8.3 Model-based property tests (the correctness backbone)

A pure Go reference model of the delivery state machine (a few hundred lines, no storage) is driven
alongside the real broker with randomised operation sequences (publish / fetch / ack / nak / extend /
term / timeout-tick / seek / redrive / restart). Invariants asserted after every step:

1. No message is acked twice for the same consumer.
2. `ack_floor` is monotone non-decreasing.
3. `attempt ≤ max_deliver` for every pending row, always.
4. In-flight count ≤ `max_ack_pending`; in-flight bytes ≤ `max_pending_bytes`.
5. Every published message eventually reaches ACKED, DLQ, or is still legitimately in flight/waiting.
6. An `ordered` consumer never has two in-flight messages with the same partition key.
7. Every state transition has exactly one corresponding audit row, and the chain verifies.
8. No message below the retention floor is deleted.

### 8.4 Deterministic simulation and crash testing

Following the FoundationDB-style approach adapted to Go: the four things a test must control are
**time, randomness, concurrency and failure injection**. We inject the first two through a `Clock`
and `rand.Source` interface used by every package, and the fourth through a `store.FaultInjector`
that can make any SQLite call return an error, or make an fsync silently "succeed" without writing.
Concurrency determinism in Go is only partial — we accept that, run the same seed many times, and
biased-randomise goroutine yields.

- Every failure prints its seed; `go test -run TestSim -seed=0x8f3c…` reproduces it exactly.
- **Kill tests**: a harness runs a real `messq serve`, drives load, and `SIGKILL`s at randomised
  points. After restart it asserts (a) every publish that returned 2xx with `durability=fsync` is
  present at the reported seq, (b) no message that was acked before the kill is redelivered, (c) no
  pre-crash lease can ack anything, (d) `messq audit verify` passes.
- **Power-loss tests** (nightly, Linux VM job): the data directory sits on a `dm-flakey` device
  configured to drop writes after a trigger, giving true power-loss semantics rather than
  process-crash semantics. This is the only way to actually test `synchronous=FULL` claims.

### 8.5 Security tests — first class, not an afterthought

1. **The authz matrix.** A registry of every route with its required capability is built at init
   time. A generated test enumerates (principal × grant-set × route × target subject) and asserts
   allow/deny against an expectations table. **A route without a matrix entry fails the build.**
   This single test is the highest-value security control in the repo: it makes "someone added an
   endpoint and forgot the authz check" impossible rather than unlikely.
2. **Coverage-rule tests.** Exhaustive differential check of `Covers` over a 3-symbol alphabet to
   depth 4 (≈ tens of thousands of pattern pairs) against a brute-force oracle.
3. **TLS conformance.** Rejects TLS < 1.2; rejects a client with no cert when mTLS is required;
   rejects a cert signed by the server CA rather than the client CA; and the
   **resumption regression test** — complete a handshake, mutate `ClientCAs` to exclude the client's
   issuer, attempt a resumed handshake, assert rejection (guards the CVE-2025-68121 class and proves
   our identity checks are in `VerifyConnection`).
4. **Listener policy tests.** Non-loopback plaintext bind → startup error. Unix socket is `0660` and
   owned by the configured group. A connection from a uid with no principal mapping gets
   `authn.failed` plus an audit row and no information about what exists.
5. **Lease abuse tests.** Ack with another consumer's lease → 409. Ack with an expired lease → 409
   and no state change. Ack with a lease from a pre-restart epoch → 409. Extend beyond
   `max_ack_extension` → 409.
6. **Canary leak test.** The integration suite publishes payloads containing
   `CANARY-6f1e2d…`; afterwards it greps all captured stdout, the audit JSONL export, and
   `/metrics` for the canary. Any hit fails the build. Also run with `--unsafe-log-payloads` to
   assert the canary *does* appear, so the flag's semantics stay honest.
7. **Resource-limit tests.** Oversized body → 413 without allocating the body; 1 MB of headers →
   431; slowloris (1 byte/s headers) → closed at `ReadHeaderTimeout`; a 200 KB gzip body that
   expands 1000× → 400 with `limit.exceeded`, memory ceiling unmoved.
8. **Permission tests.** World-readable TLS key → refuse to start. `0666` DB → refuse to start.
   `--allow-insecure-perms` → start plus `config.insecure` audit event.
9. **Timing.** A statistical test that token verification time does not correlate with the number of
   matching leading bytes (sanity check that `ConstantTimeCompare` is actually on the path).
10. **`systemd-analyze security`** runs in CI against the shipped unit; the exposure score must stay
    ≤ 2.0 or the build fails.

### 8.6 Performance and soak

`messq bench` drives publish/consume load. A 24-hour soak with fault injection and randomised
restarts runs before every minor release; regressions in p99 publish latency > 20 % versus the
previous tag fail the release pipeline.

### 8.7 Coverage policy

No global coverage number as a gate (it optimises for the wrong thing). Instead: `internal/authz`,
`internal/audit` and `internal/broker` must be ≥ 90 % line coverage, enforced per-package, because
those are the packages where untested code is dangerous rather than merely unfinished.

---

## 9. Roadmap — empty repo to ideal product

Each milestone has scope, exit criteria, and the security work that ships *inside* it rather than
after it. Estimates assume one engineer working steadily.

### M0 — Skeleton and security scaffolding (≈1 week)

**Scope.** `go.mod` (Go ≥ 1.26.0), package layout from §2.2, cobra root command with `version`,
`Makefile`, `.golangci` config. CI: build, `go vet`, `staticcheck`, `go test -race`, `govulncheck`,
dependency-budget check (fails if `go.mod` has > 6 direct requires), Actions pinned by SHA with
minimal `permissions:`. `SECURITY.md` with a disclosure address and a 90-day policy.
`docs/threat-model.md` (§1.3 expanded). `CODEOWNERS` requiring a second review on `internal/authz`
and `internal/audit`. `go mod vendor` committed.

**Exit.** `messq version` prints version/commit/build-date; CI green; `govulncheck` clean;
threat model reviewed and merged **before any feature code**.

### M1 — Storage core and delivery state machine (≈3 weeks)

**Scope.** Schema + migration runner; single-writer with group commit; read pool; the full state
machine from §4 behind a Go API (no network yet); planner, sweeper, retention floor; recovery
including lease re-randomisation; `Clock`/`Rand`/`FaultInjector` seams; model-based property tests;
kill tests.

**Exit gates.**
- Property test: 10 000 randomised ops × 100 seeds, all invariants hold, with fault injection on.
- Kill test suite green.
- **Measured**: ≥ 5 000 msg/s sustained publish at 1 KiB with `durability=fsync` on a datacenter
  SSD, p99 < 25 ms. If this fails, the group-commit design is revisited *now*, not at M7 (§10).

### M2 — Local daemon, peercred authn, first CLI verbs (≈2 weeks)

**Scope.** `net/http` server on a unix socket with the hardened `http.Server` from §5.2; peercred
authn; principals in config; `messq init`, `serve`, `stream add/ls`, `consumer add/ls`, `pub`, `sub`,
`ack`, `lag`. slog wiring with the redaction types. TOML config with env/flag precedence and
`--print-config`.

**Exit.** `messq init && messq serve`, then publish and consume from another terminal in under a
minute from a cold machine. Socket is `0660`; a process running as an unmapped uid is denied and
produces an `authn.failed` event. Canary leak test green.

### M3 — Authorization, tokens, audit chain (≈3 weeks)

**Scope.** Subject grammar and matcher; grants and the `Covers` decision procedure; the route
registry and the authz matrix test; token create/list/revoke with SHA-256 storage; the hash-chained
audit table written in the same transaction as every state change; checkpoints and Ed25519 signing;
`messq audit verify/export/checkpoint`; `messq trace`.

**Exit.** Authz matrix covers 100 % of routes and fails on an unregistered route. `Covers`
differential test green. Manually flipping a byte in an audit row makes `messq audit verify` report
the exact id, and makes the broker refuse to start. `messq trace` renders a full lifecycle including
timeout and redelivery. A published `gitleaks`/secret-scanning rule for the `msq_v1_` prefix.

### M4 — TLS, mTLS, network exposure (≈2 weeks)

**Scope.** TCP listener with mandatory TLS; cert hot reload on SIGHUP; `VerifyConnection` identity
mapping from SPIFFE/URI SAN; optional CRL; listener policy enforcement; separate metrics listener;
`messq doctor` v1.

**Exit.** TLS conformance suite including the resumption regression test. Non-loopback plaintext bind
is a startup error with a message telling the operator to terminate into the unix socket.
`messq doctor` reports every check in §6.1.

### M5 — Operational surface (≈3 weeks)

**Scope.** Replay/seek, purge, redact; DLQ list/show/redrive/drop; retention policies (age, bytes,
count) with floor protection; `messq peek`; Prometheus metrics and alert rules; systemd unit with
full sandboxing plus socket activation; deb/rpm/apk packaging; `docs/operations.md` runbooks
(backlog growing, consumer stuck, DLQ filling, disk near full, suspected tampering).

The shipped unit:

```ini
[Service]
Type=notify
User=messq
Group=messq
ExecStart=/usr/bin/messq serve --config /etc/messq/messq.toml
StateDirectory=messq
StateDirectoryMode=0700
ConfigurationDirectory=messq
RuntimeDirectory=messq
RuntimeDirectoryMode=0750
LoadCredential=audit.key:/etc/messq/audit.key
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
PrivateDevices=yes
ProtectProc=invisible
ProtectClock=yes
ProtectControlGroups=yes
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectKernelLogs=yes
ProtectHostname=yes
RestrictNamespaces=yes
RestrictRealtime=yes
RestrictSUIDSGID=yes
LockPersonality=yes
MemoryDenyWriteExecute=yes
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6
CapabilityBoundingSet=
AmbientCapabilities=
SystemCallArchitectures=native
SystemCallFilter=@system-service
SystemCallErrorNumber=EPERM
UMask=0077
LimitNOFILE=65536
Restart=on-failure
```

(`CapabilityBoundingSet=` empty means the broker holds no capabilities at all; binding a port below
1024 is not supported and not needed — the default port is 4711.)

**Exit.** `systemd-analyze security messq.service ≤ 2.0` asserted in CI. Runbooks reviewed by
someone who did not write the code.

### M6 — v0.1.0 release engineering (≈1.5 weeks)

**Scope.** GoReleaser config: `CGO_ENABLED=0`, `-trimpath`, `mod_timestamp: {{.CommitTimestamp}}`,
`gomod.proxy: true` (build from the module proxy, not the working tree); syft SBOMs for archives and
source; cosign keyless signing of the checksum file; GitHub artifact attestations for SLSA
provenance. A **double-build reproducibility job**: build the tag twice on different runners and diff
the artifacts, fail on mismatch. `RELEASE.md` with copy-pasteable `cosign verify-blob` and
`slsa-verifier` commands. Nightly `govulncheck` against the last three released tags, opening an
issue automatically.

**Exit.** A user can download, verify the signature and provenance, and run `messq serve` on a fresh
machine in under five minutes, with the whole path documented on one page.

### M7 — Hardening and scale (≈3 weeks)

**Scope.** Per-token rate limits and quotas; per-stream/prefix byte and message quotas;
`min_free_disk` behaviour; backpressure metrics; the 24-hour soak in CI; expanded fuzz corpora;
`dm-flakey` power-loss job; an external security review of `internal/authz`, `internal/audit`,
`internal/api`; act on the findings before tagging **v1.0.0**.

**Exit.** v1.0.0. Semver commitment on the HTTP API, the event taxonomy, the audit encoding, and the
config schema.

### M8 — Phase 2 features, each with its own threat note (≈ ongoing)

Ordered by value-to-risk ratio:

1. **Delayed delivery** (publish with `deliver_at`) — reuses the `redeliver` table, near-zero new risk.
2. **Priority channels** — a `priority` field consulted by the planner; risk: starvation, mitigated
   by an aging floor.
3. **Consumer groups with lease** — multiple workers under one consumer, each holding a member lease;
   risk: a member that never renews holds capacity, so member leases expire like message leases.
4. **Attenuable child tokens** — a holder derives a narrower token offline via an HMAC chain
   (macaroon construction: sign each caveat with the previous signature as key). The standard
   criticism of macaroons — every verifier needs the root secret — **does not apply to a single-node
   broker**, since there is exactly one verifier that already holds the secret. That is why we can
   have delegation without adding a Datalog engine or a public-key token format.
5. **Compression** (`zstd` per message) with a hard decompression ratio cap and streaming limits.
6. **Retention policies by subject** and audit-trail export to external sinks (S3 with object-lock,
   remote syslog) for offsite anchoring.
7. **Envelope encryption at rest** — XChaCha20-Poly1305 per message with a data key wrapped by a KEK
   loaded via systemd `LoadCredential=` or an `exec` hook, never written next to the DB. Shipped with
   an explicit statement of what it does not protect against.
8. **OpenTelemetry trace export** (behind a build tag so the default binary keeps its dependency
   budget).

### M9 — Cluster mode (explicitly deferred, not before v1.0)

Design document and threat model first: inter-node authentication via a **separate node CA with
mTLS** (never a shared bearer token), replication semantics stated precisely (leader-follower with
async replication, so it is a durability/availability improvement, not a consistency guarantee), and
the failure modes named. Implementation only after that document is reviewed. Saying no to this
until v1.0 is a feature.

---

## 10. Risks & open questions

**R1 — `modernc.org/sqlite`'s transitive dependency surface.** The pure-Go driver pulls several
`modernc.org/*` modules (libc, memory, mathutil) — machine-transpiled C, which is a large amount of
code we will never fully read. This is the single largest accepted supply-chain risk in the project.
*Mitigation*: vendored and pinned so upgrades are reviewable diffs; `govulncheck` on PRs and nightly;
fuzzing at the driver boundary; a documented, tested `mattn/go-sqlite3` build tag kept as an
emergency escape hatch (not shipped, but exercised in a weekly CI job so it does not bit-rot).

**R2 — fsync throughput ceiling.** `synchronous=FULL` plus group commit is the whole durability
story. If the M1 measurement misses the ≥ 5 000 msg/s gate, we widen the batch window, then consider
moving payloads to append-only segment files with a single fsync per batch while metadata stays in
SQLite. *This is an M1 decision point with a number attached, not a Phase-3 discovery.*

**R3 — the `Covers` decision procedure.** A subtle bug here is a silent cross-tenant read. Mitigated
by the differential fuzz target and the exhaustive small-alphabet test, but it stays on the risk
register permanently and is the first thing an external reviewer is pointed at.

**R4 — the audit chain does not stop root.** An attacker with the broker uid can rewrite the DB and
re-sign the chain if the key is local. We state this plainly and make external checkpoint shipping
and `key_mode = "external"` the documented posture for regulated deployments. The honest claim is
*tamper-evident against everyone below root, and against root for any period covered by an
externally-held checkpoint*.

**R5 — bearer tokens in environment variables.** `MESSQ_TOKEN` is readable via
`/proc/<pid>/environ` by the same uid and lands in core dumps and container inspect output. It is
the convenient option, so it will be used. *Mitigation*: `--token-file` and the credentials file are
documented first; `messq doctor` warns when the daemon itself sees a large number of distinct source
identities on one token id; mTLS is the recommendation above a certain value threshold.

**R6 — peercred authenticates a uid, not an application.** Any process running as that uid inherits
the principal's grants. On a host where several applications share a service account, this
collapses the isolation. *Mitigation*: documented prominently; the recommended pattern is one uid
per application; gid-based mapping is offered but flagged as coarser in `messq doctor`.

**R7 — clock manipulation.** Ack timeouts derived from wall-clock time can mass-redeliver if the
clock jumps backwards (NTP step, VM restore). *Mitigation*: deadlines are stored as wall-clock for
display but compared using Go's monotonic clock within a broker lifetime; a detected backward jump
larger than 1 s emits a `recovery.clock_jump` event and suppresses timeout sweeps for one
`ack_wait` period rather than redelivering everything at once.

**R8 — GDPR erasure versus an append-only stream.** "Delete this person's data" is incompatible with
immutability. *Resolution*: `messq stream redact --seq N --reason "DSR-2026-114"` zeroes the payload
bytes in place, keeps the metadata, sets `redacted_at_ns`, and writes a `stream.redact` audit event
carrying the actor, the reason, and the *previous* `payload_sha256` — so the audit chain stays intact
and the redaction is itself provable. **Open question**: whether redaction should also be possible
for messages currently in flight (leaning yes, with the in-flight delivery invalidated so the
consumer cannot ack a message that no longer exists as it was delivered).

**R9 — disk-full deadlock.** If the DB cannot be written, acks fail, leases expire, and everything is
redelivered forever. *Mitigation*: `min_free_disk` rejects publishes well before the disk is full
while continuing to accept acks and DLQ writes. **Open question**: what to do when even the DLQ move
cannot be written — current plan is to keep the message pending, stop delivering it (`hold` state),
and raise a critical alert, on the principle that stalling is better than looping.

**R10 — long-poll fetch and `WriteTimeout`.** Setting `WriteTimeout: 0` to allow long polls removes a
DoS guard. *Mitigation*: the fetch handler enforces its own deadline via
`http.NewResponseController.SetWriteDeadline` per delivery line, and a per-principal cap on
concurrent open fetches (default 64).

**R11 — is the dependency budget of 6 too tight?** It forces stdlib solutions that are sometimes more
code than a library. The counter-argument is that vendoring plus a hard cap is the only mechanism
that reliably keeps dependency creep from happening one reasonable PR at a time. Revisit at v1.0
with data on what was actually refused.

**Open questions carried forward**

- Should `fetch` remain long-poll only, or gain a server-push stream in Phase 2? Push adds a
  server-side buffering surface (§4.5) — leaning no until there is a measured latency need.
- Multi-tenant DLQ ownership when a stream is shared between tenants: per-consumer DLQ (current
  design) means a tenant sees only its own failures, but an operator wanting a global view has to
  query several. Probably right; needs a real user to confirm.
- Should `admin` be able to grant itself `consume`? Currently yes (it can edit principals), which
  makes the separation advisory rather than enforced. A true separation needs two-person control
  (`require_second_approval` on grant changes) — Phase 2, if a user asks.
- Header size and count limits: currently 8 KiB total / 32 headers. Needs validation against real
  integration workloads.

---

## 11. Library choices, grounded in the fetched documentation

**Dependency budget: at most 6 direct non-test module requirements, enforced in CI.** Every
dependency is code running with the broker's uid, with access to every payload in flight. Current
count: **5**.

### 11.1 `modernc.org/sqlite` — storage engine (direct dep 1/6)

The driver's documentation shows exactly the DSN-based configuration we rely on —
`file:///path?_journal=WAL&_synchronous=NORMAL&_foreign_keys=1&_timeout=5000&_pragma=…` and the
`_txlock=immediate` option for taking the write lock at `BEGIN`. It also documents the
`SQLITE_BUSY` retry pattern with a typed `*sqlite.Error` and `Code()`, which we use to make any
unexpected busy error a loud, dedicated log event rather than a silent retry (with a single writer,
a busy error means a design assumption broke).

Chosen over `mattn/go-sqlite3` because cgo would break `CGO_ENABLED=0` static builds, cross
compilation, and bit-for-bit reproducibility — all prerequisites for §11.6. The documented cost is
performance (roughly 2× slower on inserts than the cgo driver in published benchmarks); group commit
means our insert path is dominated by fsync, not by the driver, so we accept it and verify it at the
M1 gate.

### 11.2 `spf13/cobra` — CLI (direct dep 2/6, plus `spf13/pflag` transitively)

We use `PersistentFlags` for global options (`--config`, `--socket`, `--json`, `--verbose`),
`MarkFlagRequired`/`MarkPersistentFlagRequired` for destructive commands' `--confirm`, and the
built-in `GenBashCompletion`/`GenZshCompletion`/`GenFishCompletion` generators for `messq
completion`, all as documented in cobra's user guide and completions docs. Dynamic completion of
stream/consumer names uses `ValidArgsFunction`. Chosen over hand-rolled flag parsing because the CLI
is a first-class product surface here (`trace`, `doctor`, `peek`) and cobra's structure keeps the
command tree reviewable; `pflag` is its only transitive dependency.

### 11.3 `log/slog` (stdlib) — logging

The `log/slog` documentation demonstrates precisely our redaction pattern: a value that "replaces
itself with an alternative representation to avoid revealing secrets", rendering as
`token=REDACTED_TOKEN` via the `LogValuer` interface (`LogValue() Value`). We implement
`obs.Redacted[T]` on top of it for payloads and secrets, and use `HandlerOptions.ReplaceAttr` as the
second-layer key denylist. `slog.NewJSONHandler` gives line-delimited JSON with no dependency at all.

### 11.4 `net/http` + `crypto/tls` (stdlib) — protocol and transport

The `http.Server` documentation gives us every guard we need as plain struct fields —
`ReadHeaderTimeout`, `ReadTimeout`, `IdleTimeout`, `MaxHeaderBytes`, `ErrorLog`, `TLSConfig`,
`Protocols`. The `crypto/tls` documentation carries the warning that decided our identity-check
placement: `VerifyPeerCertificate` "is not invoked on resumed connections. **WARNING**: this includes
connections resumed across Configs returned by `Config.Clone` or `Config.GetConfigForClient`",
whereas `VerifyConnection` "will run for all connections, including resumptions". CVE-2025-68121
(CVSS 10.0, fixed in Go 1.24.13 / 1.25.7 / 1.26.0-rc.3) is the live proof that this footgun fires in
production, so: minimum Go 1.26.0 in `go.mod`, enforced in CI, and all identity logic in
`VerifyConnection`, with a regression test.

Explicitly rejected: `grpc-go` + protobuf. It is a large dependency tree parsing hostile bytes in
our address space; the gRPC ecosystem has a documented decompression-bomb class where the
receive-size limit is applied only to the *decompressed* message (a ~200 KB frame expanding ~1000× to
crash the server); and it is not `curl`-debuggable during an incident. HTTP + JSON costs us framing
bytes and buys us a dramatically smaller reviewable surface.

### 11.5 `golang.org/x/sys/unix` — peer credentials (direct dep 3/6)

Provides `Ucred{Pid, Uid, Gid}` and the `SO_PEERCRED` / `SCM_CREDENTIALS` plumbing
(`UnixCredentials`, `ParseUnixCredentials`, and the `Getsockopt*` family). Peer credentials are
"those that were in effect at the time of the `connect(2)`" and are supplied by the kernel, which
makes them the only credential in the system that cannot be stolen, replayed, or leaked into a log.
That is why the default local path uses them and why no secret needs to exist on disk for local
clients. We use only the syscall wrappers, not a third-party `peercred` listener wrapper — the
useful part is about 30 lines.

### 11.6 `prometheus/client_golang` — metrics (direct dep 4/6)

Used with a **custom registry** (`prometheus.NewRegistry()` + `promauto.With(reg)`), so nothing a
dependency registers on the global default gets exposed by us. The handler is
`promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg, MaxRequestsInFlight: 4})` — the
documented `MaxRequestsInFlight` field defaults to unlimited, which is a free DoS on any exposed
`/metrics`, so we set it. The library enables only three runtime metrics by default (GC goal percent,
memory limit, GOMAXPROCS) and requires `WithGoCollectorRuntimeMetrics` to opt into more, so our
exposition stays small and predictable.

### 11.7 `github.com/BurntSushi/toml` — config (direct dep 5/6)

Its `go.mod` declares **no requires at all** — zero transitive dependencies, a twelve-year-old
widely-audited parser. Writing our own config parser to save this would be bad security economics: we
would be trading a well-reviewed 4 k-line library for our own unreviewed one, on input that is
already trusted (root writes `/etc/messq/messq.toml`). We enable strict decoding and **reject unknown
keys** — a typo'd `tls_requird = true` must be a startup error, never a silently insecure default.

### 11.8 Build-time tooling (not linked into the binary)

- **GoReleaser** — its supply-chain documentation gives the exact configuration we adopt: `sboms:`
  blocks producing syft SBOMs for archives and source; a `signs:` block invoking
  `cosign sign-blob --output-certificate --output-signature` over the checksum file (all artifacts,
  SBOMs included, are covered by the checksum, so signing that one file suffices); and for
  reproducibility `mod_timestamp: "{{ .CommitTimestamp }}"`, `flags: [-trimpath]`, `CGO_ENABLED=0`,
  and `gomod: proxy: true` so the release builds from the module proxy rather than from whatever is
  in the working tree.
- **`govulncheck`** — on every PR and nightly against released tags, since a tag that was clean at
  release time will not stay clean.
- **`syft` / `cosign` / GitHub artifact attestations** — SBOM, signature, and SLSA provenance. The
  ecosystem has matured to where SLSA Level 2 is an afternoon of work rather than a project; there is
  no excuse not to ship it from v0.1.0.
- **`staticcheck`**, **`gosec`** (advisory), **`systemd-analyze security`** (gating at ≤ 2.0).

### 11.9 Rejected libraries, with reasons

| Rejected | Why |
|---|---|
| `etcd-io/bbolt` | Its README documents an exclusive write lock (no second process can read the file for forensics), mmap-driven memory growth on large databases, and poor page utilisation / slow bulk inserts into a new bucket — which is exactly our publish pattern. And it gives up SQL, which is the substrate for `peek`, `trace`, replay and incident forensics. |
| `mattn/go-sqlite3` | cgo breaks `CGO_ENABLED=0` static builds, cross-compilation and reproducibility, and adds libc surface. Kept as a documented emergency build tag only. |
| `grpc-go` / protobuf | Large hostile-input parsing surface, its own CVE stream, the decompression-bomb class described above, and not debuggable with `curl` during an incident. |
| A macaroon or Biscuit library | Macaroon verification requires the root secret at every verifier — a real problem for distributed verification, and a **non-problem for a single-node broker**, so if we want attenuation in Phase 2 we can implement the HMAC caveat chain in ~80 reviewable lines. Biscuit would drag a Datalog engine into a product whose pitch is "understandable in an evening". |
| A logging library (zap, zerolog) | `log/slog` covers structured JSON with `LogValuer` redaction; a dependency here buys speed we do not need on a path dominated by fsync. |
| An HTTP router (chi, gorilla) | Go 1.22+ `http.ServeMux` handles method and path-wildcard routing. Our route table is ~25 routes and is registered through a registry that the authz matrix test consumes — a third-party router would only make that registry harder to introspect. |
| A YAML config library | YAML's type coercion and anchor/alias features are an attack surface and a footgun (`no` parsing as `false`, billion-laughs expansion). TOML has neither. |

---

## Appendix A — Configuration example (`/etc/messq/messq.toml`)

```toml
[server]
state_dir       = "/var/lib/messq"
write_batch_ms  = 2
max_message_size = "1MiB"
min_free_disk   = "256MiB"

[listen.unix]
path  = "/run/messq/messq.sock"
mode  = "0660"
group = "messq"

[listen.tcp]
addr      = "0.0.0.0:4711"
tls_cert  = "/etc/messq/tls/server.crt"
tls_key   = "/etc/messq/tls/server.key"
client_ca = "/etc/messq/tls/client-ca.crt"   # presence ⇒ RequireAndVerifyClientCert
min_tls   = "1.2"

[listen.metrics]
addr = "127.0.0.1:9711"
auth = "none"

[log]
format           = "json"          # "text" when attached to a TTY
level            = "info"
header_allowlist = ["content-type", "x-idempotency-key"]
# unsafe_log_payloads = true       # config-file only; audited; never do this

[audit]
retention        = "90d"
checkpoint_every = "60s"
key_mode         = "local"         # or "external": broker holds only the public key
key_path         = "/etc/messq/audit.key"

[[principal]]
name = "billing-worker"
kind = "peer"
uid  = 1101
grants = [
  { verb = "consume", pattern = "orders.eu.>" },
  { verb = "publish", pattern = "billing.events.*" },
]

[[principal]]
name = "api-gateway"
kind = "cert"
cert_subject = "spiffe://corp/ns/prod/sa/api-gateway"
grants = [ { verb = "publish", pattern = "orders.>" } ]

[[principal]]
name = "oncall"
kind = "token"
grants = [
  { verb = "peek",       pattern = "orders.>" },
  { verb = "dlq_read",   pattern = "orders.>" },
  { verb = "dlq_redrive", pattern = "orders.>" },
  { verb = "audit_read", pattern = ">" },
  { verb = "replay",     pattern = "orders.>", effect = "deny" },   # explicit: no rewinding
]
```

## Appendix B — Security documentation set (all shipped, all reviewed)

| File | Contents |
|---|---|
| `SECURITY.md` | Disclosure address, 90-day policy, supported versions |
| `docs/threat-model.md` | §1.3 in full, updated with every milestone |
| `docs/security.md` | Auth model, token handling, why SHA-256 not argon2, key management, what messq does not protect against |
| `docs/audit.md` | Chain construction, checkpoint anchoring, verification procedure, incident response for a detected break |
| `docs/hardening.md` | systemd unit, file permissions, network posture, `messq doctor` checks explained |
| `docs/operations.md` | Runbooks: backlog, stuck consumer, DLQ growth, disk pressure, suspected tampering |
| `RELEASE.md` | How to verify signatures, SBOM and SLSA provenance before installing |
