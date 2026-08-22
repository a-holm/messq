# 0002. Store everything in one SQLite database

- Status: Accepted
- Date: 2026-08-21
- Adjudicates: D1
- Relates to: PLAN.md section 2 D1, PLAN.md section 4, issue #4, issue #5

## Context

messq's differentiator is answerability: every state transition is durably recorded in the same transaction as the state change, and `messq trace <id>` replays the whole story. The storage engine is the decision that makes that either free or expensive, and it is the hardest decision in the project to reverse, because every query, every recovery path and every forensic tool is written against it.

Three engine families were on the table. A single SQLite database file. A custom segmented append-only log for payloads plus bbolt or a checkpoint file for delivery state. bbolt alone.

Two forces decide it. First, the crash matrix: a state change, its delivery bookkeeping, its dead-letter copy and its audit event must either all land or none land. Second, the throughput the intended user actually has, which is at most five thousand durable messages per second on commodity NVMe, well inside what SQLite with group commit delivers.

## Decision

messq stores everything in one SQLite database file, `<data-dir>/messq.db`, in WAL mode, through the pure-Go driver `modernc.org/sqlite`. Every table is `STRICT`. Migrations are numbered, embedded with `go:embed`, forward-only, and applied in one transaction.

There is one read-write connection, owned exclusively by the writer goroutine, and a read-only pool sized to the CPU count for peek, trace, list, lag and metrics. A connection hook re-asserts the durability pragmas on every pooled connection and reads them back; the daemon refuses to start when `synchronous` does not match the configured durability mode.

## Deviations implemented in #5

Implementing §4.1's connection layout refined two details of this decision. Both are enforced and documented where they live (internal/store/dsn.go); they amend the letter above without changing the decision.

- **No `_txlock=immediate` on the read pool.** `query_only=1` forbids acquiring a write lock, so a `BEGIN IMMEDIATE` on a read connection fails outright; `immediate` exists to kill the deferred-upgrade `SQLITE_BUSY` class on the writer, which never upgrades its reads. Only the writer DSN carries it.
- **`mode=ro` is opt-in via `Options.ReadOnly`; `query_only(1)` is the enforcement.** A read-only OS-level handle on a WAL database cannot create the `-shm` file, so opening a quiescent copied data directory with `mode=ro` fails outright — and the same code path serves `messq verify` against a copy (#8) and `doctor` (#30). The daemon's read pool therefore opens the file read-write at the OS level and is fenced read-only by `query_only`, verified by read-back on every pooled connection. `mode=ro` remains available to offline inspection tools that accept the shm constraint.

## Alternatives

| Option | Why it was serious | Why it lost |
|---|---|---|
| Custom segmented log for payloads plus bbolt or a checkpoint file for delivery state | The storage-engineer and performance-engineer plans are right that it is faster per byte, avoids delete amplification, and gives full control over layout. | It creates two durability domains and therefore a cross-domain crash-consistency protocol. A single developer cannot exhaustively test that, and the entire crash-point enumerator would exist only to reach parity with what SQLite's WAL already does. |
| bbolt alone | One file, one dependency, no SQL, and a mature B-tree. | Every inspection feature becomes a hand-maintained index. `trace`, `pending`, `dlq ls` and `lag` are the product, and under bbolt they become the expensive part rather than a `SELECT`. |
| SQLite through `mattn/go-sqlite3` | The most-used Go SQLite driver, and faster in benchmarks. | It requires cgo, which forfeits the single static cross-compiled binary. It stays wired in behind a `//go:build cgosqlite` file and is exercised in CI, so the escape hatch is real rather than theoretical. |
| Postgres or any server database | Familiar, and someone else's recovery code. | It is a second process to operate, which is the exact cost the product exists to remove. |

Two objections to SQLite were raised and answered rather than dismissed. "Durability lives in a DSN string, and a DSN is a trap" is true, and the answer is the pragma read-back hook above: a wrong pragma is a startup failure, not a silent downgrade. "Deletes amplify and checkpoints stall" is also true, and the answer is that retention deletes are batched in the 60-second sweep, WAL checkpointing runs on messq's schedule with `wal_autocheckpoint` tuned and explicit `TRUNCATE` checkpoints off-peak, and `auto_vacuum=INCREMENTAL` is set at creation.

## Consequences

One transactional boundary collapses the crash matrix. The dead-letter procedure of SEMANTICS.md S9.2 is one transaction, and it cannot be half-applied. The event row commits with the state change, so the audit trail costs zero extra fsyncs and structurally cannot disagree with reality.

`sqlite3 messq.db` works for forensics even when the daemon is down, which is a support channel the project gets for free.

The cost is a ceiling. SQLite plus group commit will not reach a hundred thousand messages per second, and the documentation says so rather than pretending otherwise. Payload BLOBs are inline and capped at 1 MiB by default with an 8 MiB hard ceiling; larger payloads belong in object storage behind a pointer, which is the claim-check pattern and is documented as such.

The project now has to keep the read-only pool honest: readers must never block the writer, which means WAL mode is not optional and `query_only` is asserted on the read pool.

## Revisit trigger

The M2 gate: at least 5000 durable 1 KiB publish-and-ack round trips per second on commodity NVMe with `--durability=full`. If the measured number is below that, payload BLOBs move to append-only segment files while SQLite keeps all metadata. That change lives behind the store package and does not touch the wire, which is why it is a contained escape rather than a rewrite.
