# 0005. Ship two durability modes and always group-commit

- Status: Accepted
- Date: 2026-08-21
- Adjudicates: D4
- Relates to: PLAN.md section 2 D4, PLAN.md section 4.3, docs/SEMANTICS.md S11, issue #4, issue #7

## Context

An fsync policy is a promise, and a queue that is vague about it is worse than a queue with no promise at all: the operator plans for a guarantee that does not exist. The policy has to be a small number of named modes an operator can hold in their head, and each has to mean something testable.

Several plans built a lazy ack-flush window of around 200 ms, because their log engines pay a full fsync per ack. Under decision D1 that reasoning does not transfer: acks ride the same group commit as publishes, at close to zero marginal fsync cost.

## Decision

There are exactly two durability modes.

`--durability=full` is the default. SQLite runs with `synchronous=FULL`, and a 2xx response to publish, ack, nak, term, extend or an admin write is sent only after the group commit's fsync returns. **An ack response is a durability promise.**

`--durability=relaxed` runs with `synchronous=NORMAL`. It survives a process kill, may lose the last commits on power loss or a kernel panic, and never corrupts the database. It logs a WARN banner at startup and `messq doctor` flags it.

Group commit is always on and is not a mode. The writer accumulates commands for up to 2 ms or 256 commands, whichever comes first, and commits once, so concurrent operations share one fsync.

An `EIO` or `ENOSPC` from a commit is fatal: latch the process read-only, write `storage.fatal` at ERROR, refuse writes, and exit non-zero after a short drain. The fsync is never retried.

## Alternatives

| Option | Why it was serious | Why it lost |
|---|---|---|
| A third `volatile` or `none` mode | It is the honest name for what many brokers do by default, and it is the fastest. | Two options an operator can hold in their head beat three they will misconfigure. The gap between `relaxed` and no durability at all is not worth a support category. |
| A per-call strictness flag | It lets one critical publish be durable while the rest are fast. | `full` already means fsynced before the response, and batching is an implementation detail. A per-call flag turns the durability contract into a per-request negotiation that no test can summarise. |
| A lazy ack-flush window, with acks durable later than publishes | Correct and necessary for a log engine that pays per fsync, and several plans specified it in detail. | Under D1 it buys nothing and costs a documented duplicate window from ack loss. The insight survives as the asymmetry being *absent*: an ack is as durable as a publish, and that is a stronger promise for free. |
| Retry a failed fsync | It is the reflex, and it sometimes appears to work. | A failed fsync may already have discarded the dirty page, so a retry that succeeds proves nothing. The only correct response is to stop writing and restart into recovery. |

## Consequences

The durability mode is named, printed at startup, exposed on `/v1/info`, carried as a label on `messq_build_info`, and flagged by `doctor`. It is never inferred from a DSN.

Because acks are durable before their response under `full`, there is no server-side duplicate window from ack loss. A duplicate from a lost ack is a duplicate from a lost *response*, which lives on the network or in the client, and `SEMANTICS.md` S14 says so.

The cost is latency: p99 publish-to-durable-ack is roughly the commit window plus the fsync, targeted at 10 ms or better under moderate load. `messq_commit_batch_size` and `messq_commit_duration_seconds` make the batching observable, so an operator can see whether the window is the right size for their disk.

The fatal-fsync rule means a disk fault takes the process down. That is deliberate: systemd restarts into recovery, which re-derives the truth from disk, and the alternative is a broker that keeps accepting writes it cannot keep.

## Revisit trigger

A measured group-commit window that is wrong for real hardware: if `messq_commit_duration_seconds` shows the 2 ms window dominating latency on common disks, the window becomes adaptive. That is a tuning change inside the writer, not a new mode.
