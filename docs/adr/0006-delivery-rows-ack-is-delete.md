# 0006. Keep delivery state in a row, and make an ack a DELETE

- Status: Accepted
- Date: 2026-08-21
- Adjudicates: D5
- Relates to: PLAN.md section 2 D5, PLAN.md section 5, docs/SEMANTICS.md S5, S6, S7 and S8, issue #4, issue #9, issue #11

## Context

The delivery state machine needs a representation that survives a crash, that keeps the "what is outstanding" query fast on a stream with a hundred million messages, and that cannot grow without bound when a consumer misbehaves. Redis's pending-entries list is the cautionary example: a consumer that never acks leaves entries behind forever, and the list becomes the outage.

Expiry also needs a mechanism. One plan proposed a timing wheel with a timer per in-flight message; another proposed a periodic sweep.

## Decision

Delivery state is one row in `deliveries(stream, consumer, seq)` with `state` in `{READY, INFLIGHT}`, an `attempts` counter, a `visible_at` timestamp and the consumer generation that minted it.

**An ack is a DELETE.** Every terminal outcome is either the deletion of the row or its migration into the dead-letter stream. There is no per-message terminal status column: the terminal state is the absence of a row, and the reason lives in the events table.

Expiry is one indexed `UPDATE` in a 250 ms sweeper tick, issued as a command through the writer, not a timer per message. In-memory state is limited to the long-poll waiter registry.

Three details the plan left open are settled here.

- **`max_ack_pending` bounds `pending(c)`, which counts READY and INFLIGHT rows together, and it is enforced where rows are created, at top-up.** A claim does not change `pending(c)`, so its guard on the bound is an assertion rather than a decision.
- **Durable deadlines are wall-clock, and a wall-clock step moves them.** In-process waits are monotonic and a step does not disturb them.
- **An explicit `nak` delay is clamped to `[0, 86400000]` milliseconds, and an empty `backoff_ms` array is rejected at consumer create and update.**
- **An extend adds one `ack_wait` to the deadline and is capped by the deadline's distance from the claim**, so no delivery attempt holds a deadline more than `--max-ack-wait` past `delivered_at`. The cap needs no new column.

## Alternatives

| Option | Why it was serious | Why it lost |
|---|---|---|
| A pending-entries list that keeps resolved entries with a status | It preserves history in the same table as the state, so one query answers both. | The table grows with throughput rather than with outstanding work, and it becomes a leak the first time a consumer stops acking. The events table already preserves history, and it is the right table for it. |
| A timing wheel with one timer per in-flight delivery | Expiry fires with millisecond precision and no polling, and the performance-engineer plan is right that it scales better. | It is premature at five thousand messages per second: a 250 ms indexed `UPDATE` costs one query per tick regardless of how many deliveries are outstanding, and it needs no goroutine per message and no timer rebuild after a restart. |
| A terminal status column instead of deletion | `SELECT status` answers "what happened" without reading the journal. | It duplicates the events table, and duplicated truth diverges. It also makes the pending set grow with total traffic rather than with outstanding work. |
| A monotonic-anchored derived clock, so that a wall-clock step leaves ack deadlines untouched | The plan's fault-injection list asks for exactly this, and it prevents a redelivery burst after an NTP step. | The merged `internal/clock` package stores durable deadlines as wall-clock milliseconds, because a deadline must survive a restart and be readable in SQL, and a monotonic reading is meaningless once the process that took it exits. A step therefore does move deadlines, every resulting redelivery is a `msg.timeout` with a named cause, and the sweeper's per-tick cap bounds the burst. The plan's requirement is kept for in-process waits, which is the half a durable deadline can keep. |

## Consequences

The pending set *is* the `deliveries` table, so it stays small by construction and every "what is outstanding" query is fast regardless of stream size. `pending`, `inflight`, `backlog` and `oldest_pending_age` are the four formulas of `SEMANTICS.md` S5.3, and the metrics, the `lag` command and the shipped alerts all compute them the same way.

The reason an outcome happened is only in the events table. A reader who has not understood that cannot understand `messq trace`, so the specification states it as the load-bearing sentence of S5.1 rather than as a footnote.

A wall-clock step is visible as a redelivery burst with `reason=timeout`. That is correct at-least-once behaviour and it is answerable, which is the property the product sells. The divergence from the plan's wording is recorded as clarification C2 in `SEMANTICS.md`.

The 250 ms tick means a deadline may fire up to one tick late, and never early. The specification states the granularity so that an operator setting `ack_wait = 30s` expects expiry between 30.000 s and 30.250 s.

## Revisit trigger

Sweeper cost becoming measurable: if the 250 ms tick's `UPDATE` shows up in the commit-duration histogram at the M2 load, the tick becomes adaptive or the index gains a covering column. A timing wheel is reconsidered only if per-message expiry precision becomes a product requirement, which delayed delivery in phase 2 might make true.
