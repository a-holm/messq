# Fixture: T2 compares the deadline the wrong way round.

### S6.1 The table

| ID | From | Trigger | Guard | Effect | Event | On guard failure |
|---|---|---|---|---|---|---|
| T1 | UNSEEN | fetch top-up | filter matches ∧ `seq >= cursor_seq` ∧ `pending(c) < max_ack_pending` ∧ ¬`paused` | insert a READY row with `attempts = 0`, `visible_at = 0`, `generation = c.generation`; advance `cursor_seq` past every scanned seq including non-matching ones | none | `pending(c)` at the bound: no insert, `flow.blocked`, the fetch returns the batch it has and reports `errs.ErrFlowControl` as its hold reason; `paused`: `errs.ErrPaused` |
| T2 | READY | fetch claim | `now <= visible_at` ∧ `pending(c) <= max_ack_pending` | `attempts++`, `state = INFLIGHT`, `visible_at = now + ack_wait`, `delivered_at = now`, mint the token; committed before the response | `msg.deliver` | `now < visible_at`: the row is skipped, which is not an error and produces no event; the pending assertion is C3 |
| T3 | INFLIGHT | `ack(token)` | fenced per S3.3 | DELETE the row | `msg.ack` | see T3a and T3b |
| T3a | RESOLVED | `ack(token)` | consumer exists ∧ generation matches ∧ no row | idempotent success, flagged `stale` in the result; nothing is mutated | `msg.ack_dup` (DEBUG) | consumer absent: `errs.ErrNotFound`; generation mismatch: `errs.ErrWrongGen` |
| T3b | INFLIGHT | `ack(token)` with a mismatched fence | attempt or generation mismatch | the row is untouched | `msg.ack_stale` (WARN) | `errs.ErrStaleAck` on an attempt mismatch, `errs.ErrWrongGen` on a generation mismatch |
| T4 | INFLIGHT | `nak(token, delay?, reason?)` | fenced per S3.3 ∧ ¬(`max_deliver > 0` ∧ `attempts >= max_deliver`) | `state = READY`, `visible_at = now + (delay ?? backoff(attempts))`, `last_reason = reason` | `msg.nak` | fence failure: as T3b; at the delivery bound: T5; `delay` outside `[0, 86400000]`: `errs.ErrBadRequest` |
| T5 | INFLIGHT | `nak` or sweeper timeout with `max_deliver > 0` ∧ `attempts >= max_deliver` | fenced per S3.3 for `nak`; unconditional for the sweeper | dead-letter per S9.2 with `Messq-Cause: max_deliver` | `msg.dead` | fence failure on the `nak`: as T3b, and the row stays INFLIGHT until the sweeper reaches it |
| T6 | INFLIGHT | `term(token, reason)` | fenced per S3.3 | dead-letter per S9.2 immediately, with `Messq-Cause: terminated`; remaining attempts are skipped | `msg.term`, `msg.dead` | as T3b |
| T7 | INFLIGHT | `extend(token)` | fenced per S3.3 ∧ `(visible_at + ack_wait) - delivered_at <= max_ack_wait` | `visible_at += ack_wait`; `attempts` unchanged | `msg.extend` (DEBUG) | past the total-extension cap: `errs.ErrBadRequest`, the deadline is unchanged, and the delivery times out on schedule |
| T8 | INFLIGHT | sweeper tick with `now > visible_at` | ¬(`max_deliver > 0` ∧ `attempts >= max_deliver`) | `state = READY`, `visible_at = now + backoff(attempts)`, `last_reason = "timeout"` | `msg.timeout` (WARN) | at the delivery bound: T5 |
| T9 | INFLIGHT | broker restart | none | `state = READY`, `visible_at = now + jitter(0, 1s)`, `last_reason = "broker_restart"`; `attempts` unchanged | `recovery.reclaimed` | none: recovery has no guard, and refusing to reclaim would strand the delivery |
| T10 | any | `seek` or `purge` | operator, confirmed by name | `generation++`, `cursor_seq` reset and clamped to `>= first_seq`, every delivery row of the consumer deleted and counted | `consumer.seek` (WARN) | unconfirmed: `errs.ErrBadRequest`; unknown consumer: `errs.ErrNotFound` |
| T11 | DEAD in `<s>.dlq` | `dlq redrive` | operator, rate-limited, `Messq-Redrive-Count < 3` without `--force` | republish into the origin stream per S4.4: new id, new seq, `Messq-Redrive-Of`, trace preserved | `dlq.redrive` | already redriven three times without `--force`: `errs.ErrConflict` |
