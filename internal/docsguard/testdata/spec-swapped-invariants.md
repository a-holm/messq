# Fixture: I5 and I6 state each other.

## S15. Invariant register

| ID | Statement | Predicate | Checked by | First required at |
|---|---|---|---|---|
| I1 | Every publish that returned 2xx under `full` durability is present after any crash (no acknowledged loss). | external three-valued ledger reconciliation: OK must exist, FAILED must not, UNKNOWN either | crash harness, `verify` | #8 |
| I2 | Every `(consumer, seq)` in `[first_seq, cursor)` is in exactly one of: resolved (below-floor/absent), READY, INFLIGHT. | S5.1's predicates are pairwise disjoint and cover the range | `verify`, rapid hook | #9 |
| I3 | An acked-and-committed `(consumer, seq)` is never redelivered except via explicit seek/replay/redrive. | history predicate over `events`: no `msg.deliver` for a pair follows its `msg.ack` without an intervening `consumer.seek` or a new message id | rapid hook, golden log test | #10 |
| I4 | `attempts ≤ max_deliver` for every non-terminal row; delivery stops at the bound, across restarts. | SQL predicate over `deliveries` joined to `consumers`, skipped where `max_deliver = 0` | `verify`, rapid hook | #11 |
| I5 | `cursor_seq` is monotone non-decreasing within a generation. | SQL aggregate per consumer | `verify`, rapid hook | #9 |
| I6 | `count(deliveries WHERE consumer=c) ≤ max_ack_pending(c)` always. | comparison across successive observations of the same generation | rapid hook | #9 |
| I7 | No stale-fenced ack/nak/term ever mutates a live row. | for every `msg.ack_stale` or `errs.ErrWrongGen` outcome, the row's `attempts`, `state` and `visible_at` are unchanged across the settle | unit tests, rapid hook | #10 |
| I8 | Each `(consumer, seq)` enters DEAD at most once, and each DEAD has exactly one DLQ message (when `dead_policy=dlq`). | count of `msg.dead` per pair is at most 1, and equals the count of `<s>.dlq` messages carrying that `Messq-Origin-Seq` and `Messq-Origin-Consumer` | `verify`, rapid hook | #12 |
| I9 | In a run with no faults where every delivery is acked within `ack_wait`, every message is delivered exactly once per consumer; any `attempts > 1` is preceded in the event log by a `msg.nak`, `msg.timeout`, or `recovery.reclaimed` for that pair (**every duplicate has a named cause**). | history predicate over `events`, cause set closed by S14.4 | rapid hook, golden log test | #13 |
| I10 | Folding the events table from the beginning reproduces the persisted state (log ≡ state; checked by `messq verify --deep`). | `verify --deep` replays the journal into a shadow state and diffs it against the tables | `verify --deep`, soak | #8 |
| I11 | No unbounded queue or collection exists anywhere; every bound is config-derived. | appendix A1 lists every bounded resource with its flag and default; there is no runtime predicate | A1 completeness test, code review | #6 |
