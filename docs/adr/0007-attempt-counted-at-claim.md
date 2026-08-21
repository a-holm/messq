# 0007. Count an attempt at claim, durably, before the response

- Status: Accepted
- Date: 2026-08-21
- Adjudicates: D6
- Relates to: PLAN.md section 2 D6, PLAN.md section 5.1, docs/SEMANTICS.md S6.3, issue #4, issue #9, issue #11

## Context

`max_deliver` is the bound that stops a poison message from being retried forever. What it counts, and when the count becomes durable, decides whether the bound survives a crash.

If the counter increments when a delivery *fails*, a broker that crashes between the fetch response and the failure never counts that attempt, and a crash-looping broker loops a poison message forever. If the counter increments at claim but is committed after the response, the same hole is narrower and still there.

A second proposal was to keep two counters: a dispatch count and a failure count, so that a redelivery caused by the broker could be told apart from one caused by the handler.

## Decision

`attempts` increments exactly once per delivery, in the same transaction that claims the row, and that transaction is committed before the fetch response is sent.

During attempt *n* the row reads `attempts = n`. `max_deliver = 5` therefore means exactly five handler invocations for a message that never succeeds. `max_deliver = 1` means exactly one, with no retry. `max_deliver = 0` means unlimited: the dead-letter guard is `max_deliver > 0 AND attempts >= max_deliver`, so it never fires, and the consumer create and update paths warn.

Restart recovery does **not** re-increment. A reclaimed in-flight row becomes immediately redeliverable with `reason=broker_restart` recorded and a startup jitter of up to one second, and its `attempts` is untouched.

## Alternatives

| Option | Why it was serious | Why it lost |
|---|---|---|
| Increment on failure rather than on claim | It counts what an operator thinks `max_deliver` counts: failures, not dispatches. | A crash between the response and the failure loses the count, so a crash loop retries a poison message forever. The bound has to be durable before the message leaves the building. |
| Increment at claim but commit lazily with the next batch | It saves nothing under group commit, and it looked cheaper. | It reintroduces the same hole with a smaller window. Under D4 the commit is already batched, so the durable increment costs no extra fsync. |
| Two counters: dispatch count and failure count | The correctness-purist plan is right that they are different quantities, and the distinction matters when diagnosing whether the broker or the handler caused a redelivery. | It is a second durable counter and a subtler rule on the one state machine every contributor must understand. Its spirit survives in the `reason` and `cause` recorded on every redelivery, which answers the same question from the event journal without a second column. |
| Re-increment on restart recovery | It is the conservative reading of "this delivery did not succeed". | The delivery already counted at claim, so re-incrementing spends a worker's attempts on a broker restart. A restart is not the handler's fault, and `SEMANTICS.md` I9 requires only that the redelivery has a named cause, which `recovery.reclaimed` provides. |

## Consequences

Counters survive crashes, so the delivery bound is real rather than best-effort, and invariant I4 holds across restarts.

The rule reads oddly at the boundary until it is worked through, so the specification carries the worked table for `max_deliver` in {0, 1, 5} and states the exact number of handler invocations. That table is what `messq consumer add` and the retry-horizon output are checked against.

Because the increment is durable before the response, a fetch that the client never receives still counted. That is the correct trade: an at-least-once broker may deliver twice, and it may never exceed the bound.

The dead-letter guard needs the `max_deliver > 0` term explicitly, because the plan's literal wording dead-letters every first failure when `max_deliver = 0`. That is clarification C4 in `SEMANTICS.md`.

## Revisit trigger

None. A second counter can be added later without changing this rule, because `attempts` would keep its meaning.
