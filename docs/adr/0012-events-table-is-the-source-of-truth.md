# 0012. Make the in-transaction events table the source of truth

- Status: Accepted
- Date: 2026-08-21
- Adjudicates: D11
- Relates to: PLAN.md section 2 D11, PLAN.md section 9, docs/SEMANTICS.md S2.4, issue #4, issue #19, issue #20

## Context

Answerability is the product. "A webhook did not get processed last Tuesday and we cannot prove what happened" is the pain messq exists for, so the audit trail is a feature with an API contract, not instrumentation.

An audit trail written outside the transaction that changed the state can disagree with the state. It disagrees exactly when it matters: during a crash, during a fault, during the incident somebody is trying to reconstruct.

There are three observability surfaces and they can easily grow three vocabularies, so that a `grep`, a `SELECT` and a PromQL query about the same thing use three different names.

## Decision

The durable `events` table is the source of truth. Every state transition writes its event row **in the same transaction as the state change**. Logs and metrics are projections of that vocabulary, never independent sources.

The vocabulary is a closed set, listed once in PLAN.md section 9.2 and reproduced in `SEMANTICS.md` S2.4. Renaming, adding or removing a member is a breaking change. All three surfaces use the same identifiers.

The events table is never sampled, so `messq trace` is always complete. Logs may be sampled per message, meaning the whole lifecycle of a sampled message or nothing, because half a story is worse than none. Hot-path events log at DEBUG; problem events log at WARN or ERROR and are never sampleable, enforced by an allow-list in code.

Metrics use `prometheus/client_golang` on a custom registry. Labels are `stream` and `consumer` only. Never `subject`, never an id.

## Alternatives

| Option | Why it was serious | Why it lost |
|---|---|---|
| Log lines as the audit trail, with no events table | It is what most brokers do, it costs nothing, and log shipping already exists everywhere. | Logs are outside the transaction, so they can disagree with the state, and they are sampled, rotated and lossy. `messq trace` would then be a best-effort reconstruction rather than an answer. |
| An events table written after the commit | Simpler code, and no schema coupling between the state tables and the journal. | It reintroduces the disagreement it was meant to remove, at the exact moment it matters. Under decision D1 the in-transaction write costs zero extra fsyncs, so there is nothing to buy. |
| A hand-rolled Prometheus exposition format | It saves a dependency, and the format is simple. | `client_golang` is the one place where buying the standard is obviously right: histograms, the registry model and the exposition edge cases are all solved. |
| An OpenTelemetry SDK | Traces and metrics in one vendor-neutral pipeline. | A large dependency tree for a single-node broker. messq emits W3C trace ids and correlates on them, so spans can bridge from logs without the SDK. |
| Allowing `subject` as a metric label | It is the label an operator asks for first. | Subject cardinality is unbounded by design. It is the classic way to take down a Prometheus server, and the events table answers per-subject questions with a `SELECT`. |

## Consequences

The audit trail structurally cannot lie: if the state changed, the event is there, and if the event is not there, the state did not change. That is what makes invariant I10 checkable at all, because folding the journal must reproduce the state.

One vocabulary across three surfaces means `grep`, SQL and PromQL speak the same language, which is worth more than any single feature in the observability stack.

The closed vocabulary is a compatibility surface. Adding an event name is a specification change with an ADR, which is deliberate friction: an open vocabulary drifts into noise.

Because the vocabulary is closed and holds no clock event, a detected wall-clock step is reported through the log and `messq doctor` rather than as an event row. That is clarification C8 in `SEMANTICS.md`.

The events table grows and has to be trimmed. `--event-retention` defaults to 72 hours and `--event-max-rows` caps it, which puts the journal in the bounds register of `SEMANTICS.md` A1 like every other bounded resource.

## Revisit trigger

Measured journaling overhead above 10 % of throughput when comparing full journaling against none. The published benchmark reports that number, and crossing it moves the event write to a separate table with a coarser schema rather than out of the transaction.
