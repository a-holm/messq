# 0016. Order phase 2 by demand, and keep replication off the roadmap

- Status: Accepted
- Date: 2026-08-21
- Adjudicates: D15
- Relates to: PLAN.md section 2 D15, PLAN.md section 14, docs/SEMANTICS.md S15, issue #4, issue #37, issue #38, issue #39

## Context

Every queue accumulates feature requests that are individually reasonable: delayed delivery, priorities, consumer groups, per-consumer rate limits, ordered delivery, replication. Building them speculatively is how a product that is understandable in an evening stops being one.

Some of these are nearly free on machinery that already exists. Delayed delivery is a `visible_at` in the future, which the delivery-row model already supports. Multiple workers per consumer already works, because acks are fenced by attempt and generation, so "consumer groups" is a visibility and per-worker-cap feature rather than a new mechanism.

Others are not free at all. Replication needs either consensus or an honest story about what is lost on failover, and both are a different product.

## Decision

Phase 2 is a named set, built strictly in order of demonstrated demand: delayed delivery, ordered-by-subject consumers with head-of-line blocking displayed rather than hidden, per-consumer rate limiting, worker attribution, audit export, and native TLS.

Each phase 2 feature ships with a new invariant, its events, its metrics and its documentation, or it does not ship. `SEMANTICS.md` reserves invariant IDs I12 and above for exactly that, and no v1 implementation may claim one.

**Replication is not on the roadmap.** The ceiling is a design document gated on real demand. If it were ever built, the honest shape is asynchronous log shipping to a read-only follower with manual promotion, never consensus.

## Alternatives

| Option | Why it was serious | Why it lost |
|---|---|---|
| Build delayed delivery in v1, since it is nearly free | It is close to free, and it is a common request. | Nearly free is still a new state to specify, test and document, in the milestone where the core has to prove itself. It is first in phase 2 precisely because it is cheap. |
| Build consumer groups as a distinct concept | It is the vocabulary users arrive with, from Kafka and from JetStream. | Multiple workers on one consumer already works and is already correct, because the fence is per attempt. Adding a second concept for a capability that exists would teach a distinction messq does not have. |
| Priorities as separate lanes | It is the most requested queue feature after retries. | It multiplies the delivery state machine by the number of lanes and makes the fairness rules a permanent argument. Separate streams and separate consumers already express it. |
| Replication or clustering in v2 | It is the natural next step for a durable queue, and it is what users ask about first. | It is a different product with a different operational model. The non-goals list says so in the README, so "can it do X" is a link rather than a debate. |
| A plugin system, so users can build these themselves | It moves the roadmap pressure to the users. | A plugin system is an API surface, a stability promise and a sandbox question. It is a permanent non-goal. |

## Consequences

The v1 surface stays small enough to read in an evening, which is a stated success criterion rather than an aesthetic.

The specification reserves invariant ID space for phase 2, so a phase 2 feature does not renumber anything. Renumbering an invariant is a cross-repository rename, and reserving is free.

Publishing the non-goals is a trust tactic, not modesty. `docs/non-goals.md` gives each refused feature one line and a reason.

The cost is that some users will need a feature messq does not have yet and will choose something else. That is the correct outcome when the alternative is shipping six half-features.

## Revisit trigger

Per feature: three independent users blocked by its absence. For replication specifically, the trigger produces a design document, not an implementation.
