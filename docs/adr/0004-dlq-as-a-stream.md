# 0004. Make the dead-letter queue a real stream

- Status: Accepted
- Date: 2026-08-21
- Adjudicates: D3
- Relates to: PLAN.md section 2 D3, PLAN.md sections 4.2, 5.1 and 7, docs/SEMANTICS.md S4.4, S9, C1, C12 and C13, ADR-0011, issue #4, issue #12, issue #20, issue #29

## Context

A message that exhausts its delivery attempts has to go somewhere. Two shapes were proposed: copy it into a real stream named `<stream>.dlq`, or mark the row terminal in place and maintain an index of terminal rows.

The objection to the stream shape was atomicity: a copy into a second journal plus a deletion from the first is two writes, and two writes need a protocol. That objection evaporates under decision D1, because both writes and the audit event are one SQLite transaction.

The second question was not left open. The plan answers it, in four places, and this record **overrides** that answer. Saying so plainly is the point of what follows, because an override that reads like a clarification is how a decision gets lost.

PLAN.md asserts that the message id survives the dead-letter hop:

1. **Section 2 D3**, in a section whose preamble reads "The decision is final": provenance headers "and the **original `msg_id`/`trace_id` preserved** so `trace` shows one continuous story (03, 10)".
2. **Section 5.1's dead-lettering paragraph**, in a section whose heading marks it "normative" and requires the conformance tests to mirror it one row to one row: "the original `id` and `trace_id` are preserved; delete the delivery row; write `msg.dead`".
3. **Section 4.2's schema comment**, on the column itself: `id TEXT NOT NULL, -- ULID, stable across DLQ/replay lineage`.
4. **Section 2 D10's rejection reason** for `stream:seq` as the only identifier: "seq is not stable across DLQ copies and replays; we expose both". That is an argument for the ULID only if the ULID is stable there.

Against those four, section 4.2 also declares `CREATE UNIQUE INDEX messages_id ON messages(id)` across every stream, and section 7's `GET /v1/messages/{id}` needs that uniqueness to be answerable at all. A copy that keeps the origin id violates the index while the origin row still exists, which is precisely when a dead-letter copy is made. The plan cannot have all of it.

What D3 was *for* is not in doubt: one continuous story. That purpose is what this record keeps.

## Decision

A dead-letter queue is an ordinary stream named `<stream>.dlq`, auto-created on the first dead-letter into it. The copy, the deletion of the delivery row and the `msg.dead` event are one transaction.

The dead-letter copy is a **new message with a new id and a new seq**. This overrides the letter of the four passages above and keeps their purpose:

- **Overridden**: "the original `id` is preserved", in PLAN.md sections 2 D3, 4.2 and 5.1. It is not preserved. Message ids stay globally unique, so `GET /v1/messages/{id}` and `messq trace <id>` stay unambiguous.
- **Overridden**: PLAN.md section 5.1's dead-letter header set, which carries no origin id because under its own reading none was needed. This record adds **`Messq-Origin-Id`** to it. That is a second override, and it exists only because of the first: once the copy mints a new id, the origin id has nowhere else to live on the message itself.
- **Kept**: `trace_id`, preserved across every hop, exactly as all four passages require.
- **Kept, and made a testable requirement**: one continuous story. Both ids go into the `detail` of `msg.dead`, `dlq.redrive` and the replay event, so the chain is walkable from either end through the `events_msg` and `events_trace` indexes. #20 carries the acceptance criterion: **`messq trace` given any id in a lineage renders the whole story across every hop**, not only the hop that id belongs to. D3's purpose is met by that test or it is not met at all.

A redrive and a replay follow the same rule.

The `.dlq` suffix is reserved: a user may not create a stream whose name ends in it, a consumer on a `.dlq` stream defaults to `dead_policy = drop`, and `<stream>.dlq.dlq` is never created. The `Messq-` header prefix is reserved on publish, and a user header inside it is rejected rather than silently dropped.

Redriving is bounded. A message already redriven three times is refused with `errs.ErrConflict` unless `--force` is given, counted in `Messq-Redrive-Count`. PLAN.md section 5.1 says only "operator, rate-limited"; a redrive loop is the shortest path from one poison message to an outage, so the counter is part of the mechanism rather than a policy left to the operator.

## Alternatives

| Option | Why it was serious | Why it lost |
|---|---|---|
| In-place terminal state plus an index of terminal rows | No copy, no second stream, no name collisions, and the correctness-purist plan is right that it is the smaller mechanism. | It needs new machinery for every operation the stream shape gets free: inspection, depth metrics, retention, alerting and redrive. A dead-letter queue nobody watches is the documented failure mode, and a first-class stream gets lag metrics and shipped alerts without a line of new code. |
| Preserve the origin message id on the dead-letter copy and scope the unique index to `(stream, id)` | It matches the plan's "one continuous story" wording literally, and it makes lineage trivially greppable. | It destroys `GET /v1/messages/{id}` and makes `messq trace <id>` ambiguous the moment a message is dead-lettered, which is exactly the moment somebody runs it. The story is preserved by `trace_id` and by both ids in the event `detail` instead, which costs one header and keeps the by-id lookup. |
| Delete on exhaustion and record only the event | The smallest possible mechanism, and the event journal still explains what happened. | It loses the payload, so a redrive is impossible and an operator cannot see what actually failed. |
| One shared dead-letter stream for the whole node | Fewer streams, one place to watch. | It mixes retention policies and access scopes across streams, and it makes per-stream depth alerting impossible. |

## Consequences

One mechanism buys four features. Inspection is `peek`. Redrive is consume plus republish. Alerting is a depth metric on an ordinary stream. Retention is ordinary stream retention.

Lineage costs one extra header and two ids in the `msg.dead` event `detail`. `messq trace` walks the chain in both directions through the `events_msg` and `events_trace` indexes without a table scan, and it keeps working after retention deletes the origin message.

The cost is that a reader who expects the id to be stable across the dead-letter hop is wrong, and every document that touches message ids has to say so. It is not a hypothetical cost: `internal/id`'s package documentation, merged in #3 from the plan's wording, told readers the opposite, and this record is what corrected it. `SEMANTICS.md` S4.4 is the table, ADR-0011 states the consequence where a reader looking up message ids will find it, and the recommended consumer-side deduplication key is `stream/seq`, which is stable across every redelivery and is explicitly not stable across a dead-letter hop.

The reserved `.dlq` suffix is why `internal/subject` caps a new stream name at 60 bytes while accepting existing names up to 64: a 60-byte name plus `.dlq` is still a valid name.

## Revisit trigger

None. If a future release needs a stable cross-lineage identifier, it is a new column with a new name, not a redefinition of `id`.
