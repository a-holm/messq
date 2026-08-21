# 0004. Make the dead-letter queue a real stream

- Status: Accepted
- Date: 2026-08-21
- Adjudicates: D3
- Relates to: PLAN.md section 2 D3, PLAN.md section 5.1, docs/SEMANTICS.md S4.4 and S9, issue #4, issue #12

## Context

A message that exhausts its delivery attempts has to go somewhere. Two shapes were proposed: copy it into a real stream named `<stream>.dlq`, or mark the row terminal in place and maintain an index of terminal rows.

The objection to the stream shape was atomicity: a copy into a second journal plus a deletion from the first is two writes, and two writes need a protocol. That objection evaporates under decision D1, because both writes and the audit event are one SQLite transaction.

A second question was left open by the plan and had to be settled here. The schema declares `messages.id` as "stable across DLQ/replay lineage" and, four lines later, declares `CREATE UNIQUE INDEX messages_id ON messages(id)` across all streams. Both cannot hold while the origin row still exists, and `GET /v1/messages/{id}` needs a globally unique id to be answerable at all.

## Decision

A dead-letter queue is an ordinary stream named `<stream>.dlq`, auto-created on the first dead-letter into it. The copy, the deletion of the delivery row and the `msg.dead` event are one transaction.

The dead-letter copy is a **new message with a new id and a new seq**. The `trace_id` is preserved, and the origin id travels in the `Messq-Origin-Id` header and in the `detail` of the `msg.dead` event. A redrive and a replay follow the same rule. Message ids stay globally unique.

The `.dlq` suffix is reserved: a user may not create a stream whose name ends in it, a consumer on a `.dlq` stream defaults to `dead_policy = drop`, and `<stream>.dlq.dlq` is never created. The `Messq-` header prefix is reserved on publish, and a user header inside it is rejected rather than silently dropped.

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

The cost is that a reader who expects the id to be stable across the dead-letter hop is wrong, and the documentation has to say so plainly. `SEMANTICS.md` S4.4 is the table that says it. The recommended consumer-side deduplication key is `stream/seq`, which is stable across every redelivery and is explicitly not stable across a dead-letter hop.

The reserved `.dlq` suffix is why `internal/subject` caps a new stream name at 60 bytes while accepting existing names up to 64: a 60-byte name plus `.dlq` is still a valid name.

## Revisit trigger

None. If a future release needs a stable cross-lineage identifier, it is a new column with a new name, not a redefinition of `id`.
