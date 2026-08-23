# messq delivery semantics

- Spec-Version: 1.0-draft
- Status: normative
- Relates to: PLAN.md sections 2, 4, 5, 6, 7, 9; issue #4

This document is the normative specification of messq's per-`(consumer, seq)` delivery lifecycle. It defines the states, the transitions between them, the guards that admit each transition, the outcome when a guard fails, and the invariants that hold at every quiescent point. An implementation that disagrees with this document is wrong, or this document is wrong; section S16 says what to do about it.

## S1. Scope, audience, and how to read this document

### S1.1 Audience

Three readers. An implementer of the store, the delivery engine, the settle verbs, the sweeper or the dead-letter path, who needs one answer per question and no archaeology. A test author writing the conformance suite (#13), the invariant checker (#8) or a contract test (#18), who needs a stable name for every rule so a test can cite it. An operator or an integrator who needs to know what a 2xx means and when a duplicate is possible.

### S1.2 Keyword convention

The key words MUST, MUST NOT, REQUIRED, SHALL, SHALL NOT, SHOULD, SHOULD NOT, RECOMMENDED, MAY and OPTIONAL are to be interpreted as described in RFC 2119, and appear in upper case only when they carry that meaning. Lower-case "must" in an explanatory sentence is prose, not a requirement.

### S1.3 Identifier conventions

Five ID spaces appear in this document, and each is stable: renaming an ID after v1.0 is a breaking change under S16.

| Prefix | Space | Example | Defined in |
|---|---|---|---|
| `S` | a section of this document | S6 | this document |
| `T` | a transition of the delivery state machine | T3b | S6 |
| `I` | an invariant | I5 | S15 |
| `C` | a clarification: a place where PLAN.md was ambiguous and this document resolved it | C4 | S1.5 |
| `D` | an adjudicated project decision | D6 | PLAN.md section 2, one ADR each under `docs/adr/` |

The subject grammar has its own rule IDs `S1` through `S11` in [docs/generated/subject-rules.md](generated/subject-rules.md), which collide with this document's section numbers. They are always written as **grammar rule S3**, never as a bare `S3`. A bare S-number in this document is a section of this document.

### S1.4 Sources and precedence

PLAN.md is the source for every decision recorded here. Where PLAN.md and merged code disagree, merged code wins and S1.5 records the divergence, because the code passed review and the plan did not run. Where PLAN.md is silent or ambiguous, S1.5 records the resolution and names the ADR that ratifies it. This document never resolves an ambiguity silently.

Three merged packages are facts this document is written against, not proposals it may overrule:

- `internal/subject` defines the subject grammar and the stream and consumer name grammar. Its rules are generated into [docs/generated/subject-rules.md](generated/subject-rules.md) from the tables the tests iterate, so the documented grammar cannot drift from the tested one.
- `internal/errs` defines the closed sentinel set. Every semantic outcome named in S13 is one of those sentinels.
- `internal/clock` defines which readings are wall-clock and which are monotonic, and therefore what a clock step does to a deadline. S7 states the consequence.

### S1.5 Clarifications register

Three entries, C1, C13 and C15, do not resolve an ambiguity. They **override** something PLAN.md states plainly, and each says what it overrides and what it keeps.

| ID | Ambiguity or override | Resolution | Ratified in |
|---|---|---|---|
| C1 | **Override.** PLAN.md asserts that a message id survives the dead-letter hop in four places. Section 2 D3, under a preamble reading "The decision is final", says provenance headers and "the **original `msg_id`/`trace_id` preserved** so `trace` shows one continuous story". Section 5.1's dead-lettering paragraph, under a heading marked "normative", says "the original `id` and `trace_id` are preserved". Section 4.2's column comment calls `messages.id` "ULID, stable across DLQ/replay lineage". Section 2 D10 rejects `stream:seq` as the only id because "seq is not stable across DLQ copies and replays", which reads as the ULID being stable there. Section 4.2 also declares `CREATE UNIQUE INDEX messages_id ON messages(id)` across every stream, and `GET /v1/messages/{id}` in section 7 needs that uniqueness to be answerable at all. The two cannot both hold while the origin row still exists. | This overrides the **letter** of all four and keeps D3's **purpose**. The dead-letter copy, a redrive and a replay each mint a new message id, so ids stay globally unique and the by-id lookup keeps working. One continuous story, which is what D3 was for, is carried by `trace_id`, by `Messq-Origin-Id`, and by both ids in the `detail` of `msg.dead`, `dlq.redrive` and the replay event. It becomes an acceptance criterion of #20: `messq trace` given **any** id in a lineage renders the whole story across every hop. S4.4 has the lineage table. | ADR-0004 (D3) |
| C2 | PLAN.md section 11.4 lists "clock jumps (monotonic deadlines unaffected)" as a fault-injection case without saying which deadlines are monotonic. Section 4.2 stores `deliveries.visible_at` as wall-clock milliseconds, so a durable ack deadline is not one of them. | The monotonic readings are the in-process waits: the sweeper tick, the long-poll park, the group-commit window and the `--exec` heartbeat. A step does not disturb those, which is what the fault case asserts. Durable deadlines are wall-clock, which is what `internal/clock` implements: a forward step makes in-flight deliveries look overdue and the sweeper redelivers them with `reason=timeout`. S7 is normative. | ADR-0006 (D5) |
| C3 | PLAN.md section 5.1 guards the **claim** on the bound: T2's guard carries `pending < max_ack_pending`, and the section 5 diagram repeats it on the claim arrow. Those two are the only occurrences in the plan; T1 carries no such guard. A claim creates no row, so by I5 the guard already holds whenever a READY row exists, and it can never decide anything. The strict comparison would also refuse the last claim once top-up has filled the pending set to the bound. | The bound is enforced where rows are created: T1 inserts only while `pending(c) < max_ack_pending`. T2 keeps the guard as an assertion, written `pending(c) <= max_ack_pending`; an implementation that finds it violated at claim has a bug in T1, not a flow-control decision to make. S5.3 defines `pending`. | ADR-0006 (D5) |
| C4 | PLAN.md section 5.1 bounds redelivery three times: T4's guard `attempts < max_deliver`, T5's trigger `attempts ≥ max_deliver`, and T8's guard `attempts < max_deliver`. The schema documents `max_deliver = 0` as unlimited. Read literally at that value, `attempts < 0` is never true and `0 >= 0` always is, so an unlimited consumer dead-letters on its first failure. | All three carry the term `max_deliver > 0`. S6.3 works the boundary through for `max_deliver` in {0, 1, 5}. | ADR-0007 (D6) |
| C5 | PLAN.md section 5.1 reserves the `Messq-` header prefix and PLAN.md section 7 accepts user headers as `Messq-Header-*`. The two rules overlap. | A user header is supplied as `Messq-Header-<key>` and stored under `<key>`. `<key>` MUST NOT itself begin with `Messq-`, case-insensitively. S3.4 is normative. | ADR-0004 (D3) |
| C6 | PLAN.md gives no bound for the ack token's length, and a parser without one is a denial-of-service surface. | The longest well-formed token is 171 bytes, derived in S3.3 from the name and integer bounds. A token longer than that MUST be rejected before parsing. | ADR-0008 (D7) |
| C7 | PLAN.md gives no clamp for `nak --delay`. | `delay_ms` is an integer in `[0, 86400000]`. Outside that range the settle is rejected with `errs.ErrBadRequest`. `delay_ms = 0` means immediately eligible. S8.3 is normative. | ADR-0006 (D5) |
| C8 | Detecting a wall-clock step would need an event name, and PLAN.md section 9.2's vocabulary is closed and has none. | A detected step is reported through the log and through `messq doctor`, never as an event row. Adding a `clock.*` event is a vocabulary change under S16. | ADR-0012 (D11) |
| C9 | PLAN.md section 5.1 requires a non-empty `backoff` array but does not say what enforces it. | An empty array is rejected at consumer create and update with `errs.ErrBadRequest`. A consumer whose stored array is empty, which only a hand-edited database can produce, schedules with a delay of 0 ms. S8.2 is normative. | ADR-0006 (D5) |
| C10 | PLAN.md section 5.1 caps T7 at "total extension ≤ `--max-ack-wait`" without saying what total extension means or where it is counted. | It is the deadline's distance from the claim: the guard is `(visible_at + ack_wait) - delivered_at <= max_ack_wait`, so one delivery attempt never holds a deadline more than `max_ack_wait` past `delivered_at`. The budget therefore includes the delivery's initial `ack_wait`, one `ack_wait` stricter than the literal phrase: at the default 30 s deadline and a 1 h cap it admits 119 extends, not 120. It needs no new column and is unaffected by a change to `ack_wait`. S6.1 T7 is normative. | ADR-0006 (D5) |
| C11 | PLAN.md section 5.1's T1 does not mention a paused consumer, and PLAN.md section 8 gives `messq consumer pause` no delivery semantics. | T1 also requires ¬`paused`, and a fetch against a paused consumer is `errs.ErrPaused`. The `paused` column and the sentinel are both merged, so the alternative is a pause verb that pauses nothing. | ADR-0006 (D5) |
| C12 | PLAN.md section 5.1's T11 guards a redrive on "operator, rate-limited" only. It defines neither a redrive counter nor a limit. | T11 also refuses a message already redriven three times unless `--force` is given. The count lives in `Messq-Redrive-Count` and the refusal is `errs.ErrConflict`. A redrive loop is the shortest path from one poison message to an outage. A1 carries the bound and #29 owns the flag. | ADR-0004 (D3) |
| C13 | **Override.** PLAN.md section 5.1 fixes the dead-letter header set as `Messq-Origin-Stream/-Seq/-Consumer`, `Messq-Attempts`, `Messq-Cause`, `Messq-Last-Reason`, `Messq-Dead-At`. It carries no origin id, because under its own reading the id did not change. | This overrides that set by adding `Messq-Origin-Id`. It rides on C1: once the copy mints a new id, the origin id has nowhere else to live on the message itself. S9.2 lists the full set. | ADR-0004 (D3) |
| C14 | PLAN.md section 4.2 types `attempts` and `generation` as SQLite `INTEGER`, which is 64 bits, while the ack-token grammar has to bound its fields before a parser can be safe. | The token's `attempt` and `generation` fields are bounded at 4294967295, which is what makes the 171-byte length of S3.3 derivable. `max_deliver` is validated at or below that bound at consumer create, and a consumer already at that generation refuses a further seek or purge. A1 carries both. | ADR-0008 (D7) |
| C15 | PLAN.md section 7 annotates the per-token `/v1/ack` result with the three-value shorthand `ok`, `stale`, `unknown` — an abbreviation that omits the two fenced outcomes the token grammar (S3.3) can produce. | The settle-result status is FROZEN at five values: `ok`, `stale`, `stale_ack`, `wrong_generation`, `unknown`. `ok` is a live mutation; `stale` is the idempotent success of the absent row (T3a); `stale_ack` is a token-attempt mismatch (T3b, `errs.ErrStaleAck`); `wrong_generation` is a token predating a `seek` or `purge` (T3b, `errs.ErrWrongGen`); `unknown` is a token that does not parse (S3.3 step 1, `errs.ErrUnknownToken`). Two adjudicated settle conflicts keep the merged spec normative, so neither needs a separate ADR: an extend past the total-extension cap returns `errs.ErrBadRequest` with the deadline unchanged (S6.1 T7, S13), and `nak --delay D` is not jittered (S8.3); the issue #10 body's `capped:true` and jittered-explicit-delay proposals are rejected. The delivery-dead seam's transition vocabulary belongs to `internal/queue`: `DeadCause` and `DeadCtx` are defined there by #10 and consumed by #12. | ADR-0008 (D7) |

## S2. Object model and vocabulary

### S2.1 Nouns

| Term | Definition |
|---|---|
| stream | A named, append-only sequence of messages, plus the publish subject patterns it accepts and its retention limits. One row in `streams`. |
| subject | A dot-separated label carried by a message, matched against consumer filters. Grammar in S3.1. |
| message | One published payload with its headers, its per-stream `seq`, its message id and its `trace_id`. One row in `messages`. |
| consumer | A durable cursor over one stream with its own filters, delivery policy and generation. One row in `consumers`. |
| delivery | The unfinished delivery state of one `(stream, consumer, seq)` triple. One row in `deliveries`, or no row. |
| attempt | One claim of a delivery by a consumer. `attempts` counts them and increments at claim (D6), so during attempt *n* the row reads `attempts = n`. |
| generation | A per-consumer counter, bumped by `seek` and by `purge`, that fences tokens minted before the reset. |
| lineage | The chain a payload travels: publish, dead-letter copy, redrive, replay. Each hop is a distinct message with its own id; `trace_id` is what makes them one story. S4.4. |
| event | One durable row in `events`, written in the same transaction as the state change it records. The audit trail, and the source of truth for `messq trace`. |
| trace | The verb that reads the event journal for one message id or one `trace_id`. |

### S2.2 The unit of state

State is per `(consumer, seq)`. "The message is acked" is not a statement this specification can evaluate: a message published to a stream with three consumers has three independent lifecycles, and one of them being resolved says nothing about the other two.

### S2.3 The single writer

Every state change is applied by exactly one writer goroutine, which owns the sole read-write database connection (PLAN.md section 3.2). All transitions are therefore totally ordered. There is no race between an `extend` and the sweeper, only an order: whichever command the writer applies first wins, and the second sees the result of the first.

A transition MUST therefore be specified, implemented and tested as a function of the state **at apply time**, never of the state a handler observed while parsing the request. A handler that reads state and then sends a command has read a hint, not a fact.

### S2.4 Event vocabulary

The vocabulary is closed. Renaming a member, adding one, or removing one is a breaking change under S16. Every event this document names is a member of this set, and this set is character-for-character the set in PLAN.md section 9.2.

```
server.start server.stop server.reload recovery.unclean recovery.reclaimed storage.fatal
stream.create stream.update stream.delete stream.purge retention.expire retention.blocked
consumer.create consumer.update consumer.delete consumer.seek consumer.pause consumer.lag
msg.publish msg.dup msg.deliver msg.ack msg.ack_dup msg.ack_stale
msg.nak msg.term msg.extend msg.timeout msg.dead
dlq.redrive flow.blocked disk.degraded auth.denied api.error admin.action
```

The events table is never sampled, so `messq trace` is always complete. Logs and metrics are projections of this vocabulary and MAY be sampled under the rules of PLAN.md section 9.3. The per-event field schema is out of scope here and lands in `docs/log-schema.md` (#19).

## S3. Names and grammars

### S3.1 Subjects

The subject grammar, the pattern grammar and the matcher's behaviour on arbitrary input are specified by grammar rules S1 through S11 in [docs/generated/subject-rules.md](generated/subject-rules.md), which is generated from the tables that `internal/subject`'s tests iterate. That document is normative for this one. Two consequences are restated here because the rest of this specification depends on them:

- A publish target is a literal subject: it holds no `*` and no `>` (grammar rule S10). A consumer filter is a pattern and MAY hold both.
- Matching does not re-validate. `Pattern.Match` treats structural nonsense as a non-match, so validating a subject is the publish boundary's job, done once, and not the top-up path's job, done once per candidate message. A caller that matches unvalidated input has skipped a step.

### S3.2 Stream and consumer names

Grammar rule S11 is normative. Restated for the two properties this document leans on:

- A name is 1 to 64 bytes of `[A-Za-z0-9_-]`, plus `.` for stream names only. A name never starts or ends with `.`, never holds `..` or `/`, and is never `.` or `..`.
- A **new** stream name is capped at 60 bytes, not 64, so that its derived `<stream>.dlq` name is itself a valid stream name. `internal/subject` exposes the two caps as two validators, and the distinction is load-bearing: `ValidateStreamName` accepts a name that already exists, including a 64-byte `.dlq` stream, while `ValidateNewStreamName` gates creation.

The `/` ban is what keeps the ack-token grammar of S3.3 unambiguous, and the ack-token grammar is why the `/` ban may not be relaxed. Both directions are stated so that neither can be changed alone.

### S3.3 Ack tokens

The ack token is plain, fenced and human-readable on purpose (D7): an operator reads the attempt number out of a log line without a tool. It carries no signature and no secret.

```abnf
ack-token   = stream "/" consumer "/" seq "/" attempt "/" generation
stream      = name                     ; grammar rule S11, 1*64 bytes, "." allowed
consumer    = name                     ; grammar rule S11, 1*64 bytes, no "."
seq         = 1*19DIGIT                ; 1 .. 9223372036854775807, no leading zeros
attempt     = 1*10DIGIT                ; 1 .. 4294967295, no leading zeros
generation  = 1*10DIGIT                ; 1 .. 4294967295, no leading zeros
```

`attempt` and `generation` are bounded at 4294967295 by this document, not by the schema, which types both columns as SQLite `INTEGER` and therefore as 64-bit (C14). The narrowing is what makes the token length derivable, and it is enforced upstream: `max_deliver` is validated at or below that bound at consumer create, and a consumer already at that generation refuses a further seek or purge. A1 carries both bounds.

The longest well-formed token is therefore **171 bytes**: `64 + 1 + 64 + 1 + 19 + 1 + 10 + 1 + 10`. A parser MUST reject an input longer than 171 bytes before it allocates or splits (C6). A parser MUST NOT panic on any input, MUST NOT allocate proportionally to a repeated separator, and MUST reject a token with a leading zero, an empty field, a sixth field or a missing field. The value is `errs.ErrUnknownToken` in every one of those cases.

Fencing is four checks in this order, and the order is normative because it decides which sentinel a caller sees:

1. Parse. Failure is `errs.ErrUnknownToken`.
2. Resolve `(stream, consumer)` to a live consumer row. Absent is `errs.ErrNotFound`, including when the consumer was deleted while a delivery was in flight. A deleted consumer is never an idempotent success.
3. Compare `token.generation` with `consumers.generation`. A mismatch is `errs.ErrWrongGen`, and the delivery row MUST NOT be touched.
4. Look up the delivery row. If it exists, compare `token.attempt` with `deliveries.attempts`. A mismatch is `errs.ErrStaleAck`, and the row MUST NOT be touched. If it does not exist, the settle is an idempotent success flagged stale (T3a).

The cost of the whole scheme after the lookup is two integer comparisons.

### S3.4 Reserved namespaces

- **The `.dlq` stream suffix is reserved.** A stream created by a user MUST NOT end in `.dlq`; the outcome is `errs.ErrBadRequest`. A `<stream>.dlq` stream is created by the daemon on the first dead-letter into it. There is no dead-letter queue of a dead-letter queue: a consumer on a `.dlq` stream defaults to `dead_policy = drop`, and the daemon MUST NOT create `<stream>.dlq.dlq`.
- **The `Messq-` header prefix is reserved.** User headers are supplied as `Messq-Header-<key>` and stored under `<key>`. A `<key>` that itself begins with `Messq-`, compared case-insensitively, is rejected with `errs.ErrBadRequest` rather than silently dropped (C5). Reserved names in use are listed in S4.4 and S9.2.
- **`Messq-Msg-Id`, `Messq-Subject` and `Messq-Trace-Id` are request headers**, not user headers, and never appear in the stored header object.

## S4. Identity, sequencing and lineage

### S4.1 Message id

A message id is a ULID (D10): 26 Crockford base32 characters, time-sortable, free of ambiguous characters, with its mint millisecond embedded. It is globally unique across every stream, which is what makes `GET /v1/messages/{id}` and `messq trace <id>` unambiguous, and it is enforced by `UNIQUE INDEX messages_id ON messages(id)`.

Minting never fails, and two policies make that true (`internal/id`). If the wall clock moves backwards, the millisecond handed out does not decrease, so ids stay monotonic and the regression is reported through a hook rather than through an error. If the 2^80 bits of entropy inside one millisecond are exhausted, the generator steps to the next millisecond and draws again. Publish MUST NOT be able to fail for id reasons.

The ordering contract is per generator: for one generator, if `New` returns A before it returns B, then A sorts before B. Across generators there is no ordering guarantee, and the daemon has exactly one.

Parsing is case-insensitive, because a user reads an id off a screenshot and types it back. Rendering is always upper case.

### S4.2 Stream sequence

`seq` is per stream, starts at 1, and is monotonically increasing. It is allocated from `stream_seq.next` and is never reused, including across a purge: purging seqs 1 to 100 does not let seq 50 be handed out again. `first_seq` is the lowest seq still present in the stream and moves forward as retention and purge delete messages.

The RECOMMENDED consumer-side deduplication key is `stream/seq`, which is stable across every redelivery of the same message to the same consumer. It is not stable across a dead-letter copy, a redrive or a replay, because each of those is a new message in the sense of S4.4.

### S4.3 Trace id

A `trace_id` is the 16-byte W3C Trace Context identifier rendered as 32 lower-case hex characters. It is taken from `Messq-Trace-Id`, or parsed out of a W3C `traceparent`, or minted at publish. Parsing a `traceparent` never fails: an unparsable header means "mint a fresh trace id", because a malformed upstream header MUST NOT fail a publish.

The trace id is stored on the message, echoed on every delivery, carried into the dead-letter copy, into a redrive and into a replay, and stamped on every event row. It is the identifier that makes lineage one story.

### S4.4 Lineage

Each hop creates a new message with a new id and a new seq in its destination stream. The trace id is preserved, and provenance headers plus event `detail` fields carry the origin id, so `messq trace` walks the chain in both directions through the `events_msg` and `events_trace` indexes without a table scan (C1).

| Operation | Message id | `seq` | `trace_id` | Provenance headers on the new message |
|---|---|---|---|---|
| publish | new ULID | next in the target stream | from the request, or minted | none |
| redelivery | unchanged | unchanged | unchanged | none: no new message exists |
| dead-letter into `<s>.dlq` | **new ULID** | next in `<s>.dlq` | preserved | `Messq-Origin-Id`, `Messq-Origin-Stream`, `Messq-Origin-Seq`, `Messq-Origin-Consumer`, `Messq-Attempts`, `Messq-Cause`, `Messq-Last-Reason`, `Messq-Dead-At` |
| redrive (#29) | **new ULID** | next in the origin stream | preserved | `Messq-Redrive-Of`, `Messq-Redrive-Count`, and the origin headers of the dead-letter copy |
| replay (#28) | **new ULID** | next in the target stream | preserved | `Messq-Replay-Of` |

The `msg.dead`, `dlq.redrive` and replay events MUST carry both the origin id and the new id in `detail`, so the chain is walkable from either end even after the origin message is deleted by retention.

## S5. Delivery state model

### S5.1 States as predicates

Every state is a predicate over the schema of PLAN.md section 4.2, so that there is no gap between "the specification" and "the rows". Let `c` be a consumer row and `(s, c.name, seq)` the triple in question.

| State | Predicate | Notes |
|---|---|---|
| `UNSEEN` | no `deliveries` row for `(s, c, seq)` **and** `seq >= c.cursor_seq` | Implicit. Costs no storage, which is why a stream with a hundred million messages and a fresh consumer costs one row. |
| `READY` | a `deliveries` row exists with `state = 0` | Deliverable if and only if `now >= visible_at`. A READY row whose `visible_at` is in the future is scheduled, not deliverable. |
| `INFLIGHT` | a `deliveries` row exists with `state = 1` | `visible_at` is the ack deadline. `delivered_at` is when the current attempt was claimed. |
| `RESOLVED` | no `deliveries` row for `(s, c, seq)` **and** `seq < c.cursor_seq` | Terminal. |

`RESOLVED` is the load-bearing row. messq deliberately keeps no per-message terminal status column: **the terminal state is the absence of a row, and the reason lives in `events`.** Acked, termed, dead-lettered, dropped by `dead_policy = drop` and filtered out at top-up are indistinguishable at rest, and are distinguished only by the event journal. This is D5 and D11 in one sentence, and a reader who has not understood it cannot understand `messq trace`.

The pending set therefore *is* the `deliveries` table. It stays small by construction, every "what is outstanding" query is fast regardless of stream size, and there is no separate pending-entries list that can leak.

### S5.2 States are exhaustive

For a given consumer `c` and a seq in `[first_seq, c.cursor_seq)`, exactly one of RESOLVED, READY or INFLIGHT holds. For a seq at or above `c.cursor_seq` with no row, the state is UNSEEN. This is invariant I2.

A row may exist for a seq at or above `cursor_seq` only transiently inside the writer's transaction, never at a quiescent point, because T1 inserts the row and advances the cursor in the same transaction.

### S5.3 Derived quantities

These four formulas are defined once, here, because `consumer info` (#9), the metrics of PLAN.md section 9.4, the `lag` command and every shipped alert have to produce the same number.

```
pending(c)            = count(deliveries where stream = c.stream and consumer = c.name)
inflight(c)           = count(deliveries where stream = c.stream and consumer = c.name and state = 1)
backlog(c)            = pending(c) + count(messages where stream = c.stream
                                             and seq >= c.cursor_seq
                                             and subject matches any of c.filters)
oldest_pending_age(c) = now - min(messages.published_at over the rows counted by pending(c))
                        and 0 when pending(c) = 0
```

`pending(c)` counts READY and INFLIGHT rows together. `max_ack_pending` bounds `pending(c)`, not `inflight(c)`, which is what invariant I5 states, and it is therefore enforced at top-up (T1) rather than only at claim (T2). See C3.

`oldest_pending_age` is measured from `messages.published_at`, not from `delivered_at`, because it is the user-facing service-level indicator: "how long has the oldest unfinished piece of work been waiting", not "how long has the current attempt been running".

## S6. Transition rules

### S6.1 The table

This table carries the transitions of PLAN.md section 5.1, one row to one row, with one column PLAN.md does not have: the outcome when the guard fails. The defining predicate of each state is not repeated per row; it is in S5.1, once.

What "mirrors" means here is stated exactly, so that no reader trusts more of it than is true. `internal/docsguard` asserts three things against PLAN.md section 5.1: the transition IDs match in the same order; the emitted-event cells match name for name; and **no symbol PLAN.md names has been dropped**. A configuration name, a column name, a flag, a reserved header or a relational operator that appears in a PLAN.md Trigger, Guard or Effect cell MUST appear in the corresponding cell below, or be quoted in the S1.5 entry that explains its removal. That is what makes a silently flipped comparison or a quietly deleted bound a build failure.

Two things are left to code review, named here so that neither is mistaken for a guarantee. Symbols this table **adds** are not machine-checked, because no tractable grammar separates a new constraint from a rewording; every addition is registered in S1.5 by hand, C3 for T1's and T2's flow-control terms, C4 for the `max_deliver > 0` term on T4, T5 and T8, C10 for T7's cap, C11 for T1's `paused` term, C12 for T11's redrive counter. And once a register entry quotes a symbol for a transition, that symbol stays excused for that transition, so a further change to an **already overridden** term is a review failure rather than a build failure. Everything else PLAN.md names is a build failure, and S1.4's rule that nothing is resolved silently is what review enforces on the rest.

The conformance suite (#13) mirrors this table in the other direction: every ID below MUST appear in at least one test name.

| ID | From | Trigger | Guard | Effect | Event | On guard failure |
|---|---|---|---|---|---|---|
| T1 | UNSEEN | fetch top-up | filter matches ∧ `seq >= cursor_seq` ∧ `pending(c) < max_ack_pending` ∧ ¬`paused` | insert a READY row with `attempts = 0`, `visible_at = 0`, `generation = c.generation`; advance `cursor_seq` past every scanned seq including non-matching ones | none | `pending(c)` at the bound: no insert, `flow.blocked`, the fetch returns the batch it has and reports `errs.ErrFlowControl` as its hold reason; `paused`: `errs.ErrPaused` |
| T2 | READY | fetch claim | `now >= visible_at` ∧ `pending(c) <= max_ack_pending` | `attempts++`, `state = INFLIGHT`, `visible_at = now + ack_wait`, `delivered_at = now`, mint the token; committed before the response | `msg.deliver` | `now < visible_at`: the row is skipped, which is not an error and produces no event; the pending assertion is C3 |
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

### S6.2 Normative notes

**Apply-time semantics.** Every guard above is evaluated against the state at apply time inside the writer's transaction (S2.3). A guard evaluated in a handler is a hint.

**T1 advances the cursor past non-matches.** A narrow filter is therefore amortised O(1) per message rather than O(stream) per fetch. The cursor advance is irreversible within a generation (I6), and the consequence MUST be documented for users: **changing a consumer's filters does not retroactively deliver messages the old filter skipped.** `seek` is the tool for that, and it bumps the generation (T10).

**Configuration changes apply forward only.** A new `ack_wait` applies to the next claim; deadlines already in flight are unchanged. A new `backoff` array applies to the next scheduling decision. A new `max_ack_pending` applies at the next top-up, so lowering it below the current `pending(c)` does not delete rows and does not violate I5 going forward; it stops new inserts until `pending(c)` drops below the new bound.

**T5 has no state of its own.** DEAD is an outcome, not a resting state: the delivery row is gone afterwards, and what distinguishes DEAD from ACKED at rest is the `msg.dead` event (S5.1).

**T10 covers `purge` as well as `seek`.** PLAN.md section 5.1 names only `seek` in the trigger column; both operations bump the generation and drop delivery rows, and a purge that left tokens valid would let a stale ack delete a row belonging to a message that no longer exists.

**T2 mints the token after the increment.** The token carries the post-increment attempt, so the token handed to the worker for attempt *n* reads `n`, and the fence in S3.3 compares equal for exactly that attempt.

**T7 adds to the deadline; it does not reset it.** An extend moves `visible_at` forward by one `ack_wait` from where it already stands, and the guard bounds the deadline's distance from `delivered_at` (C10). A worker heart-beating at `ack_wait/2`, which is what `messq sub --exec` does, therefore gains ground on its deadline until the cap stops it, and a worker that dies mid-attempt leaves the message invisible until that deadline rather than for ever.

### S6.3 Attempt counting worked through

`attempts` increments at claim, in the transaction that claims the row, committed before the fetch response is sent (D6). During attempt *n* the row reads `attempts = n`. Restart recovery does not re-increment (T9), because the increment already happened and the delivery already counted.

`max_deliver = 5`, every attempt failing:

| Event | `attempts` after | T5 guard `attempts >= 5` | Outcome |
|---|---|---|---|
| claim 1 | 1 | false | delivered, handler invocation 1 |
| failure of attempt 1 | 1 | false | T4 or T8, back to READY |
| claim 2 | 2 | false | handler invocation 2 |
| failure of attempt 2 | 2 | false | back to READY |
| claim 3 | 3 | false | handler invocation 3 |
| failure of attempt 3 | 3 | false | back to READY |
| claim 4 | 4 | false | handler invocation 4 |
| failure of attempt 4 | 4 | false | back to READY |
| claim 5 | 5 | false at claim | handler invocation 5 |
| failure of attempt 5 | 5 | **true** | T5, dead-letter |

**Exactly five handler invocations.** `max_deliver = n` means exactly *n* invocations for a message that never succeeds.

`max_deliver = 1`: one claim, `attempts = 1`, and its first failure matches `attempts >= 1`, so the message dead-letters after exactly one handler invocation. There is no retry, and the backoff array is never consulted.

`max_deliver = 0`: unlimited. The T5 guard is `max_deliver > 0 AND attempts >= max_deliver` (C4), so it is never true and the message never dead-letters. The consumer create and update paths MUST warn, because an unlimited consumer with a poison message retries it until retention deletes it.

`max_ack_pending = 1`: T1 inserts at most one row, so delivery is serial per consumer. The next top-up happens only after the outstanding delivery resolves.

## S7. Time, deadlines and clocks

### S7.1 Which readings are wall-clock

Durable deadlines and durable timestamps are wall-clock Unix milliseconds: `deliveries.visible_at`, `messages.published_at` and `events.ts`. They have to survive a restart and be readable in SQL, and a monotonic reading is meaningless after the process that took it exits.

In-process waits are monotonic: the sweeper's 250 ms tick, the long-poll park, the group-commit window and the `--exec` heartbeat. They are measured with a monotonic reading and are unaffected by an NTP step. This is the guarantee PLAN.md section 11.4 asks for, and it is the only part of it that a durable deadline can keep.

### S7.2 What a clock step does

A **forward** wall-clock step makes in-flight deliveries look overdue, and the sweeper redelivers them. That is correct at-least-once behaviour, not a defect: every such redelivery is a T8 with `last_reason = "timeout"` and a `msg.timeout` event, so it has a named cause and satisfies I9. The sweeper's per-tick expiry cap keeps the resulting burst from becoming a stampede.

A **backward** wall-clock step delays expiry. It never resurrects a resolved delivery, because a resolved delivery has no row (S5.1) and the sweeper only ever reads rows.

There is no re-anchoring and no derived clock. See C2 for the divergence from PLAN.md section 11.4's reading, and ADR-0006 for the reasoning.

A detected step is reported through the log and through `messq doctor`, never as an event row: the vocabulary of S2.4 is closed and has no clock event (C8). The ULID generator's own clock-regression hook is the detection point already merged.

### S7.3 Sweeper granularity

The sweeper runs every 250 ms. Consequently:

- A deadline MAY fire up to one tick late. An operator who sets `ack_wait = 30s` MUST expect expiry between 30.000 s and 30.250 s plus the queueing delay behind the writer.
- A deadline MUST NOT fire early. The sweeper's predicate is `now > visible_at`, strictly greater, so a delivery is never expired in the millisecond it becomes due.
- Sweeper work runs as commands through the writer (S2.3), so an expiry is ordered against every other transition rather than racing it.

## S8. Backoff and jitter

### S8.1 The algorithm

```
backoff(attempts):                       # attempts is the attempt that just failed, >= 1
    b = consumer.backoff_ms              # non-empty array, validated at consumer create
    i = min(attempts - 1, len(b) - 1)    # the last value repeats
    d = b[i]
    return jitter(d)                     # uniform in [0.8*d, 1.2*d)
```

The default array is `[1000, 5000, 30000, 120000, 600000]` milliseconds, which is `[1s, 5s, 30s, 2m, 10m]`.

### S8.2 Jitter

Jitter is ±20 %, uniform, always applied, and not configurable off. A synchronised retry wave from a recovering downstream service is a self-inflicted second outage, and the operator who would switch jitter off is exactly the operator about to cause one.

This specification constrains the **range** and not the source of randomness. An implementation and the reference model may draw differently; the property test asserts `0.8*d <= delay < 1.2*d`, never a particular value.

An empty `backoff_ms` array is rejected at consumer create and update with `errs.ErrBadRequest`. A stored empty array, which only a hand-edited database produces, schedules with a delay of 0 ms rather than crashing (C9).

### S8.3 Explicit delay

`nak --delay D` overrides the schedule for that attempt and is **not** jittered: the handler knows something the schedule does not, and a stampede from an explicit delay is the caller's choice. It does not change `attempts` and does not consume a backoff slot; the next failure uses `backoff(attempts)` for the then-current `attempts`.

`delay_ms` is an integer in `[0, 86400000]`, which is 24 hours. Outside that range the settle is rejected with `errs.ErrBadRequest` (C7). `delay_ms = 0` means immediately eligible: the row becomes READY with `visible_at = now`.

### S8.4 Retry horizon, worked

The retry horizon is what `messq consumer add` prints (#24), so it is specified here rather than computed twice.

Default array, `max_deliver = 5`, `ack_wait = 30s`, every attempt failing by **timeout**, jitter ignored:

| Attempt | Claimed at | Deadline | Expires at | Backoff after | Next claim |
|---|---|---|---|---|---|
| 1 | 0 s | 30 s | 30 s | 1 s | 31 s |
| 2 | 31 s | 61 s | 61 s | 5 s | 66 s |
| 3 | 66 s | 96 s | 96 s | 30 s | 126 s |
| 4 | 126 s | 156 s | 156 s | 120 s | 276 s |
| 5 | 276 s | 306 s | 306 s | none: T5 | dead-letter at 306 s |

**306 s, which is 5 minutes 6 seconds.** Four backoff delays are consumed, not five: five attempts have four gaps. Jitter applies to the four delays, whose nominal sum is 156 s, so the horizon spans `[274.8 s, 337.2 s]`.

Same array and bound, every attempt failing by an **immediate nak** with no explicit delay: the in-flight time is nil, so the horizon is the sum of the four delays alone, 156 s, which is 2 minutes 36 seconds, spanning `[124.8 s, 187.2 s]`.

## S9. Termination and dead-lettering

### S9.1 The two policies

`dead_policy` is per consumer. `dlq`, the default, copies the payload into `<stream>.dlq`. `drop` deletes the delivery row and writes the event only, which is the right choice for a `.dlq` stream's own consumer and for a consumer whose payloads are reproducible.

A consumer created on a stream whose name ends in `.dlq` defaults to `dead_policy = drop` (S3.4). Setting `dead_policy = dlq` on such a consumer is rejected with `errs.ErrBadRequest`, because it would create `<stream>.dlq.dlq`.

### S9.2 The dead-letter procedure

Dead-lettering is **one transaction**. Partial dead-lettering does not exist, which is D1's decisive argument made concrete (D3): a copy without a deletion double-delivers, and a deletion without a copy loses the payload.

Under `dead_policy = dlq`, in one transaction:

1. Create `<stream>.dlq` if it does not exist, with the origin stream's `max_msg_size` and the default retention limits. Emit `stream.create`.
2. Insert the payload into `<stream>.dlq` under the **original subject**, with a new message id and the next seq of the dead-letter stream, and the origin's `trace_id` preserved (S4.4).
3. Set the provenance headers on the copy: `Messq-Origin-Id`, `Messq-Origin-Stream`, `Messq-Origin-Seq`, `Messq-Origin-Consumer`, `Messq-Attempts`, `Messq-Cause` which is `max_deliver` or `terminated`, `Messq-Last-Reason`, `Messq-Dead-At`.
4. Delete the `deliveries` row for the origin triple.
5. Write `msg.dead` carrying the origin id, the new id, the consumer, the attempt count and the cause.

Under `dead_policy = drop`, steps 1 to 3 are skipped and `msg.dead` records `detail.dropped = true`.

The dead-letter copy is an ordinary message. It is inspected with `peek`, its depth is an ordinary stream metric, its retention is ordinary stream retention, and redriving it is consume plus republish. One mechanism buys four features, which is why the DLQ is a stream and not a terminal flag on a row.

### S9.3 Term

`term` is the handler saying "this will never succeed". It skips the remaining attempts and dead-letters immediately with `Messq-Cause: terminated` (T6). It is fenced exactly like `ack` (S3.3): a stale `term` MUST NOT terminate a live redelivery.

## S10. Publish deduplication

A publish carrying `Messq-Msg-Id: <key>` is deduplicated within the target stream's `dedup_window_ms`, which defaults to 120000 ms. The insert is `INSERT ... ON CONFLICT DO NOTHING` against `UNIQUE INDEX messages_dedup ON messages(stream, dedup_key) WHERE dedup_key IS NOT NULL`. On conflict the publish returns the **original** `{seq, id}` with `duplicate: true` and writes a `msg.dup` event. The response is a success, not an error: a publisher retrying after a timeout is the case this exists for.

The janitor clears `dedup_key` to `NULL` after `dedup_window_ms`, which is what keeps the partial index bounded.

Three properties MUST be documented plainly, because each is a place where a reader's assumption is stronger than the guarantee:

- **The window is per stream, not global.** The same key published to two streams is two messages.
- **Reuse of a key after the window is a new message, not a duplicate.** This is the one place where messq's deduplication is weaker than a reader might assume, and it is the price of a bounded index.
- **Deduplication is publish-side only.** It does not deduplicate deliveries. The recommended consumer-side key is `stream/seq` (S4.2), not `msg_id`.

## S11. Durability contract

### S11.1 What a 2xx means

| Verb | `--durability=full` | `--durability=relaxed` |
|---|---|---|
| publish | The message row and its `msg.publish` event are committed and fsynced. It survives a power cut. | Committed to the WAL without fsync. It survives SIGKILL of the daemon. The last commits may be lost on power loss or kernel panic. |
| fetch (T2) | The `attempts` increment and the `msg.deliver` event are committed and fsynced before the response. | Committed before the response, not fsynced. |
| ack | The DELETE and the `msg.ack` event are committed and fsynced. **An ack response is a durability promise.** | Committed, not fsynced. |
| nak, term, extend | Committed and fsynced. | Committed, not fsynced. |
| seek, purge, stream and consumer admin | Committed and fsynced. | Committed, not fsynced. |
| peek, trace, list, lag, metrics | No durability claim: they are reads. | The same. |

Under `full`, an ack is durable before its response at no extra fsync cost, because acks ride the same group commit as publishes (D4). There is therefore **no documented duplicate window from ack loss inside the server**; a duplicate from a lost ack is a duplicate from a lost *response*, on the network or in the client, which S14 covers.

`relaxed` never corrupts the database. It trades the last commits for throughput, and nothing else. It logs a WARN banner at startup and `messq doctor` flags it.

### S11.2 Group commit

The writer accumulates commands for up to 2 ms or 256 commands, whichever comes first, and commits once, so N concurrent operations share one fsync. Batching is an implementation detail of the durability mode and not a third mode: there is no per-call strictness flag, because `full` already means fsynced before the response.

### S11.3 fsync failure is fatal

An `EIO` or `ENOSPC` from a commit MUST latch the process read-only, write `storage.fatal` at ERROR, refuse further writes with `errs.ErrReadOnly`, and exit non-zero after a short drain. The fsync MUST NOT be retried: a failed fsync may have already discarded the dirty page, so a retry that succeeds proves nothing. Recovery is a restart, which re-derives the truth from disk.

Disk pressure is a different state and a softer one. Below `--min-free-bytes` publishes are rejected with `errs.ErrDiskFull` while **acks, naks, terms and dead-letter writes continue**, because a wedged ack path becomes mass redelivery and turns a disk problem into an outage. `/readyz` is not tied to disk pressure.

## S12. Recovery

On `messq serve` start, before any listener opens:

1. Open the database. SQLite replays or rolls back the WAL; messq has no bespoke replay code, which is D1's third reason. If `meta.clean_shutdown` is absent, log `recovery.unclean` at WARN and run `PRAGMA quick_check`, or a full `integrity_check` under `--fsck`.
2. Apply migrations. A data directory whose schema is newer than the binary MUST be refused with `errs.ErrSchemaNewer`.
3. Reclaim leases (T9): one indexed UPDATE flips every INFLIGHT row to READY with `visible_at = now + jitter(0, 1s)` and `last_reason = "broker_restart"`. `attempts` is **not** changed: the in-flight delivery already counted, and re-incrementing would spend a worker's attempts on a broker restart. Emit `recovery.reclaimed` with the count.
4. Trim expired dedup keys and stale events, then write `clean_shutdown = 0`.
5. Emit `server.start` with the version, the durability mode, the database size and the per-stream counts.

The jitter in step 3 exists so that a restart with ten thousand in-flight deliveries does not deliver them all in the same millisecond.

Graceful shutdown on SIGTERM stops accepting, releases parked long-polls, drains handlers for up to 10 s, makes a final commit, checkpoints the WAL with `TRUNCATE`, sets `clean_shutdown` and exits 0. **The drain is an optimisation and never a correctness requirement.** `kill -9` is always safe, and the crash suite asserts it.

## S13. Error outcomes

A semantic outcome is one of the sentinels of `internal/errs`. The set is closed. The HTTP status mapping (#14, frozen in #35) and the CLI exit-code mapping (#14) both iterate this set, and the message column below is what `internal/docsguard` compares against the merged sentinel set, so a sentinel added without a row here fails CI.

| Sentinel | Message | Raised by |
|---|---|---|
| `errs.ErrNotFound` | not found | A named stream, consumer or message that does not exist, including a settle whose consumer was deleted (S3.3 step 2). |
| `errs.ErrConflict` | already exists | A create that collides with an existing name, and a redrive past the redrive-count guard (T11). |
| `errs.ErrBadRequest` | invalid request | A malformed or out-of-range argument: a `delay_ms` outside `[0, 86400000]` (S8.3), an empty `backoff_ms` (S8.2), an extend past `max_ack_wait` (T7), a user header inside the reserved prefix (S3.4), a user stream name ending in `.dlq` (S3.4), an unconfirmed destructive action (T10). |
| `errs.ErrBadSubject` | subject is not valid or not accepted by this stream | A subject or pattern the grammar rejects, or one the target stream's patterns do not accept. |
| `errs.ErrTooLarge` | message exceeds max_msg_size | A body above the stream's `max_msg_size`. |
| `errs.ErrStreamFull` | stream is at its limit and discard=new | A publish into a stream at its limit with `discard = new`. |
| `errs.ErrFlowControl` | max_ack_pending reached | A fetch whose top-up is held by the `pending(c)` bound (T1). |
| `errs.ErrStaleAck` | stale ack: the message was already redelivered | A settle whose token attempt does not match the live row (T3b). |
| `errs.ErrUnknownToken` | unknown or malformed ack token | A token that does not parse, is over 171 bytes, or names nothing (S3.3 step 1). |
| `errs.ErrWrongGen` | token generation is stale; the consumer was reset | A settle whose token predates a `seek` or a `purge` (S3.3 step 3). |
| `errs.ErrPaused` | consumer is paused | A fetch against a paused consumer (T1). |
| `errs.ErrDiskFull` | insufficient free disk space | A publish below `--min-free-bytes` (S11.3). Settles are not rejected for this reason. |
| `errs.ErrReadOnly` | storage is latched read-only | Any write after a commit fault latched the process (S11.3). |
| `errs.ErrShuttingDown` | shutting down | A request that arrived during the graceful drain (S12). |
| `errs.ErrUnauthorized` | authentication required | A request with no credentials where credentials are required. |
| `errs.ErrForbidden` | not permitted for this token | A request whose token role or stream scope does not cover the operation. |
| `errs.ErrLocked` | data directory is locked by another process | A second `messq serve` against a data directory held under `flock`. |
| `errs.ErrSchemaNewer` | data directory schema is newer than this binary | A data directory written by a newer binary (S12 step 2). |
| `errs.ErrUnavailable` | daemon unreachable | A client-side failure to reach the daemon. It is never produced by the daemon. |

A guard failure that this specification calls "not an error" produces no sentinel and no event: T2 skipping a row whose `visible_at` is in the future is scheduling, not failure.

## S14. Guarantees, non-guarantees and the duplicate contract

### S14.1 The guarantee, as published

> messq delivers each message to each consumer **at least once**. Duplicates are possible whenever a worker completes work but its ack does not reach the server: after an ack timeout, a nak, a network failure, or a broker restart. **Consumers must be idempotent.** messq helps: `Messq-Msg-Id` deduplicates publishes within a window; every delivery carries its attempt number; a stale ack is rejected and reported rather than silently accepted; and every redelivery records its cause, so `messq trace` explains every duplicate. messq does not offer exactly-once and will not pretend to.

### S14.2 Ordering

Within a consumer, READY messages are claimed in ascending `seq`. With more than one message in flight, *processing* order is not guaranteed, because processing happens in the worker and messq does not control it. `ordered = subject` (phase 2, #38) gives per-subject serial processing at the documented cost of head-of-line blocking.

### S14.3 Not guaranteed

Exactly-once delivery. Global cross-subject ordering. Delivery of a message that retention already expired. Survival of the disk. Delivery to a consumer that does not exist yet, unless it seeks to `start`. Ordering of *processing* under concurrency. Any behaviour of a `.dlq` stream that the origin stream does not have, since it is an ordinary stream.

### S14.4 The duplicate contract

Every duplicate has a named cause, or it is a bug. This is invariant I9, and the cause set is closed:

| Cause | Recorded as | Transition |
|---|---|---|
| the ack deadline passed | `msg.timeout`, `last_reason = "timeout"` | T8 |
| the worker naked | `msg.nak`, `last_reason` from the caller | T4 |
| the broker restarted while the delivery was in flight | `recovery.reclaimed`, `last_reason = "broker_restart"` | T9 |

Any `attempts > 1` for a `(consumer, seq)` MUST be preceded in the event journal by one of those three events for that same pair. An `attempts` value that rose without one of them is a defect, and the rapid invariant hook (#13) is where it is caught.

A duplicate caused by a lost *response* is not a duplicate messq can see: the server acked, the client did not learn that it had, and the client retried. The recommended consumer-side deduplication key `stream/seq` (S4.2) covers it, with the canonical `INSERT ... ON CONFLICT DO NOTHING` in the same transaction as the work.

## S15. Invariant register

Each invariant has a stable ID. **The Statement column is PLAN.md section 5.2 verbatim**, and `internal/docsguard` compares it character for character, so a restatement, a swapped pair of rows or a quietly weakened wording is a build failure. The other three columns are this document's. "Checked by" names the mechanism; "First required at" names the issue whose merge must leave the invariant enforced. `messq verify` (#8), the rapid invariant hook (#13), test names, log messages and `docs/guarantees.md` (#35) all cite these IDs, so renaming one is a cross-repository rename and a breaking change under S16.

| ID | Statement | Predicate | Checked by | First required at |
|---|---|---|---|---|
| I1 | Every publish that returned 2xx under `full` durability is present after any crash (no acknowledged loss). | external three-valued ledger reconciliation: OK must exist, FAILED must not, UNKNOWN either | crash harness, `verify` | #8 |
| I2 | Every `(consumer, seq)` in `[first_seq, cursor)` is in exactly one of: resolved (below-floor/absent), READY, INFLIGHT. | S5.1's predicates are pairwise disjoint and cover the range | `verify`, rapid hook | #9 |
| I3 | An acked-and-committed `(consumer, seq)` is never redelivered except via explicit seek/replay/redrive. | history predicate over `events`: no `msg.deliver` for a pair follows its `msg.ack` without an intervening `consumer.seek` or a new message id | rapid hook, golden log test | #10 |
| I4 | `attempts ≤ max_deliver` for every non-terminal row; delivery stops at the bound, across restarts. | SQL predicate over `deliveries` joined to `consumers`, skipped where `max_deliver = 0` | `verify`, rapid hook | #11 |
| I5 | `count(deliveries WHERE consumer=c) ≤ max_ack_pending(c)` always. | SQL aggregate per consumer | `verify`, rapid hook | #9 |
| I6 | `cursor_seq` is monotone non-decreasing within a generation. | comparison across successive observations of the same generation | rapid hook | #9 |
| I7 | No stale-fenced ack/nak/term ever mutates a live row. | for every `msg.ack_stale` or `errs.ErrWrongGen` outcome, the row's `attempts`, `state` and `visible_at` are unchanged across the settle | unit tests, rapid hook | #10 |
| I8 | Each `(consumer, seq)` enters DEAD at most once, and each DEAD has exactly one DLQ message (when `dead_policy=dlq`). | count of `msg.dead` per pair is at most 1, and equals the count of `<s>.dlq` messages carrying that `Messq-Origin-Seq` and `Messq-Origin-Consumer` | `verify`, rapid hook | #12 |
| I9 | In a run with no faults where every delivery is acked within `ack_wait`, every message is delivered exactly once per consumer; any `attempts > 1` is preceded in the event log by a `msg.nak`, `msg.timeout`, or `recovery.reclaimed` for that pair (**every duplicate has a named cause**). | history predicate over `events`, cause set closed by S14.4 | rapid hook, golden log test | #13 |
| I10 | Folding the events table from the beginning reproduces the persisted state (log ≡ state; checked by `messq verify --deep`). | `verify --deep` replays the journal into a shadow state and diffs it against the tables | `verify --deep`, soak | #8 |
| I11 | No unbounded queue or collection exists anywhere; every bound is config-derived. | appendix A1 lists every bounded resource with its flag and default; there is no runtime predicate | A1 completeness test, code review | #6 |

I12 and above are **reserved** for phase 2 and are unspecified in v1. Delayed delivery (#37), ordered-by-subject consumers (#38) and per-consumer rate limiting (#39) each ship with a new invariant, and each takes the next free ID at that time. No v1 implementation may claim an ID at or above I12.

## S16. Conformance and change control

### S16.1 Conformance

An implementation conforms when all of the following hold:

- Every transition of S6.1 is implemented with the stated guard, effect, event and guard-failure outcome, and each transition ID appears in at least one test name (#13).
- Every invariant of S15 holds at every quiescent point, where quiescent means the writer has no command in flight.
- Every semantic outcome is one of the sentinels of S13, and no other sentinel exists.
- Every emitted event name is a member of S2.4.
- Every bound of A1 exists, is enforced, and is reachable from configuration.

### S16.2 Divergence

If the implementation and this document disagree, **the specification is the bug report**. Fix one or the other in the same pull request. Never neither, and never by weakening a test.

### S16.3 What is a breaking change after v1.0

The document carries `Spec-Version: 1.0-draft` until #36 tags v1.0 and flips it to `1.0`. After that tag, each of the following is a breaking change to the compatibility promise, and needs a superseding ADR and a row in A3:

- Weakening a MUST to a SHOULD or a MAY.
- Renaming or renumbering a transition ID, an invariant ID or a section number.
- Adding a state to S5.1, or changing a state's predicate.
- Renaming, adding or removing an event name in S2.4.
- Renaming or removing a sentinel in S13, or changing which sentinel a transition's guard failure produces.
- Changing the ack-token grammar of S3.3, or the name grammar it depends on.
- Changing what a 2xx means in S11.1.

Adding a MAY, adding a clarification to S1.5, reserving a new ID, and tightening prose without changing behaviour are not breaking changes.

## A1. Bounds register

The concrete form of invariant I11. Every bounded resource in the design appears here with the flag or column that sets it and its default. A bound without a row is a bug in this appendix, and the issue that introduces a bound owns its final flag name and updates this table.

| Bound | Set by | Default | Owner issue |
|---|---|---|---|
| writer command channel | `--cmd-queue` | 1024 | #6 |
| group-commit batch | `--commit-max-batch` | 256 | #7 |
| group-commit window | `--commit-window` | 2ms | #7 |
| parked long-poll waiters | `--max-waiters` | 4096 | #9 |
| per-follower event ring | `--event-follow-buffer` | 1024 | #19 |
| sweeper expiries per tick | `--sweep-max-batch` | 1024 | #11 |
| total extension per delivery | `--max-ack-wait` | 1h | #10 |
| explicit nak delay | none: a fixed clamp | `[0, 86400000]` ms | #10 |
| request body | `streams.max_msg_size` | 1 MiB, hard ceiling 8 MiB | #7 |
| stored user headers | fixed | 4 KiB per message | #7 |
| ack token length | fixed, derived in S3.3 | 171 bytes | #10 |
| ack token `attempt` and `generation` fields | fixed, narrowed from the schema's 64-bit columns by C14 | 1 to 4294967295 | #10 |
| subject length and depth | fixed, grammar rule S1 | 512 bytes, 32 tokens | merged in #3 |
| stream and consumer name length | fixed, grammar rule S11 | 64 bytes, 60 for a new stream | merged in #3 |
| pending set per consumer | `consumers.max_ack_pending` | 1000 | #9 |
| delivery attempts | `consumers.max_deliver` | 5, 0 means unlimited, ceiling 4294967295 (C14) | #11 |
| backoff schedule | `consumers.backoff_ms` | `[1s, 5s, 30s, 2m, 10m]` | #11 |
| deduplication window | `streams.dedup_window_ms` | 120000 ms | #7 |
| stream retention | `streams.max_msgs`, `max_bytes`, `max_age_ms` | 0, 0, 7 d | #27 |
| events table | `--event-retention`, `--event-max-rows` | 72h | #27 |
| WAL size before a truncating checkpoint | `--wal-max-bytes` | 256 MiB | #27 |
| free disk floor | `--min-free-bytes` | 256 MiB | #27 |
| graceful drain | fixed | 10 s | #17 |
| redrive count without `--force` | fixed | 3 | #29 |

## A2. Traceability

| PLAN.md | This document | Implementing issues |
|---|---|---|
| 1.3 vocabulary | S2 | merged in #3 |
| 2 (D1 to D15) | ADR-0002 to ADR-0016 | various |
| 4.2 schema | S5.1 | #5 |
| 4.3 durability and group commit | S11 | #7 |
| 4.4 crash recovery | S12 | #5 |
| 4.5 retention and disk safety | S11.3, A1 | #27 |
| 5 state machine | S5 | #9, #10, #11 |
| 5.1 transition rules | S6 | #9, #10, #11, #12 |
| 5.1 backoff | S8 | #11 |
| 5.1 dead-lettering | S9 | #12 |
| 5.1 publish dedup | S10 | #7 |
| 5.2 invariants | S15 | #8, #13 |
| 6 guarantees | S14 | #35 |
| 7 ack token and error envelope | S3.3, S13 | #10, #14 |
| 9.2 event vocabulary | S2.4 | #19 |
| 9.4 metrics | S5.3 | #20 |
| 11.4 clock jumps | S7 | #32 |

## A3. Spec changelog

S16.3 requires a superseding ADR **and a row here** for every breaking change made after the v1.0 tag. This table is that landing place, kept from the first version so the requirement has somewhere to point rather than being invented under pressure. It carries one row until there is a second version.

| Spec-Version | Change |
|---|---|
| 1.0-draft | First normative text. |
