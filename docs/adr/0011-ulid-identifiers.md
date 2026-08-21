# 0011. Identify messages with ULIDs

- Status: Accepted
- Date: 2026-08-21
- Adjudicates: D10
- Relates to: PLAN.md section 2 D10, docs/SEMANTICS.md S4.1, issue #4, issue #3

## Context

The message id is the spine of `messq trace <id>`. It appears in log lines, in support tickets, in screenshots and in URLs, and a user reads it off a screen and types it back. It is also the identifier `GET /v1/messages/{id}` resolves, so it has to be globally unique across streams.

The per-stream `seq` is a natural candidate and is not sufficient on its own: it is not unique across streams, and it is not stable across a replay or a dead-letter hop, so `stream:seq` cannot be the only identifier.

## Decision

A message id is a ULID, minted through `oklog/ulid/v2`: 26 characters of Crockford base32, time-sortable, with the mint millisecond embedded.

Both identifiers are exposed. The ULID answers "which message", globally. The `stream/seq` pair answers "where in the stream", and it is the recommended consumer-side deduplication key because it is stable across every redelivery.

Minting never fails. A backwards wall-clock step does not lower the millisecond handed out, and the regression is reported through a hook. Exhausting the 2^80 bits of entropy inside one millisecond steps to the next millisecond and draws again. Publish must not be able to fail for id reasons.

Parsing is case-insensitive because a user retypes what they read; rendering is always upper case.

## Alternatives

| Option | Why it was serious | Why it lost |
|---|---|---|
| UUIDv7 | Standardised, time-ordered, and already in many ecosystems. | 36 characters of case-sensitive hex against 26 characters of unambiguous base32. It is longer in every log line and worse to read off a screenshot, which is where these ids are actually read. |
| `stream:seq` as the only id | It already exists, costs nothing, and is meaningful. | It is not globally unique, and it is not stable across a dead-letter copy or a replay. Both identifiers are exposed instead, each answering the question it is good at. |
| UUIDv4 | Universally available and needs no clock. | Not sortable, so logs do not cluster by time and an incident's ids scatter. The embedded timestamp is a real forensic feature. |
| A monotonic integer node-wide | Smallest and fastest. | It leaks total volume, it is not meaningful across a backup or restore, and it collides with the per-stream `seq` in every conversation. |

## Consequences

Log lines sort by time when they sort by id, and an incident's ids cluster. A message can be dated from its id alone, without a lookup.

Crockford base32 has no ambiguous characters, so an id survives being read off a screenshot and typed back, which is the workflow that actually happens during an incident.

The generator holds a lock and hands out monotonic values within one generator. Across generators there is no ordering guarantee, and the daemon has exactly one. That contract is stated so a test can assert it.

The never-fail policy means a slightly optimistic timestamp is possible after a backwards clock step. That is the cheaper wrong answer: refusing to mint would take the daemon down on an NTP step.

The dependency is one row of the eight-module budget.

## Revisit trigger

None. The id is in the wire contract, in URLs and in stored rows from the first release.
