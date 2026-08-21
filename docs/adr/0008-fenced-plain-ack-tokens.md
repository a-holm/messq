# 0008. Fence ack tokens by attempt and generation, without cryptography

- Status: Accepted
- Date: 2026-08-21
- Adjudicates: D7
- Relates to: PLAN.md section 2 D7, PLAN.md section 5.1, docs/SEMANTICS.md S3.3, issue #4, issue #10

## Context

An ack token identifies which delivery a settle refers to. It has to solve one real problem: a worker whose delivery already timed out and was redelivered must not be able to ack the redelivery that another worker is now processing. That is the "my worker acked but the message was processed twice" mystery, and it is the most common at-least-once support ticket.

Two plans proposed cryptography: HMAC-signed tokens, or random single-use lease identifiers stored server-side. Both defend against forgery.

The threat model matters. messq is a single-node broker whose default listener is a Unix socket with filesystem permissions, and whose TCP listener requires a bearer token with per-stream roles. A caller who can reach the settle endpoint is already authorised to settle for that stream. Forgery is not the gap; staleness is.

## Decision

The ack token is `"<stream>/<consumer>/<seq>/<attempt>/<generation>"`, plain text, human-readable on purpose. It carries no signature and no secret.

Validation is four checks in a normative order: parse, resolve the consumer, compare the generation, compare the attempt against the live row. A wrong attempt is `errs.ErrStaleAck` with a WARN event and a metric. A wrong generation, which a `seek` or a `purge` produces, is `errs.ErrWrongGen`. A token for a delivery that is already resolved is an **idempotent success flagged stale**. A token for a consumer that no longer exists is `errs.ErrNotFound`, never an idempotent success.

The token is bounded at 171 bytes, which is the longest well-formed token derivable from the name and integer grammars. Anything longer is rejected before parsing.

## Alternatives

| Option | Why it was serious | Why it lost |
|---|---|---|
| HMAC-signed tokens | They make forgery impossible without the key, and the protocol-designer plan is right that a self-validating token needs no lookup. | The token is validated against the live delivery row anyway, so the signature saves no lookup. It buys key management, key rotation and a crypto dependency for a forgery threat that authorisation already covers on a single-node broker. |
| Random single-use lease identifiers held server-side | They leak nothing about internal structure, and they cannot be guessed. | They need a second durable table keyed by an opaque identifier, and they destroy the property that an operator can read the attempt number straight out of a log line. |
| A token that is just `stream/consumer/seq` | Simplest possible, and enough to name a delivery. | It cannot tell attempt 1 from attempt 2, which is precisely the case the fence exists for. A late ack from a timed-out worker would delete a live redelivery. |
| Reject a stale ack silently, or accept it | Silence is less noise; acceptance is more forgiving. | A stale ack is the symptom of an `ack_wait` that is too short, and it is actionable. Reporting it as a distinct outcome with its own metric turns a mystery into an alertable line. |

## Consequences

Fencing costs two integer comparisons after the row lookup. There is no key material anywhere in the settle path.

The token is readable. An operator reading a log line sees which stream, which consumer, which sequence number and which attempt, without a decoder.

Because `/` separates the fields, `/` may never appear in a stream or consumer name. The merged `internal/subject` name grammar bans it, and `SEMANTICS.md` S3.2 and S3.3 state the dependency in both directions so that nobody relaxes one without noticing the other.

The token exposes internal structure: stream name, consumer name and sequence number. On a broker where a settle already requires authorisation for that stream, that is information the caller has by definition.

The parser is a fuzz target from the start, with the rule that no input forges a valid token and no input panics.

## Revisit trigger

A deployment model where a settle endpoint is reachable by a party not authorised for the stream. Native TLS and multi-tenant access are phase 2 questions; if either arrives, the fence gains a signature rather than a new shape.
