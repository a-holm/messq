# 0015. Ship one thin Go client, and let the CLI consume it

- Status: Accepted
- Date: 2026-08-21
- Adjudicates: D14
- Relates to: PLAN.md section 2 D14, PLAN.md section 8, issue #4, issue #22

## Context

Client libraries are a maintenance surface that grows without bound: each language adds a release cadence, an idiom debate and a way to fall behind the server. Shipping none is defensible when the protocol is `curl`-able, which decision D2 makes true.

The counter-argument is that a client library is where the hardest at-least-once footgun gets solved once. A worker whose handler runs longer than `ack_wait` loses its delivery and causes a duplicate. The fix is an automatic `extend` heartbeat, and every user who does not know that will hit it.

## Decision

messq ships exactly one client library: `pkg/client`, a thin Go package over the HTTP API. **The CLI consumes it**, which is what guarantees the client is real and exercised rather than a sample.

It ships a `Worker` helper that runs a fetch loop and sends automatic `extend` heartbeats at half the ack deadline while a handler runs, so the ack-wait footgun is solved in the library rather than in every user's code.

Python and TypeScript get copy-pasteable snippets in the documentation, executed in CI. There are no maintained SDKs at 1.0.

`pkg/client` may not import any `internal/` package. `scripts/layers.sh` enforces it.

## Alternatives

| Option | Why it was serious | Why it lost |
|---|---|---|
| No client library at all | The API is `curl`-able, so every language already has a client. It is the smallest possible surface. | The CLI would then carry its own HTTP code, which nobody else can use, and the heartbeat footgun would be left to every user to discover. One package that the CLI already depends on costs almost nothing extra. |
| Maintained Python and TypeScript SDKs | They are the two languages that would use messq most. | Three release cadences and three idiom debates for one developer. Tested snippets deliver most of the value at a fraction of the cost, and they cannot silently fall behind because CI runs them. |
| A generated client from an OpenAPI description | One description, many languages, no hand-written clients. | It reintroduces a code generation step that decision D2 removed, and a generated client would not carry the `Worker` helper, which is the part that matters. |
| A fat client with local batching, retry policy and a connection pool | It would make the fast path faster. | It moves protocol decisions into the client, where they are versioned separately from the server. The server owns flow control, by design. |

## Consequences

The client is real: it is on the critical path of the CLI, so a broken client is a broken `messq pub`, caught by the testscript suite rather than by a user.

The `Worker` helper makes the correct at-least-once pattern the default one. A user who writes a handler and hands it to `Worker` gets heartbeats without knowing they exist.

The public import path is part of the compatibility promise from the first release, which is what makes the module path in ADR-0017 expensive to change.

The cost is that non-Go users get snippets rather than a package. The snippets are executed in CI, so they are at least correct, and they are short because the protocol is.

## Revisit trigger

Demonstrated demand: a language whose users are blocked by the absence of a package rather than inconvenienced by it. An SDK is a project, and it starts when someone is prepared to maintain it.
