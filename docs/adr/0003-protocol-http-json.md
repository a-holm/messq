# 0003. Speak HTTP/1.1 and JSON on the standard library

- Status: Accepted
- Date: 2026-08-21
- Adjudicates: D2
- Relates to: PLAN.md section 2 D2, PLAN.md section 7, issue #4, issue #17

## Context

The wire protocol decides who can talk to messq without installing anything, and it decides how much machinery sits between a request and the writer goroutine. It is part of the 1.0 compatibility contract, so it is expensive to change after the tag.

Four candidates appeared across the plans: HTTP/1.1 with JSON on the standard library, ConnectRPC, gRPC, and custom binary framing.

The point every plan agreed on is that `curl` is the universal client and the tool an operator reaches for at three in the morning. Even the ConnectRPC advocates chose ConnectRPC because its unary path is curl-able.

The second force is the throughput ceiling that decision D1 already set. At five thousand messages per second, HTTP framing overhead is not the bottleneck; the fsync is. Wire efficiency buys nothing that can be spent.

## Decision

messq speaks HTTP/1.1 with JSON bodies on the standard library's `net/http`, routed by Go's `ServeMux` with method and pattern matching. There is no router dependency, no code generation and no protoc.

The default listener is a Unix socket at `/run/messq/messq.sock` with mode 0660. TCP is opt-in through `--listen`. Consumption is pull with long-poll: a fetch parks until a message arrives or `wait_ms` elapses.

All errors share one envelope with a stable machine-readable code. Publish takes a raw body so that `curl --data-binary @file` works; fetch returns base64 in a uniform JSON shape, with a raw-data endpoint for the cases where that matters.

## Alternatives

| Option | Why it was serious | Why it lost |
|---|---|---|
| ConnectRPC | Schema-first, generated clients in several languages, and a curl-able unary path. | It buys a code generation step and a dependency tree that parses hostile bytes, in exchange for efficiency the throughput ceiling makes unusable. Its bidirectional streaming needs an h2c compatibility story that the pull model does not need at all. |
| gRPC | The default answer for service-to-service RPC, with mature tooling. | Not curl-able without a plugin, and a large dependency tree on a single-node broker whose whole pitch is that it fits in one binary. |
| Custom binary framing | The fastest option, and full control over the wire. | It makes messq unusable from a shell, and every client becomes a project. The ceiling from D1 means the speed is unspendable. |
| Push delivery with credit-based flow control | The conventional design, and it lowers latency. | It requires a credit state machine on both sides. Pull with long-poll makes backpressure structural: flow control becomes an argument to a request, and a five-line shell worker becomes possible. |

## Consequences

A worker can be written in five lines of shell: fetch, work, ack, each with `curl`. That transcript is a README feature and a CI-executed golden test, which means it cannot rot.

JSON shapes are frozen at 1.0 and schema-tested. `Accept: application/x-ndjson` content negotiation is in from the start, so a binary or gRPC gateway can be added after 1.0 without a v2 API.

The cost is bytes on the wire and CPU in the JSON codec. Both are measured and published rather than argued about, and both are below the fsync in every profile the design predicts.

The project now has to hand-write handlers and keep the error envelope's code enum closed. The closed enum is a benefit disguised as a chore: it is what makes the CLI exit codes and the HTTP status mapping testable from one list.

## Revisit trigger

Three independent users asking for a binary or streaming transport for a workload messq is otherwise right for. A gateway in front of the same handlers is the shape of the answer, not a second protocol inside the daemon.
