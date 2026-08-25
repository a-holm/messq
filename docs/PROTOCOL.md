# messq wire protocol

This document pins the HTTP/JSON wire contract of `messq serve`: the transport, the
route set, the error envelope and the closed machine-code enum. It is the
operator-facing half of ADR-0003 ("speak HTTP/1.1 and JSON on the standard library")
and the contract issue #18's wire gates freeze. Delivery semantics remain normative in
[SEMANTICS.md](SEMANTICS.md); the reasoning behind the protocol decisions lives in
[ADR-0003](adr/0003-protocol-http-json.md).

Status: seeded minimal by issue #18 over the merged #7 surface. Routes for settle
(ack/nak/term/extend) and auth join this document as their milestones (#9/#10/#14/#16)
merge. The machine-readable source for section "Error codes" is `internal/wirecode` in
the repository; this table and that package must never disagree.

## Transport

```
messq serve --data-dir /var/lib/messq [--listen unix:///run/messq/messq.sock]
```

- Default listener is a Unix socket at `/run/messq/messq.sock`; `--listen tcp://127.0.0.1:4222`
  serves loopback TCP instead. Every request is identical on both listeners — same
  routes, same bodies, same codes.
- The daemon refuses to start on a non-loopback TCP address unless an auth file is
  configured (issue #16 pins the refusal; until then only loopback listeners exist).
- Bodies are JSON (`application/json`). Publish takes a raw request body so
  `curl --data-binary @file` works.

## Error envelope

Every error response carries one envelope shape:

```json
{"error":{"code":"stream_exists","message":"stream \"orders\" already exists with a different configuration (max_msgs)","next":["messq stream edit orders"],"trace_id":"<trace-id>"}}
```

- `code` is a member of the closed enum below — never free text.
- `message` is human-readable, may quote the offending value, never quotes secrets.
- `next` suggests remediation commands (may be empty).
- `trace_id` correlates with the daemon log.

## Error codes

Codes are a closed, documented enum and part of the 1.0 compatibility contract
(PLAN.md §7). Adding a member is additive; removing, renaming or changing an HTTP
status is breaking. Clients must tolerate unknown members.

Produced today:

| code | HTTP | meaning |
|---|---|---|
| `not_found` | 404 | stream, consumer or message does not exist |
| `stream_exists` | 409 | create hit an existing stream with a different config |
| `conflict` | 409 | generic state conflict |
| `immutable_field` | 409 | update tried to change a field that cannot change |
| `would_lose_data` | 409 | update would discard existing messages |
| `reserved_name` | 400 | name collides with a reserved stream name |
| `bad_request` | 400 | body failed validation |
| `bad_subject` | 400 | subject violates the naming rules |
| `subject_mismatch` | 400 | publish subject outside the stream's subjects |
| `header_too_large` | 400 | headers exceed the configured budget |
| `reserved_header` | 400 | use of a `Messq-` reserved header |
| `unsupported` | 400 | route or option exists but is not enabled here |
| `too_large` | 413 | body exceeds `--max-msg-size-ceiling` |
| `read_only` | 503 | daemon started read-only; writes refused |
| `shutting_down` | 503 | drain in progress; new work refused |
| `unauthorized` | 401 | missing or malformed bearer token (#14 auth) |
| `forbidden` | 403 | authenticated but not allowed (#14 roles) |
| `invalid_token` | 400 | unknown or malformed ack token |
| `stale_ack` | 409 | ack/settle against a redelivered or reset consumer |
| `paused` | 409 | consumer is paused |
| `flow_control` | 429 | max_ack_pending reached; slow down |
| `stream_full` | 507 | stream at its limit and discard=new |
| `disk_full` | 507 | insufficient free disk space (#17 adds degraded-writes semantics) |
| `internal` | 500 | unclassified failure (the catch-all) |

Frozen ahead of their milestones:

| code | HTTP | status |
|---|---|---|
| `rate_limited` | 429 | reserved for #39 (max in-flight / flow control) |

Never over HTTP: `locked`, `schema_newer`, `unavailable`. These three exist as
process/client errors only — two are startup refusals before any listener exists, one
lives entirely client-side. No handler may map them into an envelope; the wire-freeze
tests enforce exactly that set via `internal/wirecode.NeverOverHTTPSet()`.

## Routes

| method & path | purpose |
|---|---|
| `GET /healthz` | liveness; answers `ok`, no auth required once #16 lands |
| `GET /v1/info` | version, uptime, durability, db size, node id |
| `POST /v1/streams` | create a stream |
| `GET /v1/streams` | list streams |
| `GET /v1/streams/{stream}` | inspect one stream |
| `PATCH /v1/streams/{stream}` | update limits/subjects within compatibility rules |
| `DELETE /v1/streams/{stream}` | delete a stream |
| `POST /v1/streams/{stream}/messages` | publish one message (`?subject=` or `Messq-Subject`) |
| `POST /v1/streams/{stream}/messages:batch` | publish up to `--max-batch-messages` |
| `GET /v1/streams/{stream}/messages` | list messages |
| `GET /v1/streams/{stream}/messages/{seq}` | peek one message by sequence |
| `GET /v1/streams/{stream}/messages/{seq}/data` | raw message bytes |
| `GET /v1/messages/{id}` | peek one message by id |
| `POST /v1/streams/{stream}/consumers` | create a consumer |
| `GET /v1/streams/{stream}/consumers` | list consumers |
| `GET /v1/streams/{stream}/consumers/{consumer}` | inspect one consumer |
| `PATCH /v1/streams/{stream}/consumers/{consumer}` | update consumer config |
| `DELETE /v1/streams/{stream}/consumers/{consumer}` | delete a consumer |
| `POST /v1/streams/{stream}/consumers/{consumer}/fetch` | long-poll delivery |

## A curl transcript

Everything below was executed against a fresh daemon exactly as shown; volatile values
(`<trace-id>`, `<msg-id>`, timestamps, counters) vary run to run.

Start the daemon and talk to it through the socket:

```console
$ messq serve --data-dir /tmp/messq-demo &
$ curl -s --unix-socket /run/messq/messq.sock http://localhost/healthz
ok

$ curl -s --unix-socket /run/messq/messq.sock http://localhost/v1/info
{"version":"<version>","uptime_ms":41,"durability":"full","synchronous":2,"db_bytes":118784,"node_id":"<node-id>"}
```

Create a stream, then publish to it:

```console
$ curl -s -X POST http://localhost/v1/streams \
    -d '{"name":"orders","subjects":["orders.*"],"max_msgs":10000}'
{"name":"orders","subjects":["orders.*"],"retention":"limits","max_msgs":10000,"max_bytes":0,"max_age_ms":604800000,"max_msg_size":1048576,"discard":"old","dedup_window_ms":120000,"created_at":<created-at>,"first_seq":0,"last_seq":0,"msgs":0,"bytes":0}

$ curl -s -X POST 'http://localhost/v1/streams/orders/messages?subject=orders.created' \
    -d 'hello messq'
{"stream":"orders","seq":1,"id":"<msg-id>","trace_id":"<trace-id>","duplicate":false,"published_at":<published-at>}
```

Read it back — as metadata and as raw bytes — and watch an error envelope happen:

```console
$ curl -s http://localhost/v1/streams/orders/messages/1
{"stream":"orders","seq":1,"id":"<msg-id>","subject":"orders.created","size":11,"published_at":<published-at>,"trace_id":"<trace-id>"}

$ curl -s http://localhost/v1/streams/orders/messages/1/data
hello messq

$ curl -s -w '\nHTTP %{http_code}\n' -X POST http://localhost/v1/streams \
    -d '{"name":"orders","subjects":["orders.*"]}'
{"error":{"code":"stream_exists","message":"stream \"orders\" already exists with a different configuration (max_msgs)","next":["messq stream edit orders"],"trace_id":"<trace-id>"}}
HTTP 409
```
