# lifecycle

## The daemon state machine

Every `messq serve` walks this machine; the transitions below are the closed
set the manager enforces (a move absent from the table is refused before any
component is touched). Every duplicate has a named cause; every exit is
documented in `messq help exit-codes`.

| from | to | cause |
|---|---|---|
| STARTING | RECOVERING | the store is opening (WAL replay, quick_check, lease reclaim) |
| RECOVERING | READY | recovery finished; listeners bound, `server.start` emitted |
| READY | READY | reload self-transition (SIGHUP flips through RELOADING semantics without leaving service) |
| READY | DRAINING | SIGINT/SIGTERM: readiness flipped to 0, components stop in canonical order |
| READY | FATAL | fsyncgate or another fault latched read-only (`--fatal-drain` keeps reads) |
| DRAINING | STOPPED | the last component stopped; `clean_shutdown` written, exit 0 |
| DRAINING | FATAL | a fault during drain latched the daemon; exit 74 |

STARTING, DRAINING, STOPPED and FATAL are terminal in their direction: nothing
resurrects a draining daemon and nothing follows Stopped or Fatal.

## The message lifecycle

A message is published (durable before `201`), delivered to a consumer
(attempt 1), and settled: **ack** deletes it, **nak** redelivers on the
consumer's backoff schedule, **term** closes it, and **extend** buys more
`ack_wait`. An unsettled delivery whose `ack_wait` expires redelivers with
`cause=timeout`; when `max_deliver` attempts are spent the message is **dead**
and lands in the stream's DLQ. `messq trace <id>` replays every one of those
transitions from the journal.
