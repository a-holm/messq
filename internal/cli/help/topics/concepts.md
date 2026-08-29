# concepts

messq's vocabulary is JetStream's vocabulary, one paragraph each.

**Stream** — a durable, append-only sequence of messages with a name, a set of
subject patterns it accepts, and retention limits. `messq stream add` creates
one; everything else hangs off it.

**Subject** — the address a message is published to and consumers filter on,
NATS syntax: dot-separated tokens, `*` matches exactly one token, `>` matches
one or more trailing tokens. A stream's `subjects` are the patterns it accepts.

**Consumer** — a durable cursor over one stream with its own filter, its own
`ack_wait`, its own `max_deliver` and its own backoff schedule. Consumers are
named; two workers sharing a name share the cursor (that is how you scale out).

**Ack / nak / term / extend** — the four settlements. `ack` deletes the
delivery; `nak` asks for a redelivery on the backoff schedule; `term` closes
the message as resolved-without-ack; `extend` pushes the `ack_wait` deadline
out for long work.

**Attempt** — which delivery this is, 1-of-`max_deliver`. Redeliveries
increment it; when the budget is spent the message is dead.

**DLQ** — the dead-letter stream (`<stream>.dlq` by default). Dead messages
land there with their provenance headers, ready for `messq dlq redrive` once
the bug is fixed.

**Trace** — the event journal for one message: every publish, deliver, settle,
timeout and death, written in the same transaction as the change. Start here
when something did not arrive.

The fastest way to feel all of this is `messq quickstart`: seven commands,
eleven seconds, one deliberate dead letter.
