# messq

Lightweight, single-binary queue daemon for Linux, written in Go. A practical middle ground between an in-memory queue and a heavy distributed event platform — "Kafka-minimum without Kafka overhead", with more readable operations than a traditional broker.

## Core idea

Messages are stored in an append-only stream and exposed through consumers with explicit ack responsibility. A message is not done until the consumer confirms it; if confirmation never arrives, the message is redelivered. Simple, understandable at-least-once semantics.

## Guarantees

- At-least-once delivery
- Explicit ack/nak
- Ack timeout with redelivery
- Max delivery count + dead-letter queue
- Consumer-stored cursors, replay from cursor or start
- Per-subject ordering (opt-in)
- Flow control via max in-flight / max pending

## Primary objects

Stream (append-only storage) · Subject (routing key) · Consumer (stateful reader with offset + ack status) · Message (data + metadata + id) · Ack/Nak · Ack timeout · Delivery attempt counter · Dead-letter queue

## What makes it different

- CLI-first operations
- Human-friendly logs of every message transition (publish, delivery, ack, nak, timeout, redelivery, DLQ)
- Replay and inspection as core features
- Single-binary mode with local disk (SQLite/WAL)
- Small enough to understand in an evening, strong enough for internal production workloads

## Status

Planning. See `docs/PLAN.md` and the issue tracker.
