# messq

[![ci](https://github.com/a-holm/messq/actions/workflows/ci.yml/badge.svg)](https://github.com/a-holm/messq/actions/workflows/ci.yml)

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

## Build from source

Linux, `make`, `bash`, and Go 1.25 or newer. `go.mod` pins the exact toolchain, which the Go command downloads on its own, so any recent Go builds the same binary.

```console
$ git clone https://github.com/a-holm/messq.git
$ cd messq
$ make build
$ ./dist/messq version
messq v0.1.0 (a1b2c3d4e5f6, 2026-08-24T09:00:00Z, go1.26.5, linux/amd64)
```

`make build` writes a static, `CGO_ENABLED=0`, `-trimpath` binary to `dist/messq`. `make build-all` cross-compiles `linux/amd64` and `linux/arm64` into the same directory, and `scripts/assert-static.sh dist/messq-linux-amd64` verifies a binary is static. `messq version --output json` is the machine-readable form of the version line.

Builds are reproducible: `make repro` builds the same clean commit twice, with a cold compiler cache in between, and compares the checksums.

## Development

```console
$ make hooks     # one-time: route git at .githooks
$ make ci        # the whole gate, the same target GitHub Actions runs
$ make help      # every target
```

`make hooks` activates a `pre-commit` hook that runs formatting and vet, and a `pre-push` hook that runs `make ci`. Gates run locally first and GitHub Actions is the backstop; see [docs/adr/0001-local-first-gating.md](docs/adr/0001-local-first-gating.md). Contributor rules, including the DCO sign-off and the absence of a CLA, are in [CONTRIBUTING.md](CONTRIBUTING.md).

## Licence

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

## Status

Planning. See `docs/PLAN.md` and the issue tracker.
