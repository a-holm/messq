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
messq <version> (<commit>, <build date>, <go version>, linux/amd64)
```

The placeholders are filled in from the checkout. `<version>` is `git describe --tags --always`: a tag once the repository has one, a short commit until then. `<commit>` is 12 hex characters and `<build date>` is the commit timestamp as RFC3339 UTC. Building from a modified worktree appends `+dirty` to the commit. Modified follows git's own definition, so it means tracked files that differ from `HEAD`; untracked files do not make a build dirty.

`make build` writes a static, `CGO_ENABLED=0`, `-trimpath` binary to `dist/messq`. `make build-all` cross-compiles `linux/amd64` and `linux/arm64` into the same directory, and `make static-check` verifies both are static. `messq version --output json` is the machine-readable form of the version line.

Builds are reproducible: `make repro` builds the same clean commit twice with cold, isolated compiler caches and compares the checksums.

## Development

```console
$ make hooks     # one-time: route git at .githooks
$ make ci        # the whole gate, the same target GitHub Actions runs
$ make help      # every target
```

`make hooks` activates a `pre-commit` hook that checks the formatting of staged content and vets the worktree, and a `pre-push` hook that runs `make ci`. Hooks are the fast local gate and are bypassable; GitHub Actions runs the same `make ci` as the backstop. What each hook does and does not catch is in [docs/adr/0001-local-first-gating.md](docs/adr/0001-local-first-gating.md). Contributor rules, including the DCO sign-off and the absence of a CLA, are in [CONTRIBUTING.md](CONTRIBUTING.md).

## Licence

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

## Status

Planning. See `docs/PLAN.md` and the issue tracker.
