# Contributing to messq

Thank you for helping. This document is the whole process; there is nothing else to sign.

## Legal

messq is licensed under Apache-2.0. **There is no CLA.** Contributions are accepted under the [Developer Certificate of Origin](https://developercertificate.org/): you certify that you wrote the patch or otherwise have the right to submit it under the project licence.

Certify it by signing off every commit:

```console
$ git commit -s -m "#12: fix the sweeper backoff"
```

That appends a `Signed-off-by: Your Name <you@example.com>` trailer. Commits without it are not merged.

Every `.go` file carries `// SPDX-License-Identifier: Apache-2.0` as its first line.

## Setup

```console
$ git clone https://github.com/a-holm/messq.git
$ cd messq
$ make hooks     # route git at .githooks: pre-commit formats and vets, pre-push runs make ci
$ make ci
```

`make hooks` is the one setup step. It configures `core.hooksPath`, so no hook framework and no extra dependency is involved.

## The gate

`make ci` is the single gate. GitHub Actions runs exactly the same target, so a green `make ci` locally is a green pipeline. Run it before you push; the pre-push hook runs it for you.

| Target | What it enforces |
|---|---|
| `make fmt-check` | Every Go file is gofumpt-clean. `make fmt` fixes it. |
| `make vet` | `go vet` is clean. |
| `make tidy-check` | `go mod tidy` would not change `go.mod` or `go.sum`. |
| `make dep-budget` | Every direct module is on the allow-list in `scripts/dep-budget.sh`. |
| `make layers` | No package imports across a forbidden layer boundary. |
| `make test` | The test suite passes. |
| `make build-all` | Static `linux/amd64` and `linux/arm64` binaries build. |

`make lint` and `make fmt` fetch their pinned tool through `go run` on first use, so the first run needs network access. The tool never enters `go.mod`.

## Rules that get patches merged

**Every bug fix lands with a committed reproduction artifact.** A rapid failfile, a fuzz input, a seed, or a `testscript` case. The artifact goes in the same commit as the fix and fails without it.

**A new direct dependency needs an ADR.** Add the record under `docs/adr/`, extend `scripts/dep-budget.sh`, and say in the pull request why the standard library is not enough. The budget is eight direct non-test modules.

**A new noun in the command tree needs a written justification.** messq is meant to stay understandable in an evening. If a feature adds a concept an operator has to learn, argue for it against that goal in the pull request before writing the code.

**Data goes to stdout, narration goes to stderr.** Exit codes are a contract: 0 ok, 1 error, 2 usage, 3 not found, 4 conflict or stale, 5 empty or timeout, 6 daemon unreachable, 7 permission.

## Tests

No `time.Sleep` outside soak tests; use `testing/synctest` or the `Clock` seam. Table-driven tests, `t.TempDir()` for anything on disk, and no assertion DSL: plain `if got != want` with `google/go-cmp` for structs.

## Commits and pull requests

Imperative subject, at most 72 characters, prefixed with the issue number: `#12: fix the sweeper backoff`. The body explains what the diff cannot.

A pull request describes what changed, the evidence (commands run and their output), and any deliberate deviation from the issue with the reason.
