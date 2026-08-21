# Contributing to messq

Thank you for helping. This document is the whole process; there is nothing else to sign.

## Legal

messq is licensed under Apache-2.0. **There is no CLA.** Contributions are accepted under the [Developer Certificate of Origin](https://developercertificate.org/): you certify that you wrote the patch or otherwise have the right to submit it under the project licence.

Certify it by signing off every commit:

```console
$ git commit -s -m "#12: fix the sweeper backoff"
```

That appends a `Signed-off-by: Your Name <you@example.com>` trailer. Commits without it are not merged.

Every `.go` file carries `// SPDX-License-Identifier: Apache-2.0` as its first line, and every script, hook, workflow and the Makefile carries it within their first three lines, under the shebang or the name. `make spdx` enforces it.

## Setup

```console
$ git clone https://github.com/a-holm/messq.git
$ cd messq
$ make hooks     # route git at .githooks: pre-commit checks staged formatting and vets, pre-push runs make ci
$ make ci
```

`make hooks` is the one setup step. It configures `core.hooksPath`, so no hook framework and no extra dependency is involved.

## The gate

`make ci` is the single gate, and it is the one command that reproduces a red pipeline locally: GitHub Actions runs exactly the same target. Run it before you push; the pre-push hook runs it for you. It prints the wall clock of every target when it finishes, because PLAN.md section 11 budgets the pull-request lane at ten minutes and that is a constraint, not an aspiration.

| Target | What it enforces |
|---|---|
| `make fmt-check` | Every Go file is gofumpt-clean. `make fmt` fixes it. |
| `make vet` | `go vet` is clean. |
| `make tidy-check` | `go mod tidy` would not change `go.mod` or `go.sum`. |
| `make dep-budget` | Every direct module is on the allow-list in `scripts/dep-budget.sh`. |
| `make layers` | No package, and no package's tests, imports across a forbidden layer boundary. |
| `make spdx` | Every source file carries its licence header. |
| `make lint` | `.golangci.yml` validates against the v2 schema, golangci-lint is clean, and `actionlint` is clean over the workflows. |
| `make test` | The suite passes under the race detector, uncached and shuffled. |
| `make cover` | Every package in `coverage.floors` meets its floor. |
| `make cover-ratchet-check` | This branch does not lower a floor without saying why. |
| `make vuln` | No vulnerability is reachable from messq's own code, and no suppression has expired. |
| `make gates-selftest` | Every gate above still fails when you break it. |
| `make static-check` | Both cross-compiled binaries record `CGO_ENABLED=0` and `-trimpath`, and `file(1)` calls them statically linked. |

Two more targets exist for people rather than for CI: `make cover-html` opens the profile from the last `make cover`, and `make cover-ratchet` raises the floors that coverage has outgrown.

The race detector needs cgo and the shipped binary must not have it, so `make test` and `make cover` run with `CGO_ENABLED=1` while every build target pins `CGO_ENABLED=0`. Both are mandatory and neither may be dropped.

`make fmt`, `make fmt-check`, `make lint` and `make vuln` run a pinned tool from its own module file under `tools/`, downloaded on first use, so the first `make ci` needs network access. After that fetch everything works offline except `make vuln`, which queries the Go vulnerability database on every run: a CVE published today is not in yesterday's copy. The tools never enter `go.mod` and never spend the dependency budget.

Update a tool pin with `go get -tool -modfile=tools/gofumpt.mod mvdan.cc/gofumpt@<version>`. Do not run `go mod tidy` against a tool module file; `docs/adr/0001-local-first-gating.md` explains why it cannot work.

Nightly, `.github/workflows/nightly.yml` runs what does not fit the ten-minute budget or has nothing to do with a diff: `make vuln-strict` against an unchanged tree, and ten shuffled runs of the suite hunting flakes. Both keep their output as a run artifact for a fortnight, because a flake is not reproducible on demand and the run that caught it is the only evidence there will be. Any failure opens a `nightly-failure` issue, or comments on the one already open: a lane nobody watches is a lane nobody reads. The remaining jobs are green placeholders naming the issue that fills them in.

## Proving the gates

A gate nobody has seen fail is a gate nobody knows works. `make gates-selftest` applies one mutation per gate to a scratch copy of the tree, runs the make target that must notice it, and asserts a non-zero exit **and** a matching message: a build that failed for an unrelated reason would otherwise look like a working gate. Two rows sabotage nothing and must stay green, which is what catches a scratch copy that no longer resembles the tree.

The mutations live in `test/gates/testdata/` and the driver in `test/gates/gates_test.go`, behind the `gatecheck` build tag. A new gate lands with its row in that matrix, in the same commit. Nothing in the matrix is skipped, and no test in this repository calls `t.Skip`.

## Coverage floors

`coverage.floors` names the packages that carry the semantics and the statement coverage each must reach. PLAN.md section 11 wants no vanity global number, so a package earns a line there when it is worth a number.

Floors ratchet upward only.

- A floored package that exists but declares no functions yet reports `PENDING` and passes. Once it has code, a missing profile entry is a failure: deleting a package's tests must not silently satisfy its floor, and neither must deleting the package.
- `make cover-ratchet` raises a floor once measured coverage clears it by a whole point, rounded down. Run it yourself, review the diff, commit it. CI never runs it: a bot that edits the gate is not a gate.
- Lowering a floor needs a commit on the branch whose message carries a `coverage-floor-lowered: <package> <reason>` line, starting at the beginning of a line and naming every package whose floor the branch lowers. A trailer explains only the floors it names: cutting `internal/store` while explaining a move of `internal/id` refuses the branch. Without an explaining trailer for each lowered floor, `make cover-ratchet-check` refuses it. The reason is the point; the check only makes it deliberate. The whole branch is searched rather than its tip, because a pull-request runner checks out a synthetic merge commit and would never see the tip.

## Vulnerabilities and suppressions

`make vuln` fails on a vulnerability govulncheck reports as reachable from messq's own code. A vulnerable module that no messq code path reaches is a fact about the dependency graph, not about this build, and does not block it.

`.govulncheck-allow` suppresses a finding by its `GO-YYYY-NNNN` identifier, and every field is mandatory:

```
# osv-id         expires      reason
GO-2026-1234     2026-09-30   only reachable via the cgosqlite build tag; tracked in #5
```

Past its expiry an entry fails the build whether or not the vulnerability is still reported, so a suppression rots loudly instead of becoming a list nobody reads. An empty file is the expected steady state, and the nightly lane also fails on an entry that no longer suppresses anything.

## What the hooks do and do not catch

`make hooks` installs two hooks. Both are bypassable with `--no-verify`; GitHub Actions is the backstop that is not.

`pre-commit` checks formatting against the **staged** content, because that is what a commit records. It mirrors the staged blobs into a temporary directory and checks those, so tidying a worktree copy without re-staging it cannot sneak an unformatted blob into history.

`pre-commit` runs `go vet` against the **worktree**, because vet needs a compilable package rather than the staged subset of files. A commit whose staged content vets differently from your worktree is therefore not caught at commit time; `pre-push` and CI catch it.

`pre-push` runs `make ci` against the tip of what you are pushing, not against each commit in the range, so intermediate commits are not gated individually. Pull requests are squash-merged and CI runs on the result, so the commit that reaches `main` is always gated.

The reasoning is in [docs/adr/0001-local-first-gating.md](docs/adr/0001-local-first-gating.md).

## Rules that get patches merged

**Every bug fix lands with a committed reproduction artifact.** A rapid failfile, a fuzz input, a seed, or a `testscript` case. The artifact goes in the same commit as the fix and fails without it.

**A new direct dependency needs an ADR.** Add the record under `docs/adr/`, extend `scripts/dep-budget.sh`, and say in the pull request why the standard library is not enough. The budget is eight direct non-test modules.

**A new noun in the command tree needs a written justification.** messq is meant to stay understandable in an evening. If a feature adds a concept an operator has to learn, argue for it against that goal in the pull request before writing the code.

**Data goes to stdout, narration goes to stderr.** Exit codes are a contract: 0 ok, 1 error, 2 usage, 3 not found, 4 conflict or stale, 5 empty or timeout, 6 daemon unreachable, 7 permission.

## Tests

Table-driven tests, `t.TempDir()` for anything on disk, and no assertion DSL: plain `if got != want` with `google/go-cmp` for structs. Everything runs under the race detector.

The architectural bans are lint-enforced by `forbidigo` in `.golangci.yml`, and each message names the alternative:

- `time.Sleep` anywhere. Use a `testing/synctest` bubble or the `Clock` seam. `internal/clock` and `test/soak` are the only exemptions: the seam is where the sleep the rest of the tree may not call directly has to live.
- `time.Now` and its neighbours outside `internal/clock`. A wall clock inside `internal/queue` is what stops the state machine from being a pure function. Inside the seam the allowance is narrower still: `internal/clock/system.go` is the one file that calls them, and the repository's only other caller is `internal/tools/vulngate/main.go`, a build-gate command whose `-now` flag is its own seam. `TestWallClockCallsAreConfinedToTheSeam` walks the whole tree and fails on a third caller, on one hiding behind an aliased `import stdtime "time"`, and on an allowance that stopped being needed.
- `prometheus.MustRegister`, the default registerer and package-level `promauto` outside `internal/obs`. messq registers against a custom registry only.
- `os.Exit` outside a command entry point, and `fmt.Print*` anywhere. Data goes to the injected stdout writer, narration to stderr.

A `//nolint` must name the linter it silences and say why: `//nolint:gosec // G304: the path is an operator flag.` An unused one fails the build.

Durable deadlines (`deliveries.visible_at`, `messages.published_at`, `events.ts`) are wall-clock Unix milliseconds, because they have to survive a restart and be readable in SQL: read them with `Clock.Now`. In-process waits are monotonic: read them with `Clock.Since`. A forward wall-clock jump therefore makes in-flight deliveries look overdue and the sweeper redelivers them, which is correct at-least-once behaviour; a backwards jump delays expiry and never resurrects a resolved delivery.

Every parser gets a fuzz target and a committed seed corpus under `<package>/testdata/fuzz/<Target>/`. `make fuzz` runs each target for `FUZZTIME` (60 s by default) and the `fuzz` job in CI runs the same target list; a crasher it finds is written into that directory and is the reproduction artifact the fix ships with.

Property tests use `pgregory.net/rapid`, which drops a failfile under `<package>/testdata/rapid/` whenever a property fails. Those paths are deliberately not in `.gitignore`: a failfile from a genuine failure is the reproduction artifact its fix ships with, and needing `git add -f` to commit one would point the friction the wrong way. A failfile you produced by breaking the code on purpose is deleted, not committed — `git status` showing it is the reminder to decide which kind you have.

## Commits and pull requests

Imperative subject, at most 72 characters, prefixed with the issue number: `#12: fix the sweeper backoff`. The body explains what the diff cannot.

A pull request describes what changed, the evidence (commands run and their output), and any deliberate deviation from the issue with the reason.
