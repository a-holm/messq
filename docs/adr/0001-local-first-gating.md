# 0001. Local-first gating with git hooks

- Status: Accepted
- Date: 2026-08-21
- Adjudicates: none
- Relates to: PLAN.md section 11, issue #1

## Context

`make ci` is the single definition of "green". A hosted runner is the slowest and most expensive place to discover that a file is unformatted or that a package no longer vets. Every such discovery costs a push, a queue wait, a runner minute and a context switch, and the answer was available locally in seconds.

## Decision

Quality gates run locally first. Git hooks under `.githooks` run them, and `make hooks` activates them with `git config core.hooksPath .githooks`.

- `pre-commit` checks formatting and runs `go vet`. It is the fast gate: it stays in the seconds range so it never tempts anyone into `--no-verify`.
- `pre-push` runs `make ci`, the same target the pipeline runs.

Hooks are the default local gate, not a barrier. `git commit --no-verify` and `git push --no-verify` bypass them, and a contributor can edit them or simply never run `make hooks`. GitHub Actions is the backstop that cannot be skipped: it runs `make ci` on a clean checkout with a cold cache, on every pull request and on every push to `main`. The hooks make the common case fast; Actions makes the guarantee real.

The hooks call `make` targets rather than repeating commands, so each gate has exactly one definition and a hook cannot drift from the pipeline.

## What the hooks check, precisely

`pre-commit` checks **formatting against staged content**. A commit records the index, so checking the worktree gates a different artifact than the one being committed: stage an unformatted file, tidy the worktree copy without re-staging, and a worktree check passes while the unformatted blob lands in history. The hook mirrors the staged blobs into a temporary directory with `git show :<path>`, copies the staged `go.mod` alongside them so gofumpt resolves the same language version and module path it would in the real tree, and checks that mirror.

`pre-commit` runs **`go vet` against the worktree**, not the index. `go vet` needs a compilable package and its dependencies, not the staged subset of files; reconstructing the whole module on every commit costs a build and would push the hook out of the seconds range. The asymmetry is real and accepted: a commit whose staged content vets differently from the worktree is not caught until `pre-push` or Actions.

`pre-push` runs `make ci` **against the worktree at the tip only**. Intermediate commits in a pushed range are never gated individually. This is acceptable because pull requests are squash-merged and Actions runs `make ci` on the result, so the commit that lands on `main` is always gated. It stops being acceptable the day the project merges without squashing.

## Formatter

gofumpt is the formatter everywhere: `make fmt`, `make fmt-check`, the `pre-commit` hook, and therefore `make ci`. Plain `gofmt` is not used as a second, weaker check. gofumpt is a strict superset, so gofumpt-clean code is gofmt-clean code; running both would only add a gate that can never fire on its own.

gofumpt is pinned in its own module file, `tools/gofumpt.mod`, and invoked as `go tool -modfile=tools/gofumpt.mod gofumpt`. It never enters `go.mod`, so the eight-module dependency budget of PLAN.md section 13 stays untouched. `make lint` pins golangci-lint the same way, which is also what golangci-lint upstream recommends.

## Alternatives

| Option | Why not |
|---|---|
| No hooks, CI is the only gate | Moves a two-second answer onto a runner and into a review cycle. |
| lefthook or pre-commit framework | Adds an installed dependency and a second configuration language for two shell scripts. |
| `pre-commit` runs the full `make ci` | Cross-compiling two architectures on every commit is slow enough that people bypass the hook. |
| `pre-commit` checks the worktree | Gates content that is not what gets committed. |
| Plain `gofmt` in `make ci`, gofumpt only locally | Two formatters that disagree by design, with the weaker one in the gate that matters. |

## Tool pinning and the network

Each pinned tool has its own module file and checksum file under `tools/`, and runs through `go tool -modfile=<file> <name>`. That form names no version at the call site, which is what makes the tools behave.

`go run <tool>@<version>`, the obvious alternative, performs a deprecation lookup against the module proxy on *every* invocation, not only the first. A warm module cache is therefore not enough to work offline: a `GOPROXY=off` run fails with `loading deprecation for mvdan.cc/gofumpt: module lookup disabled by GOPROXY=off`.

Setting `GONOPROXY` silences that lookup, but it buys offline capability by giving up the proxy: with a cold module cache it makes the go command fetch the tool straight from upstream VCS. Measured with `GOVCS='*:off'` and an empty `GOMODCACHE`, `GONOPROXY` fails with `unrecognized import path "mvdan.cc/gofumpt": GOVCS disallows using git`, while the default configuration downloads through `proxy.golang.org`. An environment that only permits the proxy, which is the common corporate setup, would break on its first `make ci`. `GOPRIVATE` is worse again: it sets `GONOSUMDB` too and drops checksum-database verification.

The tool module file avoids the trade entirely. There is no version to resolve, so there is no lookup and no need to touch `GOPROXY` or `GONOPROXY` at all: a warm cache runs offline, and a cold cache fetches through whatever proxy is configured. The tool's whole dependency graph is hash-pinned in `tools/<tool>.sum`, which `go run <tool>@<version>` never gave us.

The first `make ci` after a clone still downloads the tool, so it needs network access.

Update a pin with `go get -tool -modfile=tools/gofumpt.mod mvdan.cc/gofumpt@<version>`, never with `go mod tidy`. `-modfile` keeps the module root at the repository root, so `go mod tidy -modfile=tools/gofumpt.mod` walks this repository's own Go files and tries to resolve `internal/cli` and `internal/buildinfo` as dependencies of the tool module. That is why `make tidy-check` gates `go.mod` only; the `.sum` file is what defends the tool module.

## Consequences

`make hooks` is a required setup step after cloning, documented in the README and in CONTRIBUTING.md. Nothing enforces that a contributor ran it, which is precisely why the backstop exists.

The `pre-commit` mirror costs one `git show` per staged Go file, which is negligible next to the gofumpt invocation itself.

## Revisit trigger

The project merging without squashing. `pre-push` gates the tip of a pushed range only, so intermediate commits would then land on `main` ungated and each commit would need its own gate.
