# 0001. Local-first gating with git hooks

- Status: Accepted
- Date: 2026-08-21
- Relates to: PLAN.md section 11, issue #1

## Context

`make ci` is the single definition of "green". A hosted runner is the slowest and most expensive place to discover that a file is unformatted or that a package no longer vets. Every such discovery costs a push, a queue wait, a runner minute and a context switch, and the answer was available locally in seconds.

## Decision

Quality gates run locally first. Git hooks under `.githooks` enforce them, and `make hooks` activates them with `git config core.hooksPath .githooks`.

- `pre-commit` runs `make fmt-check vet`. It is the fast gate: it must stay in the seconds range so it never tempts anyone into `--no-verify`.
- `pre-push` runs `make ci`. Nothing reaches GitHub Actions without passing the same target the pipeline runs.

GitHub Actions is the backstop, not the primary gate. It exists to catch what a contributor's machine cannot show: a clean checkout, a cold cache, a foreign architecture, and a run nobody can skip.

The hooks call `make` targets rather than repeating commands. There is exactly one definition of each gate, so a hook can never drift from the pipeline.

## Formatter

gofumpt is the formatter everywhere: `make fmt`, `make fmt-check`, the `pre-commit` hook, and therefore `make ci`. Plain `gofmt` is not used as a second, weaker check. gofumpt is a strict superset, so gofumpt-clean code is gofmt-clean code; running both would only add a gate that can never fire on its own.

gofumpt is pinned and invoked through `go run mvdan.cc/gofumpt@<version>`. It never enters `go.mod`, so the eight-module dependency budget of PLAN.md section 13 stays untouched.

## Alternatives

| Option | Why not |
|---|---|
| No hooks, CI is the only gate | Moves a two-second answer onto a runner and into a review cycle. |
| lefthook or pre-commit framework | Adds an installed dependency and a second configuration language for two shell scripts. |
| `pre-commit` runs the full `make ci` | Cross-compiling two architectures on every commit is slow enough that people bypass the hook. |
| Plain `gofmt` in `make ci`, gofumpt only locally | Two formatters that disagree by design; the weaker one in the gate that matters. |

## Consequences

`make hooks` is a required setup step after cloning, documented in the README and in CONTRIBUTING.md. Hooks are advisory: `git commit --no-verify` bypasses them, which is why `pre-push` repeats the whole gate and GitHub Actions repeats it again.

`make fmt-check` fetches gofumpt through the module cache on first use, so the first `make ci` after a clone needs network access. Afterwards it is offline.
