# 0014. Test with a pure state machine, an independent oracle and a crash harness

- Status: Accepted
- Date: 2026-08-21
- Adjudicates: D13
- Relates to: PLAN.md section 2 D13, PLAN.md section 11, issue #4, issue #13, issue #32

## Context

The value proposition is trustworthiness. For a queue, that means the test suite *is* the product: a guarantee without a named passing test is marketing.

The constraint is one developer and a pull-request lane budgeted at ten minutes of wall clock. Every technique has to earn its minutes.

The failure modes that matter are specific to decision D1's single SQLite file: process kill, a torn WAL tail, `ENOSPC`, `EIO` on commit, a clock jump, and a client disconnecting mid-fetch. Nine fault families were proposed; six survive that filter.

## Decision

The state machine in `internal/queue` is a pure function: no I/O, no `time.Now`, no map-iteration-order dependence. That single choice makes property testing, the reference model and deterministic timing tests all cheap, so it is the load-bearing decision here rather than a style preference.

On top of it:

- An **independent reference model** in `internal/model`, written from `docs/SEMANTICS.md` and not from the implementation, driven alongside the real broker by `pgregory.net/rapid` through random sequences of publish, fetch, ack, nak, term, extend, timeout, seek, purge and **restart**. Every invariant of `SEMANTICS.md` S15 is checked after every action. Failing seeds are committed as the regression corpus.
- A **SIGKILL crash harness** running a real `messq serve` subprocess against a real data directory, killing at random and at named fault points, restarting, and asserting against a **three-valued external ledger**: OK must exist, FAILED must not exist, UNKNOWN may go either way. Three-valued is the only correct oracle for at-least-once under crashes.
- **No `time.Sleep` in tests** outside the soak lane, enforced by a lint rule. Timing uses `testing/synctest` bubbles or the `internal/clock` seam.
- `-race` everywhere. Fuzzing on every parser. `testscript` CLI goldens, so that every documented command block executes in CI and documentation cannot rot. Golden log and metric schema tests. Coverage floors per package, with no vanity global number.
- Every bug fix lands with a committed reproduction artifact: a rapid failfile, a seed or a fuzz input.

## Alternatives

| Option | Why it was serious | Why it lost |
|---|---|---|
| Mutation testing | It measures whether the tests actually constrain the code, which coverage does not. | Minutes per run against a lane budgeted at ten, for a signal the property suite already provides more directly. |
| Porcupine linearizability checking | The rigorous way to check a concurrent history. | messq has one writer and therefore a total order by construction. There is no linearizability question to answer. |
| LazyFS or `dm-log-writes` as merge gates | They find real torn-write bugs that a plain SIGKILL never reaches. | They are slow and environment-specific. They belong in the nightly lane, not on the critical path of every pull request. |
| A deterministic simulator such as gosim | It makes concurrency bugs reproducible, which is the hardest class. | The concurrency model is one writer, a sweeper, a janitor and a fan-out. `testing/synctest` covers the choreography, and the simulator's cost is a second execution model to maintain. |
| A reference model derived from the implementation | It is far cheaper to write and stays in sync automatically. | An oracle that shares a source with the thing it checks is not an oracle. The model is written from the specification, which is why the specification had to be merged before the feature code. |
| Testify or another assertion DSL | Faster to write, familiar to everyone. | `go-cmp` gives readable diffs without a second vocabulary, and the dependency budget is eight rows. |

## Consequences

`internal/queue` must stay pure. `scripts/layers.sh` enforces it: the package may not reach `database/sql`, `net/http`, `os` or any layer above it, and the rule is checked with the test binary linked in, so a test that imports the store is caught too.

The reference model must not import `internal/queue` or `internal/store`. That is also a layer rule, and it is what keeps the oracle independent.

Property tests find the real bugs, and they find them as a seed rather than as a description. The committed failfile corpus turns each into a permanent regression test.

The cost is discipline that never relaxes: a timing test that reaches for `time.Sleep` fails lint, and a guarantee added without a named test is an incomplete change.

## Revisit trigger

The pull-request lane crossing ten minutes of wall clock. The answer is to shard a job or move a suite to nightly, never to weaken a gate. `make ci` prints its per-target wall clock so the crossing is visible before it is painful.
