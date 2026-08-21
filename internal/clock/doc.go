// SPDX-License-Identifier: Apache-2.0

// Package clock is where messq reads the operating-system clock.
//
// Everything that observes time takes a [Clock]; everything that decides takes now as a
// parameter, which is what keeps internal/queue a pure function (PLAN.md section 3.3). The
// forbidigo rule configured in .golangci.yml bans time.Now, time.Since, time.Until,
// time.NewTimer, time.NewTicker, time.After, time.Tick, time.AfterFunc and time.Sleep
// everywhere else in the tree.
//
// Inside this package the allowance is narrower still: system.go is the one file that calls
// them. The repository holds exactly one other caller, internal/tools/vulngate/main.go, which
// is a build-gate command rather than the daemon and whose -now flag is that command's own
// seam. TestWallClockCallsAreConfinedToTheSeam walks the whole tree and enforces both halves
// of that sentence: a third caller fails, and so does an allowance that stopped being needed.
//
// # Wall clock or monotonic
//
// Durable deadlines (deliveries.visible_at, messages.published_at, events.ts) are wall-clock
// Unix milliseconds, because they must survive a restart and be readable in SQL. Read them
// with [Clock.Now]. In-process waits are monotonic: read them with [Clock.Since], whose result
// is unaffected by an NTP step because a time.Time carries a monotonic reading.
//
// A forward wall-clock jump therefore makes in-flight deliveries look overdue and the sweeper
// redelivers them. That is correct at-least-once behaviour: every such redelivery records
// reason=timeout, and the sweeper's per-tick expiry cap keeps it from becoming a stampede. A
// backwards jump delays expiry; it never resurrects a resolved delivery.
//
// # Choosing between Fake and testing/synctest
//
//   - Goroutine choreography (sweeper tick, long-poll park and wake, group-commit window,
//     --exec heartbeat): a synctest.Test bubble with [System]. Inside a bubble the real time
//     package is already virtualized, and synctest.Wait removes the "has it armed its timer
//     yet?" race.
//   - A struct driven step by step (state machine, backoff schedule, id generator): [NewFake].
//   - Time that must move backwards (clock regression, doctor checks): [NewFake]. A synctest
//     bubble cannot do this.
//   - Never time.Sleep. The lint rule enforces it.
//
// # Fake determinism
//
// Due timers fire in (deadline, arming sequence) order, never in map order. [Fake.Advance]
// delivers on buffered size-one channels and never blocks on an unread channel.
// Advance(0) fires timers armed for exactly now. [Timer.Reset] re-arms with a fresh sequence
// number. Stop on an already-fired timer reports false. A ticker fires at most once per
// Advance and its next deadline stays on the period grid, which is how a real ticker behaves
// when its reader falls behind.
package clock
