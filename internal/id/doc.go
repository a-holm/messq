// SPDX-License-Identifier: Apache-2.0

// Package id mints and parses the two identifiers messq exposes.
//
// # Message ids
//
// A message id is a ULID (decision D10): 26 Crockford base32 characters, time-sortable, free
// of ambiguous characters, with the millisecond it was minted embedded in it. It is globally
// unique across streams, which is what makes it the spine of `messq trace <id>`: the same id
// survives every redelivery of a message, while the dead-letter copy, a redrive and a replay
// each mint a fresh id and carry the origin id in a provenance header. The trace id is what
// makes those hops one story (docs/SEMANTICS.md S4.4, decision D3).
//
// [Gen] is the factory. It never fails and never returns an error: publish must not be able to
// fail for id reasons. Two policies make that true.
//
//   - The wall clock moved backwards. The millisecond handed out never decreases, so the id
//     stays monotonic; the regression is reported through [WithClockRegressionHook] instead.
//     Refusing to mint would take the daemon down on an NTP step, and a slightly optimistic
//     timestamp is the cheaper wrong answer.
//   - The entropy inside one millisecond is exhausted, all 2^80 of it. The generator steps to
//     the next millisecond and draws again rather than surfacing the overflow.
//
// The ordering contract, stated so a test can assert it: for one [Gen], if New returns A
// before New returns B, established by the generator's own lock, then A.Compare(B) < 0. Across
// generators there is no ordering guarantee, and the daemon has exactly one.
//
// Parsing is case-insensitive because a user reads an id off a screenshot and types it back;
// rendering is always upper case.
//
// # Trace ids
//
// A trace id is the 16-byte W3C Trace Context identifier, rendered as 32 lowercase hex
// characters. It is taken from the Messq-Trace-Id header, or parsed out of a W3C traceparent,
// or minted at publish; it is stored on the message, echoed on every delivery, carried into
// the dead-letter copy and stamped on every event row, so one grep spans every service
// (PLAN.md section 7).
//
// [ParseTraceparent] never returns an error. An unparsable header means "mint a fresh trace
// id", which is the behaviour the specification asks for and the only one that keeps a
// malformed upstream header from failing a publish.
package id
