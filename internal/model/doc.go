// SPDX-License-Identifier: Apache-2.0

// Package model is the independent, deliberately naive reference implementation of
// docs/SEMANTICS.md — a spec-derived in-memory broker (PLAN §3.3 / §5.1; ADR-0014 "an oracle
// that shares a source with the thing it checks is not an oracle").
//
// It exists to disagree with internal/queue and internal/store, so it imports nothing outside
// the standard library: TestModelIsIndependent enforces that over a production-scope
// `go list -deps`, and scripts/layers.sh forbids it from importing the queue or the store.
// Because it cannot share code with the subject it checks, a divergence between the two sides
// is evidence about the specification, not about a copied bug.
//
// The model is deliberately naive — maps, slices, linear scans — and the whole package stays
// within a hard non-blank/non-comment line budget across model.go + match.go
// (TestModelLineBudget), which is a smell detector rather than a target: raising it requires a
// why-line in the commit message.
//
// This suite is strictly sequential (decision D13 rejected porcupine): it proves the state
// machine, not the concurrency of the writer. Restart is an in-process close/reopen that
// demonstrates recovery (T9), not power loss; the durability half of invariant I1 belongs to
// #8 and #32.
package model
