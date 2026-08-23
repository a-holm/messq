// SPDX-License-Identifier: Apache-2.0

//go:build !messq_fault

package api

// faultAfterCommit is the commit-vs-reply fault point (issue #7 §acceptance, generalised by
// #32 into build-tagged OS-level faults). In a normal build it does nothing: the fault is
// compiled in only under `-tags messq_fault`, so production binaries and the default test
// suite are unaffected.
func faultAfterCommit() {}
