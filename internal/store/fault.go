// SPDX-License-Identifier: Apache-2.0

package store

import "sync/atomic"

// The fault-point seam of the dead path (issue #12 §16, exercised by #32). faultHook is
// a process-wide, nil-by-default callback that the crash harness (or a test) can install
// to SIGKILL / abort the writer at a precise point INSIDE the one dead-letter
// transaction. Because every declared point sits inside the caller's already-open
// transaction, killing at any of them must leave the message either fully in the DLQ
// (delivery row gone) or fully pending — never both, never neither (G1). #32 activates
// them under the messq_fault build tag; here they are a cheap, atomic, no-op seam.

// Fault points declared here (issue #12 §16), each invoked at a precise point INSIDE the
// one dead-letter transaction; #32 asserts the set when it activates the messq_fault
// grammar:
//
//	dlq.before_copy               before the copy INSERT…SELECT (DLQ exists, origin read)
//	dlq.after_copy                after the copy, before the delivery-row delete
//	dlq.before_delete             before the delivery-row delete in the calling command
//	dlq.after_commit_before_reply after the batch commit, before the reply (engine-level, #32)

// faultHook runs the installed fault callback, if any, for the named point. No-op when
// none is installed. Runs on the writer goroutine inside the transaction.
func faultHook(point string) {
	if h := faultHookValue.Load(); h != nil {
		(*h)(point)
	}
}

// faultHookValue holds the installed callback. Atomic so the harness can swap it in while
// the writer goroutine runs without a data race.
var faultHookValue atomic.Pointer[func(string)]

// SetFaultHook installs (or clears, with nil) the dead-path fault callback.
func SetFaultHook(fn func(string)) {
	if fn == nil {
		faultHookValue.Store(nil)
		return
	}
	faultHookValue.Store(&fn)
}
