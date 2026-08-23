// SPDX-License-Identifier: Apache-2.0

//go:build messq_fault

package api

import (
	"os"
	"strconv"
	"strings"
	"sync/atomic"
)

// publishOrdinal counts successful publishes through this process, so MESSQ_FAULT can arm the
// fault on the Nth one rather than always the first.
var publishOrdinal atomic.Int64

// faultAfterCommit is the commit-vs-reply fault point (issue #7 §acceptance, generalised by
// #32). It exists only under `-tags messq_fault` and is armed by MESSQ_FAULT. When armed for
// the Nth successful publish it terminates the process after that publish's group-commit fsync
// has returned but before the 2xx is written to the client — the exact seam that makes
// "a 2xx publish is a durability promise" (D4) observable.
//
// A client that saw a 2xx has necessarily passed this point, so its message is committed; a
// message whose reply never arrived may be on either side of the fault (present if its commit
// completed first, absent if the process died before it). Both legs are asserted by the crash
// tests in internal/cli.
//
//	MESSQ_FAULT=commit_before_reply[:N]  terminate on the Nth successful publish (default 1)
func faultAfterCommit() {
	spec := os.Getenv("MESSQ_FAULT")
	if spec == "" {
		return
	}
	name, n := spec, int64(1)
	if i := strings.IndexByte(spec, ':'); i >= 0 {
		name = spec[:i]
		if v, err := strconv.ParseInt(spec[i+1:], 10, 64); err == nil && v > 0 {
			n = v
		}
	}
	if name != "commit_before_reply" {
		return
	}
	if publishOrdinal.Add(1) == n {
		// The fault IS the process boundary: a deliberate crash between commit and reply, not
		// a Run-style exit-code mapping.
		os.Exit(1) //nolint:forbidigo // crash seam behind messq_fault; not an exit-code mapping
	}
}
