// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/a-holm/messq/internal/cli/uierr"
)

// Startup guards (issue #25 §2/§3): flag conflicts, target resolution and
// concurrency clamps. Everything here runs BEFORE the first fetch — burning
// max_deliver on a typo'd binary is "the single worst failure mode this feature
// can have", so every refusal below is teaching-shaped and message-free.

// ExecConfig is the validated view of `messq sub --exec ...` flags; the sub
// command lane (#24) populates it from cobra flags + env fallbacks.
type ExecConfig struct {
	Cmd           string
	Shell         bool
	Manual        bool // --manual chosen for non-exec mode handling by #24
	AutoAck       bool
	Concurrency   int
	MaxAckPending int  // consumer view value; 0 = unknown (skip clamp)
	Ordered       bool // ordered=1 consumer (#38 enforces properly)
	NoHints       bool
}

// ValidateFlags rejects the documented mutual exclusions verbatim:
//
//	--exec + --manual   → usage error 2 naming the pick-one rule
//	--exec + --auto-ack → usage error 2 (exec acks on exit 0 itself)
func ValidateFlags(c ExecConfig) error {
	if c.Cmd == "" {
		return nil // not exec mode; nothing to validate here
	}
	switch {
	case c.Manual:
		return uierr.Usage("--exec is on and --manual prints ack tokens for you to settle by hand; --exec settles from the child's exit code. Pick one.")
	case c.AutoAck:
		return uierr.Usage("--exec already acks on exit 0; --auto-ack would ack before the child ran. Pick one.")
	}
	return nil
}

// ResolveTarget finishes startup validation: argv resolution plus LookPath once,
// before any fetch. A missing/non-executable target exits 1 with zero messages
// consumed.
func ResolveTarget(cmd, shellPath string) ([]string, error) {
	argv, err := ResolveArgv(cmd, shellPath)
	if err != nil {
		return nil, err
	}
	if shellPath != "" {
		return argv, nil // shell itself was LookPath'd by the caller's seam choice
	}
	path := argv[0]
	if _, lookErr := os.Stat(path); lookErr == nil {
		// Direct path exists; executability failure surfaces at fork/exec as a
		// classified runtime spawn refusal — still zero messages consumed.
		return argv, nil
	}
	resolved, lookErr := exec.LookPath(path)
	if lookErr != nil {
		return nil, uierr.Usage("--exec %q: %s (fix the path or install the worker)", cmd, lookErr.Error())
	}
	argv[0] = resolved
	return argv, nil
}

// ClampConcurrency applies §7: N clamps to max_ack_pending with a printed note
// naming the knob; ordered consumers cap to 1. Unknown server bounds pass
// through untouched — the daemon still enforces its own hold_reason row.
func ClampConcurrency(n, maxAckPending int, ordered bool) (effective int, note string) {
	effective = n
	switch {
	case n <= 0:
		effective = 1
	case ordered && n > 1:
		effective = 1
		note = "note: ordered consumer caps --concurrency to 1; raise per-subject throughput after #38 lands"
	case maxAckPending > 0 && n > maxAckPending:
		effective = maxAckPending
		note = fmt.Sprintf("note: --concurrency %d exceeds consumer max_ack_pending=%d; running %d workers.\n      raise it with: messq consumer edit <stream> <consumer> --max-ack-pending %d",
			n, maxAckPending, effective, effective*4)
	}
	return effective, note
}
