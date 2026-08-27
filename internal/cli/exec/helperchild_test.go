// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

// Hermetic helper-child battery (issue #25 §11.3): every cross-process test
// re-execs THIS binary with MESSQ_TEST_CHILD=<behaviour>. No shell, no sleep,
// no jq — the suite must pass in a scratch container. Behaviours grow only as
// a test needs them; adding one means teaching this switch to speak it.
const envTestChild = "MESSQ_TEST_CHILD"

func TestMain(m *testing.M) {
	if bev := strings.TrimSpace(os.Getenv(envTestChild)); bev != "" {
		runHelperChild(bev)
		return
	}
	os.Exit(m.Run())
}

// runHelperChild executes the requested scripted behaviour inside the child.
// Every branch is deterministic: fixed exit codes, fixed stderr bytes, or a
// synchronous self-signal that kills the process outright.
func runHelperChild(behaviour string) {
	switch {
	case bevIs(behaviour, "exit0"):
		os.Exit(0)
	case bevIs(behaviour, "exit75"):
		fmt.Fprint(os.Stderr, "upstream 503")
		os.Exit(75)
	case bevIs(behaviour, "exit65"):
		fmt.Fprint(os.Stderr, "bad json at offset 12")
		os.Exit(65)
	case bevIs(behaviour, "exit77"):
		os.Exit(77)
	case bevIs(behaviour, "exit-other"):
		fmt.Fprint(os.Stderr, "mystery failure mode")
		os.Exit(42)
	case bevIs(behaviour, "exit137"):
		os.Exit(137)
	case bevIs(behaviour, "kill-self-term"):
		if err := unix.Kill(os.Getpid(), unix.SIGTERM); err != nil {
			os.Exit(9)
		}
		select {} // the SIGTERM must end us; hanging fails loudly via outer timeout
	default:
		fmt.Fprintf(os.Stderr, "messq-exec-helper: unknown behaviour %q\n", behaviour)
		os.Exit(9)
	}
}

// bevIs matches optional parameters ("exit0@pfx") against a base name; kept
// trivial today but lets richer behaviours pass arguments without new env vars.
func bevIs(behaviour, base string) bool { return behaviour == base }

// newTestChildProc returns the argv/env pair that re-execs the test binary as
// the given helper child behaviour.
func newTestChildProc(t *testing.T, behaviour string) ([]string, []string) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("executable path: %v", err)
	}
	return []string{exe}, append(os.Environ(), envTestChild+"="+behaviour)
}
