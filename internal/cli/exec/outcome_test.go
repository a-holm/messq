// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

// waitedChild spawns the named helper-child behaviour and waits it out,
// returning the completed command — cmd.ProcessState is populated exactly as
// Classify will see it in production. The outer context exists ONLY as the
// hang guard: if it ever fires, a framework bug made the child unkillable,
// and the test must fail rather than hang.
func waitedChild(t *testing.T, behaviour string) *exec.Cmd {
	t.Helper()
	argv, env := newTestChildProc(t, behaviour)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, argv[0])
	cmd.Env = env
	done := make(chan error, 1)
	go func() { done <- cmd.Run() }()
	if err := <-done; err != nil {
		var ee *exec.ExitError
		if !errors.As(err, &ee) && ctx.Err() == nil {
			t.Fatalf("child %q run failed hard: %v", behaviour, err)
		}
	}
	if ctx.Err() != nil {
		t.Fatalf("helper child %q hung: guard context fired", behaviour)
	}
	return cmd
}

func TestClassifyTable(t *testing.T) {
	exit0 := waitedChild(t, "exit0").ProcessState
	exit75 := waitedChild(t, "exit75").ProcessState
	exit65 := waitedChild(t, "exit65").ProcessState
	exit77 := waitedChild(t, "exit77").ProcessState
	exit42 := waitedChild(t, "exit-other").ProcessState
	exit137 := waitedChild(t, "exit137").ProcessState
	sigTerm := waitedChild(t, "kill-self-term").ProcessState

	tests := []struct {
		name     string
		st       *os.ProcessState
		kind     CauseKind
		detail   string
		stderr   string
		wantOut  Outcome
		wantCode int
		wantSig  syscall.Signal
		wantSubs []string // substrings the Reason must contain, in order
	}{
		{
			name: "exit0 acks", st: exit0, wantOut: OutcomeAck, wantCode: 0,
			wantSubs: nil,
		},
		{
			name: "exit75 naks carrying stderr verbatim",
			st:   exit75, stderr: "upstream 503",
			wantOut: OutcomeNak, wantCode: 75, wantSubs: []string{"upstream 503"},
		},
		{
			// THE refinement row of this slice: ONLY 65 terminates. If anything
			// from the operator-misconfig block (77 EX_NOPERM, 78 EX_CONFIG)
			// maps to term, fine payloads die over an environment bug.
			name: "exit77 stays nak, never term", st: exit77, stderr: "",
			wantOut: OutcomeNak, wantCode: 77,
			wantSubs: []string{"exit 77"},
		},
		{
			name: "exit65 terms toward DLQ", st: exit65, stderr: "bad json at offset 12",
			wantOut: OutcomeTerm, wantCode: 65, wantSubs: []string{"bad json at offset 12"},
		},
		{
			name: "other non-zero prefixes exit code", st: exit42, stderr: "mystery failure mode",
			wantOut: OutcomeNak, wantCode: 42,
			wantSubs: []string{"exit 42", "mystery failure mode"},
		},
		{
			// --exec-shell's signalled grandchild surfaces as plain exit 137;
			// no 128+N special-casing (mapping is on the DIRECT child only).
			name: "shell 137 is ordinary nak", st: exit137, stderr: "",
			wantOut: OutcomeNak, wantCode: 137, wantSubs: []string{"exit 137"},
		},
		{
			name: "killed by signal names it", st: sigTerm, stderr: "partial log line",
			wantOut: OutcomeNak, wantCode: -1, wantSig: syscall.SIGTERM,
			wantSubs: []string{"killed by SIGTERM", "partial log line"},
		},
		{
			// Cause beats wait status: our own escalation SIGKILL must not read
			// as "killed by SIGKILL" feedback about the payload.
			name: "exec timeout wins over killed state", kind: CauseExecTimeout,
			detail: "1h0m0s", stderr: "still working on it",
			wantOut: OutcomeNak, wantCode: -1,
			wantSubs: []string{"exec timeout after 1h0m0s", "still working on it"},
		},
		{
			name: "lease loss abandons regardless of state", kind: CauseLeaseLoss, st: exit0,
			wantOut: OutcomeAbandon, wantCode: 0, wantSubs: nil,
		},
		{
			name: "runtime spawn failure naks with teaching text",
			kind: CauseSpawnFailure, detail: "./h.sh: resource temporarily unavailable",
			wantOut: OutcomeNak, wantCode: -1,
			wantSubs: []string{"could not start ./h.sh", "resource temporarily unavailable"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Classify(tt.st, tt.kind, tt.detail, tt.stderr)
			if got.Outcome != tt.wantOut {
				t.Fatalf("Classify(%v).Outcome = %v, want %v", tt.name, got.Outcome, tt.wantOut)
			}
			if got.ExitCode != tt.wantCode {
				t.Fatalf("ExitCode = %d, want %d", got.ExitCode, tt.wantCode)
			}
			if got.Signal != tt.wantSig {
				t.Fatalf("Signal = %v, want %v", got.Signal, tt.wantSig)
			}
			last := 0
			for _, sub := range tt.wantSubs {
				idx := strings.Index(got.Reason[last:], sub)
				if idx < 0 {
					t.Fatalf("Reason %q lacks substring %q in order", got.Reason, sub)
				}
				last += idx + len(sub)
			}
			if tt.wantOut == OutcomeAck && got.Reason != "" {
				t.Fatalf("ack must carry no reason, got %q", got.Reason)
			}
		})
	}
}

// Red killer S2: exit 77 (EX_NOPERM) must be an ordinary RETRYABLE nak. A
// whole-sysexits-block mapping dead-letters healthy payloads over operator
// misconfiguration — precisely the failure mode Decision 1 bans.
func TestClassifySevenSevenIsNeverTerm(t *testing.T) {
	st := waitedChild(t, "exit77").ProcessState
	got := Classify(st, CauseNone, "", "")
	if got.Outcome == OutcomeTerm {
		t.Fatal("exit 77 classified as term: sysexits-block mutant alive")
	}
	if got.Outcome != OutcomeNak || !strings.HasPrefix(got.Reason, "exit 77") {
		t.Fatalf("exit 77 → %+v, want nak with \"exit 77\" reason", got)
	}
}

// signalNames table sanity: names render uppercase without syscall's lowercase
// prose, and unknown values degrade numerically instead of panicking.
func TestSignalNameRenderings(t *testing.T) {
	tests := []struct {
		sig  syscall.Signal
		want string
	}{
		{syscall.SIGTERM, "SIGTERM"},
		{syscall.SIGKILL, "SIGKILL"},
		{syscall.SIGUSR1, "SIGUSR1"},
		{syscall.Signal(63), "signal 63"}, // unknown degrades numerically
	}
	for _, tt := range tests {
		if got := signalName(tt.sig); got != tt.want {
			t.Fatalf("signalName(%v) = %q, want %q", tt.sig, got, tt.want)
		}
	}
}
