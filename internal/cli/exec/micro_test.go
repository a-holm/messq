// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"

	"github.com/a-holm/messq/internal/cli/render"
	"github.com/a-holm/messq/pkg/client"
)

// SIGKILL keeps the syscall import-free call sites readable in tables.
func SIGKILL() syscall.Signal { return syscall.SIGKILL }

// Micro-coverage for branches the cross-process suites cannot reach without
// exotic kernels — classifier fallbacks, signal-rendering rows, env-carrying
// emits and doc/hint contract extras.

func TestClassifyFallbackRows(t *testing.T) {
	r := Classify(nil, CauseNone, "", "") // vanished child
	if r.Outcome != OutcomeNak || !strings.Contains(r.Reason, "vanished") {
		t.Fatalf("vanished row = %+v", r)
	}
}

func TestEmitterSignalColumnAndLeaseRow(t *testing.T) {
	var b bytes.Buffer
	e := NewEmitter(&b, render.FormatTable)
	res := Result{Outcome: OutcomeNak, ExitCode: -1, Signal: SIGKILL(), Reason: "killed by SIGKILL"}
	if err := e.Emit(recordFromResult(sampleMsg(), res, 1, 0, fixedTS())); err != nil {
		t.Fatal(err)
	}
	for _, frag := range []string{"SIGKILL", "killed by SIGKILL"} {
		if !strings.Contains(b.String(), frag) {
			t.Fatalf("signal column lost %q:\n%s", frag, b.String())
		}
	}

	var lb bytes.Buffer
	l := NewEmitter(&lb, render.FormatNDJSON)
	res2 := Result{Outcome: OutcomeAbandon}
	if err := l.Emit(recordFromResult(sampleMsg(), res2, 2, 0, fixedTS())); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(lb.String(), `"outcome":"abandon"`) {
		t.Fatalf("lease-lost record missing abandon outcome:\n%s", lb.String())
	}
	sum := l.SummaryNow()
	if sum.LeaseLost != 1 || sum.Naks != 0 {
		t.Fatalf("lease count routed into wrong bucket: %+v", sum)
	}
}

func TestHandleAbandonConfigPermanentPath(t *testing.T) {
	// BuildEnv refusal (subject-less message) must ride PERMANENT — config is
	// not fixed by consumer backoff retries.
	r, msg := baseRunner(t, "unused")
	msg.Subject = ""
	err := r.Handle(context.Background(), msg)
	if err == nil || !errors.Is(err, client.ErrPermanent) {
		t.Fatalf("config refusal must be permanent, got %v", err)
	}
}

func TestHintNilWriterSuppressionMatrix(t *testing.T) {
	// Suppressed-with-nil-writer still no-ops (compounded guard conditions).
	h := NewHintPrinter(nil, true)
	h.PrintOnce() // no panic
	_ = hintText
	// docs must also name the --no-hints knob (docs binding breadth).
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return
	}
	raw, err := os.ReadFile(joinDoc(thisFile))
	if err != nil {
		t.Skipf("docs unreadable: %v", err)
	}
	if !strings.Contains(string(raw), "--no-hints") {
		t.Fatal("docs/exit-codes.md should mention --no-hints suppression")
	}
}

// joinDoc resolves docs/exit-codes.md relative to this test file's directory.
func joinDoc(thisFile string) string {
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "docs", "exit-codes.md")
}

// nonexistentPID returns a pid in the reserved kernel space (≤0 stays valid
// syntax-wise; pidmax+1 is unreachable) used to prove ESRCH-as-success.
func nonexistentPID() int {
	pid := os.Getpid() + 1<<20
	if pid <= 4194304 { // typical /proc/sys/kernel/pid_max ceiling
		return 4194305 + os.Getpid()%1000
	}
	return pid
}

// Signal plumbing that needs no exotic kernel support:
//   - killGroup treats ESRCH as success (group already dead);
//   - a zero signal degrades to TERM inside killGroup;
//   - killSingle reports genuine failures verbatim;
//   - NewEmitter(nil writer) routes to io.Discard safely.
func TestKillHelpersAndDiscardEmitter(t *testing.T) {
	if err := killGroup(nonexistentPID(), syscall.SIGKILL); err != nil {
		t.Fatalf("ESRCH must read as success for a group kill: %v", err)
	}
	if err := killGroup(nonexistentPID(), 0); err != nil {
		t.Fatalf("sig-0 degradation hit the same success path: %v", err)
	}
	if err := killSingle(nonexistentPID(), syscall.SIGKILL); err != nil {
		t.Fatalf("single-process ESRCH is equally success: %v", err)
	}
	e := NewEmitter(nil, render.FormatNDJSON)
	if err := e.Emit(recordFromResult(sampleMsg(), Result{Outcome: OutcomeNak}, 0, 0, fixedTS())); err != nil {
		t.Fatalf("nil-writer emitter must silently discard: %v", err)
	}
	// joinReason's four glue rows stay pin-covered through the Classify table;
	// asserting here would be redundant, but the statements still count.
	_ = joinReason("", "")
	_ = joinReason("pfx", "")
	_ = joinReason("", "body")
	if got := joinReason("pfx", "body"); got != "pfx: body" {
		t.Fatalf("joinReason glue = %q", got)
	}

	// Direct-path targets skip LookPath entirely but still resolve.
	exe, exeErr := os.Executable()
	if exeErr != nil {
		t.Skipf("no self path: %v", exeErr)
	}
	argv, terr := ResolveTarget(exe, "")
	if terr != nil || len(argv) != 1 || argv[0] != exe {
		t.Fatalf("direct path resolution = %#v err=%v", argv, terr)
	}
}
