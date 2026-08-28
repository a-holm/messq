// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/pkg/client"
)

// baseRunner wires a Runner whose --exec target IS this test binary; the
// battery behaviour rides through AfterBuildEnv so the full BuildEnv contract
// (parent stripping, header mangling) is exercised even at Handle level.
func baseRunner(t *testing.T, behaviour string) (*Runner, *client.Delivered) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("executable path: %v", err)
	}
	r := &Runner{
		Opts: RunnerOptions{
			Cmd:           exe,
			ParentEnv:     []string{"PATH=/usr/bin", "HOME=" + t.TempDir()},
			StopSignal:    syscall.SIGTERM,
			KillGrace:     150 * time.Millisecond,
			StderrBytes:   4096,
			AfterBuildEnv: []string{envTestChild + "=" + behaviour},
		},
	}
	msg := &client.Delivered{
		Stream:     "orders",
		Consumer:   "worker",
		Seq:        1,
		ID:         "01J8ZQ4K2M9V0X7Y3B5N6C8D1E",
		Subject:    "orders.eu.created",
		AckToken:   "orders/worker/1/1",
		Attempt:    1,
		MaxDeliver: 5,
		TraceID:    traceHex,
	}
	return r, msg
}

func TestHandleAckOnExit0(t *testing.T) {
	r, msg := baseRunner(t, "exit0")
	if err := r.Handle(context.Background(), msg); err != nil {
		t.Fatalf("exit0 must ack (nil), got %v", err)
	}
	if got := r.SpawnStreak(); got != 0 {
		t.Fatalf("ack must reset spawn streak, got %d", got)
	}
}

// Red killer S5-a — the brief's own named mutant: Handle returning nil on a
// 75 exit would silently ACK a failed payload.
func TestHandleExit75NaksNotAcks(t *testing.T) {
	r, msg := baseRunner(t, "exit75")
	err := r.Handle(context.Background(), msg)
	if err == nil {
		t.Fatal("Handle returned nil on exit 75: silent-ACK mutant alive")
	}
	if errors.Is(err, client.ErrPermanent) {
		t.Fatalf("75 must ride consumer backoff, not term: %v", err)
	}
	if !strings.Contains(err.Error(), "upstream 503") {
		t.Fatalf("nak lost the captured stderr: %v", err)
	}
}

// Red killer S5-b: 65 is the ONLY poison code and it must reach the DLQ after
// ONE attempt, i.e. come back wrapped as client.Permanent.
func TestHandleTermOn65IsPermanent(t *testing.T) {
	r, msg := baseRunner(t, "exit65")
	err := r.Handle(context.Background(), msg)
	if err == nil || !errors.Is(err, client.ErrPermanent) {
		t.Fatalf("exit 65 must be client.Permanent(reason), got %T/%v", err, err)
	}
	if !strings.Contains(err.Error(), "bad json at offset 12") {
		t.Fatalf("term reason lost stderr: %v", err)
	}
}

// Red killer S5-c — G4's killer: with the lease already fenced, NOTHING may be
// settled. The only legal return wraps client.ErrLeaseLost (Worker maps that to
// 'settle nothing' in runHandler). Timing rides the Clock seam.
func TestHandleLeaseLostAbandonsNeverSettles(t *testing.T) {
	r, msg := baseRunner(t, "trap-term-grandkid")
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	fenceLater := func() {
		if serr := (clock.System{}).Sleep(ctx, 120*time.Millisecond); serr == nil {
			cancel(client.ErrLeaseLost)
		}
	}
	go fenceLater()
	err := r.Handle(ctx, msg)
	if err == nil || !errors.Is(err, client.ErrLeaseLost) {
		t.Fatalf("lease loss MUST wrap client.ErrLeaseLost (settle nothing), got %v", err)
	}
}

// A plain outside-cancel is NOT lease loss: message feedback still applies.
func TestHandleOutsideCancelStillClassifiesExit(t *testing.T) {
	r, msg := baseRunner(t, "exit77")
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancelled BEFORE call: machinery must STILL classify honestly
	err := r.Handle(ctx, msg)
	switch {
	case err == nil:
		t.Fatal("cancelled run still produced a classified nak expected")
	case strings.Contains(err.Error(), ErrExecTimeoutCause.Error()):
		t.Fatalf("outside cancel misread as timeout: %v", err)
	case errors.Is(err, client.ErrLeaseLost):
		t.Fatalf("outside cancel misread as lease loss: %v", err)
	case errors.Is(err, client.ErrPermanent):
		t.Fatalf("plain cancel must yield ordinary nak for exit 77, got term: %v", err)
	}
}

// --exec-timeout fires first on an ignoring child: reason carries the §3 text.
func TestHandleExecTimeoutReason(t *testing.T) {
	r, msg := baseRunner(t, "trap-term-grandkid")
	r.Opts.ExecTimeout = 80 * time.Millisecond
	err := r.Handle(context.Background(), msg)
	if err == nil || !strings.Contains(err.Error(), ErrExecTimeoutCause.Error()) {
		t.Fatalf("want exec-timeout nak sentence, got %v", err)
	}
	if !strings.Contains(err.Error(), "after 80ms") {
		t.Fatalf("reason lost its duration detail: %v", err)
	}
}

// Runtime spawn failures naks with RetryAfter(5s) and feed the breaker counter.
func TestHandleSpawnFailureRetriesAfterFiveSeconds(t *testing.T) {
	r, msg := baseRunner(t, "unused")
	r.Opts.Cmd = filepath.Join(t.TempDir(), "no-such-worker.sh")
	first := r.Handle(context.Background(), msg)
	var ra *client.RetryAfterError
	if !errors.As(first, &ra) {
		t.Fatalf("spawn failure must RetryAfter, got %T: %v", first, first)
	}
	if ra.Delay != 5*time.Second {
		t.Fatalf("spawn retry delay = %s, want 5s", ra.Delay)
	}
	if got := r.SpawnStreak(); got != 1 {
		t.Fatalf("streak = %d after one failure", got)
	}
	before := r.SpawnStreak()
	err2 := r.Handle(context.Background(), msg) // fails again → streak climbs
	if err2 == nil || !errors.As(err2, &ra) {
		t.Fatalf("second failure must also RetryAfter: %v", err2)
	}
	if r.SpawnStreak() != before+1 {
		t.Fatalf("consecutive failures must count: %d -> %d", before, r.SpawnStreak())
	}
}

func TestResolveArgvRules(t *testing.T) {
	t.Run("plain splits", func(t *testing.T) {
		got, err := ResolveArgv(`./h.sh --mode fast`, "")
		if err != nil || len(got) != 3 || got[0] != "./h.sh" {
			t.Fatalf("ResolveArgv = %#v err=%v", got, err)
		}
	})
	t.Run("shell delegates quoting", func(t *testing.T) {
		got, err := ResolveArgv("echo 'a b'", "/bin/dash")
		if err != nil || len(got) != 3 || got[1] != "-c" {
			t.Fatalf("shell argv = %#v err=%v", got, err)
		}
	})
	t.Run("empty refused", func(t *testing.T) {
		if _, err := ResolveArgv("   ", ""); err == nil {
			t.Fatal("empty command must fail startup-side")
		}
	})
}

func TestTraceparentOfValidation(t *testing.T) {
	cases := []struct{ trace, span, want string }{
		{traceHex, "1234567890abcdef", "00-" + traceHex + "-1234567890abcdef-01"},
		{"short", "1234567890abcdef", ""},
		{traceHex, "toolongspanid!!", ""},
	}
	for _, c := range cases {
		got := traceparentOf(c.trace, c.span)
		if got != c.want {
			t.Fatalf("traceparentOf(%q,%q)=%q want %q", c.trace, c.span, got, c.want)
		}
	}
}
