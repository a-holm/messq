// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
)

func defaultOpts(env []string) ChildOptions {
	return ChildOptions{
		Env:        env,
		StopSignal: syscall.SIGTERM,
		KillGrace:  200 * time.Millisecond,
		StderrCap:  4096,
	}
}

type runResult struct {
	run *ChildRun
	err error
}

// spawnAsync runs runChild off-loop with the given context; tests read one
// result and always receive exactly one — exactly-one-Wait is the contract.
func spawnAsync(ctx context.Context, clk clock.Clock, argv []string, payload []byte, opts ChildOptions) <-chan runResult {
	out := make(chan runResult, 1)
	go func() {
		run, err := runChild(ctx, clk, argv, payload, opts)
		out <- runResult{run: run, err: err}
	}()
	return out
}

// sleepPace paces through the Clock seam without ignoring its error.
func sleepPace(t *testing.T, clk clock.Clock, ctx context.Context, d time.Duration) {
	t.Helper()
	if serr := clk.Sleep(ctx, d); serr != nil {
		t.Fatalf("paced sleep interrupted: %v", serr)
	}
}

// processLive reports whether the pid holds a SCHEDULABLE kernel task: 'Z'
// zombies hold memory but no future; container PID1s rarely reap orphans, so
// demanding full reap would trade logic assertions for environmental noise.
func processLive(pid int) bool {
	raw, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return false // fully gone
	}
	s := string(raw)
	i := strings.LastIndexByte(s, ')')
	if i < 0 || i+2 >= len(s) {
		return false
	}
	state := s[i+2:]
	switch {
	case strings.HasPrefix(state, "Z"), strings.HasPrefix(state, "X"):
		return false // exited residue, not a live worker
	default:
		return true
	}
}

// Red killer S4-a: a child whose stderr floods FAR beyond the cap must never
// block, and the capture window must respect the byte budget exactly.
func TestChildStderrFloodBoundedAndNonBlocking(t *testing.T) {
	argv, env := newTestChildProc(t, "stderr-flood")
	env = append(env, "MESSQ_FLOOD_BYTES=6000000")

	guard, cancelGuard := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelGuard()
	res := spawnAsync(context.Background(), clock.System{}, argv, nil, defaultOpts(env))

	select {
	case r := <-res:
		run := r.run
		if run == nil {
			t.Fatalf("flood produced no run: %v", r.err)
		}
		if !run.State.Exited() || run.State.ExitCode() != 7 {
			t.Fatalf("flood child should finish on its own with exit 7, got %+v", run.State)
		}
		if got := len(run.Capture.raw()); got > 4096 || got == 0 {
			t.Fatalf("capture stored %d bytes, want 0 < n ≤ 4096", got)
		}
		if !run.Capture.truncated() {
			t.Fatal("truncation flag not raised although 6 MB poured through")
		}
	case <-guard.Done():
		t.Fatal("runChild did not return within guard budget: flood deadlocked the capture")
	}
}

// Red killer S4-b: the contract is the EXIT CODE, not input delivery — an
// instant-exit child that never reads stdin must still yield a clean exit 0.
// The oversize payload guarantees the pump hits EPIPE mid-stream.
func TestChildNoStdinReaderStillExitsZero(t *testing.T) {
	argv, env := newTestChildProc(t, "exit0")
	payload := make([]byte, 512*1024) // >> any pipe buffer, forces a broken pipe

	r := <-spawnAsync(context.Background(), clock.System{}, argv, payload,
		defaultOpts(env))
	if r.err != nil {
		t.Fatalf("EPIPE must be success, got run error %v", r.err)
	}
	if !r.run.State.Exited() || r.run.State.ExitCode() != 0 {
		t.Fatalf("child state %+v, want clean exit 0", r.run.State)
	}
}

// Red killer S4-c: TERM→grace→KILL to the whole GROUP. The child traps SIGTERM
// and leaves a grandchild blocking forever on stdin; only a group-wide KILL
// sweeps both. No-process-group/no-escalate mutants leave stragglers alive.
func TestGroupKillSweepTrappingTermAndGrandchild(t *testing.T) {
	argv, env := newTestChildProc(t, "trap-term-grandkid")
	clk := clock.System{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Deterministic settle via the Clock seam: ample time for the child to
	// install its TERM swallow, spawn the grandkid, announce it.
	res := spawnAsync(ctx, clk, argv, []byte("nobody reads me"), defaultOpts(env))
	sleepPace(t, clk, ctx, 500*time.Millisecond)
	cancel()

	r := <-res
	run := r.run
	if run == nil {
		t.Fatalf("sweep produced no run: %v", r.err) // only start failures do this
	}
	if !run.GroupLed() {
		t.Logf("host refused Setpgid; asserting the single-child death guarantee only")
	}
	raw := string(run.Capture.raw())
	i := strings.Index(raw, "GK=")
	if i < 0 {
		t.Fatalf("grandchild announced nothing (err=%v); capture=%q", r.err, raw)
	}
	fields := strings.Fields(raw[i:])
	if len(fields) == 0 || !strings.HasPrefix(fields[0], "GK=") {
		t.Fatalf("malformed announce near %q", raw[max(0, i-16):])
	}
	gk, cerr := strconv.Atoi(strings.TrimPrefix(fields[0], "GK="))
	if cerr != nil {
		t.Fatalf("cannot parse grandchild pid from %q: %v", fields[0], cerr)
	}

	if processLive(gk) && run.GroupLed() {
		st, _ := os.ReadFile("/proc/" + strconv.Itoa(gk) + "/stat") //nolint:errcheck // diagnostic only
		t.Fatalf("grandchild %d survived the sweep (err=%v); announce=%q; now=%q",
			gk, r.err, raw, st)
	}
	if ws, ok := run.State.Sys().(syscall.WaitStatus); ok {
		if !ws.Signaled() || (ws.Signal() != syscall.SIGKILL) {
			t.Fatalf("direct child must die by OUR KILL (TERM was swallowed), got %+v", run.State)
		}
	} else {
		t.Fatalf("terminal state lost: err=%v state=%+v", r.err, run.State)
	}
}

// Spawn failure surfaces as the teaching sentence, not as a panic: slash-bearing
// targets fork/exec directly (*fs.PathError), PATH-looked-up ones as *exec.Error.
func TestSpawnFailureSentence(t *testing.T) {
	opts := defaultOpts([]string{"PATH=/nonexistent"})
	_, err := runChild(context.Background(), clock.System{},
		[]string{"./definitely-missing-binary.sh"}, []byte(""), opts)
	if err == nil {
		t.Fatal("expected spawn failure")
	}
	var execErr *exec.Error
	var pathErr *fs.PathError
	if !errors.As(err, &execErr) && !errors.As(err, &pathErr) {
		t.Fatalf("want exec-failure taxonomy, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "no such file") {
		t.Fatalf("unexpected failure text %q", err)
	}

	// The sentence the runner will submit as nak reason renders verbatim.
	sent := (&ChildRun{startErr: fs.ErrNotExist}).spawnSentence([]string{"./h.sh"})
	want := "could not start ./h.sh: file does not exist"
	if sent != want {
		t.Fatalf("spawnSentence = %q, want %q", sent, want)
	}
}
