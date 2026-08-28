// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"golang.org/x/sys/unix"
)

// Sabotage switches: compile-time mutants used ONLY to demonstrate slice-4
// killers red-first; every commit lands with both false.
const (
	sabotageNoGroup    = false
	sabotageNoEscalate = false
)

// ChildOptions carries everything one child run needs besides argv/payload.
// Env arrives pre-built from BuildEnv; Stdout decides inherit/discard/mirror
// by which writer the caller hands over.
type ChildOptions struct {
	Dir        string         // working directory
	Env        []string       // complete environment (BuildEnv output)
	StopSignal syscall.Signal // signal to the PROCESS GROUP on stop (TERM default)
	KillGrace  time.Duration  // between stop signal and SIGKILL, Clock-seam timed
	StderrCap  int            // capture budget in bytes (ClampReasonCap applied)
	StderrMode StderrMode     // head keeps first bytes, tail keeps last
	Stdout     io.Writer      // nil discards; os.Stdout inherits; render pipes mirror
}

// captured is the bounded stderr sink: it ALWAYS drains — a blocked capture
// would hang children whose stderr floods — stores at most cap bytes worth,
// and remembers whether anything was thrown away. Mutex-guarded because the
// exec stdio goroutine writes it while tests may legitimately peek mid-run.
type captured struct {
	mu         sync.Mutex
	cap        int
	mode       StderrMode
	buf        []byte // head mode: prefix store; tail mode: sliding window
	droppedOff int64  // bytes observed past capacity (head mode)
	totalIn    int64
}

func newCaptured(capBytes int, mode StderrMode) *captured {
	return &captured{cap: ClampReasonCap(capBytes), mode: mode}
}

func (c *captured) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writeLocked(p)
}

func (c *captured) writeLocked(p []byte) (int, error) {
	c.totalIn += int64(len(p))
	if c.cap <= 0 || len(p) == 0 {
		return len(p), nil
	}
	switch c.mode {
	case HeadStderr:
		free := c.cap - len(c.buf)
		if free > 0 {
			take := free
			if take > len(p) {
				take = len(p)
			}
			c.buf = append(c.buf, p[:take]...)
			return len(p), nil
		}
		c.droppedOff += int64(len(p))
		return len(p), nil
	case TailStderr:
		take := len(p)
		if take > c.cap {
			take = c.cap
			p = p[len(p)-take:]
		}
		c.buf = append(c.buf, p...)
		if extra := len(c.buf) - c.cap; extra > 0 {
			c.buf = append([]byte(nil), c.buf[extra:]...)
		}
		return take, nil
	default:
		return len(p), nil
	}
}

func (c *captured) truncated() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch c.mode {
	case HeadStderr:
		return c.droppedOff > 0 || c.totalIn > int64(len(c.buf))
	case TailStderr:
		return c.totalIn > int64(c.cap)
	default:
		return false
	}
}

// raw returns the stored window bytes (NOT yet sanitised).
func (c *captured) raw() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.buf) == 0 {
		return nil
	}
	out := make([]byte, len(c.buf))
	copy(out, c.buf)
	return out
}

// ChildRun is one completed child lifecycle: terminal state plus capture.
type ChildRun struct {
	State   *os.ProcessState
	Capture *captured

	startErr error // fork/exec failure: reported upstream for CauseSpawnFailure
	groupLed bool  // kernel honoured Setpgid for this child (verified post-start)
}

// GroupLed reports whether the child verifiably became its own process-group
// leader. Sweep assertions key on this: hosts that refuse the pre-exec call
// cannot promise group-wide kills, and tests must say so instead of flaking.
func (r *ChildRun) GroupLed() bool { return r.groupLed }

// spawnSentence renders the teaching sentence §3 requires for runtime spawn
// failures ("could not start ./h.sh: …"). Verbatim per the exit-code table:
// the reason is operator prose, and the server-side sanitiser owns safety.
func (r *ChildRun) spawnSentence(argv []string) string {
	return "could not start " + strings.Join(argv, " ") + ": " + r.startErr.Error()
}

// runChild runs ONE child per the lifecycle contract:
//
//   - own process group (Setpgid): --exec-shell background jobs die WITH their
//     parent instead of leaking;
//   - context cancel sends opts.StopSignal to the GROUP through the Cancel hook;
//   - opts.KillGrace later, SIGKILL to the group — escalation exists because
//     handlers may trap or ignore TERM; the grace timer rides the Clock seam so
//     fake clocks drive every timing test deterministically;
//   - payload pumps through stdin independently and treats EPIPE/ErrClosed as
//     SUCCESS: whether the child reads its input is not the contract, its exit
//     code is (issue #25 §6 failure mode 1);
//   - stderr drains forever into the bounded window above, so a multi-megabyte
//     flood cannot block the pipe nor grow messq's RSS past the cap;
//   - exactly one Wait per Start, bounded further by cmd.WaitDelay so a
//     lingering grandchild holding descriptors cannot hang the reaper.
func runChild(ctx context.Context, clk clock.Clock, argv []string, payload []byte, opts ChildOptions) (*ChildRun, error) {
	run := &ChildRun{Capture: newCaptured(opts.StderrCap, opts.StderrMode)}
	if len(argv) == 0 || argv[0] == "" {
		return nil, fmt.Errorf("empty command")
	}
	if clk == nil {
		clk = clock.System{}
	}

	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...) //nolint:gosec // G204: operator-supplied --exec CMD IS the feature; §10 keeps only the PAYLOAD out of argv
	cmd.Dir = opts.Dir
	cmd.Env = opts.Env

	// Leadership is resolved RACE-SAFELY: Cancel/escalation fire on other
	// goroutines, so group-single routing reads an atomic instead of a plain
	// field written after Start.
	useGroup := !sabotageNoGroup
	var led atomic.Bool
	stopNow := func(sig syscall.Signal) error {
		if cmd.Process == nil {
			return nil
		}
		if useGroup && led.Load() {
			return killGroup(cmd.Process.Pid, sig)
		}
		// Leadership refused or unavailable: promise the direct child's death,
		// which is what Wait reaps and what every integration path needs.
		return killSingle(cmd.Process.Pid, sig)
	}
	cmd.Cancel = func() error { return stopNow(opts.StopSignal) }
	cmd.WaitDelay = opts.KillGrace

	stdin, err := cmd.StdinPipe()
	if err != nil {
		run.startErr = err
		return nil, err
	}
	cmd.Stdout = stdoutSink(opts.Stdout)
	cmd.Stderr = run.Capture

	if err := cmd.Start(); err != nil {
		run.startErr = err
		return nil, err
	}
	if useGroup {
		// Verify LEADERSHIP explicitly rather than assume it. The pre-exec
		// SysProcAttr.Setpgid normally wins; on hosts where its effect was
		// not taken (observed in restricted sandboxes), say so in ChildRun so
		// callers/tests can react deterministically instead of guessing.
		g, gerr := unix.Getpgid(cmd.Process.Pid)
		run.groupLed = gerr == nil && g == cmd.Process.Pid
		led.Store(run.groupLed)
	}

	finished := make(chan struct{})

	go func() { // stdin pump: delivery is best-effort by design
		// EOF signalling matters more than close-error reporting; the pump's
		// verdict never reaches the settle path (§6 mode 1).
		defer stdin.Close() //nolint:errcheck // close error cannot improve delivery here
		err := writeAll(stdin, payload)
		if err != nil && !isBenignStdinFailure(err) {
			// Deliberate silence: delivery is best-effort (§6 mode 1); any
			// surviving evidence flows through the exit code and stderr.
			_ = err
		}
	}()

	go func() { // TERM→grace→KILL escalation, armed only when stopping begins
		select {
		case <-ctx.Done():
		case <-finished:
			return
		}
		timer := clk.NewTimer(opts.KillGrace)
		defer timer.Stop()
		select {
		case <-timer.C():
			_ = stopNow(syscall.SIGKILL) //nolint:errcheck // ESRCH races ARE success for a kill
		case <-finished:
		}
	}()

	werr := cmd.Wait()
	// Wait may return EARLY via WaitDelay while ignored-signal stragglers are
	// still running (issue #25 §6 mode 3). Whatever unblocked us, the sweep
	// must complete: deliver the terminal KILL ourselves when stopping began.
	if ctx.Err() != nil || errors.Is(werr, exec.ErrWaitDelay) {
		_ = stopNow(syscall.SIGKILL) //nolint:errcheck // ESRCH races ARE success for a kill
	}
	close(finished)
	run.State = cmd.ProcessState
	return run, werr
}

func stdoutSink(w io.Writer) io.Writer {
	if w == nil {
		return io.Discard
	}
	return w
}

// isBenignStdinFailure reports pump errors the contract explicitly blesses:
// whether the child reads its input is irrelevant to the settle decision.
func isBenignStdinFailure(err error) bool {
	return errors.Is(err, syscall.EPIPE) || errors.Is(err, os.ErrClosed)
}

func writeAll(w io.WriteCloser, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if n > 0 {
			b = b[n:]
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// killGroup signals an entire process group; pid must BE a group leader
// (Setpgid made it one at birth). ESRCH races are successes — the group died.
func killGroup(pid int, sig syscall.Signal) error {
	if sig == 0 {
		sig = syscall.SIGTERM
	}
	err := unix.Kill(-pid, unix.Signal(sig))
	if err != nil && !errors.Is(err, unix.ESRCH) {
		return err
	}
	return nil
}

// killSingle signals one process; kept for the sabotage comparison path.
func killSingle(pid int, sig syscall.Signal) error {
	err := unix.Kill(pid, unix.Signal(sig))
	if err != nil && !errors.Is(err, unix.ESRCH) {
		return err
	}
	return nil
}

var _ = time.Second
