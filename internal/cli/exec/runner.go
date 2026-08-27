// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/a-holm/messq/internal/cli/render"
	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/pkg/client"
)

// ErrExecTimeoutCause marks a context cancelled because --exec-timeout fired;
// distinct from lease loss and outside interruptions. Wrapped WITH the duration
// so reasons read "exec timeout after 30s".
var ErrExecTimeoutCause = errors.New("exec timeout")

// spawnRetryDelay backs the §3 spawn-failure row: naks come back after 5 s so
// a transient fork storm does not hammer EAGAIN into max_deliver burn.
const spawnRetryDelay = 5 * time.Second

// RunnerOptions carries the resolved --exec configuration for one worker
// process. ParentEnv is injected (the sub-command owns os.Environ()) keeping
// every behavioural input hermetic in tests. AfterBuildEnv appends entries
// AFTER the sanitised build — reserved for the hermetic battery in tests; a
// production lane leaves it nil so no parent MESSQ_ noise can re-enter.
type RunnerOptions struct {
	Cmd           string
	ShellPath     string // "" splits Cmd; otherwise shell -c Cmd ("you own quoting")
	Dir           string
	ParentEnv     []string
	ExtraEnv      []string // --exec-env entries
	CleanEnv      bool     // --exec-clean-env
	StopSignal    syscall.Signal
	KillGrace     time.Duration
	ExecTimeout   time.Duration // 0 = as long as the lease extends (§8)
	StderrBytes   int
	StderrMode    StderrMode
	Stdout        io.Writer
	AfterBuildEnv []string      // documented test seam ONLY (see above)
	Out           io.Writer     // --output destination; nil disables rendering
	Mode          render.Format // resolved output face for records/summary
	Hints         *HintPrinter  // one-time exit-code explainer (G9)
}

// Runner is the client.Worker handler factory: ONE message in, ONE child out,
// ONE settle-shaped error back (issue #25 §5). No fetch, no settle, no retry
// loop lives here — scripts/layers.sh proves that boundary, D14 demands it.
type Runner struct {
	Opts RunnerOptions
	Clk  clock.Clock

	spawnStreak atomic.Int64 // consecutive runtime fork/exec failures
}

// SpawnStreak reports consecutive runtime spawn failures; slice 7's breaker
// (--exec-max-spawn-failures) consumes it.
func (r *Runner) SpawnStreak() int64 { return r.spawnStreak.Load() }

// ResolveArgv turns --exec Cmd into exact argv: plain mode honours SplitWords
// quoting rules; shell mode delegates quoting ($SHELL -c / /bin/sh -c).
func ResolveArgv(cmd, shellPath string) ([]string, error) {
	if strings.TrimSpace(cmd) == "" {
		return nil, fmt.Errorf("--exec requires a command")
	}
	if shellPath == "" {
		return SplitWords(cmd)
	}
	return []string{shellPath, "-c", cmd}, nil
}

// mintSpanID produces the W3C span-id half of traceparent — one fresh id per
// delivery so each child run is individually traceable.
func mintSpanID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "0000000000000000"
	}
	return hex.EncodeToString(b[:])
}

// traceparentOf joins the delivered trace id with a fresh span; invalid or
// missing upstream ids suppress the variable rather than lie about lineage.
func traceparentOf(traceID, span string) string {
	if !isHex32(traceID) || !isHex16(span) || strings.ContainsRune(traceID, '\x00') {
		return ""
	}
	return "00-" + traceID + "-" + span + "-01"
}

func isHex32(s string) bool { return len(s) == 32 && allHex(s) }
func isHex16(s string) bool { return len(s) == 16 && allHex(s) }

func allHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'f', c >= 'A' && c <= 'F':
		default:
			return false
		}
	}
	return true
}

// resolveTerminalCause decides WHICH §3 non-exit row applies using causes
// carried on the contexts, never guessing from wait status alone.
func (r *Runner) resolveTerminalCause(ctx, tctx context.Context) (CauseKind, string) {
	for _, c := range []context.Context{tctx, ctx} {
		switch {
		case c == nil:
			continue
		case errors.Is(context.Cause(c), client.ErrLeaseLost):
			return CauseLeaseLoss, ""
		case errors.Is(context.Cause(c), ErrExecTimeoutCause):
			d := r.Opts.ExecTimeout
			if d > 0 {
				return CauseExecTimeout, formatDuration(d)
			}
		}
	}
	return CauseNone, ""
}

func formatDuration(d time.Duration) string {
	if d < time.Second && d > 0 {
		return d.String()
	}
	s := d.Truncate(time.Millisecond).String()
	return s
}

// clampTimeoutCtx derives the --exec-timeout child context with a typed cause.
func clampTimeoutCtx(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeoutCause(
		ctx, d,
		fmt.Errorf("%w after %s", ErrExecTimeoutCause, formatDuration(d)),
	)
}

// abandonError is THE lease-lost shape: wrapping client.ErrLeaseLost tells the
// Worker "settle NOTHING" — dead tokens never ack, nak or term (G4).
func abandonError(detail string) error {
	return fmt.Errorf("lease lost during exec (%s): %w", detail, client.ErrLeaseLost)
}

// Handle implements client.Handler: spawn → pump → wait → classify → outcome.
// The four-way mapping below IS the delivery grammar G2 guards:
//
//	exit 0          → nil                          (ack)
//	exit 65         → client.Permanent(reason)     (term → DLQ after 1 attempt)
//	exit 75/other/
//	signal/timeout  → plain error                  (nak, consumer backoff)
//	runtime spawn   → client.RetryAfter(5s, …)     (nak with operator-friendly pace)
//	lease lost      → error wrapping client.ErrLeaseLoss  (ABANDON: settle nothing)
//
// Rendering and the one-time hint hang off later slices without touching this
// decision tree.
func (r *Runner) Handle(ctx context.Context, m *client.Delivered) error {
	clk := r.Clk
	if clk == nil {
		clk = clock.System{}
	}

	argv, argvErr := ResolveArgv(r.Opts.Cmd, r.Opts.ShellPath)

	tctx, cancelTimeout := clampTimeoutCtx(ctx, r.Opts.ExecTimeout)
	defer cancelTimeout()

	tp := traceparentOf(m.TraceID, mintSpanID())
	env, _, envErr := BuildEnv(m, r.Opts.ParentEnv, EnvOptions{
		ExtraEnv:    r.Opts.ExtraEnv,
		CleanEnv:    r.Opts.CleanEnv,
		Traceparent: tp,
	})
	if envErr == nil {
		env = append(env, r.Opts.AfterBuildEnv...)
	}

	childOpts := ChildOptions{
		Dir:        r.Opts.Dir,
		Env:        env,
		StopSignal: r.Opts.StopSignal,
		KillGrace:  r.Opts.KillGrace,
		StderrCap:  ClampReasonCap(r.Opts.StderrBytes),
		StderrMode: r.Opts.StderrMode,
		Stdout:     r.Opts.Stdout,
	}

	// Pre-flight refusals are OPERATOR problems, never message feedback:
	// counting them onto the spawn streak keeps the breaker story uniform.
	if argvErr != nil {
		return r.spawnFailure(argvErr.Error())
	}
	if envErr != nil {
		return client.Permanent(fmt.Errorf("--exec configuration unusable: %w", envErr))
	}

	runStart := clk.Now()
	run, runErr := runChild(tctx, clk, argv, m.Body, childOpts)
	if runErr != nil && run == nil {
		durMS := clk.Since(runStart).Milliseconds()
		sent := spawnSentence(argv, runErr.Error())
		r.spawnStreak.Add(1)
		if r.Opts.Out != nil {
			rec := Record{
				TS:     clk.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
				Stream: m.Stream, Consumer: m.Consumer, Seq: m.Seq,
				MsgID: m.ID, Subject: m.Subject,
				Attempt: m.Attempt, MaxDeliver: m.MaxDeliver,
				ExitCode: -1, Outcome: OutcomeNak.String(),
				DurationMS: durMS, RetryInMS: spawnRetryDelay.Milliseconds(),
				Reason: sent, TraceID: m.TraceID,
			}
			if e := r.emitter(); e != nil {
				_ = e.Emit(rec) //nolint:errcheck // rendering failures never redirect settles
			}
			if r.Opts.Hints != nil {
				r.Opts.Hints.PrintOnce()
			}
		}
		return client.RetryAfter(spawnRetryDelay, errors.New(sent))
	}

	kind, detail := r.resolveTerminalCause(ctx, tctx)
	reason := SanitizeStderr(run.Capture.raw(), r.Opts.StderrBytes, r.Opts.StderrMode)
	res := Classify(run.State, kind, detail, reason)
	res.Truncated = run.Capture.truncated()

	durMS := clk.Since(runStart).Milliseconds()
	var retryMS int64
	switch res.Outcome {
	case OutcomeAck:
		r.spawnStreak.Store(0)
	case OutcomeTerm:
		r.spawnStreak.Store(0)
	case OutcomeNak:
		r.spawnStreak.Store(0)
	case OutcomeAbandon:
	default: // unreachable; exhaustive switch satisfies the closed set
		return abandonError("unrouted classifier result")
	}

	if r.Opts.Out != nil {
		rec := recordFromResult(m, res, durMS, retryMS, clk.Now())
		if e := r.emitter(); e != nil {
			_ = e.Emit(rec) //nolint:errcheck // rendering failures never redirect settles
		}
		if r.Opts.Hints != nil && (res.ExitCode != 0 || res.Signal != 0) {
			r.Opts.Hints.PrintOnce()
		}
	}

	switch res.Outcome {
	case OutcomeAck:
		return nil
	case OutcomeTerm:
		return client.Permanent(errors.New(res.Reason))
	case OutcomeNak:
		return errors.New(res.Reason)
	case OutcomeAbandon:
		return abandonError("token fenced while child ran")
	default: // unreachable; exhaustive switch satisfies the closed set
		return abandonError("unrouted classifier result")
	}
}

// spawnFailure keeps the §3 spawn-failure shape helper free-standing so both
// Handle and future flag-lane breakers share one sentence.
func spawnSentence(argv []string, cause string) string {
	return "could not start " + strings.Join(argv, " ") + ": " + cause
}

// emitter builds a throwaway emitter per message; a per-run Emitter lives with
// the sub-command once #24's tree owns the output writer.
func (r *Runner) emitter() *Emitter {
	if r.Opts.Out == nil {
		return nil
	}
	return NewEmitter(r.Opts.Out, r.Opts.Mode)
}

// spawnFailure feeds the consecutive-failure counter and shapes the §3
// runtime-spawn row: nak with the fixed 5 s retry-after and teaching text.
func (r *Runner) spawnFailure(text string) error {
	r.spawnStreak.Add(1)
	return client.RetryAfter(spawnRetryDelay, errors.New(text))
}
