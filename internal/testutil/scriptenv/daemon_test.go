// SPDX-License-Identifier: Apache-2.0

package scriptenv

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
)

// failRecorder is the fatalf surface runDaemon is given in tests: it records the
// messages instead of panicking, which is exactly the difference the seam exists
// to bridge (ts.Fatalf panics; the recorder returns).
type failRecorder struct{ msgs []string }

func (r *failRecorder) fatalf(format string, args ...any) {
	r.msgs = append(r.msgs, fmt.Sprintf(format, args...))
}

// startRecorder is the injected stand-in for StartDaemon: it records the
// workDir/clock it was handed and returns the configured daemon or error, so a
// test can walk the start/restart arms — including the failure arms the golden
// scripts cannot fail into — without opening a store.
type startRecorder struct {
	workDirs []string
	clks     []*clock.Fake
	daemon   *Daemon
	err      error
}

func (r *startRecorder) start(workDir string, clk *clock.Fake) (*Daemon, error) {
	r.workDirs = append(r.workDirs, workDir)
	r.clks = append(r.clks, clk)
	if r.err != nil {
		return nil, r.err
	}
	return r.daemon, nil
}

// newDispatchState is the per-test state runDaemon dispatches over.
func newDispatchState() (*State, *clock.Fake) {
	clk := newFakeClock(time.Time{})
	return &State{workDir: "/work", clk: clk}, clk
}

// TestRunDaemonFatalArms pins every arm that only reports and changes nothing:
// the messages are the script-visible contract, and none of these arms may
// build a daemon or touch MESSQ_ADDR.
func TestRunDaemonFatalArms(t *testing.T) {
	for _, tc := range []struct {
		name  string
		armed func(*State)
		args  []string
		start *startRecorder // nil means start must never be called
		want  string
	}{
		{
			name:  "no subcommand",
			armed: func(*State) {},
			args:  nil,
			want:  "daemon: want start|stop|kill|restart",
		},
		{
			name:  "unknown subcommand",
			armed: func(*State) {},
			args:  []string{"frobnicate"},
			want:  `daemon: unknown subcommand "frobnicate" (want start|stop|kill|restart)`,
		},
		{
			name:  "double start",
			armed: func(st *State) { st.daemon = &Daemon{} },
			args:  []string{"start"},
			want:  "daemon: already started",
		},
		{
			name:  "stop without daemon",
			armed: func(*State) {},
			args:  []string{"stop"},
			want:  "daemon: not started",
		},
		{
			name:  "kill without daemon",
			armed: func(*State) {},
			args:  []string{"kill"},
			want:  "daemon: not started",
		},
		{
			name:  "restart without daemon",
			armed: func(*State) {},
			args:  []string{"restart"},
			want:  "daemon: not started",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, _ := newDispatchState()
			tc.armed(st)
			rec := &failRecorder{}
			sr := tc.start
			if sr == nil {
				sr = &startRecorder{}
			}
			runDaemon(st, tc.args, sr.start, rec.fatalf,
				func(string, string) { t.Error("setenv ran on a fatal arm") })

			if len(rec.msgs) != 1 || rec.msgs[0] != tc.want {
				t.Errorf("runDaemon(%q) reported %q, want exactly %q", tc.args, rec.msgs, tc.want)
			}
			if len(sr.workDirs) != 0 {
				t.Errorf("runDaemon(%q) built a daemon on a fatal arm", tc.args)
			}
		})
	}
}

// TestRunDaemonStartInstallsTheDaemon walks the success arm: the recorder's
// daemon is installed, MESSQ_ADDR carries its address, and the start call went
// out with the state's own work dir and clock.
func TestRunDaemonStartInstallsTheDaemon(t *testing.T) {
	st, clk := newDispatchState()
	rec := &failRecorder{}
	sr := &startRecorder{daemon: &Daemon{}}
	set := map[string]string{}
	runDaemon(st, []string{"start"}, sr.start, rec.fatalf,
		func(k, v string) { set[k] = v })

	if len(rec.msgs) != 0 {
		t.Errorf("start reported %q, want silence", rec.msgs)
	}
	if st.daemon == nil || st.daemon != sr.daemon {
		t.Fatal("start did not install the daemon returned by the start call")
	}
	if got := set["MESSQ_ADDR"]; got != st.daemon.Addr() {
		t.Errorf("MESSQ_ADDR = %q, want the daemon address %q", got, st.daemon.Addr())
	}
	if len(sr.workDirs) != 1 || sr.workDirs[0] != st.workDir {
		t.Errorf("start received workDirs %v, want [%s]", sr.workDirs, st.workDir)
	}
	if len(sr.clks) != 1 || sr.clks[0] != clk {
		t.Error("start did not receive the state's fake clock")
	}
}

// TestRunDaemonStartFailure pins the arm a healthy start never reaches: the
// message names the phase, and the state stays empty.
func TestRunDaemonStartFailure(t *testing.T) {
	st, _ := newDispatchState()
	rec := &failRecorder{}
	sr := &startRecorder{err: errors.New("boom")}
	runDaemon(st, []string{"start"}, sr.start, rec.fatalf, func(string, string) {
		t.Error("setenv ran after a failed start")
	})

	if len(rec.msgs) != 1 || rec.msgs[0] != "daemon start: boom" {
		t.Errorf("failed start reported %q, want [daemon start: boom]", rec.msgs)
	}
	if st.daemon != nil {
		t.Error("a failed start left a daemon installed")
	}
}

// TestRunDaemonRestartStopsTheOldDaemon proves the ordering the scripts rely
// on: the old daemon's context is cancelled (its graceful stop ran) before the
// replacement is installed and advertised.
func TestRunDaemonRestartStopsTheOldDaemon(t *testing.T) {
	st, _ := newDispatchState()
	ctx, cancel := context.WithCancel(context.Background())
	st.daemon = &Daemon{cancel: cancel}
	rec := &failRecorder{}
	sr := &startRecorder{daemon: &Daemon{}}
	set := map[string]string{}
	runDaemon(st, []string{"restart"}, sr.start, rec.fatalf,
		func(k, v string) { set[k] = v })

	select {
	case <-ctx.Done():
	default:
		t.Error("restart left the old daemon's context alive — its stop never ran")
	}
	if len(rec.msgs) != 0 {
		t.Errorf("restart reported %q, want silence", rec.msgs)
	}
	if st.daemon == nil || st.daemon != sr.daemon {
		t.Error("restart did not install the replacement daemon")
	}
	if got := set["MESSQ_ADDR"]; got != st.daemon.Addr() {
		t.Errorf("MESSQ_ADDR = %q, want the replacement's address %q", got, st.daemon.Addr())
	}
}

// TestRunDaemonRestartFailureClearsTheState pins the failure ordering: the old
// daemon is stopped first, and a failed replacement leaves the slot empty
// rather than pointing at a dead daemon.
func TestRunDaemonRestartFailureClearsTheState(t *testing.T) {
	st, _ := newDispatchState()
	ctx, cancel := context.WithCancel(context.Background())
	st.daemon = &Daemon{cancel: cancel}
	rec := &failRecorder{}
	sr := &startRecorder{err: errors.New("boom")}
	runDaemon(st, []string{"restart"}, sr.start, rec.fatalf, func(string, string) {
		t.Error("setenv ran after a failed restart")
	})

	<-ctx.Done() // the old daemon's stop ran before the start was attempted
	if len(rec.msgs) != 1 || rec.msgs[0] != "daemon restart: boom" {
		t.Errorf("failed restart reported %q, want [daemon restart: boom]", rec.msgs)
	}
	if st.daemon != nil {
		t.Error("a failed restart left the stopped old daemon installed")
	}
}

// TestRunDaemonStopAndKillClearTheState walk the two teardown arms: both clear
// the slot, and stop takes the graceful path (observable through the daemon's
// cancelled context).
func TestRunDaemonStopAndKillClearTheState(t *testing.T) {
	t.Run("stop is graceful", func(t *testing.T) {
		st, _ := newDispatchState()
		ctx, cancel := context.WithCancel(context.Background())
		st.daemon = &Daemon{cancel: cancel}
		rec := &failRecorder{}
		runDaemon(st, []string{"stop"}, (&startRecorder{}).start, rec.fatalf,
			func(string, string) { t.Error("setenv ran on stop") })

		select {
		case <-ctx.Done():
		default:
			t.Error("stop did not cancel the daemon's context")
		}
		if len(rec.msgs) != 0 || st.daemon != nil {
			t.Errorf("stop = (msgs %q, daemon %v), want (silence, cleared)", rec.msgs, st.daemon)
		}
	})

	t.Run("kill clears the state", func(t *testing.T) {
		st, _ := newDispatchState()
		st.daemon = &Daemon{}
		rec := &failRecorder{}
		runDaemon(st, []string{"kill"}, (&startRecorder{}).start, rec.fatalf,
			func(string, string) { t.Error("setenv ran on kill") })

		if len(rec.msgs) != 0 || st.daemon != nil {
			t.Errorf("kill = (msgs %q, daemon %v), want (silence, cleared)", rec.msgs, st.daemon)
		}
	})
}

// TestMustStateRejectsMissing covers the guard stateFrom splits out: anything
// but an installed *State panics with the mis-wired-suite message.
func TestMustStateRejectsMissing(t *testing.T) {
	if st := mustState(&State{}); st == nil {
		t.Error("mustState rejected an installed state")
	}
	for _, v := range []any{nil, "junk", 42, (*State)(nil)} {
		func() {
			defer func() {
				r := recover()
				if r == nil {
					t.Errorf("mustState(%v) did not panic", v)
					return
				}
				if !strings.Contains(fmt.Sprint(r), "per-script state missing") {
					t.Errorf("mustState(%v) panicked with %q, want the missing-state message", v, r)
				}
			}()
			mustState(v)
		}()
	}
}

// TestPredicateOverRealDaemon drives the predicate builder against a real
// inproc daemon: the ready/healthz faces, gone's dial-is-the-answer rule in
// both directions, the info field walk over the wire, and every error arm the
// builder can return. Cleanup covers Close's daemon arm.
func TestPredicateOverRealDaemon(t *testing.T) {
	dir := t.TempDir()
	st := &State{workDir: dir, clk: newFakeClock(time.Time{})}
	d, startErr := StartDaemon(dir, st.clk)
	if startErr != nil {
		t.Fatalf("StartDaemon: %v", startErr)
	}
	st.daemon = d
	t.Cleanup(func() { st.Close() })

	if d.DataDir() != filepath.Join(dir, "data") {
		t.Errorf("DataDir = %q, want %q", d.DataDir(), filepath.Join(dir, "data"))
	}
	if d.Clock() != st.clk {
		t.Error("Clock returned a clock other than the state's")
	}
	if err := d.Quiesce(); err != nil {
		t.Errorf("Quiesce on a live daemon: %v", err)
	}

	// readyz and its healthz alias answer ready through the real client.
	for _, form := range []string{"readyz", "healthz"} {
		if ok, pErr := probePredicate(t, st, form); pErr != nil || !ok {
			t.Errorf("%s predicate = (%v, %v), want (true, nil)", form, ok, pErr)
		}
	}

	// gone against a live daemon: the dial succeeding is the not-gone answer.
	if ok, pErr := probePredicate(t, st, "gone"); pErr != nil || ok {
		t.Errorf("gone against a live daemon = (%v, %v), want (false, nil)", ok, pErr)
	}

	// info over the wire: the durability field matches (StartDaemon pins
	// DurabilityFull), a wrong version never matches, and an unknown field
	// walks to the not-found answer.
	if ok, pErr := probePredicate(t, st, "info durability=full"); pErr != nil || !ok {
		t.Errorf("info durability=full = (%v, %v), want (true, nil)", ok, pErr)
	}
	if ok, pErr := probePredicate(t, st, "info version=no-such-version"); pErr != nil || ok {
		t.Errorf("info version=<wrong> = (%v, %v), want (false, nil)", ok, pErr)
	}
	if ok, pErr := probePredicate(t, st, "info nosuchfield=x"); pErr != nil || ok {
		t.Errorf("info <unknown field> = (%v, %v), want (false, nil)", ok, pErr)
	}

	// gone after the socket file is unlinked: the refused dial IS the answer.
	if err := os.Remove(d.SocketPath()); err != nil {
		t.Fatal(err)
	}
	if ok, pErr := probePredicate(t, st, "gone"); pErr != nil || !ok {
		t.Errorf("gone with an unlinked socket = (%v, %v), want (true, nil)", ok, pErr)
	}

	// The builder's error arms: no daemon, bad info spelling, unknown form.
	for _, tc := range []struct {
		st   *State
		args []string
		want string
	}{
		{&State{}, []string{"readyz"}, "no daemon started"},
		{&State{}, []string{"info", "a=b"}, "no daemon started"},
		{st, []string{"info"}, "info predicate wants"},
		{st, []string{"info", "noequals"}, "wants <field>=<value>"},
		{st, []string{"wat"}, "unknown predicate"},
	} {
		_, buildErr := predicate(tc.st, tc.args)
		if buildErr == nil || !strings.Contains(buildErr.Error(), tc.want) {
			t.Errorf("predicate(%q) = %v, want an error naming %q", tc.args, buildErr, tc.want)
		}
	}

	// gone with no daemon at all is immediately true: nothing to wait for.
	if ok, pErr := probePredicate(t, &State{}, "gone"); pErr != nil || !ok {
		t.Errorf("gone with no daemon = (%v, %v), want (true, nil)", ok, pErr)
	}
}

// probePredicate builds one predicate over st and takes a single probe,
// failing the test when the builder refuses the form.
func probePredicate(t *testing.T, st *State, form string) (bool, error) {
	t.Helper()
	pred, err := predicate(st, strings.Fields(form))
	if err != nil {
		t.Fatalf("predicate(%q): %v", form, err)
	}
	return pred()
}

// TestStartDaemonSocketGuards walks the three socket/data-dir guards the happy
// scripts never trip: a live socket is refused, a stale socket file is cleared,
// and an uncleerable path fails the start instead of hanging it.
func TestStartDaemonSocketGuards(t *testing.T) {
	t.Run("refuses a live socket", func(t *testing.T) {
		dir := t.TempDir()
		d, err := StartDaemon(dir, newFakeClock(time.Time{}))
		if err != nil {
			t.Fatalf("first start: %v", err)
		}
		t.Cleanup(func() { d.Stop(true) })
		if _, err := StartDaemon(dir, newFakeClock(time.Time{})); err == nil || !strings.Contains(err.Error(), "live socket") {
			t.Errorf("second start on a live socket = %v, want the live-socket refusal", err)
		}
	})

	t.Run("clears a stale socket file", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "messq.sock"), []byte("stale"), 0o600); err != nil {
			t.Fatal(err)
		}
		d, err := StartDaemon(dir, newFakeClock(time.Time{}))
		if err != nil {
			t.Fatalf("start over a stale socket file: %v", err)
		}
		t.Cleanup(func() { d.Stop(true) })
	})

	t.Run("fails when the data dir path is taken by a file", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "data"), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := StartDaemon(dir, newFakeClock(time.Time{})); err == nil || !strings.Contains(err.Error(), "create data dir") {
			t.Errorf("start over a file at the data dir = %v, want the data-dir error", err)
		}
	})

	t.Run("fails when the socket path cannot be cleared", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.MkdirAll(filepath.Join(dir, "messq.sock", "inner"), 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := StartDaemon(dir, newFakeClock(time.Time{})); err == nil {
			t.Error("start over an uncleerable socket path unexpectedly succeeded")
		}
	})
}

// TestQuiesceFailsOnAStoppedDaemon covers the probe's failure arm: after the
// daemon is torn down there is no answer to wait for.
func TestQuiesceFailsOnAStoppedDaemon(t *testing.T) {
	dir := t.TempDir()
	d, err := StartDaemon(dir, newFakeClock(time.Time{}))
	if err != nil {
		t.Fatalf("StartDaemon: %v", err)
	}
	d.Stop(true)
	if err := d.Quiesce(); err == nil || !strings.Contains(err.Error(), "quiesce probe failed") {
		t.Errorf("Quiesce after stop = %v, want the probe failure", err)
	}
}
