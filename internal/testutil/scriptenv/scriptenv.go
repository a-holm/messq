// SPDX-License-Identifier: Apache-2.0

// Package scriptenv is the shared environment of the testscript (.txtar) golden
// suite (issue #26 §5): the determinism environment every script runs under, the
// custom command set (daemon, clock, waitfor, capture, exitcode, cmpjson,
// cmpshape, mask), and the in-process daemon the inproc lane talks to.
//
// Two lanes run through one Params: the inproc lane constructs the daemon inside
// the test process on a clock.Fake — the `messq` commands a script writes re-exec
// the test binary via testscript.RunMain and speak real HTTP over the socket —
// and the subproc lane (added with the quickstart/dev_mode scripts) runs a real
// `messq serve` where the process boundary is the thing under test.
//
// This package is imported by tests only; nothing in the production tree links
// it. Its dependency on rogpeppe/go-internal is PLAN.md §13's test-only row.
package scriptenv

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rogpeppe/go-internal/testscript"

	"github.com/a-holm/messq/internal/clock"
)

// stateKey keys the per-script [State] inside testscript.Env.Values.
type stateKey struct{}

// Suite configures one testscript run. Tests build it, hand it to Params, and
// never touch the per-script state themselves.
type Suite struct {
	// Dir is the directory holding the .txtar scripts (testscript.Params.Dir).
	Dir string
	// Update mirrors the repo-wide -update convention: goldens rewrite instead of
	// failing on drift. cmpshape assertions are NEVER rewritten — see [shapecheck].
	Update bool
	// FakeClockStart pins the inproc lane's fake clock. Zero means the suite
	// default, so every script starts from the same instant.
	FakeClockStart time.Time
}

// defaultClockStart is the suite's frozen now (2026-08-26 12:00 UTC). Matches
// clitest's frozen instant so every lane renders the same relative times.
var defaultClockStart = time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)

// Params returns the testscript.Params for this suite: the command registry, the
// determinism setup, and the -update wiring.
func (s *Suite) Params() testscript.Params {
	return testscript.Params{
		Dir:           s.Dir,
		Cmds:          commands(),
		Setup:         s.setup,
		UpdateScripts: s.Update,
	}
}

// setup installs the determinism environment and the per-script state. It runs
// once per script, before the first command.
//
// The environment is the contract that makes goldens stable: TZ and locale pin
// rendering, NO_COLOR/TERM pin colour off, COLUMNS pins table width, HOME and the
// XDG roots point into $WORK so nothing outside the work dir is touched, and every
// MESSQ_* variable from the outer environment is dropped unless the script sets it
// itself. A script that needs a poisoned variable SETS it — the suite never
// inherits one.
func (s *Suite) setup(e *testscript.Env) error {
	st := &State{
		update:    s.Update,
		workDir:   e.WorkDir,
		scriptDir: s.Dir,
		shapes:    Shapes(),
		clk:       newFakeClock(s.FakeClockStart),
	}
	e.Values[stateKey{}] = st
	e.Defer(st.Close)

	var dirs []string
	e.Vars, dirs = deterministicEnv(e.WorkDir, e.Vars)
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return err
		}
	}
	return nil
}

// deterministicEnv builds the script environment: every MESSQ_* variable from the
// outer process is dropped (a script that needs one SETS it), and the determinism
// variables — locale, timezone, colour off, fixed width, XDG roots under $WORK —
// are appended. It returns the cleaned variables and the directories the caller
// must create.
func deterministicEnv(workDir string, vars []string) ([]string, []string) {
	cleaned := make([]string, 0, len(vars)+16)
	for _, kv := range vars {
		if strings.HasPrefix(kv, "MESSQ_") {
			continue
		}
		cleaned = append(cleaned, kv)
	}
	home := filepath.Join(workDir, "home")
	dirs := []string{
		home,
		filepath.Join(workDir, "config"),
		filepath.Join(workDir, "cache"),
		filepath.Join(workDir, "state"),
	}
	cleaned = append(cleaned,
		"TZ=UTC",
		"LC_ALL=C",
		"LANG=C",
		"NO_COLOR=1",
		"TERM=dumb",
		"COLUMNS=100",
		"HOME="+home,
		"XDG_CONFIG_HOME="+filepath.Join(workDir, "config"),
		"XDG_CACHE_HOME="+filepath.Join(workDir, "cache"),
		"XDG_STATE_HOME="+filepath.Join(workDir, "state"),
	)
	return cleaned, dirs
}

// state is the per-script state custom commands share: the fake clock, the
// inproc daemon (once `daemon start` ran), the script directory (-update needs
// it), and the update flag.
type State struct {
	update    bool
	workDir   string
	scriptDir string
	shapes    map[string]any
	clk       *clock.Fake
	daemon    *Daemon
}

func stateFrom(ts *testscript.TestScript) *State {
	return mustState(ts.Value(stateKey{}))
}

// mustState is stateFrom's core over the raw stored value, split out so the
// missing-state guard is unit-testable without a TestScript: Setup always
// installs the state in a real run, so the panic arm is reachable only from a
// mis-wired suite.
func mustState(v any) *State {
	st, ok := v.(*State)
	if !ok || st == nil {
		panic("scriptenv: per-script state missing — Params().Setup not installed?")
	}
	return st
}

// Close tears the script's daemon down. Registered with e.Defer, so it runs even
// when the script fails.
func (st *State) Close() {
	if st.daemon != nil {
		st.daemon.Stop(true)
		st.daemon = nil
	}
}

// newFakeClock builds the lane's clock. The default instant keeps every script's
// relative times identical.
func newFakeClock(start time.Time) *clock.Fake {
	if start.IsZero() {
		start = defaultClockStart
	}
	return clock.NewFake(start)
}
