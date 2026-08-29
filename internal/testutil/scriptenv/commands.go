// SPDX-License-Identifier: Apache-2.0

package scriptenv

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/rogpeppe/go-internal/testscript"
	"github.com/rogpeppe/go-internal/txtar"

	"github.com/a-holm/messq/internal/buildinfo"
	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/wirecheck"
	"github.com/a-holm/messq/pkg/client"
)

// commands is the custom-command registry handed to testscript.Params.Cmds. The
// commands run inside the TEST process; the `messq` binary a script invokes is
// re-exec'd from testscript.RunMain's PATH entry and talks to the daemon over its
// socket.
//
//	waitfor pending demo worker == 0 [30s]  — waits for delivery predicates; the
//	                                        pending form lands with #13/#14
//	clock advance 3s | clock set <RFC3339> — move the fake clock, then quiesce
//	daemon start|stop|kill|restart          — the lane's daemon lifecycle
//	capture VAR $.path                      — export a JSON field of the last stdout
//	exitcode 3 messq …                      — assert an exact exit status
//	cmpjson stdout want.json                — canonical-JSON compare (key order and
//	                                          whitespace are not contract)
//	cmpshape stdout StreamView              — validate against the committed shape
//	mask stdout got.txt                     — normalise volatile values for cmp
func commands() map[string]func(ts *testscript.TestScript, neg bool, args []string) {
	return map[string]func(ts *testscript.TestScript, neg bool, args []string){
		"daemon":   cmdDaemon,
		"clock":    cmdClock,
		"waitfor":  cmdWaitFor,
		"capture":  cmdCapture,
		"exitcode": cmdExitCode,
		"cmpjson":  cmdCmpJSON,
		"cmpshape": cmdCmpShape,
		"mask":     cmdMask,
	}
}

// Shapes is the cmpshape registry: shape name → prototype value of the wire type.
// Only importable types appear; a command's private view type joins when its
// owning package exports a prototype or the shape moves next to it.
func Shapes() map[string]any {
	return map[string]any{
		"BuildInfo":     buildinfo.Info{},
		"StreamView":    client.StreamView{},
		"ConsumerView":  client.ConsumerView{},
		"MessageView":   client.MessageView{},
		"MessagePage":   client.MessagePage{},
		"DeleteResult":  client.DeleteResult{},
		"PublishAck":    client.PublishAck{},
		"PublishBatch":  client.PublishBatchAck{},
		"FetchResponse": client.FetchResponse{},
		"DaemonInfo":    client.Info{},
	}
}

// ---- daemon ----

// cmdDaemon adapts the TestScript command to [runDaemon]: stateFrom pulls the
// per-script state, and the TestScript's Fatalf/Setenv become the injected
// surface the unit tests stand in for.
func cmdDaemon(ts *testscript.TestScript, _ bool, args []string) {
	runDaemon(stateFrom(ts), args, StartDaemon, ts.Fatalf, ts.Setenv)
}

// runDaemon is the daemon command's dispatch over an injected surface: fatalf
// and setenv stand in for the TestScript, and start stands in for [StartDaemon]
// so a unit test can walk every arm — including the double-start, not-started
// and start-failure arms the golden scripts cannot fail into — without building
// a daemon. ts.Fatalf panics and never returns; the unit tests' recorder does
// return, so every fatalf call here is followed by a return and the dispatch
// never acts on an arm after reporting it.
func runDaemon(
	st *State,
	args []string,
	start func(workDir string, clk *clock.Fake) (*Daemon, error),
	fatalf func(format string, args ...any),
	setenv func(key, value string),
) {
	if len(args) == 0 {
		fatalf("daemon: want start|stop|kill|restart")
		return
	}
	switch args[0] {
	case "start":
		if st.daemon != nil {
			fatalf("daemon: already started")
			return
		}
		d, err := start(st.workDir, st.clk)
		if err != nil {
			fatalf("daemon start: %v", err)
			return
		}
		st.daemon = d
		setenv("MESSQ_ADDR", d.Addr())
	case "stop":
		if st.daemon == nil {
			fatalf("daemon: not started")
			return
		}
		st.daemon.Stop(true)
		st.daemon = nil
	case "kill":
		if st.daemon == nil {
			fatalf("daemon: not started")
			return
		}
		st.daemon.Stop(false)
		st.daemon = nil
	case "restart":
		if st.daemon == nil {
			fatalf("daemon: not started")
			return
		}
		st.daemon.Stop(true)
		d, err := start(st.workDir, st.clk)
		if err != nil {
			st.daemon = nil
			fatalf("daemon restart: %v", err)
			return
		}
		st.daemon = d
		setenv("MESSQ_ADDR", d.Addr())
	default:
		fatalf("daemon: unknown subcommand %q (want start|stop|kill|restart)", args[0])
	}
}

// ---- clock ----

func cmdClock(ts *testscript.TestScript, _ bool, args []string) {
	st := stateFrom(ts)
	if st.daemon == nil {
		ts.Fatalf("clock: no daemon; the fake clock is the daemon's clock")
	}
	if len(args) != 2 {
		ts.Fatalf("clock: want `clock advance <dur>` or `clock set <RFC3339>`")
	}
	switch args[0] {
	case "advance":
		d, err := time.ParseDuration(args[1])
		if err != nil {
			ts.Fatalf("clock advance: %v", err)
		}
		st.clk.Advance(d)
	case "set":
		t, err := time.Parse(time.RFC3339, args[1])
		if err != nil {
			ts.Fatalf("clock set: %v", err)
		}
		st.clk.Set(t)
	default:
		ts.Fatalf("clock: unknown subcommand %q", args[0])
	}
	if err := st.daemon.Quiesce(); err != nil {
		ts.Fatalf("clock: quiesce: %v", err)
	}
}

// ---- waitfor ----

// waitForPollInterval is the real-time poll cadence. The predicate deadline is
// what bounds the command; the interval only keeps it from spinning.
const waitForPollInterval = 20 * time.Millisecond

func cmdWaitFor(ts *testscript.TestScript, neg bool, args []string) {
	st := stateFrom(ts)
	if len(args) == 0 {
		ts.Fatalf("waitfor: want a predicate (readyz | healthz | gone | info <field>=<value>) [timeout]")
	}
	timeout := 30 * time.Second
	if n := len(args); n >= 2 {
		last := args[n-1]
		if strings.HasPrefix(last, "[") && strings.HasSuffix(last, "]") {
			d, err := time.ParseDuration(strings.Trim(last, "[]"))
			if err != nil {
				ts.Fatalf("waitfor: bad timeout %q: %v", last, err)
			}
			timeout = d
			args = args[:n-1]
		}
	}
	pred, err := predicate(st, args)
	if err != nil {
		ts.Fatalf("waitfor: %v", err)
	}
	deadline := (clock.System{}).Now().Add(timeout)
	for {
		ok, pErr := pred()
		if pErr != nil {
			ts.Fatalf("waitfor: %v", pErr)
		}
		if ok != neg {
			return
		}
		if !(clock.System{}).Now().Before(deadline) {
			ts.Fatalf("waitfor: predicate %q not satisfied within %s", strings.Join(args, " "), timeout)
		}
		if sleepErr := (clock.System{}).Sleep(context.Background(), waitForPollInterval); sleepErr != nil {
			ts.Fatalf("waitfor: cancelled: %v", sleepErr)
		}
	}
}

// probeBudget bounds one predicate probe. A probe is local; a slow answer is a
// bug, not a reason to wait.
const probeBudget = 2 * time.Second

// predicate builds the polling predicate. `gone` is true when the socket no
// longer answers — how a script proves a stop actually stopped.
func predicate(st *State, args []string) (func() (bool, error), error) {
	switch args[0] {
	case "readyz", "healthz":
		if st.daemon == nil {
			return nil, errors.New("no daemon started")
		}
		cl, err := client.New(st.daemon.Addr())
		if err != nil {
			return nil, err
		}
		return func() (bool, error) {
			ctx, cancel := context.WithTimeout(context.Background(), probeBudget)
			defer cancel()
			return cl.Healthz(ctx) == nil, nil
		}, nil
	case "gone":
		if st.daemon == nil {
			return func() (bool, error) { return true, nil }, nil
		}
		sock := st.daemon.SocketPath()
		return func() (bool, error) {
			conn, err := (&net.Dialer{Timeout: 250 * time.Millisecond}).
				DialContext(context.Background(), "unix", sock)
			if err != nil {
				return true, nil //nolint:nilerr // the refused connect IS the "gone" answer
			}
			sink(conn.Close())
			return false, nil
		}, nil
	case "info":
		if len(args) != 2 {
			return nil, errors.New("info predicate wants `info <field>=<value>`")
		}
		field, want, found := strings.Cut(args[1], "=")
		if !found {
			return nil, fmt.Errorf("info predicate wants <field>=<value>, got %q", args[1])
		}
		if st.daemon == nil {
			return nil, errors.New("no daemon started")
		}
		cl, err := client.New(st.daemon.Addr())
		if err != nil {
			return nil, err
		}
		return func() (bool, error) {
			ctx, cancel := context.WithTimeout(context.Background(), probeBudget)
			defer cancel()
			info, iErr := cl.Info(ctx)
			if iErr != nil {
				return false, nil // not yet — the daemon may still be coming up
			}
			got, ok := infoField(info, field)
			return ok && got == want, nil
		}, nil
	default:
		return nil, fmt.Errorf("unknown predicate %q (want readyz | healthz | gone | info <field>=<value>; the pending form lands with #13/#14)", args[0])
	}
}

// infoField reads one field of the daemon info by its JSON name. The predicate is
// spelled in wire names, not Go names, because scripts quote the wire.
func infoField(info client.Info, field string) (string, bool) {
	switch field {
	case "version":
		return info.Version, true
	case "durability":
		return info.Durability, true
	default:
		return "", false
	}
}

// ---- capture ----

func cmdCapture(ts *testscript.TestScript, _ bool, args []string) {
	if len(args) != 2 {
		ts.Fatalf("capture: want `capture VAR $.field.path`")
	}
	doc, err := parseJSON([]byte(ts.ReadFile("stdout")))
	if err != nil {
		ts.Fatalf("capture: last stdout is not JSON: %v", err)
	}
	v, err := jsonPath(doc, args[1])
	if err != nil {
		ts.Fatalf("capture: %v", err)
	}
	ts.Setenv(args[0], stringify(v))
}

// jsonPath walks a decoded JSON tree by a "$.a.b[0].c" style path.
func jsonPath(doc any, path string) (any, error) {
	trimmed := strings.TrimPrefix(path, "$")
	if trimmed == path {
		return nil, fmt.Errorf("path %q must start with $", path)
	}
	cur := doc
	for _, seg := range strings.Split(trimmed, ".") {
		if seg == "" {
			continue
		}
		name := seg
		idx := -1
		if i := strings.IndexByte(seg, '['); i >= 0 {
			if !strings.HasSuffix(seg, "]") {
				return nil, fmt.Errorf("bad segment %q in %q", seg, path)
			}
			name = seg[:i]
			n, err := strconv.Atoi(strings.Trim(seg[i:], "[]"))
			if err != nil {
				return nil, fmt.Errorf("bad index in %q: %w", seg, err)
			}
			idx = n
		}
		if name != "" {
			m, ok := cur.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("%q: %T is not an object", name, cur)
			}
			cur, ok = m[name]
			if !ok {
				return nil, fmt.Errorf("no key %q", name)
			}
		}
		if idx >= 0 {
			arr, ok := cur.([]any)
			if !ok {
				return nil, fmt.Errorf("segment %q: %T is not an array", seg, cur)
			}
			if idx < 0 || idx >= len(arr) {
				return nil, fmt.Errorf("index %d out of range (%d items)", idx, len(arr))
			}
			cur = arr[idx]
		}
	}
	return cur, nil
}

func stringify(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case json.Number:
		return t.String()
	case bool:
		return strconv.FormatBool(t)
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64)
	case nil:
		return ""
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return fmt.Sprintf("%v", t)
		}
		return string(b)
	}
}

// ---- exitcode ----

func cmdExitCode(ts *testscript.TestScript, neg bool, args []string) {
	if len(args) < 2 {
		ts.Fatalf("exitcode: want `exitcode <n> messq <args…>`")
	}
	want, err := strconv.Atoi(args[0])
	if err != nil {
		ts.Fatalf("exitcode: bad code %q: %v", args[0], err)
	}
	rest := args[1:]
	if rest[0] == "messq" {
		rest = rest[1:]
	}
	runErr := ts.Exec("messq", rest...)
	got := 0
	if runErr != nil {
		var coder interface{ ExitCode() int }
		if !errors.As(runErr, &coder) {
			ts.Fatalf("exitcode: cannot determine exit status: %v", runErr)
		}
		got = coder.ExitCode()
		if got < 0 {
			ts.Fatalf("exitcode: command died by signal (status %d)", got)
		}
	}
	// ts.Exec wrote the captured bytes into ts.stdout/ts.stderr, but when this
	// builtin returns the runner's clearBuiltinStd overwrites both from the
	// builtin builders — so relay through the builders to leave the captured
	// output visible to the following stdout/stderr assertions. The builders are
	// in-memory strings: a write error here cannot exist, but errcheck wants the
	// value consumed and Fatalf is the honest consumption.
	if out := ts.ReadFile("stdout"); out != "" {
		if _, err := fmt.Fprint(ts.Stdout(), out); err != nil {
			ts.Fatalf("exitcode: relay stdout: %v", err)
		}
	}
	if errOut := ts.ReadFile("stderr"); errOut != "" {
		if _, err := fmt.Fprint(ts.Stderr(), errOut); err != nil {
			ts.Fatalf("exitcode: relay stderr: %v", err)
		}
	}
	if got != want {
		ts.Fatalf("exitcode: want %d, got %d", want, got)
	}
	if neg {
		ts.Fatalf("exitcode: ! on an exitcode assertion is meaningless")
	}
}

// ---- cmpjson ----

func cmdCmpJSON(ts *testscript.TestScript, neg bool, args []string) {
	if neg {
		ts.Fatalf("cmpjson: ! is meaningless; a canonical-JSON compare must hold")
	}
	if len(args) != 2 {
		ts.Fatalf("cmpjson: want `cmpjson <src> <want.json>` (src: stdout|stderr|file)")
	}
	gotRaw, err := readSrc(ts, args[0])
	if err != nil {
		ts.Fatalf("cmpjson: %v", err)
	}
	got, err := canonJSON(gotRaw)
	if err != nil {
		ts.Fatalf("cmpjson: %v", err)
	}
	wantRaw := []byte(ts.ReadFile(args[1]))
	want, err := canonJSON(wantRaw)
	if err != nil {
		ts.Fatalf("cmpjson: golden %s is not JSON: %v", args[1], err)
	}
	if bytes.Equal(got, want) {
		return
	}
	st := stateFrom(ts)
	if st.update {
		if err := updateGolden(st, ts, args[1], got); err != nil {
			ts.Fatalf("cmpjson: update: %v", err)
		}
		ts.Logf("cmpjson: rewrote %s under -update", args[1])
		return
	}
	ts.Fatalf("cmpjson: %s differs from %s\n--- got (canonical) ---\n%s\n--- want (canonical) ---\n%s",
		args[0], args[1], got, want)
}

// canonJSON canonicalises one JSON document: key order and whitespace stop being
// contract the moment this runs (issue #18's rule, applied to CLI goldens).
func canonJSON(raw []byte) ([]byte, error) {
	doc, err := parseJSON(raw)
	if err != nil {
		return nil, err
	}
	return wirecheck.Canonical(doc)
}

// ---- mask ----

func cmdMask(ts *testscript.TestScript, _ bool, args []string) {
	if len(args) != 2 {
		ts.Fatalf("mask: want `mask <src> <dst>` (src: stdout|stderr|file)")
	}
	raw, err := readSrc(ts, args[0])
	if err != nil {
		ts.Fatalf("mask: %v", err)
	}
	st := stateFrom(ts)
	var masked []byte
	if _, jErr := parseJSON(raw); jErr == nil {
		norm := wirecheck.NewNormalizer(st.workDir)
		masked, jErr = norm.Normalize(raw)
		if jErr != nil {
			ts.Fatalf("mask: %v", jErr)
		}
	} else {
		masked = []byte(maskText(string(raw), st.workDir))
	}
	dst := ts.MkAbs(args[1])
	if wErr := os.WriteFile(dst, masked, 0o600); wErr != nil {
		ts.Fatalf("mask: write %s: %v", args[1], wErr)
	}
}

// maskText applies the textual mask rules to non-JSON output: ULIDs, trace ids,
// RFC3339 timestamps and the work dir. Ack tokens, seq, attempt and counts are
// never masked — a token in a golden is a free fencing-arithmetic assertion.
func maskText(s, workDir string) string {
	s = ulidTextRe.ReplaceAllString(s, "<ULID>")
	s = traceTextRe.ReplaceAllString(s, "<TRACE>")
	s = tsTextRe.ReplaceAllString(s, "<TS>")
	if workDir != "" {
		s = strings.ReplaceAll(s, workDir, "$WORK")
	}
	return s
}

// Textual twins of the wirecheck mask regexes, widened to match anywhere in a
// line instead of whole-string.
var (
	ulidTextRe  = regexp.MustCompile(`\b[0-9A-HJKMNP-TV-Z]{26}\b`)
	traceTextRe = regexp.MustCompile(`\b[0-9a-f]{32}\b`)
	tsTextRe    = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})`)
)

// ---- source reading ----

func readSrc(ts *testscript.TestScript, src string) ([]byte, error) {
	switch src {
	case "stdout", "stderr":
		return []byte(ts.ReadFile(src)), nil
	default:
		b := ts.ReadFile(src)
		if b == "" {
			return nil, fmt.Errorf("cannot read %q", src)
		}
		return []byte(b), nil
	}
}

func parseJSON(raw []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var doc any
	if err := dec.Decode(&doc); err != nil {
		return nil, err
	}
	return doc, nil
}

// ---- -update plumbing ----

// updateGolden rewrites a golden section inside the running script's txtar, or
// the file itself when the golden lives outside the archive. testscript's builtin
// cmp cannot be driven from a custom command (its update path is unexported), so
// cmpjson does its own section rewrite against the same on-disk format.
func updateGolden(st *State, ts *testscript.TestScript, name string, content []byte) error {
	scriptPath, err := locateScript(st, ts)
	if err != nil {
		return err
	}
	ar, err := txtar.ParseFile(scriptPath)
	if err != nil {
		return err
	}
	base := filepath.Base(name)
	for i := range ar.Files {
		if filepath.Base(ar.Files[i].Name) != base {
			continue
		}
		ar.Files[i].Data = content
		return writeFileAtomic(scriptPath, txtar.Format(ar))
	}
	// Not a section: the golden is a plain file next to the scripts.
	return writeFileAtomic(ts.MkAbs(name), content)
}

// locateScript finds the running script's file. testscript exposes the script's
// name but not its directory, so the Suite records the directory in the state.
func locateScript(st *State, ts *testscript.TestScript) (string, error) {
	name := ts.Name()
	for _, ext := range []string{".txtar", ".txt"} {
		p := filepath.Join(st.scriptDir, name+ext)
		if fileExists(p) {
			return p, nil
		}
	}
	return "", fmt.Errorf("cannot locate script %s under %s", name, st.scriptDir)
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

// sink consumes an error whose only possible handler is a log line no one would
// see: teardown after a failed script and cleanup of a temp file are best-effort
// by design. The guard keeps the call legal when debugSink is nil.
func sink(err error) {
	if err != nil && debugSink != nil {
		debugSink(err)
	}
}

// debugSink is set by tests that want teardown errors visible; production keeps
// it nil and the errors stay consumed-and-silent.
var debugSink func(error)

// writeFileAtomic writes via temp+rename so a failed update never truncates the
// script it is rewriting.
func writeFileAtomic(path string, content []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".scriptenv-update-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		if tmpName != "" {
			sink(os.Remove(tmpName))
		}
	}()
	if _, err := tmp.Write(content); err != nil {
		sink(tmp.Close())
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpName, 0o644); err != nil { //nolint:gosec // the target is a committed repo source, 0644 is its mode
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	tmpName = "" // renamed into place; nothing left for the deferred remove
	return nil
}
