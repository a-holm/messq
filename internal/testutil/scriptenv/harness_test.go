// SPDX-License-Identifier: Apache-2.0

package scriptenv

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rogpeppe/go-internal/testscript"
)

// The harness lanes: the package's own testscript run over tiny scripts written
// to a temp dir. The golden suite in test/script drives the same command set
// through the re-exec'd real CLI; these lanes cover the behaviours that suite
// cannot reach — the daemon arms a healthy script never fails into, the clock
// commands' quiesce path, and the rendering commands against payloads whose
// expected forms are computed here rather than recorded from a daemon.

// fakeOut is the lanes' stand-in for the re-exec'd messq binary: it prints a
// named payload to the captured stdout, so cmpshape/capture/mask/cmpjson run
// against known bytes. `show` echoes a captured variable back, which is how the
// capture lane asserts what `capture` installed into the environment.
func fakeOut(ts *testscript.TestScript, _ bool, args []string) {
	if len(args) == 0 {
		ts.Fatalf("fakeout: want a payload name")
	}
	switch args[0] {
	case "show":
		if len(args) != 2 {
			ts.Fatalf("fakeout: show wants the value to echo")
		}
		printOut(ts, "captured VER=%s\n", args[1])
	case "buildinfo":
		printOut(ts, "%s", buildinfoPayload)
	case "doc":
		printOut(ts, "%s", docPayload)
	case "tracejson":
		printOut(ts, "%s", traceJSONPayload)
	case "plain":
		// The work dir only enters through the setup-installed XDG root, so the
		// textual mask's $WORK rewrite has a deterministic input to normalise.
		printOut(ts, "published 01ARZ3NDEKTSV4RRFFQ69G5FAV seq 1\n"+
			"trace 4bf92f3577b34da6a3ce929d0e0e4736 at %s\n"+
			"2026-08-26T12:00:00.114Z durable\n", ts.Getenv("XDG_STATE_HOME"))
	default:
		ts.Fatalf("fakeout: unknown payload %q", args[0])
	}
}

func printOut(ts *testscript.TestScript, format string, args ...any) {
	if _, err := fmt.Fprintf(ts.Stdout(), format, args...); err != nil {
		ts.Fatalf("fakeout: write: %v", err)
	}
}

// TestCommandsThroughTestscript runs the daemon and rendering lanes through the
// suite's own Params: the same setup, command registry and per-script state the
// golden suite uses, with fakeOut added as the stdout fixture.
func TestCommandsThroughTestscript(t *testing.T) {
	dir := t.TempDir()
	for name, body := range map[string]string{
		"daemon_lane.txtar": daemonLane,
		"render_lane.txtar": renderLane,
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	suite := Suite{Dir: dir}
	params := suite.Params()
	params.Cmds["fakeout"] = fakeOut
	testscript.Run(t, params)
}

// TestUpdateRewritesTheGoldenSection pins cmpjson's -update path end to end: a
// drifted golden section is rewritten in place inside the running script's
// txtar, holding the canonical form of what the command printed.
func TestUpdateRewritesTheGoldenSection(t *testing.T) {
	dir := t.TempDir()
	const script = "# cmpjson under -update rewrites the golden section in place.\n\n" +
		"fakeout doc\ncmpjson stdout want.json\n\n-- want.json --\n{\"a\":9}\n"
	if err := os.WriteFile(filepath.Join(dir, "update_lane.txtar"), []byte(script), 0o600); err != nil {
		t.Fatal(err)
	}
	suite := Suite{Dir: dir, Update: true}
	params := suite.Params()
	params.Cmds["fakeout"] = fakeOut
	testscript.Run(t, params)

	// testscript.Run returns while its scripts are parked in t.Parallel; the
	// resumed lane may still be rewriting, so the on-disk assertion belongs in
	// cleanup, which runs after the subtests finish.
	t.Cleanup(func() {
		data, err := os.ReadFile(filepath.Join(dir, "update_lane.txtar"))
		if err != nil {
			t.Fatal(err)
		}
		const want = "-- want.json --\n{\n  \"a\": 1,\n  \"b\": 2\n}\n"
		if !strings.Contains(string(data), want) {
			t.Errorf("the drifted golden section was not rewritten in place; got:\n%s", data)
		}
	})
}

// daemonLane walks the daemon lifecycle through every arm the golden scripts
// can reach (start, stop, restart, kill) plus the clock commands' quiesce path.
// The rm between restart and stop is what turns `waitfor gone` into the
// dial-refused answer while the daemon still holds the listener.
const daemonLane = `# The command harness's daemon lane: the full lifecycle over a real inproc
# daemon on a unix socket under $WORK, with the clock driven between lines.

daemon start
waitfor readyz
waitfor healthz
waitfor info durability=full
clock advance 1s
clock set 2026-08-26T13:00:00Z
daemon restart
waitfor readyz
rm messq.sock
waitfor gone
daemon stop
waitfor gone
daemon start
daemon kill
waitfor gone
`

// renderLane drives the shape check, the capture round-trip, both mask faces
// and the canonical-JSON compare over fakeOut payloads. The goldens are
// computed expectations: `dev` is the captured field, the textual mask's output
// is the masked input with the work dir normalised, and want.json is a
// differently-ordered document that must compare equal after canonicalisation.
const renderLane = `# The command harness's rendering lane: shape, capture, mask and cmpjson
# against payloads whose expected forms are computed, not recorded.

fakeout buildinfo
cmpshape stdout BuildInfo
capture VER $.version
fakeout show $VER
cmp stdout want-captured.txt
fakeout plain
mask stdout masked.txt
cmp masked.txt want-masked.txt
fakeout tracejson
mask stdout masked-json.txt
grep '<TRACE>' masked-json.txt
grep '<TS>' masked-json.txt
fakeout doc
cmpjson stdout want.json

-- want-captured.txt --
captured VER=dev
-- want-masked.txt --
published <ULID> seq 1
trace <TRACE> at $WORK/state
<TS> durable
-- want.json --
{"b":2,"a":1}
`

// The fakeOut payloads. buildinfo carries every BuildInfo field so the shape
// check sees the full prototype; doc is deliberately mis-ordered so only the
// canonical compare can accept it; tracejson carries the two volatile string
// forms the JSON mask rewrites.
const (
	buildinfoPayload = `{"version":"dev","commit":"","date":"","dirty":false,"go_version":"go1.26","platform":"test"}`
	docPayload       = `{"b":2,"a":1}`
	traceJSONPayload = `{"trace":"4bf92f3577b34da6a3ce929d0e0e4736","when":"2026-08-26T12:00:00.114Z"}`
)
