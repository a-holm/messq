// SPDX-License-Identifier: Apache-2.0

package clitest_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/cli/clitest"
)

// The review-probe envelope: a daemon 404 with every teaching field set. The CLI
// must re-emit THESE bytes, not its own reformatted sentence.
const reviewEnvelope = `{"error":{"code":"not_found",` +
	`"message":"consumer \"bilingg\" not found in stream \"orders\"",` +
	`"next":["messq consumer add orders bilingg --ack-wait 30s"],` +
	`"detail":{"stream":"orders","consumer":"bilingg"},` +
	`"trace_id":"01J8ZTRACE"}}`

func routeReviewEnvelope(t *testing.T) *clitest.FakeDaemon {
	t.Helper()
	d := clitest.NewFakeDaemon(t)
	d.Route("GET", "/v1/info", clitest.Response{Status: 404, Body: reviewEnvelope})
	return d
}

// asEnvelope parses stderr as exactly one JSON error document.
func asEnvelope(t *testing.T, stderr string) map[string]any {
	t.Helper()
	line := strings.TrimSpace(stderr)
	var doc map[string]any
	if err := json.Unmarshal([]byte(line), &doc); err != nil {
		t.Fatalf("machine stderr is not one JSON document (%v):\n%q", err, stderr)
	}
	inner, ok := doc["error"].(map[string]any)
	if !ok {
		t.Fatalf("stderr is JSON but carries no error object:\n%q", stderr)
	}
	return inner
}

// TestMachineErrorFaceThroughRealFunnel is the review's breaking probe, pinned:
// with --output json the funnel must re-emit the server's error envelope as JSON on
// stderr — verbatim message, next[], detail and trace_id — never prose. It runs the
// executing child (version --remote), so it fails while the resolved format lives
// only on the child's context and the funnel reads the root's.
func TestMachineErrorFaceThroughRealFunnel(t *testing.T) {
	d := routeReviewEnvelope(t)
	res := clitest.Run(t, clitest.Runner{}, "--addr", d.Addr(), "--output", "json", "version", "--remote")

	if res.Exit != 3 {
		t.Fatalf("exit = %d, want 3 (stderr %q)", res.Exit, res.Stderr)
	}
	if strings.Contains(res.Stdout, "not_found") || strings.Contains(res.Stdout, `"error"`) {
		t.Errorf("an error object rode stdout (%q); errors belong on stderr alone", res.Stdout)
	}
	env := asEnvelope(t, res.Stderr)
	if env["code"] != "not_found" {
		t.Errorf("envelope code = %v, want not_found", env["code"])
	}
	if env["message"] != `consumer "bilingg" not found in stream "orders"` {
		t.Errorf("envelope message drifted from the daemon's sentence: %v", env["message"])
	}
	next, ok := env["next"].([]any)
	if !ok || len(next) != 1 || next[0] != "messq consumer add orders bilingg --ack-wait 30s" {
		t.Errorf("envelope next[] drifted: %v", env["next"])
	}
	detail, ok := env["detail"].(map[string]any)
	if !ok || detail["stream"] != "orders" || detail["consumer"] != "bilingg" {
		t.Errorf("envelope detail drifted: %v", env["detail"])
	}
	if env["trace_id"] != "01J8ZTRACE" {
		t.Errorf("trace_id lost: %v", env["trace_id"])
	}
}

// TestHumanErrorFaceIsTheDaemonsSentence pins the table-mode twin: no invented
// "messq: [404] code:" prefix in front of the daemon's sentence, and the server's
// next-command survives verbatim into the what-to-type block.
func TestHumanErrorFaceIsTheDaemonsSentence(t *testing.T) {
	d := routeReviewEnvelope(t)
	res := clitest.Run(t, clitest.Runner{}, "--addr", d.Addr(), "--output", "table", "version", "--remote")

	if res.Exit != 3 {
		t.Fatalf("exit = %d, want 3 (stderr %q)", res.Exit, res.Stderr)
	}
	err := res.Stderr
	if !strings.Contains(err, `Error: consumer "bilingg" not found in stream "orders"`) {
		t.Errorf("human face lost the daemon's verbatim message:\n%s", err)
	}
	if strings.Contains(err, "messq: [404]") {
		t.Errorf("human face invented an Error() prefix around the daemon's sentence:\n%s", err)
	}
	if !strings.Contains(err, "messq consumer add orders bilingg --ack-wait 30s") {
		t.Errorf("human face lost the daemon's next[] verbatim:\n%s", err)
	}
}

// TestTypoedAddrExitsUsage2 pins the exit-code half of the bad-address refusal: the
// classifier maps the client-local bad_address wire code to usage (2) through
// ByWireCode, and the refusal teaches the accepted forms instead of exiting 1.
func TestTypoedAddrExitsUsage2(t *testing.T) {
	d := clitest.NewFakeDaemon(t) // nothing routed; a dial would be visible below
	res := clitest.Run(t, clitest.Runner{},
		"--addr", "xtcp://127.0.0.1:9", "--output", "json", "version", "--remote")

	if res.Exit != 2 {
		t.Fatalf("exit = %d, want 2 for a typoed --addr (stderr %q)", res.Exit, res.Stderr)
	}
	env := asEnvelope(t, res.Stderr)
	if env["code"] != "bad_address" {
		t.Errorf("envelope code = %v, want bad_address", env["code"])
	}
	msg, msgOK := env["message"].(string)
	if !msgOK {
		t.Fatalf("envelope message missing: %v", env["message"])
	}
	if !strings.Contains(msg, "is not a messq address") {
		t.Errorf("refusal lost the client's teaching message: %v", env["message"])
	}
	next, ok := env["next"].([]any)
	if !ok || len(next) == 0 {
		t.Errorf("bad_address teaches nothing to type next: %v", env["next"])
	}
	if n := len(d.Requests()); n != 0 {
		t.Errorf("a refused address dialled the fake daemon %d time(s)", n)
	}
}
