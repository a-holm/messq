// SPDX-License-Identifier: Apache-2.0

package uierr

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/cli/render"
	"github.com/a-holm/messq/pkg/client"
)

const canary = "\x1b]0;DAEMON-CANARY\x07 cannot reach replica bilingg-3"

func daemonError() *client.Error {
	return &client.Error{
		Code:    "not_found",
		Message: "consumer \"bilingg\" not found in stream \"orders\"",
		Next:    []string{"messq consumer add orders bilingg --ack-wait 30s"},
		Detail:  map[string]any{"stream": "orders", "consumer": "bilingg"},
		TraceID: "01J8ZTRACE",
	}
}

func human(t *testing.T, err error) string {
	t.Helper()
	var buf bytes.Buffer
	Render(&Env{Stderr: &buf, Format: render.FormatTable}, err, 3)
	return buf.String()
}

func machine(t *testing.T, err error, code int) map[string]any {
	t.Helper()
	var buf bytes.Buffer
	Render(&Env{Stderr: &buf, Format: render.FormatJSON}, err, code)
	line := strings.TrimSpace(buf.String())
	var doc map[string]any
	if err := json.Unmarshal([]byte(line), &doc); err != nil {
		t.Fatalf("machine stderr is not one JSON document (%v): %q", err, buf.String())
	}
	if _, ok := doc["error"]; !ok {
		t.Fatalf("machine stderr is not an error envelope: %q", line)
	}
	return asMap(t, doc["error"])
}

// asMap/asSlice are checked assertions: a malformed envelope is a test failure with
// a message, not a panic.
func asMap(t *testing.T, v any) map[string]any {
	t.Helper()
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("expected a JSON object, got %T", v)
	}
	return m
}

func asSlice(t *testing.T, v any) []any {
	t.Helper()
	s, ok := v.([]any)
	if !ok {
		t.Fatalf("expected a JSON array, got %T", v)
	}
	return s
}

// TestNoInventedText is THE rule: when the daemon returned an envelope, everything
// the operator reads is the daemon's sentence, byte-for-byte. The CLI adds structure,
// never prose.
func TestNoInventedText(t *testing.T) {
	ue := FromClient(daemonError(), "")
	if !strings.Contains(human(t, ue), "consumer \"bilingg\" not found in stream \"orders\"") {
		t.Errorf("human face lost the daemon's message: %q", human(t, ue))
	}
	if !strings.Contains(human(t, ue), "messq consumer add orders bilingg --ack-wait 30s") {
		t.Errorf("human face lost the daemon's next-command verbatim: %q", human(t, ue))
	}
	var buf bytes.Buffer
	Render(&Env{Stderr: &buf, Format: render.FormatJSON}, ue, 3)
	var doc map[string]any
	if err := json.Unmarshal(buf.Bytes(), &doc); err != nil {
		t.Fatalf("machine stderr not one document: %v", err)
	}
	envelope := asMap(t, doc["error"])
	if envelope["message"] != "consumer \"bilingg\" not found in stream \"orders\"" {
		t.Errorf("machine face message drifted: %v", envelope["message"])
	}
	next := asSlice(t, envelope["next"])
	if len(next) != 1 || next[0] != "messq consumer add orders bilingg --ack-wait 30s" {
		t.Errorf("machine face next[] drifted: %v", next)
	}
	// A canary smuggled inside the daemon's message survives untouched — including
	// control characters, which Safe() escapes but never rewords.
	mutant := daemonError()
	mutant.Message = canary
	out := human(t, FromClient(mutant, ""))
	if !strings.Contains(out, "\\x1b]0;DAEMON-CANARY\\x07 cannot reach replica bilingg-3") {
		t.Errorf("canary was reworded instead of escaped verbatim: %q", out)
	}
}

func TestHumanFaceShape(t *testing.T) {
	ue := FromClient(daemonError(), "")
	ue.Because = "A consumer holds the cursor and the ack state."
	ue.Help = "messq help concepts"
	out := human(t, ue)
	order := []string{
		"Error: consumer \"bilingg\" not found",
		"A consumer holds the cursor and the ack state.",
		"messq consumer add orders bilingg",
		"messq help concepts",
	}
	pos := -1
	for _, want := range order {
		next := strings.Index(out, want)
		if next == -1 {
			t.Errorf("human face missing %q:\n%s", want, out)
			continue
		}
		if next < pos {
			t.Errorf("human face out of order: %q appears before the previous section\n%s", want, out)
		}
		pos = next
	}
}

func TestSuggestDidYouMean(t *testing.T) {
	ue := FromClient(daemonError(), "")
	ue.Suggest = []string{"billing"}
	out := human(t, ue)
	if !strings.Contains(out, "Did you mean") || !strings.Contains(out, "billing") {
		t.Errorf("did-you-mean block missing: %q", out)
	}
}

func TestNonUserErrorRendersPlainly(t *testing.T) {
	plain := fmt.Errorf("dial unix /run/messq/messq.sock: connect: no such file or directory")
	out := human(t, plain)
	if !strings.HasPrefix(out, "Error: dial unix /run/messq/messq.sock") {
		t.Errorf("plain error not rendered: %q", out)
	}
	doc := machine(t, plain, 6)
	if got := doc["code"]; got != "unreachable" {
		t.Errorf("synthesised code = %v, want unreachable for exit 6", got)
	}
}

func TestMachineEnvelopeCarriesDaemonFieldsVerbatim(t *testing.T) {
	doc := machine(t, FromClient(daemonError(), ""), 3)
	if doc["message"] != "consumer \"bilingg\" not found in stream \"orders\"" {
		t.Errorf("envelope message drifted: %v", doc["message"])
	}
	next := asSlice(t, doc["next"])
	if len(next) != 1 || next[0] != "messq consumer add orders bilingg --ack-wait 30s" {
		t.Errorf("envelope next[] drifted: %v", next)
	}
	detail := asMap(t, doc["detail"])
	if detail["stream"] != "orders" || detail["consumer"] != "bilingg" {
		t.Errorf("envelope detail drifted: %v", detail)
	}
	if doc["trace_id"] != "01J8ZTRACE" {
		t.Errorf("trace_id lost: %v", doc["trace_id"])
	}
}

func TestLocalCodesForCLIFailures(t *testing.T) {
	tests := []struct {
		code int
		want string
	}{
		{2, "usage"},
		{5, "wait_expired"},
		{6, "unreachable"},
		{7, "token_file_perms"},
		{130, "interrupted"},
	}
	for _, tt := range tests {
		doc := machine(t, errors.New("probe failure"), tt.code)
		if doc["code"] != tt.want {
			t.Errorf("exit %d synthesised code %v, want %q", tt.code, doc["code"], tt.want)
		}
	}
}

func TestUnwrapAndErrorFallbacks(t *testing.T) {
	cause := errors.New("the socket vanished")
	ue := &UserError{Summary: "could not publish", Cause: cause}
	if !errors.Is(ue, cause) {
		t.Error("UserError.Unwrap lost the cause chain")
	}
	bare := &UserError{Code: "internal"}
	if bare.Error() != "internal" {
		t.Errorf("Error() = %q, want the code as last resort", bare.Error())
	}
}

// TestAddrContextForTransportFailures: the one thing the daemon cannot know and the
// CLI can is which address was dialled; FromClient attaches it for local transport
// failures only, never rewriting a server suggestion.
func TestAddrContextForTransportFailures(t *testing.T) {
	local := &client.Error{Code: "unavailable", Message: "dial: connection refused", Status: 0}
	out := human(t, FromClient(local, "unix:///tmp/x.sock"))
	if !strings.Contains(out, "unix:///tmp/x.sock") {
		t.Errorf("dialled address not surfaced:\n%s", out)
	}
	envelope := daemonError()
	envelope.Status = 404
	out = human(t, FromClient(envelope, "unix:///tmp/x.sock"))
	if strings.Contains(out, "unix:///tmp/x.sock") {
		t.Errorf("address context injected into a real envelope reply (invented text):\n%s", out)
	}
}

func TestConstructorsAlwaysTeachANextCommand(t *testing.T) {
	// No UserError without a Next entry: a teaching error that teaches nothing is a
	// bare error wearing a costume.
	usage := Usage("--output yaml: use auto|table|json|ndjson")
	if len(usage.Next) == 0 {
		t.Error("Usage() built a UserError with nothing to type next")
	}
	if usage.Exit != 2 {
		t.Errorf("Usage().Exit = %d, want 2", usage.Exit)
	}
	if usage.ExitCode() != 2 {
		t.Errorf("Usage().ExitCode() = %d, want 2", usage.ExitCode())
	}
	wrapped := fmt.Errorf("flag layer: %w", usage)
	var recovered *UserError
	if !errors.As(wrapped, &recovered) || recovered.ExitCode() != 2 {
		t.Error("wrapped UserError did not survive errors.As")
	}
	if !strings.Contains(human(t, wrapped), "--output yaml") {
		t.Error("usage summary lost in human rendering")
	}
}

func TestQuietSuppressedByCallerNotRenderer(t *testing.T) {
	// Quiet is enforced upstream (the funnel decides whether to render at all);
	// the renderer itself always writes once it is called, so narration cannot be
	// silently dropped twice or dropped for data.
	var buf bytes.Buffer
	Render(&Env{Stderr: &buf, Format: render.FormatJSON}, errors.New("x"), 1)
	if buf.Len() == 0 {
		t.Error("renderer produced nothing")
	}
}
