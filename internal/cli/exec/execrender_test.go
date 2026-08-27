// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/cli/render"
)

// ---- one-time hint (G9) ----------------------------------------------------

func TestHintPrintsExactlyOnceAndSuppressible(t *testing.T) {
	var b bytes.Buffer
	h := NewHintPrinter(&b, false)
	h.PrintOnce()
	h.PrintOnce()
	h.PrintOnce()
	if got := strings.Count(b.String(), "hint: with --exec"); got != 1 {
		t.Fatalf("hint printed %d times, want exactly once", got)
	}
	if !strings.Contains(b.String(), "65   term") || !strings.Contains(b.String(), "75   nak") {
		t.Fatalf("hint lost its table rows:\n%s", b.String())
	}
	var silent bytes.Buffer
	ns := NewHintPrinter(&silent, true)
	ns.PrintOnce()
	if silent.Len() != 0 {
		t.Fatalf("--no-hints leaked output: %q", silent.String())
	}
	// Nil receiver and nil writer are both tolerated no-ops.
	var nilH *HintPrinter
	nilH.PrintOnce()
	NewHintPrinter(nil, false).PrintOnce()
}

// The hint text is GENERATED FROM docs/exit-codes.md (G9 executed): every row of
// the doc's child-side table must appear in the hint; drift fails here first.
func TestHintMatchesDocs(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	docPath := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "docs", "exit-codes.md")
	raw, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("docs/exit-codes.md unreadable (%v): hint/doc binding broken", err)
	}
	doc := string(raw)
	for _, row := range []string{"0    ack", "75   nak", "65   term"} {
		if !strings.Contains(doc, strings.TrimSpace(row)) &&
			!strings.Contains(doc, "| "+strings.Fields(row)[0]+" ") {
			t.Fatalf("docs/exit-codes.md lacks code %q rows", row)
		}
		if !strings.Contains(hintText, row) {
			t.Fatalf("hint text lost doc row %q", row)
		}
	}
	for _, phrase := range []string{"EX_TEMPFAIL", "EX_DATAERR", "messq trace <msg-id>"} {
		if !strings.Contains(doc, phrase) && !strings.Contains(hintText, phrase) {
			t.Fatalf("neither docs nor hint carries %q", phrase)
		}
	}
}

// ---- frozen NDJSON record + faces ------------------------------------------

func TestNDJSONRecordSchemaFieldsPinned(t *testing.T) {
	res := Result{Outcome: OutcomeNak, ExitCode: 75, Reason: "upstream 503"}
	rec := recordFromResult(sampleMsg(), res, 1183, 5000, fixedTS())
	b, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	var keys map[string]any
	if err := json.Unmarshal(b, &keys); err != nil {
		t.Fatal(err)
	}
	wantOrder := []string{
		"ts", "stream", "consumer", "seq", "msg_id", "subject", "attempt",
		"max_deliver", "exit_code", "signal", "outcome", "duration_ms",
		"retry_in_ms", "reason", "reason_truncated", "trace_id",
	}
	if len(keys) != len(wantOrder) {
		t.Fatalf("record has %d fields, contract pins %d exactly", len(keys), len(wantOrder))
	}
	for _, k := range wantOrder {
		if _, ok := keys[k]; !ok {
			t.Fatalf("frozen field %q missing from record", k)
		}
	}
}

func TestEmitterFacesAndCounts(t *testing.T) {
	res := Result{Outcome: OutcomeNak, ExitCode: 75, Reason: "boom"}
	rec := recordFromResult(sampleMsg(), res, 5, 1000, fixedTS())

	var nd bytes.Buffer
	e := NewEmitter(&nd, render.FormatNDJSON)
	if err := e.Emit(rec); err != nil {
		t.Fatal(err)
	}
	if !json.Valid(bytes.TrimRight(nd.Bytes(), "\n")) {
		t.Fatalf("ndjson frame invalid: %q", nd.String())
	}

	var js bytes.Buffer
	j := NewEmitter(&js, render.FormatJSON)
	for i := 0; i < 3; i++ {
		if eerr := j.Emit(recordFromResult(sampleMsg(), res, 1, 0, fixedTS())); eerr != nil {
			t.Fatalf("json emit: %v", eerr)
		}
	}
	sum := j.SummaryNow()
	if sum.Messages != 3 || sum.Naks != 3 {
		t.Fatalf("summary counters wrong: %+v", sum)
	}

	var human bytes.Buffer
	h := NewEmitter(&human, render.FormatTable)
	if herr := h.Emit(recordFromResult(sampleMsg(), res, 12, 0, fixedTS())); herr != nil {
		t.Fatalf("human emit: %v", herr)
	}
	out := human.String()
	for _, frag := range []string{"attempt 2/5", "exit 75", "nak", `"boom"`} {
		if !strings.Contains(out, frag) {
			t.Fatalf("table face missing %q: %q", frag, out)
		}
	}
	// Machine modes never receive prose.
	if strings.Contains(nd.String(), "worker stopped") {
		t.Fatal("ndjson frame polluted by prose")
	}
}

// (stdout sink defaults for machine modes are pinned once in child_unit_test)

func fixedTS() time.Time { return time.UnixMilli(1761124442114).UTC() }
