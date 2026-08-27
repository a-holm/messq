// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"bytes"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/cli/render"
)

func TestWriteSummaryFacesAndStart(t *testing.T) {
	res := Result{Outcome: OutcomeTerm, ExitCode: 65, Reason: "bad json"}

	var js bytes.Buffer
	j := NewEmitter(&js, render.FormatJSON)
	j.Start(fixedTS())
	if err := j.Emit(recordFromResult(sampleMsg(), res, 40, 0, fixedTS())); err != nil {
		t.Fatal(err)
	}
	if err := j.WriteSummary(300); err != nil {
		t.Fatal(err)
	}
	body := js.String()
	for _, frag := range []string{`"terms":1`, `"messages":1`, `"duration_ms":300`} {
		if !strings.Contains(body, frag) {
			t.Fatalf("json summary missing %q:\n%s", frag, body)
		}
	}
	if strings.Contains(body, "worker stopped") {
		t.Fatal("prose leaked into json summary")
	}

	var human bytes.Buffer
	h := NewEmitter(&human, render.FormatTable)
	if err := h.Emit(recordFromResult(sampleMsg(), res, 40, 0, fixedTS())); err != nil {
		t.Fatal(err)
	}
	if err := h.WriteSummary(300); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(human.String(), "worker stopped: 1 messages · 0 ack · 0 nak · 1 term · 0 lease-lost · 300ms") {
		t.Fatalf("human summary drifted:\n%s", human.String())
	}

	var nd bytes.Buffer
	n := NewEmitter(&nd, render.FormatNDJSON)
	if err := n.WriteSummary(300); err != nil {
		t.Fatal(err)
	}
	if nd.Len() != 0 {
		t.Fatalf("ndjson summary must stay silent, got %q", nd.String())
	}

	// Unresolved Auto is refused by design.
	a := NewEmitter(&bytes.Buffer{}, render.FormatAuto)
	rec := recordFromResult(sampleMsg(), res, 1, 0, fixedTS())
	if err := a.Emit(rec); err == nil || !strings.Contains(err.Error(), "not resolved") {
		t.Fatalf("Auto must be refused explicitly: %v", err)
	}
}

var _ = bytes.MinRead
