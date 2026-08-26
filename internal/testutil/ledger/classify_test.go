// SPDX-License-Identifier: Apache-2.0

package ledger

import (
	"testing"
)

// TestClassifyTable is G2: one case per row of the verdict table (issue §Design-3), plus
// the two asymmetric cases the table does not list — a transport failure (status 0) and an
// unmapped code, which must produce an error rather than a verdict.
func TestClassifyTable(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		code    string
		want    Verdict
		wantErr bool
	}{
		{"201 new publish", 201, "", OK, false},
		{"200 duplicate", 200, "", OK, false},
		{"400 bad_request", 400, "bad_request", Failed, false},
		{"404 not_found", 404, "not_found", Failed, false},
		{"413 too_large", 413, "too_large", Failed, false},
		{"422 subject_mismatch", 422, "subject_mismatch", Failed, false},
		{"507 disk_full", 507, "disk_full", Failed, false},
		{"500 internal", 500, "internal", Unknown, false},
		{"503 read_only", 503, "read_only", Unknown, false},
		{"503 shutting_down", 503, "shutting_down", Unknown, false},
		// Publish-path backpressure 503s (#6/#18): refusals or unknown-fate answers,
		// never a definite failure — the same convention as read_only/shutting_down.
		// Pinned because a starved writer under parallel load really emits them
		// (TestOneGreenCycle died on an unclassified commit_unknown).
		{"503 busy", 503, "busy", Unknown, false},
		{"503 commit_unknown", 503, "commit_unknown", Unknown, false},
		{"transport failure", 0, "", Unknown, false},
		{"unmapped code", 500, "something_never_seen", Unknown, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Classify(tt.status, tt.code)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Classify(%d, %q) error = %v, wantErr %v", tt.status, tt.code, err, tt.wantErr)
			}
			if err == nil && got != tt.want {
				t.Errorf("Classify(%d, %q) = %v, want %v", tt.status, tt.code, got, tt.want)
			}
		})
	}
}

// TestClassifyResponseEnvelope proves the envelope-decode path classifies the code field and
// refuses a body that is not an envelope.
func TestClassifyResponseEnvelope(t *testing.T) {
	v, code, err := ClassifyResponse(400, []byte(`{"error":{"code":"bad_request","message":"nope"}}`))
	if err != nil {
		t.Fatalf("ClassifyResponse: %v", err)
	}
	if v != Failed || code != "bad_request" {
		t.Errorf("ClassifyResponse = (%v, %q), want (Failed, bad_request)", v, code)
	}

	if _, _, err := ClassifyResponse(500, []byte(`not json`)); err == nil {
		t.Error("ClassifyResponse of a non-envelope body must error")
	}
}
