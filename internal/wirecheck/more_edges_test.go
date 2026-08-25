// SPDX-License-Identifier: Apache-2.0

package wirecheck

import (
	"bytes"
	"strings"
	"testing"
)

// The remaining edge behaviours, pinned: ParseDigest rejects hand-edited lines,
// canonicalisation refuses values it cannot represent honestly, and the classifier's
// note-change matrix separates review-only drift from a marshaler takeover. Plus the
// walker branches a plain DTO never reaches: embedded structs, unexported skips,
// fixed-length arrays, maps of structs and method-bearing interfaces.

func TestParseDigestRejectsMalformedLines(t *testing.T) {
	if _, err := ParseDigest("path-with-no-fields"); err == nil {
		t.Fatal("a one-column line parsed")
	}
	if _, err := ParseDigest("p string sideways"); err == nil {
		t.Fatal("an unknown presence parsed")
	}
	d, err := ParseDigest("\na string always\n\nb number optional\n")
	if err != nil {
		t.Fatalf("blank lines should be skipped, not rejected: %v", err)
	}
	if len(d.Fields) != 2 || d.Fields[0].Path != "a" || d.Fields[1].Path != "b" {
		t.Fatalf("parsed fields wrong: %+v", d.Fields)
	}
}

func TestCanonicalRejectsUnmarshalable(t *testing.T) {
	if _, err := Canonical(func() {}); err == nil {
		t.Fatal("Canonical accepted a value json.Marshal cannot encode")
	}
}

// writeCanonical's default branch is the loud failure for a leaf that never went
// through json.Marshal plus UseNumber decoding.
func TestWriteCanonicalRejectsUndecodedLeaf(t *testing.T) {
	var buf bytes.Buffer
	err := writeCanonical(&buf, make(chan int), 0)
	if err == nil || !strings.Contains(err.Error(), "unencodable") {
		t.Fatalf("writeCanonical(chan) = %v, want unencodable error", err)
	}
}

func TestNormalizeRejectsInvalidJSON(t *testing.T) {
	if _, err := NewNormalizer("").Normalize([]byte("{not json")); err == nil {
		t.Fatal("Normalize accepted invalid JSON")
	}
}

func digestWithNotes(path, notes string) Digest {
	return Digest{Fields: []Field{{Path: path, Kind: "string", Presence: Always, Notes: notes}}}
}

// Note drift under an unchanged JSON kind is review-only; a custom marshaler appearing
// or disappearing changes the actual bytes on the wire and is BREAKING both ways.
func TestClassifyNoteChangeMatrix(t *testing.T) {
	rep := Classify(digestWithNotes("b", "base64"), digestWithNotes("b", ""), Response)
	if len(rep.Changes) != 1 || rep.Changes[0].Kind != NoteChanged {
		t.Fatalf("note drift misclassified: %+v", rep.Changes)
	}
	if rep.Verdict != Additive {
		t.Fatalf("review-only note drift broke the build: %s", rep.Verdict)
	}

	rep = Classify(digestWithNotes("b", ""), digestWithNotes("b", "custom-marshaler"), Response)
	if rep.Verdict != Breaking || len(rep.Changes) != 1 || rep.Changes[0].Kind != Marshaler {
		t.Fatalf("marshaler takeover not breaking: %s %+v", rep.Verdict, rep.Changes)
	}

	rep = Classify(digestWithNotes("b", "custom-marshaler"), digestWithNotes("b", ""), Response)
	if rep.Verdict != Breaking {
		t.Fatalf("marshaler removal not breaking: %s", rep.Verdict)
	}
}

type covInner struct {
	Inner string `json:"inner"`
}

type covEmbedded struct {
	covInner        // anonymous: fields inline into the parent's paths
	hidden   string // unexported: encoding/json skips it, so does the digest
}

type covMarshaler interface {
	MarshalJSON() ([]byte, error)
}

type covEdges struct {
	M     map[string]covInner `json:"m,omitempty"` // map whose values are objects
	Fixed [2]int              `json:"fixed"`       // fixed-length array, not a slice
	I     covMarshaler        `json:"i,omitempty"` // method-bearing interface leaf
}

func TestShapeEdgeBranches(t *testing.T) {
	d, err := DigestOf(covEmbedded{hidden: "seeded"})
	if err != nil {
		t.Fatalf("DigestOf: %v", err)
	}
	if !d.Has("inner") {
		t.Errorf("embedded fields must inline:\n%s", d.Render())
	}
	if d.Has("hidden") {
		t.Errorf("unexported field leaked into the digest:\n%s", d.Render())
	}

	e, err := DigestOf(covEdges{})
	if err != nil {
		t.Fatalf("DigestOf: %v", err)
	}
	if !e.Has("m.<key>") || !e.Has("fixed[]") || !e.Has("i") {
		t.Errorf("edge paths missing:\n%s", e.Render())
	}
	r := e.Render()
	if !strings.Contains(r, "map[string]covInner") {
		t.Errorf("struct-valued map should name its value type:\n%s", r)
	}
	if !strings.Contains(r, "custom-marshaler") {
		t.Errorf("method-bearing interface should declare itself:\n%s", r)
	}
}
