package wirecheck

import (
	"encoding/json"
	"strings"
	"testing"
)

// Coverage-completion tests for the walker branches the synthetic-type table does not
// reach: arrays of structs, any-typed fields, named scalar types, recursion guards and
// the digest-diff summary. Each is a real behaviour pin, not a coverage chase: every
// branch here is a decision a wire type could take tomorrow.

type shapeMsg struct {
	ID string `json:"id"`
}

type shapePage struct {
	Messages []shapeMsg     `json:"messages"`        // array of structs
	Extra    map[string]any `json:"extra,omitempty"` // map with opaque values
	Any      any            `json:"any,omitempty"`   // interface{} leaf
	Named    json.Number    `json:"named,omitempty"` // named string-kind type
	Ret      shapeRetention `json:"ret"`             // named string type → enum note
	Self     *shapeNode     `json:"self,omitempty"`  // recursive type
}

type shapeRetention string

type shapeNode struct {
	Name string      `json:"name"`
	Kids []shapeNode `json:"kids,omitempty"`
}

func TestShapeArrayElementPaths(t *testing.T) {
	d, err := DigestOf(shapePage{})
	if err != nil {
		t.Fatalf("DigestOf: %v", err)
	}
	if !d.Has("messages[].id") {
		t.Errorf("array-of-struct path missing:\n%s", d.Render())
	}
	if !d.Has("extra.<key>") || !d.Has("self.name") || !d.Has("self.kids[].name") {
		t.Errorf("map/recursive paths missing:\n%s", d.Render())
	}
	if !strings.Contains(d.Render(), "ret                  string   always     shapeRetention") {
		t.Errorf("named string type should carry an enum-style note:\n%s", d.Render())
	}
	if !strings.Contains(d.Render(), "any                  json     optional") {
		t.Errorf("any-typed field should be opaque optional:\n%s", d.Render())
	}
}

// TestHasAbsentPath exercises the negative half of Digest.Has.
func TestHasAbsentPath(t *testing.T) {
	d := mustDigest(t, struct {
		A string `json:"a"`
	}{})
	if d.Has("b") {
		t.Error(`Has("b") = true for a digest that only has "a"`)
	}
}

// TestJoinPathEmptyParent keeps top-level names dot-free.
func TestJoinPathEmptyParent(t *testing.T) {
	if got := joinPath("", "x"); got != "x" {
		t.Fatalf("joinPath empty parent = %q", got)
	}
	if got := joinPath("a", ""); got != "a" {
		t.Fatalf("joinPath empty child = %q", got)
	}
}

// TestClassifySummaryRendersChanges keeps the human-readable half alive.
func TestClassifySummaryRendersChanges(t *testing.T) {
	oldD := mustDigest(t, struct {
		A string `json:"a"`
	}{})
	newD := mustDigest(t, struct{}{})
	rep := Classify(oldD, newD, Response)
	if rep.Verdict != Breaking || !strings.Contains(rep.Summary(), "removed field: a (string)") {
		t.Fatalf("summary lost the removal detail:\n%s", rep.Summary())
	}
}

// TestNormalizeWorkDirEmptyDisablesRewrite pins the embedder seam: no work dir means
// paths pass through untouched.
func TestNormalizeWorkDirEmptyDisablesRewrite(t *testing.T) {
	out, err := NewNormalizer("").Normalize([]byte(`{"p":"/tmp/x/messq.sock"}`))
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if !strings.Contains(string(out), "/tmp/x/messq.sock") {
		t.Fatalf("empty WorkDir rewrote paths anyway: %s", out)
	}
}
