// SPDX-License-Identifier: Apache-2.0

package wirecheck

import (
	"encoding/json"
	"testing"
)

// Synthetic wire types. They deliberately cover every struct feature the digest walker
// must understand before it is ever pointed at the real API types: embedded structs,
// pointers, slices, maps, json:"-", omitempty, []byte and a custom MarshalJSON.

type shapeInner struct {
	Region string `json:"region"`
}

type shapeCustom struct{ N int }

func (c shapeCustom) MarshalJSON() ([]byte, error) { return json.Marshal(c.N * 2) }

type shapeEmbed struct {
	Seq int64 `json:"seq"`
}

type shapeExample struct {
	shapeEmbed                   // anonymous: fields inline into the parent's paths
	ID         string            `json:"id"`                // always
	Duplicate  bool              `json:"duplicate"`         // never omitempty (G3)
	Attempt    int32             `json:"attempt,omitempty"` // optional
	Hidden     string            `json:"-"`                 // never on the wire
	Labels     map[string]string `json:"labels,omitempty"`  // string map
	Tags       []string          `json:"tags,omitempty"`    // slice of scalar
	Raw        []byte            `json:"raw_b64,omitempty"` // base64 on the wire
	Nested     shapeInner        `json:"nested"`            // nested object
	Ptr        *shapeInner       `json:"ptr,omitempty"`     // pointer to struct
	Custom     shapeCustom       `json:"custom"`            // custom MarshalJSON
}

func TestShapeRender(t *testing.T) {
	d, err := DigestOf(shapeExample{})
	if err != nil {
		t.Fatalf("DigestOf: %v", err)
	}
	got := d.Render()
	want := "" +
		"attempt              number   optional   int32\n" +
		"custom               json     always     custom-marshaler\n" +
		"duplicate            bool     always\n" +
		"id                   string   always\n" +
		"labels.<key>         string   optional   map[string]string\n" +
		"nested.region        string   always\n" +
		"ptr.region           string   optional\n" +
		"raw_b64              string   optional   base64\n" +
		"seq                  number   always     int64\n" +
		"tags[]               string   optional"
	if got != want {
		t.Fatalf("Render():\n got:\n%s\nwant:\n%s", got, want)
	}
}

// TestShapeIgnoresUntaggedFields: json:"-" stays off the digest — it is not wire surface.
func TestShapeIgnoresHiddenFields(t *testing.T) {
	d, err := DigestOf(shapeExample{})
	if err != nil {
		t.Fatalf("DigestOf: %v", err)
	}
	if d.Has("hidden") {
		t.Errorf(`field "hidden" must not appear in the digest:\n%s`, d.Render())
	}
}

// TestShapeFieldReorderStable: reordering struct fields must not churn the digest —
// paths are sorted, declaration order is invisible.
func TestShapeFieldReorderStable(t *testing.T) {
	type inner struct {
		V []string `json:"v"`
	}
	type a struct {
		X int   `json:"x"`
		Y inner `json:"y"`
	}
	type b struct {
		Y inner `json:"y"`
		X int   `json:"x"`
	}
	da, err := DigestOf(a{})
	if err != nil {
		t.Fatalf("DigestOf(a): %v", err)
	}
	db, err := DigestOf(b{})
	if err != nil {
		t.Fatalf("DigestOf(b): %v", err)
	}
	if da.Render() != db.Render() {
		t.Fatalf("reorder churned the digest:\n%s\n---\n%s", da.Render(), db.Render())
	}
}

// TestShapeParseRoundTrip: parse(render(d)) == d, so committed .shape files can be
// diffed structurally rather than textually.
func TestShapeParseRoundTrip(t *testing.T) {
	d, err := DigestOf(shapeExample{})
	if err != nil {
		t.Fatalf("DigestOf: %v", err)
	}
	parsed, err := ParseDigest(d.Render())
	if err != nil {
		t.Fatalf("ParseDigest: %v", err)
	}
	if parsed.Render() != d.Render() {
		t.Fatalf("round trip differs:\n%s\n---\n%s", parsed.Render(), d.Render())
	}
}

// TestShapeRejectsUnknownLine: a hand-edited digest line that does not parse must be a
// loud failure, not silently-ignored content.
func TestShapeRejectsUnknownLine(t *testing.T) {
	if _, err := ParseDigest("messages[].ack_token string sometimes"); err == nil {
		t.Fatal("ParseDigest accepted an unknown presence keyword")
	}
}
