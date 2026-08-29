// SPDX-License-Identifier: Apache-2.0

package scriptenv

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/rogpeppe/go-internal/testscript"

	"github.com/a-holm/messq/internal/wirecheck"
)

// cmdCmpShape validates a JSON document against the committed shape of a named
// wire type. This is the command that makes -update unable to launder a shape
// break: it never rewrites anything, so `-update` cannot turn a renamed response
// field into a golden refresh. On a mismatch it fails with the classifier's
// verdict (issue #18's ADDITIVE/BREAKING label), because a shape change is a
// compatibility decision, not a formatting fix.
//
// What is checked, and what deliberately is not:
//
//   - every path present in the DOCUMENT must exist in the TYPE's digest, with a
//     matching kind — a renamed field lands here as BREAKING "removed+added";
//   - a path the document cannot concretely type (empty arrays, opaque values)
//     is registered as "json" and never fails a kind compare;
//   - a type field the document omits is NOT checked here when it is optional;
//     the daemon-side golden tests (#18) own the required-field direction.
func cmdCmpShape(ts *testscript.TestScript, neg bool, args []string) {
	if neg {
		ts.Fatalf("cmpshape: ! is meaningless; a shape assertion must hold")
	}
	if len(args) != 2 {
		ts.Fatalf("cmpshape: want `cmpshape <src> <ShapeName>` (src: stdout|stderr|file)")
	}
	st := stateFrom(ts)
	proto, ok := st.shapes[args[1]]
	if !ok {
		names := make([]string, 0, len(st.shapes))
		for n := range st.shapes {
			names = append(names, n)
		}
		sort.Strings(names)
		ts.Fatalf("cmpshape: unknown shape %q; registered: %s", args[1], strings.Join(names, ", "))
	}
	raw, err := readSrc(ts, args[0])
	if err != nil {
		ts.Fatalf("cmpshape: %v", err)
	}
	doc, err := parseJSON(raw)
	if err != nil {
		ts.Fatalf("cmpshape: %s is not JSON: %v", args[0], err)
	}
	wantDigest, err := wirecheck.DigestOf(proto)
	if err != nil {
		ts.Fatalf("cmpshape: digest %s: %v", args[1], err)
	}
	gotDigest := documentDigest(doc)

	if diff := shapeDiff(wantDigest, gotDigest); diff != "" {
		verdict := wirecheck.Classify(wantDigest, gotDigest, wirecheck.Response)
		ts.Fatalf("cmpshape: stdout does not match the committed shape of %s — %s\n%s\n"+
			"This is a compatibility decision, not a golden refresh: -update refuses it.\n"+
			"Change the field back, or review the break and regenerate the shape registry deliberately.",
			args[1], verdict, diff)
	}
}

// shapeDiff renders the human-visible differences between the type digest and the
// document digest, one line per change. Empty means the shapes agree.
func shapeDiff(want, got wirecheck.Digest) string {
	wantIdx := map[string]wirecheck.Field{}
	for _, f := range want.Fields {
		wantIdx[f.Path] = f
	}
	var b strings.Builder
	gotPaths := map[string]bool{}
	for _, f := range got.Fields {
		gotPaths[f.Path] = true
		w, ok := wantIdx[f.Path]
		switch {
		case !ok:
			fmt.Fprintf(&b, "  %s: present in document, absent from type digest (renamed or added)\n", f.Path)
		case f.Kind != "json" && w.Kind != f.Kind:
			fmt.Fprintf(&b, "  %s: kind %s in document, %s in type\n", f.Path, f.Kind, w.Kind)
		}
	}
	for _, f := range want.Fields {
		if !gotPaths[f.Path] && f.Presence == wirecheck.Always {
			// An always-field the document lacks is a real break — but one
			// document cannot prove intent, so it is reported, not classified.
			fmt.Fprintf(&b, "  %s: always-field of %s missing from document\n", f.Path, want.Type)
		}
	}
	return strings.TrimRight(b.String(), "\n")
}

// documentDigest builds a wirecheck.Digest from a decoded JSON document. Presence
// is Always for everything observed — a document shows what appeared, never what
// could have been omitted.
func documentDigest(doc any) wirecheck.Digest {
	d := wirecheck.Digest{Type: "document"}
	walkDocument(doc, "", &d.Fields)
	sortDocFields(d.Fields)
	return d
}

func walkDocument(node any, path string, out *[]wirecheck.Field) {
	switch v := node.(type) {
	case map[string]any:
		if path != "" {
			*out = append(*out, wirecheck.Field{Path: path, Kind: "object", Presence: wirecheck.Always})
		}
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			child := k
			if path != "" {
				child = path + "." + k
			}
			walkDocument(v[k], child, out)
		}
	case []any:
		elemPath := path + "[]"
		if len(v) == 0 {
			// An empty array cannot type its elements; register the element slot
			// as opaque so the compare never fails on it.
			*out = append(*out, wirecheck.Field{Path: elemPath, Kind: "json", Presence: wirecheck.Always})
			return
		}
		kind := scalarKind(v[0])
		*out = append(*out, wirecheck.Field{Path: elemPath, Kind: kind, Presence: wirecheck.Always})
		for _, el := range v {
			if kind == "object" {
				walkDocument(el, path+"[]", out)
			}
		}
	default:
		*out = append(*out, wirecheck.Field{Path: path, Kind: scalarKind(node), Presence: wirecheck.Always})
	}
}

func scalarKind(v any) string {
	switch v.(type) {
	case string:
		return "string"
	case json.Number, float64, int64:
		return "number"
	case bool:
		return "bool"
	case map[string]any:
		return "object"
	default:
		return "json"
	}
}

func sortDocFields(fs []wirecheck.Field) {
	sort.Slice(fs, func(i, j int) bool { return fs[i].Path < fs[j].Path })
}
