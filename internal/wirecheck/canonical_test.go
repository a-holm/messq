// SPDX-License-Identifier: Apache-2.0

package wirecheck

import (
	"encoding/json"
	"strings"
	"testing"

	"pgregory.net/rapid"
)

// TestCanonicalSortsKeysAndIndents pins the two visible properties of the canonical
// form: object members are sorted by key regardless of the source encoding order, and
// the indentation is two spaces. Field reordering in a Go struct must therefore not
// churn a golden produced through Canonical.
func TestCanonicalSortsKeysAndIndents(t *testing.T) {
	raw := []byte("{\"b\":1,\"a\":{\"d\":true,\"c\":\"x\"}}")
	got, err := CanonBytes(raw)
	if err != nil {
		t.Fatalf("CanonBytes: %v", err)
	}
	want := "{\n  \"a\": {\n    \"c\": \"x\",\n    \"d\": true\n  },\n  \"b\": 1\n}\n"
	if string(got) != want {
		t.Fatalf("CanonBytes:\n got %q\nwant %q", got, want)
	}
}

// TestCanonicalPreservesLargeInt64 is the seq-beyond-2^53 guarantee: canonicalisation
// must never float-render an integer. Decoding without UseNumber would turn
// 9223372036854775807 into 9223372036854776000 and silently corrupt every large seq
// that crosses a golden.
func TestCanonicalPreservesLargeInt64(t *testing.T) {
	raw := []byte(`{"seq":9223372036854775807}`)
	got, err := CanonBytes(raw)
	if err != nil {
		t.Fatalf("CanonBytes: %v", err)
	}
	if !strings.Contains(string(got), "9223372036854775807") {
		t.Fatalf("large int64 was reformatted: %s", got)
	}
}

// TestCanonicalRoundTripsThroughValue checks the any-typed entry point: a decoded
// tree re-encoded canonically matches CanonBytes of the equivalent raw document.
func TestCanonicalRoundTripsThroughValue(t *testing.T) {
	var v map[string]any
	raw := []byte(`{"z":"last","a":[1,2,{"n":true}]}`)
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	viaValue, err := Canonical(v)
	if err != nil {
		t.Fatalf("Canonical: %v", err)
	}
	viaRaw, err := CanonBytes(raw)
	if err != nil {
		t.Fatalf("CanonBytes: %v", err)
	}
	if string(viaValue) != string(viaRaw) {
		t.Fatalf("Canonical and CanonBytes disagree:\n %s\n %s", viaValue, viaRaw)
	}
}

// TestCanonicalDoesNotEscapeHTML keeps curl transcripts byte-faithful: the encoder's
// HTML escaping would turn every "<" in a body into \u003c and make the document lie.
func TestCanonicalDoesNotEscapeHTML(t *testing.T) {
	got, err := CanonBytes([]byte(`{"body":"a<b>"}`))
	if err != nil {
		t.Fatalf("CanonBytes: %v", err)
	}
	if !strings.Contains(string(got), `"a<b>"`) {
		t.Fatalf("HTML escaping is on: %s", got)
	}
}

// TestCanonBytesRejectsMalformedInput: a normaliser that silently passes garbage
// through would let a broken response become a golden.
func TestCanonBytesRejectsMalformedInput(t *testing.T) {
	for _, bad := range []string{``, `{`, `{"a":1} trailing`} {
		if _, err := CanonBytes([]byte(bad)); err == nil {
			t.Errorf("CanonBytes(%q) = nil error, want rejection", bad)
		}
	}
}

// jsonScalar generates JSON leaf values.
var jsonScalar = rapid.OneOf(
	rapid.Bool().AsAny(),
	rapid.StringMatching(`[a-z ]{0,12}`).AsAny(),
	rapid.Int64().AsAny(),
)

// jsonValue generates arbitrary JSON documents: nested objects, arrays and scalars.
// The generator delegates to drawJSON, whose direct function recursion is what makes
// the generation recursive (a package-level var may not reference itself).
var jsonValue = rapid.Custom(drawJSON)

func drawJSON(t *rapid.T) any {
	switch rapid.IntRange(0, 3).Draw(t, "kind") {
	case 0:
		return jsonScalar.Draw(t, "scalar")
	case 1:
		n := rapid.IntRange(0, 3).Draw(t, "arrayLen")
		arr := make([]any, n)
		for i := range arr {
			arr[i] = drawJSON(t)
		}
		return any(arr)
	case 2:
		m := make(map[string]any)
		keys := rapid.SliceOfN(rapid.StringMatching(`[a-z_]{1,8}`), 0, 4).Draw(t, "keys")
		for _, k := range keys {
			if _, dup := m[k]; !dup {
				m[k] = drawJSON(t)
			}
		}
		return any(m)
	default:
		return any(nil)
	}
}

// TestCanonicalIdempotentProperty: canonical(canonical(x)) == canonical(x) for every
// generated document. A non-idempotent canonicaliser makes -update oscillate between
// two goldens, so this property is load-bearing for the update flow.
func TestCanonicalIdempotentProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		doc := jsonValue.Draw(t, "doc")
		once, err := Canonical(doc)
		if err != nil {
			t.Skipf("unrepresentable document: %v", err)
		}
		twice, err := CanonBytes(once)
		if err != nil {
			t.Fatalf("re-canonicalising output failed: %v\ninput: %s", err, once)
		}
		if string(once) != string(twice) {
			t.Fatalf("canonical form is not a fixed point:\nonce:  %s\ntwice: %s", once, twice)
		}
	})
}
