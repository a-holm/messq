// SPDX-License-Identifier: Apache-2.0

package wirecheck

import (
	"strings"
	"testing"

	"pgregory.net/rapid"
)

func normalize(t *testing.T, in string) string {
	t.Helper()
	out, err := NewNormalizer("/tmp/messq-test-123").Normalize([]byte(in))
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	return string(out)
}

// TestNormalizeMasksVolatileValues covers each row of the mask table end to end.
func TestNormalizeMasksVolatileValues(t *testing.T) {
	in := `{
  "created_at": 1724419200123,
  "uptime_ms": 41,
  "db_bytes": 8192,
  "version": "0.1.0-dev",
  "id": "01J8ZQ4K2M9V0X7Y3B5N6C8D1E",
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "published_at": "2026-08-23T09:14:02.114Z",
  "sock": "/tmp/messq-test-123/messq.sock"
}`
	got := normalize(t, in)
	for _, want := range []string{
		`"created_at": <NUM>`,
		`"uptime_ms": <NUM>`,
		`"db_bytes": <NUM>`,
		`"version": "<VERSION>"`,
		`"id": "<ULID>"`,
		`"trace_id": "<TRACE>"`,
		`"published_at": "<TS>"`,
		`"/run/messq/messq.sock"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("Normalize output missing %s:\n%s", want, got)
		}
	}
	if strings.Contains(got, "1724419200123") || strings.Contains(got, "/tmp/messq-test-123") {
		t.Errorf("volatile value survived:\n%s", got)
	}
}

// TestNormalizeSuffixAtKeys masks every *_at millisecond field, not a hand-listed few.
func TestNormalizeSuffixAtKeys(t *testing.T) {
	got := normalize(t, `{"delivered_at":77,"oldest_pending_age_ms":5,"other":77}`)
	if !strings.Contains(got, `"delivered_at": <NUM>`) {
		t.Errorf("*_at not masked:\n%s", got)
	}
	if !strings.Contains(got, `"oldest_pending_age_ms": <NUM>`) {
		t.Errorf("age_ms family not masked:\n%s", got)
	}
	if !strings.Contains(got, `"other": 77`) {
		t.Errorf("unrelated key was masked (over-masking):\n%s", got)
	}
}

// TestNormalizeNearMissesAreUntouched pins the narrowness of the masks: a value-shaped
// guess that fires on a near-miss corrupts real data in goldens. Numbers are only ever
// masked by path; ULID and timestamp shapes are matched exactly, never as substrings.
func TestNormalizeNearMissesAreUntouched(t *testing.T) {
	in := `{
  "seq_note": "01J8ZQ4K2M9V0X7Y3B5N6C8D1",
  "longer": "01J8ZQ4K2M9V0X7Y3B5N6C8D1EX",
  "date_only": "2026-08-23",
  "big_number": 9223372036854775807,
  "hex_short": "4bf92f3577b34da6a3ce929d0e0e47",
  "mixed": "visit 01J8ZQ4K2M9V0X7Y3B5N6C8D1E today"
}`
	got := normalize(t, in)
	for _, want := range []string{
		`"seq_note": "01J8ZQ4K2M9V0X7Y3B5N6C8D1"`,           // 25 chars: not a ULID
		`"longer": "01J8ZQ4K2M9V0X7Y3B5N6C8D1EX"`,           // 27 chars: not a ULID
		`"date_only": "2026-08-23"`,                         // date without time: not RFC3339
		`"big_number": 9223372036854775807`,                 // numbers masked by path only
		`"hex_short": "4bf92f3577b34da6a3ce929d0e0e47"`,     // 31 hex: not a trace id
		`"mixed": "visit 01J8ZQ4K2M9V0X7Y3B5N6C8D1E today"`, // whole-value match only
	} {
		if !strings.Contains(got, want) {
			t.Errorf("near-miss was corrupted; wanted %s in:\n%s", want, got)
		}
	}
}

// TestNormalizeNeverMasks pins the never-mask allow-list: these fields carry the
// contract's meaning (seq/attempt fencing arithmetic lives in the golden itself), so
// masking one is exactly the failure this package exists to prevent. D7 makes the ack
// token deterministic in scripted runs — it must survive verbatim.
func TestNormalizeNeverMasks(t *testing.T) {
	in := `{
  "seq": 10493,
  "attempt": 2,
  "max_deliver": 5,
  "pending": 3,
  "backlog": 12,
  "hold_reason": "empty",
  "duplicate": false,
  "code": "not_found",
  "applied": true,
  "ack_token": "orders/worker/10493/2/7"
}`
	got := normalize(t, in)
	if !strings.Contains(got, `"ack_token": "orders/worker/10493/2/7"`) {
		t.Fatalf("ack_token was masked — the fencing assertion would lose its teeth:\n%s", got)
	}
	for _, want := range []string{
		`"seq": 10493`, `"attempt": 2`, `"max_deliver": 5`, `"pending": 3`,
		`"backlog": 12`, `"hold_reason": "empty"`, `"duplicate": false`,
		`"code": "not_found"`, `"applied": true`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("never-mask field changed; wanted %s in:\n%s", want, got)
		}
	}
}

// TestNeverMaskedDisjointFromMaskRules is the meta-guard: if a future edit adds a
// never-mask key to the mask table, this fails before any golden can silently rot.
func TestNeverMaskedDisjointFromMaskRules(t *testing.T) {
	masked := make(map[string]bool, len(maskedNumberKeys))
	for _, k := range maskedNumberKeys {
		masked[k] = true
	}
	for _, k := range NeverMasked {
		if masked[k] {
			t.Errorf("key %q is both never-masked and in maskedNumberKeys", k)
		}
		if strings.HasSuffix(k, "_at") {
			t.Errorf("never-mask key %q ends in _at and would be caught by the *_at rule", k)
		}
	}
}

// TestNormalizeIdempotentProperty: normalise(normalise(x)) == normalise(x). The -update
// flow rewrites documents from normalised output; a non-idempotent rule oscillates
// between two goldens instead of converging.
func TestNormalizeIdempotentProperty(t *testing.T) {
	n := NewNormalizer("/tmp/messq-test-123")
	rapid.Check(t, func(t *rapid.T) {
		doc := drawJSON(t)
		raw, err := Canonical(doc)
		if err != nil {
			t.Skipf("unrepresentable document: %v", err)
		}
		once, err := n.Normalize(raw)
		if err != nil {
			t.Fatalf("Normalize: %v", err)
		}
		twice, err := n.Normalize(once)
		if err != nil {
			t.Fatalf("re-normalising output failed: %v\ninput: %s", err, once)
		}
		if string(once) != string(twice) {
			t.Fatalf("normalised form is not a fixed point:\nonce:  %s\ntwice: %s", once, twice)
		}
	})
}
