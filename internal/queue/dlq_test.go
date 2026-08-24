// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"net/textproto"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"
)

// The provenance / sanitise / strip tests (issue #12 slice 2): golden header maps,
// canonicalisation, RFC3339-ms formatting, omission of empty reasons, rune-boundary
// truncation, and namespace stripping that always passes #7's reserved_header rule.

// TestProvenanceHeadersGolden pins the exact mandatory + proposal header set for a
// fully-populated DeadCtx (PLAN §5.1 + C13 + the ratified proposals), with keys
// canonicalised and timestamps RFC3339-ms UTC.
func TestProvenanceHeadersGolden(t *testing.T) {
	d := deadCtx()
	h, trunc := ProvenanceHeaders(d, 1789982042114, 1789982095310, 1<<10)
	if trunc {
		t.Fatal("truncated=true for a short reason, want false")
	}
	want := map[string]string{
		"Messq-Origin-Id":           "01J8ZQ4K2M9V0X7Y3B5N6C8D1E",
		"Messq-Origin-Stream":       "orders",
		"Messq-Origin-Seq":          "10493",
		"Messq-Origin-Consumer":     "worker",
		"Messq-Origin-Generation":   "1",
		"Messq-Origin-Published-At": "2026-09-21T09:14:02.114Z",
		"Messq-Attempts":            "5",
		"Messq-Max-Deliver":         "5",
		"Messq-Cause":               "max_deliver",
		"Messq-Trigger":             "ack_wait",
		"Messq-Last-Reason":         "upstream 503",
		"Messq-Dead-At":             "2026-09-21T09:14:55.310Z",
	}
	if len(h) != len(want) {
		t.Fatalf("got %d headers, want %d (%v)", len(h), len(want), h)
	}
	for k, v := range want {
		if h[k] != v {
			t.Fatalf("header %q = %q, want %q", k, h[k], v)
		}
	}
	// Every key is canonical (single capitalised word, no lowercase-leading).
	for k := range h {
		if k != textproto.CanonicalMIMEHeaderKey(k) {
			t.Fatalf("key %q is not canonicalised", k)
		}
		if !strings.HasPrefix(k, "Messq-") {
			t.Fatalf("key %q escapes the reserved namespace", k)
		}
	}
}

// TestProvenanceOmissions pins the omissions: empty Last-Reason drops the header (and
// never adds the truncated flag), and empty Trigger is omitted.
func TestProvenanceOmissions(t *testing.T) {
	d := deadCtx()
	d.LastReason = ""
	d.Trigger = ""
	h, trunc := ProvenanceHeaders(d, 1, 2, 1<<10)
	if trunc {
		t.Fatal("truncated=true with no reason, want false")
	}
	if _, ok := h["Messq-Last-Reason"]; ok {
		t.Fatal("Messq-Last-Reason present for an empty reason; it must be omitted")
	}
	if _, ok := h["Messq-Last-Reason-Truncated"]; ok {
		t.Fatal("Messq-Last-Reason-Truncated present without a reason")
	}
	if _, ok := h["Messq-Trigger"]; ok {
		t.Fatal("Messq-Trigger present for an empty trigger")
	}
}

// TestProvenanceReasonTruncated pins the truncation flag + header when the sanitised
// reason exceeds the cap.
func TestProvenanceReasonTruncated(t *testing.T) {
	d := deadCtx()
	d.LastReason = strings.Repeat("x", 4096) // 4 KiB of child stderr
	h, trunc := ProvenanceHeaders(d, 1, 2, 32)
	if !trunc {
		t.Fatal("truncated=false for a 4KiB reason under a 32-byte cap, want true")
	}
	if h["Messq-Last-Reason-Truncated"] != "true" {
		t.Fatalf("Messq-Last-Reason-Truncated = %q, want \"true\"", h["Messq-Last-Reason-Truncated"])
	}
	reason := h["Messq-Last-Reason"]
	if len(reason) > 32 {
		t.Fatalf("reason = %d bytes, want <= 32", len(reason))
	}
	if !utf8.ValidString(reason) {
		t.Fatal("reason is not valid UTF-8")
	}
}

// TestSanitizeReasonTable covers invalid UTF-8, control characters, ANSI escapes,
// whitespace collapse, no bare newline, and rune-boundary truncation.
func TestSanitizeReasonTable(t *testing.T) {
	tests := []struct {
		name      string
		in        string
		max       int
		want      string
		wantTrunc bool
	}{
		{"clean", "upstream 503 on POST /charge", 1 << 10, "upstream 503 on POST /charge", false},
		{"invalid_utf8", "a\xff\xfeb", 1 << 10, "a\uFFFD\uFFFDb", false},
		{"nul", "a\x00b", 1 << 10, `a\u0000b`, false},
		{"ansi_escape", "clear\x1b[2J", 1 << 10, `clear\e[2J`, false},
		{"collapse_ws", "a\n\t b", 1 << 10, "a b", false},
		{"no_bare_newline", "a\nb", 1 << 10, "a b", false},
		{"rune_boundary", "eé", 2, "e", true}, // a 1-byte + a 2-byte rune: the straddling rune is dropped intact
		{"truncate_ascii", "abcdef", 3, "abc", true},
		{"tab_collapsed", "a\tb", 1 << 10, "a b", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, trunc := SanitizeReason(tc.in, tc.max)
			if out != tc.want {
				t.Fatalf("SanitizeReason(%q,%d) = %q, want %q", tc.in, tc.max, out, tc.want)
			}
			if trunc != tc.wantTrunc {
				t.Fatalf("truncated = %v, want %v", trunc, tc.wantTrunc)
			}
			if strings.ContainsAny(out, "\n\r\t") {
				t.Fatalf("output %q contains a bare control newline/tab", out)
			}
			if !utf8.ValidString(out) {
				t.Fatalf("output %q is not valid UTF-8", out)
			}
			if len(out) > tc.max && tc.max > 0 {
				t.Fatalf("output %d bytes > cap %d", len(out), tc.max)
			}
		})
	}
}

// TestSanitizeReasonAlwaysValid is the property in the issue's test plan: the output is
// always valid UTF-8, control-free, and <= the cap, whatever the input — child stderr is
// untrusted. Table-driven here; the fuzz target is FuzzSanitizeReason.
func TestSanitizeReasonAlwaysValid(t *testing.T) {
	inputs := []string{
		string([]byte{0xff, 0xfe, 0x00, 0x1b, 0x0a, 0x09, 'x'}),
		strings.Repeat("☃\x00", 100),
		"\x7f\x80\xc0\xc1" + "hello",
	}
	for _, in := range inputs {
		out, _ := SanitizeReason(in, 512)
		if !utf8.ValidString(out) {
			t.Fatalf("SanitizeReason(%q) = %q, invalid UTF-8", in, out)
		}
		for _, r := range out {
			if unicode.IsControl(r) {
				t.Fatalf("SanitizeReason(%q) left a control rune %q in %q", in, r, out)
			}
		}
		if len(out) > 512 {
			t.Fatalf("SanitizeReason(%q) = %d bytes > 512", in, len(out))
		}
	}
}

// TestStripProvenance pins exact namespace removal: every Messq-* key gone, user
// headers preserved, idempotent, and the output never trips #7's reserved_header rule.
func TestStripProvenance(t *testing.T) {
	in := map[string]string{
		"Messq-Origin-Stream": "orders",
		"Messq-Cause":         "max_deliver",
		"Messq-Dead-At":       "2026-09-21T09:14:55.310Z",
		"Tenant":              "acme",
		"X-Trace":             "keep-me",
	}
	out := StripProvenance(in)
	if len(out) != 2 {
		t.Fatalf("StripProvenance = %v, want exactly the 2 user headers", out)
	}
	if out["Tenant"] != "acme" || out["X-Trace"] != "keep-me" {
		t.Fatalf("user headers lost: %v", out)
	}
	for k := range out {
		if strings.HasPrefix(k, "Messq-") {
			t.Fatalf("key %q survived stripping", k)
		}
	}
	// Idempotent.
	again := StripProvenance(out)
	if len(again) != len(out) {
		t.Fatalf("StripProvenance is not idempotent: %v -> %v", out, again)
	}
	// StripProvenance must not mutate its input: a new map is returned.
	if len(in) != 5 {
		t.Fatalf("StripProvenance mutated its input; still %d keys", len(in))
	}
}

// FuzzSanitizeReason pins the sanitise contract on untrusted child stderr (#25): the
// output is always valid UTF-8, carries no control or bare-newline rune, and never
// exceeds the 1 KiB cap. The fuzzer can never make the output violate these.
func FuzzSanitizeReason(f *testing.F) {
	for _, seed := range []string{
		"upstream 503 on POST /charge",
		"\xff\xfe\x00\x1b\n	 hello",
		"", strings.Repeat("☃", 2048),
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) {
		if len(s) > 4096 {
			s = s[:4096]
		}
		out, _ := SanitizeReason(s, 1<<10)
		if !utf8.ValidString(out) {
			t.Fatalf("SanitizeReason(%q) produced invalid UTF-8: %q", s, out)
		}
		if len(out) > 1<<10 {
			t.Fatalf("SanitizeReason(%q) = %d bytes, cap 1024", s, len(out))
		}
		for _, r := range out {
			if unicode.IsControl(r) {
				t.Fatalf("SanitizeReason(%q) left control rune %q in %q", s, r, out)
			}
		}
	})
}
