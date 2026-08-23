// SPDX-License-Identifier: Apache-2.0

package auth_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/auth"
	"github.com/a-holm/messq/internal/errs"
	"github.com/google/go-cmp/cmp"
	"pgregory.net/rapid"
)

func parse(t *testing.T, content string) *auth.File {
	t.Helper()
	f, err := auth.Parse("tokens", strings.NewReader(content))
	if err != nil {
		t.Fatalf("Parse error = %v, want nil", err)
	}
	return f
}

func parseErr(t *testing.T, content, fragment string) {
	t.Helper()
	_, err := auth.Parse("tokens", strings.NewReader(content))
	if err == nil {
		t.Fatalf("Parse(%q) = nil error, want one containing %q", content, fragment)
	}
	if !errors.Is(err, errs.ErrBadRequest) {
		t.Fatalf("Parse(%q) error = %v, want errors.Is ErrBadRequest", content, err)
	}
	if !strings.Contains(err.Error(), fragment) {
		t.Fatalf("Parse(%q) error = %q, want it to contain %q", content, err, fragment)
	}
}

const (
	idPublisher = "ci-publisher"
	idWorker    = "orders-worker"
	hashPublish = "3f786850e387550fdab836ed7e6dc881de23001b19b2d1b0b3d1a37f7cbbd3ae"
	hashWorker  = "7d793037a0760186574b0282f2f435e7b6d1a3c0b6b0b3e3a6b1f5b3b8a3d4c1"
)

// TestParseHappyPath parses the canonical example file: comments, blank lines and four-field
// entries, with roles and comma-separated stream patterns.
func TestParseHappyPath(t *testing.T) {
	t.Parallel()

	in := "# /etc/messq/tokens\n" +
		"\n" +
		idPublisher + " " + hashPublish + " publish orders,payments\n" +
		idWorker + " " + hashWorker + " consume orders*\n"

	f := parse(t, in)
	if len(f.Tokens) != 2 {
		t.Fatalf("got %d tokens, want 2", len(f.Tokens))
	}

	got := f.Tokens[0]
	if got.ID != idPublisher {
		t.Errorf("token 0 id = %q, want %q", got.ID, idPublisher)
	}
	if !got.Roles.Has(auth.RolePublish) {
		t.Error("token 0 does not hold publish")
	}
	gotPatterns := patternStrings(got.Patterns)
	if diff := cmp.Diff([]string{"orders", "payments"}, gotPatterns); diff != "" {
		t.Errorf("token 0 patterns (-want +got):\n%s", diff)
	}

	if f.Tokens[1].ID != idWorker {
		t.Errorf("token 1 id = %q, want %q", f.Tokens[1].ID, idWorker)
	}
}

func patternStrings(ps []auth.Pattern) []string {
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = p.String()
	}
	return out
}

// TestParseWhitespaceForms pins that tabs, CRLF, mixed whitespace runs, a missing trailing
// newline and a UTF-8 BOM are all accepted, because the grammar is whitespace-separated fields
// and line-oriented, not column-aligned.
func TestParseWhitespaceForms(t *testing.T) {
	t.Parallel()

	inputs := []string{
		"\ufeff" + idPublisher + "\t" + hashPublish + "\tpublish\torders\n",       // BOM + tabs
		idPublisher + " " + hashPublish + "  publish  orders\r\n",                 // CRLF + double spaces
		idPublisher + " " + hashPublish + " publish orders",                       // no trailing newline
		"\n\n# comment\n" + idPublisher + " " + hashPublish + " publish orders\n", // leading blank/comment
	}

	for _, in := range inputs {
		f := parse(t, in)
		if len(f.Tokens) != 1 || f.Tokens[0].ID != idPublisher {
			t.Fatalf("Parse(%q) = %+v, want one ci-publisher token", in, f.Tokens)
		}
	}
}

// TestParseRejectsWrongFieldCount is the core grammar rule: a line is exactly four fields. A
// fifth field is an error, not an inline comment — a stray "#" must not silently truncate a
// grant — and three fields mean a field is missing.
func TestParseRejectsWrongFieldCount(t *testing.T) {
	t.Parallel()

	// The red case: a fifth field must be refused, never silently ignored.
	parseErr(t, idPublisher+" "+hashPublish+" publish orders # trailing comment", "fifth field")

	parseErr(t, idPublisher+" "+hashPublish+" publish", "got 3")
	parseErr(t, idPublisher+" "+hashPublish+" publish orders extra", "got 5")
}

// TestParseRejectsBadFields pins each field's exact teaching error and line number.
func TestParseRejectsBadFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		line     string
		fragment string
	}{
		{name: "bad id", line: "CI_Pub " + hashPublish + " publish orders", fragment: `token id "CI_Pub" must match [a-z0-9][a-z0-9._-]{1,63}`},
		{name: "id too short", line: "a " + hashPublish + " publish orders", fragment: "must match"},
		{name: "hash is the secret", line: idPublisher + " " + strings.Repeat("x", 43) + " publish orders", fragment: "expected 64 hex characters, got 43 — is this the secret rather than its SHA-256?"},
		{name: "hash not hex", line: idPublisher + " " + strings.Repeat("z", 64) + " publish orders", fragment: "not 64 hexadecimal"},
		{name: "unknown role", line: idPublisher + " " + hashPublish + " peek orders", fragment: `unknown role "peek" (valid: publish, consume, admin)`},
		{name: "duplicate role", line: idPublisher + " " + hashPublish + " admin,admin orders", fragment: "duplicate role"},
		{name: "bad pattern", line: idPublisher + " " + hashPublish + " publish or*ers", fragment: "trailing character"},
		{name: "subject pattern", line: idPublisher + " " + hashPublish + " publish orders.>", fragment: "not subject patterns"},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := auth.Parse("tokens", strings.NewReader(tc.line+"\n"))
			if err == nil {
				t.Fatalf("Parse(%q) = nil error", tc.line)
			}
			if !strings.Contains(err.Error(), tc.fragment) {
				t.Errorf("Parse(%q) error = %q, want it to contain %q", tc.line, err, tc.fragment)
			}
			if !strings.HasPrefix(err.Error(), "tokens:1: ") {
				t.Errorf("error %q does not carry the line prefix tokens:1:", err)
			}
		})
	}

	// Line numbers are reported accurately, not always as 1.
	_, err := auth.Parse("tokens", strings.NewReader("\n"+idPublisher+" "+hashPublish+" peek orders\n"))
	if err == nil || !strings.HasPrefix(err.Error(), "tokens:2: ") {
		t.Errorf("Parse error = %v, want a tokens:2: prefix", err)
	}
}

// TestParseRejectsDuplicates pins that a duplicate id and a duplicate hash are both fatal: a
// duplicate hash is a copy-paste accident that would give two ids the same credential.
func TestParseRejectsDuplicates(t *testing.T) {
	t.Parallel()

	two := idPublisher + " " + hashPublish + " publish orders\n"

	parseErr(t, two+two, "duplicate token id")
	parseErr(t, two+idWorker+" "+hashPublish+" consume orders\n", "duplicate hash")
}

// TestParseRejectsOversizedLine pins the 8 KiB line bound.
func TestParseRejectsOversizedLine(t *testing.T) {
	t.Parallel()

	long := idPublisher + " " + hashPublish + " publish orders" + strings.Repeat("x", 9*1024) + "\n"
	parseErr(t, long, "line is longer than")
}

// TestParseUppercaseHashNormalized pins that a hash is case-insensitive on input but stored and
// rendered lowercased.
func TestParseUppercaseHashNormalized(t *testing.T) {
	t.Parallel()

	upper := strings.ToUpper(hashPublish)
	f := parse(t, idPublisher+" "+upper+" publish orders\n")
	got := f.Tokens[0].String()
	if !strings.Contains(got, hashPublish) {
		t.Errorf("rendered token %q does not contain the lowercased hash %q", got, hashPublish)
	}
	if strings.Contains(got, upper) {
		t.Errorf("rendered token %q still carries the uppercase hash", got)
	}
}

// TestTokenPrincipal maps a parsed token onto its immutable principal: one grant per pattern,
// each holding the full role set.
func TestTokenPrincipal(t *testing.T) {
	t.Parallel()

	f := parse(t, idPublisher+" "+hashPublish+" publish,consume orders,orders*\n")
	p := f.Tokens[0].Principal()

	if p.ID != idPublisher || p.Actor() != "tok:"+idPublisher {
		t.Errorf("principal id/actor = %q/%q, want %q/tok:%s", p.ID, p.Actor(), idPublisher, idPublisher)
	}
	if !p.Allows(auth.RolePublish, "orders") || !p.Allows(auth.RoleConsume, "orders.dlq") {
		t.Error("principal should hold publish on orders and consume on orders.dlq via the prefix")
	}
	if p.Allows(auth.RoleAdmin, "orders") {
		t.Error("principal unexpectedly holds admin")
	}
}

// TestTokenRoundTrip is the rapid property: any generated token renders to a line that parses
// back to the same token (id, hash, roles and patterns).
func TestTokenRoundTrip(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		tok := genToken().Draw(t, "token")
		rendered := tok.String() + "\n"

		f, err := auth.Parse("tokens", strings.NewReader(rendered))
		if err != nil {
			t.Fatalf("Parse(render(%q)) = %v", tok.ID, err)
		}
		if len(f.Tokens) != 1 {
			t.Fatalf("Parse(render) gave %d tokens, want 1", len(f.Tokens))
		}
		got := f.Tokens[0]
		if got.ID != tok.ID {
			t.Fatalf("id round-trip = %q, want %q", got.ID, tok.ID)
		}
		if got.Hash != tok.Hash {
			t.Fatalf("hash round-trip = %x, want %x", got.Hash, tok.Hash)
		}
		if got.Roles != tok.Roles {
			t.Fatalf("roles round-trip = %q, want %q", got.Roles, tok.Roles)
		}
		if diff := cmp.Diff(patternStrings(tok.Patterns), patternStrings(got.Patterns)); diff != "" {
			t.Fatalf("patterns round-trip (-want +got):\n%s", diff)
		}
	})
}

func genToken() *rapid.Generator[auth.Token] {
	return rapid.Custom(func(t *rapid.T) auth.Token {
		id := rapid.StringMatching(`[a-z0-9][a-z0-9._-]{1,63}`).Draw(t, "id")
		roles := rapid.Map(rapid.IntRange(1, 7), func(n int) auth.RoleSet {
			return auth.RoleSet(n)
		}).Draw(t, "roles")
		patterns := rapid.Map(
			rapid.SliceOfN(rapid.SampledFrom([]string{"*", "orders", "orders*", "payments", "events*"}), 1, 4),
			func(ss []string) []auth.Pattern {
				out := make([]auth.Pattern, len(ss))
				for i, s := range ss {
					p, err := auth.ParsePattern(s)
					if err != nil {
						panic(err)
					}
					out[i] = p
				}
				return out
			},
		).Draw(t, "patterns")
		var hash [32]byte
		for i := range hash {
			hash[i] = rapid.Byte().Draw(t, "hash byte")
		}
		return auth.Token{ID: id, Hash: hash, Roles: roles, Patterns: patterns}
	})
}
