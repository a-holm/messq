// SPDX-License-Identifier: Apache-2.0

package subject_test

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/subject"
	"github.com/google/go-cmp/cmp"
)

// rejectRow is one input the grammar refuses, and the sentinel it refuses it with.
type rejectRow struct {
	In   string
	Show string // how the input is written in the generated document, when In is unprintable
	As   string // "subject", "pattern", "stream name" or "consumer name"
	Err  string // the sentinel's name, for the generated document
	want error
}

// longSubject is 513 bytes: one over the limit of rule S1.
var longSubject = strings.Repeat("a", 513)

// deepSubject is 33 tokens: one over the limit of rule S1.
var deepSubject = strings.TrimSuffix(strings.Repeat("a.", 33), ".")

// rejectTable is the second normative table. Every row is a named subtest and a row of the
// generated document.
var rejectTable = []rejectRow{
	{In: "", As: "pattern", Err: "ErrEmptySubject", want: subject.ErrEmptySubject},
	{In: "", As: "subject", Err: "ErrEmptySubject", want: subject.ErrEmptySubject},
	{In: ".", As: "subject", Err: "ErrEmptyToken", want: subject.ErrEmptyToken},
	{In: "a..b", As: "subject", Err: "ErrEmptyToken", want: subject.ErrEmptyToken},
	{In: ".a", As: "subject", Err: "ErrEmptyToken", want: subject.ErrEmptyToken},
	{In: "a.", As: "subject", Err: "ErrEmptyToken", want: subject.ErrEmptyToken},
	{In: ".", As: "pattern", Err: "ErrEmptyToken", want: subject.ErrEmptyToken},
	{In: "a..b", As: "pattern", Err: "ErrEmptyToken", want: subject.ErrEmptyToken},
	{In: "foo*", As: "pattern", Err: "ErrWildcardInToken", want: subject.ErrWildcardInToken},
	{In: "f*o", As: "pattern", Err: "ErrWildcardInToken", want: subject.ErrWildcardInToken},
	{In: "foo>", As: "pattern", Err: "ErrWildcardInToken", want: subject.ErrWildcardInToken},
	{In: ">.a", As: "pattern", Err: "ErrGTNotLast", want: subject.ErrGTNotLast},
	{In: "a.>.b", As: "pattern", Err: "ErrGTNotLast", want: subject.ErrGTNotLast},
	{In: "a.*", As: "subject", Err: "ErrWildcardNotAllowed", want: subject.ErrWildcardNotAllowed},
	{In: "a.>", As: "subject", Err: "ErrWildcardNotAllowed", want: subject.ErrWildcardNotAllowed},
	{In: "a.b*", As: "subject", Err: "ErrWildcardNotAllowed", want: subject.ErrWildcardNotAllowed},
	{In: "a\xffb", Show: `a\xffb`, As: "subject", Err: "ErrInvalidUTF8", want: subject.ErrInvalidUTF8},
	{In: "a\x00b", Show: `a\x00b`, As: "subject", Err: "ErrControlChar", want: subject.ErrControlChar},
	{In: "a\nb", Show: `a\nb`, As: "subject", Err: "ErrControlChar", want: subject.ErrControlChar},
	{In: "a\tb", Show: `a\tb`, As: "subject", Err: "ErrControlChar", want: subject.ErrControlChar},
	{In: "a b", As: "subject", Err: "ErrControlChar", want: subject.ErrControlChar},
	{In: "a\x7fb", Show: `a\x7fb`, As: "subject", Err: "ErrControlChar", want: subject.ErrControlChar},
	{In: longSubject, Show: "513 bytes", As: "subject", Err: "ErrSubjectTooLong", want: subject.ErrSubjectTooLong},
	{In: deepSubject, Show: "33 tokens", As: "subject", Err: "ErrTooManyTokens", want: subject.ErrTooManyTokens},
	{In: "", As: "stream name", Err: "ErrNameEmpty", want: subject.ErrNameEmpty},
	{In: "orders/eu", As: "stream name", Err: "ErrNameCharset", want: subject.ErrNameCharset},
	{In: "orders eu", As: "stream name", Err: "ErrNameCharset", want: subject.ErrNameCharset},
	{In: "orders*", As: "stream name", Err: "ErrNameCharset", want: subject.ErrNameCharset},
	{In: ".", As: "stream name", Err: "ErrNameDot", want: subject.ErrNameDot},
	{In: "..", As: "stream name", Err: "ErrNameDot", want: subject.ErrNameDot},
	{In: ".orders", As: "stream name", Err: "ErrNameDot", want: subject.ErrNameDot},
	{In: "orders.", As: "stream name", Err: "ErrNameDot", want: subject.ErrNameDot},
	{In: "orders..dlq", As: "stream name", Err: "ErrNameDot", want: subject.ErrNameDot},
	{In: strings.Repeat("a", 65), Show: "65 bytes", As: "stream name", Err: "ErrNameTooLong", want: subject.ErrNameTooLong},
	{In: strings.Repeat("a", 61), Show: "61 bytes", As: "new stream name", Err: "ErrNameTooLong", want: subject.ErrNameTooLong},
	{In: "orders/eu", As: "new stream name", Err: "ErrNameCharset", want: subject.ErrNameCharset},
	{In: "", As: "consumer name", Err: "ErrNameEmpty", want: subject.ErrNameEmpty},
	{In: "workers.eu", As: "consumer name", Err: "ErrNameCharset", want: subject.ErrNameCharset},
	{In: "workers/eu", As: "consumer name", Err: "ErrNameCharset", want: subject.ErrNameCharset},
	{In: strings.Repeat("a", 65), Show: "65 bytes", As: "consumer name", Err: "ErrNameTooLong", want: subject.ErrNameTooLong},
}

// label is how the row is named in a subtest and written in the generated document: the raw
// input, unless it is unprintable and the row spells out a readable form.
func (r rejectRow) label() string {
	if r.Show != "" {
		return r.Show
	}
	return r.In
}

func TestRejectTable(t *testing.T) {
	t.Parallel()

	for _, row := range rejectTable {
		name := row.As + "_" + displayName(row.label())
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var err error
			switch row.As {
			case "subject":
				_, err = subject.ParseSubject(row.In)
			case "pattern":
				_, err = subject.ParsePattern(row.In)
			case "stream name":
				err = subject.ValidateStreamName(row.In)
			case "new stream name":
				err = subject.ValidateNewStreamName(row.In)
			case "consumer name":
				err = subject.ValidateConsumerName(row.In)
			default:
				t.Fatalf("unknown reject kind %q", row.As)
			}

			if err == nil {
				t.Fatalf("%s %q was accepted, want %s", row.As, row.In, row.Err)
			}
			if !errors.Is(err, row.want) {
				t.Fatalf("%s %q returned %v, want %s", row.As, row.In, err, row.Err)
			}
			// The message quotes the input so a CLI user sees what was rejected, and
			// quoting is what keeps a control character out of the log line.
			if row.In != "" && !strings.Contains(err.Error(), strconv.Quote(row.In)) {
				t.Errorf("error %q does not quote the offending input", err)
			}
		})
	}
}

// TestEverySubjectErrorIsABadSubject keeps the HTTP layer and the CLI on one mapping table:
// they classify with errs.ErrBadSubject and never need to know the grammar's own sentinels.
func TestEverySubjectErrorIsABadSubject(t *testing.T) {
	t.Parallel()

	for _, row := range rejectTable {
		if row.As != "subject" && row.As != "pattern" {
			continue
		}
		var err error
		if row.As == "subject" {
			_, err = subject.ParseSubject(row.In)
		} else {
			_, err = subject.ParsePattern(row.In)
		}
		if !errors.Is(err, errs.ErrBadSubject) {
			t.Errorf("%s %q: %v does not classify as errs.ErrBadSubject", row.As, row.In, err)
		}
	}
	if !errors.Is(subject.ErrNameCharset, errs.ErrBadRequest) {
		t.Error("name errors do not classify as errs.ErrBadRequest")
	}
}

func TestParseSubjectAccepts(t *testing.T) {
	t.Parallel()

	accepted := []string{
		"a",
		"a.b",
		"orders.eu.de.created",
		"ordrer.å",
		"a-b_c",
		"UPPER.lower",
		strings.TrimSuffix(strings.Repeat("a.", 32), "."), // exactly 32 tokens
		strings.Repeat("a", 512),                          // exactly 512 bytes
	}
	for _, in := range accepted {
		got, err := subject.ParseSubject(in)
		if err != nil {
			t.Errorf("ParseSubject(%q) = %v, want a subject", in, err)
			continue
		}
		if string(got) != in {
			t.Errorf("ParseSubject(%q) = %q, want the input back", in, string(got))
		}
	}
}

func TestPatternStringRoundTrips(t *testing.T) {
	t.Parallel()

	for _, in := range []string{">", "*", "a.b", "a.>", "a.*", "*.>", "orders.eu.*.created", "ordrer.å.>"} {
		pat, err := subject.ParsePattern(in)
		if err != nil {
			t.Fatalf("ParsePattern(%q) = %v", in, err)
		}
		if got := pat.String(); got != in {
			t.Errorf("ParsePattern(%q).String() = %q, want the input back", in, got)
		}
		again, err := subject.ParsePattern(pat.String())
		if err != nil {
			t.Fatalf("re-parsing %q = %v", pat.String(), err)
		}
		if again.String() != pat.String() {
			t.Errorf("round trip changed %q into %q", pat.String(), again.String())
		}
	}
}

// TestGtDoesNotMatchItsOwnPrefix is the off-by-one the issue singles out. It is the one bug in
// a subject matcher that misroutes silently instead of failing.
func TestGtDoesNotMatchItsOwnPrefix(t *testing.T) {
	t.Parallel()

	pat, err := subject.ParsePattern("a.>")
	if err != nil {
		t.Fatal(err)
	}
	if pat.Match("a") {
		t.Fatal(`ParsePattern("a.>").Match("a") = true, want false`)
	}
	if !pat.Match("a.b") {
		t.Fatal(`ParsePattern("a.>").Match("a.b") = false, want true`)
	}
}

// TestBarePatternDoesNotMatchTheEmptySubject stops `>` from being read as "match anything,
// including nothing".
func TestBarePatternDoesNotMatchTheEmptySubject(t *testing.T) {
	t.Parallel()

	pat, err := subject.ParsePattern(">")
	if err != nil {
		t.Fatal(err)
	}
	if pat.Match("") {
		t.Fatal(`ParsePattern(">").Match("") = true, want false`)
	}
}

func TestZeroPatternMatchesNothing(t *testing.T) {
	t.Parallel()

	var zero subject.Pattern
	for _, s := range []string{"", "a", "a.b"} {
		if zero.Match(s) {
			t.Errorf("the zero Pattern matched %q", s)
		}
	}
	if zero.String() != "" {
		t.Errorf("zero Pattern String() = %q, want empty", zero.String())
	}
}

func TestPatternShape(t *testing.T) {
	t.Parallel()

	type want struct {
		MinTokens int
		IsLiteral bool
		Prefix    string
	}
	cases := map[string]want{
		">":                   {MinTokens: 1, IsLiteral: false, Prefix: ""},
		"*":                   {MinTokens: 1, IsLiteral: false, Prefix: ""},
		"*.a":                 {MinTokens: 2, IsLiteral: false, Prefix: ""},
		"a.b":                 {MinTokens: 2, IsLiteral: true, Prefix: "a.b"},
		"a.>":                 {MinTokens: 2, IsLiteral: false, Prefix: "a."},
		"a.*":                 {MinTokens: 2, IsLiteral: false, Prefix: "a."},
		"orders.eu.*.created": {MinTokens: 4, IsLiteral: false, Prefix: "orders.eu."},
		"a.b.c":               {MinTokens: 3, IsLiteral: true, Prefix: "a.b.c"},
	}
	for in, w := range cases {
		pat, err := subject.ParsePattern(in)
		if err != nil {
			t.Fatalf("ParsePattern(%q) = %v", in, err)
		}
		got := want{MinTokens: pat.MinTokens(), IsLiteral: pat.IsLiteral(), Prefix: pat.Prefix()}
		if diff := cmp.Diff(w, got); diff != "" {
			t.Errorf("ParsePattern(%q) shape (-want +got):\n%s", in, diff)
		}
	}
}

// TestPrefixIsAPrefixOfEveryMatch is what makes Prefix safe to push into a SQL LIKE: narrowing
// the scan with it must never hide a message the matcher would have accepted.
func TestPrefixIsAPrefixOfEveryMatch(t *testing.T) {
	t.Parallel()

	for _, row := range truthTable {
		if !row.Want {
			continue
		}
		pat, err := subject.ParsePattern(row.Pattern)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(row.Subject, pat.Prefix()) {
			t.Errorf("%q matches %q but the subject does not start with the pattern prefix %q",
				row.Pattern, row.Subject, pat.Prefix())
		}
	}
}

func TestValidateStreamNameAccepts(t *testing.T) {
	t.Parallel()

	for _, name := range acceptedStreamNames {
		if err := subject.ValidateNewStreamName(name); err != nil {
			t.Errorf("ValidateNewStreamName(%q) = %v, want nil", name, err)
		}
		if err := subject.ValidateStreamName(name); err != nil {
			t.Errorf("ValidateStreamName(%q) = %v, want nil", name, err)
		}
	}
}

// acceptedStreamNames is reused by the DLQ derivation test, so every addition here is also an
// assertion that its .dlq name is expressible.
var acceptedStreamNames = []string{
	"a",
	"orders",
	"orders.eu",
	"orders_eu-2",
	"ORDERS",
	"9",
	strings.Repeat("a", 60), // exactly the cap on a name an operator may create
}

// TestDerivedDLQNameIsItselfValid is decision D3: the dead-letter queue is a real stream named
// <stream>.dlq, so every name this package accepts has to leave room for the suffix.
func TestDerivedDLQNameIsItselfValid(t *testing.T) {
	t.Parallel()

	for _, name := range acceptedStreamNames {
		dlq := name + ".dlq"
		if err := subject.ValidateStreamName(dlq); err != nil {
			t.Errorf("ValidateStreamName(%q) = %v, but %q is accepted; D3 needs the derived name to be valid", dlq, err, name)
		}
	}
}

// TestStreamNameRejectsTheAckTokenSeparator is decision D7: the ack token is
// stream/consumer/seq/attempt/generation, so a name carrying a slash would make the grammar
// ambiguous.
func TestStreamNameRejectsTheAckTokenSeparator(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"a/b", "/a", "a/", "orders/eu"} {
		if err := subject.ValidateStreamName(name); !errors.Is(err, subject.ErrNameCharset) {
			t.Errorf("ValidateStreamName(%q) = %v, want ErrNameCharset", name, err)
		}
		if err := subject.ValidateNewStreamName(name); !errors.Is(err, subject.ErrNameCharset) {
			t.Errorf("ValidateNewStreamName(%q) = %v, want ErrNameCharset", name, err)
		}
		if err := subject.ValidateConsumerName(name); !errors.Is(err, subject.ErrNameCharset) {
			t.Errorf("ValidateConsumerName(%q) = %v, want ErrNameCharset", name, err)
		}
	}
}

func TestValidateConsumerNameAccepts(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"w", "workers", "workers-eu_2", "WORKERS", strings.Repeat("a", 64)} {
		if err := subject.ValidateConsumerName(name); err != nil {
			t.Errorf("ValidateConsumerName(%q) = %v, want nil", name, err)
		}
	}
}

func TestSet(t *testing.T) {
	t.Parallel()

	t.Run("matches any member", func(t *testing.T) {
		t.Parallel()

		set, err := subject.ParseSet([]string{"orders.>", "billing.invoice.*"})
		if err != nil {
			t.Fatal(err)
		}
		for _, subj := range []string{"orders.eu", "orders.eu.de.created", "billing.invoice.paid"} {
			if !set.Match(subj) {
				t.Errorf("Set.Match(%q) = false, want true", subj)
			}
		}
		for _, subj := range []string{"orders", "billing.invoice", "billing.invoice.paid.late", "shipping.x"} {
			if set.Match(subj) {
				t.Errorf("Set.Match(%q) = true, want false", subj)
			}
		}
	})

	t.Run("deduplicates on compile", func(t *testing.T) {
		t.Parallel()

		set, err := subject.ParseSet([]string{"a.*", "b.>", "a.*"})
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff([]string{"a.*", "b.>"}, set.Strings()); diff != "" {
			t.Fatalf("Strings() (-want +got):\n%s", diff)
		}
	})

	t.Run("rejects an empty set", func(t *testing.T) {
		t.Parallel()

		for _, raw := range [][]string{nil, {}} {
			if _, err := subject.ParseSet(raw); !errors.Is(err, subject.ErrEmptySet) {
				t.Errorf("ParseSet(%v) = %v, want ErrEmptySet", raw, err)
			}
		}
	})

	t.Run("reports the member that failed", func(t *testing.T) {
		t.Parallel()

		_, err := subject.ParseSet([]string{"a.*", "b.>.c"})
		if !errors.Is(err, subject.ErrGTNotLast) {
			t.Fatalf("ParseSet = %v, want ErrGTNotLast", err)
		}
		if !strings.Contains(err.Error(), "b.>.c") {
			t.Errorf("error %q does not name the offending member", err)
		}
	})

	t.Run("the zero set matches nothing", func(t *testing.T) {
		t.Parallel()

		var zero subject.Set
		if zero.Match("a.b") {
			t.Error("the zero Set matched a.b")
		}
		if got := zero.Strings(); got != nil {
			t.Errorf("zero Set Strings() = %v, want nil", got)
		}
	})

	t.Run("Strings hands out a copy", func(t *testing.T) {
		t.Parallel()

		set, err := subject.ParseSet([]string{"a.*", "b.>"})
		if err != nil {
			t.Fatal(err)
		}
		got := set.Strings()
		got[0] = "tampered"
		if set.Strings()[0] != "a.*" {
			t.Error("Strings() handed out the set's own storage")
		}
	})
}

// matchSink keeps the allocation benchmarks honest: without it the compiler is free to delete
// the call it is measuring.
var matchSink bool

// TestMatchAllocatesNothing is an acceptance criterion, not an observation: cursor top-up calls
// Match once per candidate message, so an allocation here is an allocation per message.
func TestMatchAllocatesNothing(t *testing.T) {
	// No t.Parallel: testing.AllocsPerRun refuses to run in a parallel test, and a shared
	// allocation counter across parallel tests would be meaningless anyway.
	pat, err := subject.ParsePattern("orders.eu.*.created")
	if err != nil {
		t.Fatal(err)
	}
	gt, err := subject.ParsePattern("orders.>")
	if err != nil {
		t.Fatal(err)
	}
	set, err := subject.ParseSet([]string{"orders.eu.*.created", "orders.>", "billing.*"})
	if err != nil {
		t.Fatal(err)
	}

	cases := map[string]func(){
		"Pattern.Match/star": func() { matchSink = pat.Match("orders.eu.de.created") },
		"Pattern.Match/gt":   func() { matchSink = gt.Match("orders.eu.de.created") },
		"Pattern.Match/miss": func() { matchSink = pat.Match("billing.invoice.paid") },
		"Set.Match":          func() { matchSink = set.Match("orders.eu.de.created") },
	}
	for name, fn := range cases {
		if got := testing.AllocsPerRun(1000, fn); got != 0 {
			t.Errorf("%s allocated %.1f times per run, want 0", name, got)
		}
	}
}
