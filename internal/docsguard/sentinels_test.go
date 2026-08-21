// SPDX-License-Identifier: Apache-2.0

package docsguard_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/docsguard"
)

// writeGo puts one throwaway source file in a scratch directory and returns its path.
func writeGo(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "errs.go")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestParseSentinels_ReadsDeclarationOrder(t *testing.T) {
	t.Parallel()

	path := writeGo(t, `package errs

import "errors"

const notASentinel = "x"

var (
	// ErrFirst is first.
	ErrFirst = errors.New("first")
	ErrSecond = errors.New("second")
	notExported = errors.New("ignored")
)

var ErrThird = errors.New("third")
`)

	got, err := docsguard.ParseSentinels(path)
	if err != nil {
		t.Fatal(err)
	}
	want := []docsguard.Sentinel{
		{Name: "ErrFirst", Message: "first"},
		{Name: "ErrSecond", Message: "second"},
		{Name: "ErrThird", Message: "third"},
	}
	if len(got) != len(want) {
		t.Fatalf("ParseSentinels returned %d sentinels, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("sentinel %d = %+v, want %+v", i+1, got[i], want[i])
		}
	}
}

func TestParseSentinels_Rejects(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		body string
	}{
		{"not Go at all", "this is not a Go file"},
		{"no sentinels", "package errs\n\nvar x = 1\n"},
		{"declared with fmt.Errorf", "package errs\n\nimport \"fmt\"\n\nvar ErrBad = fmt.Errorf(\"bad\")\n"},
		{"declared from another sentinel", "package errs\n\nvar ErrBad = ErrOther\n"},
		{"errors.New with no argument", "package errs\n\nimport \"errors\"\n\nvar ErrBad = errors.New()\n"},
		{"a call that is not New", "package errs\n\nimport \"errors\"\n\nvar ErrBad = errors.Join(nil)\n"},
		{"New from another package", "package errs\n\nimport \"x\"\n\nvar ErrBad = x.New(\"bad\")\n"},
		{"message is not a literal", "package errs\n\nimport \"errors\"\n\nvar ErrBad = errors.New(message)\n"},
		{"method call rather than a package call", "package errs\n\nvar ErrBad = v.f.New(\"bad\")\n"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			if _, err := docsguard.ParseSentinels(writeGo(t, c.body)); err == nil {
				t.Fatal("the malformed source was accepted")
			}
		})
	}
}

func TestParseSentinels_MissingFile(t *testing.T) {
	t.Parallel()

	if _, err := docsguard.ParseSentinels(filepath.Join(t.TempDir(), "absent.go")); err == nil {
		t.Fatal("a missing file was accepted")
	}
}

// TestBuildIDIndex_NeedsEveryTable keeps the index honest: an index built from a document that
// has lost one of its registers would silently stop resolving that whole ID space.
func TestBuildIDIndex_NeedsEveryTable(t *testing.T) {
	t.Parallel()

	full := string(read(t, specPath))
	cases := []struct {
		name    string
		heading string
	}{
		{"no transition table", "### S6.1 The table"},
		{"no invariant register", "## S15. Invariant register"},
		{"no clarification register", "### S1.5 Clarifications register"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			if _, err := docsguard.BuildIDIndex([]byte(removeHeading(full, c.heading))); err == nil {
				t.Fatalf("a document with %q removed was accepted", c.heading)
			}
		})
	}
}

// removeHeading drops one heading line, which orphans the table under it.
func removeHeading(md, heading string) string {
	lines := strings.Split(md, "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		if line == heading {
			continue
		}
		out = append(out, line)
	}
	return strings.Join(out, "\n")
}
