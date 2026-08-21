// SPDX-License-Identifier: Apache-2.0

package errs_test

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/errs"
	"github.com/google/go-cmp/cmp"
)

// TestAllHoldsEverySentinel is the registry's whole purpose: #14 maps every sentinel onto a
// machine code and #23 maps the same set onto an exit code, and both iterate All(). A
// sentinel declared but left out of All() would be invisible to those tests, so this one
// reads the package source and compares what is declared with what is registered.
func TestAllHoldsEverySentinel(t *testing.T) {
	t.Parallel()

	declared := declaredSentinels(t)
	if len(declared) == 0 {
		t.Fatal("no Err* sentinels found in errs.go; the source scan is broken, not the package")
	}

	registered := make([]string, 0, len(errs.All()))
	for _, err := range errs.All() {
		registered = append(registered, err.Error())
	}

	wantNames := make([]string, 0, len(declared))
	wantMessages := make([]string, 0, len(declared))
	for _, d := range declared {
		wantNames = append(wantNames, d.name)
		wantMessages = append(wantMessages, d.message)
	}

	if diff := cmp.Diff(wantMessages, registered); diff != "" {
		t.Fatalf("All() does not match the sentinels declared in errs.go, in declaration order (-declared +All()):\n%s\ndeclared names: %v",
			diff, wantNames)
	}
}

func TestAllIsStableAndPrivate(t *testing.T) {
	t.Parallel()

	first, second := errs.All(), errs.All()
	if diff := cmp.Diff(messages(first), messages(second)); diff != "" {
		t.Fatalf("two calls to All() differ (-first +second):\n%s", diff)
	}
	for i := range first {
		// Every sentinel is distinguishable from every other, which TestSentinelsAreDistinctValues
		// asserts, so errors.Is here is value identity written the way the linter wants it.
		if !errors.Is(first[i], second[i]) {
			t.Fatalf("All()[%d] is not the same error value across calls", i)
		}
	}

	// A caller that sorts or truncates the slice must not disturb the registry. The
	// expected order is snapshotted first: if All handed out its own backing array, both
	// `first` and `second` would be reversed by the line below and comparing them would
	// prove nothing.
	want := messages(second)
	slices.Reverse(first)
	if diff := cmp.Diff(want, messages(errs.All())); diff != "" {
		t.Fatalf("All() changed after a caller mutated the returned slice (-want +got):\n%s", diff)
	}
}

func TestSentinelMessages(t *testing.T) {
	t.Parallel()

	seen := map[string]int{}
	for i, err := range errs.All() {
		msg := err.Error()
		switch {
		case msg == "":
			t.Errorf("sentinel %d has an empty message", i)
		case strings.TrimSpace(msg) != msg:
			t.Errorf("sentinel %q is padded with white space", msg)
		case strings.ContainsAny(msg[len(msg)-1:], ".!?:;,"):
			t.Errorf("sentinel %q ends in punctuation; it is wrapped into longer sentences", msg)
		case msg != strings.ToLower(msg[:1])+msg[1:]:
			t.Errorf("sentinel %q starts with a capital; it is wrapped into longer sentences", msg)
		}
		if first, dup := seen[msg]; dup {
			t.Errorf("sentinel %d repeats the message of sentinel %d: %q", i, first, msg)
		}
		seen[msg] = i
	}
}

func TestSentinelsAreDistinctValues(t *testing.T) {
	t.Parallel()

	all := errs.All()
	for i, a := range all {
		for j, b := range all {
			if i != j && errors.Is(a, b) {
				t.Errorf("sentinel %d (%v) is indistinguishable from sentinel %d (%v)", i, a, j, b)
			}
		}
	}
}

func TestErrorsIsSurvivesWrapping(t *testing.T) {
	t.Parallel()

	for _, sentinel := range errs.All() {
		once := fmt.Errorf("store.Publish: %w", sentinel)
		twice := fmt.Errorf("api.publish: %w", once)
		if !errors.Is(twice, sentinel) {
			t.Errorf("errors.Is lost %v through two levels of %%w", sentinel)
		}
	}
}

func TestE(t *testing.T) {
	t.Parallel()

	err := errs.E(errs.ErrNotFound, "store.Fetch", "consumer %q does not exist", "workers")

	if got, want := err.Error(), `store.Fetch: consumer "workers" does not exist`; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
	if !errors.Is(err, errs.ErrNotFound) {
		t.Fatal("errors.Is(err, ErrNotFound) = false, want true")
	}
	if !errors.Is(fmt.Errorf("api.fetch: %w", err), errs.ErrNotFound) {
		t.Fatal("errors.Is lost the sentinel through a further %w")
	}
	if got := err.Unwrap(); !errors.Is(got, errs.ErrNotFound) {
		t.Fatalf("Unwrap() = %v, want ErrNotFound", got)
	}

	var target *errs.Error
	if !errors.As(fmt.Errorf("outer: %w", err), &target) {
		t.Fatal("errors.As could not recover the *errs.Error")
	}
	if target.Op != "store.Fetch" {
		t.Fatalf("Op = %q, want %q", target.Op, "store.Fetch")
	}
}

func TestEWithoutAnOp(t *testing.T) {
	t.Parallel()

	err := errs.E(errs.ErrBadRequest, "", "batch must be between 1 and 1000")
	if got, want := err.Error(), "batch must be between 1 and 1000"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
}

func TestWithNext(t *testing.T) {
	t.Parallel()

	base := errs.E(errs.ErrNotFound, "cli.consumer", "consumer %q does not exist", "workers")

	withOne := errs.WithNext(base, "messq consumer ls orders")
	if diff := cmp.Diff([]string{"messq consumer ls orders"}, errs.NextOf(withOne)); diff != "" {
		t.Fatalf("NextOf after one WithNext (-want +got):\n%s", diff)
	}

	withTwo := errs.WithNext(withOne, "messq stream ls")
	want := []string{"messq consumer ls orders", "messq stream ls"}
	if diff := cmp.Diff(want, errs.NextOf(withTwo)); diff != "" {
		t.Fatalf("NextOf after a second WithNext (-want +got):\n%s", diff)
	}

	// Extending must not reach back and edit the error the caller already has.
	if diff := cmp.Diff([]string{"messq consumer ls orders"}, errs.NextOf(withOne)); diff != "" {
		t.Fatalf("the first error changed when the second was built (-want +got):\n%s", diff)
	}
	if errs.NextOf(base) != nil {
		t.Fatalf("NextOf(base) = %v, want nil", errs.NextOf(base))
	}

	if !errors.Is(withTwo, errs.ErrNotFound) {
		t.Fatal("WithNext lost the sentinel")
	}
	if got, want := withTwo.Error(), base.Error(); got != want {
		t.Fatalf("WithNext changed the message: %q, want %q", got, want)
	}
}

func TestWithNextOnAPlainError(t *testing.T) {
	t.Parallel()

	plain := fmt.Errorf("store.Open: %w", errs.ErrLocked)
	wrapped := errs.WithNext(plain, "messq doctor")

	if !errors.Is(wrapped, errs.ErrLocked) {
		t.Fatal("WithNext on a plain error lost the sentinel")
	}
	if diff := cmp.Diff([]string{"messq doctor"}, errs.NextOf(wrapped)); diff != "" {
		t.Fatalf("NextOf (-want +got):\n%s", diff)
	}
	if got, want := wrapped.Error(), plain.Error(); got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
}

func TestWithNextEdges(t *testing.T) {
	t.Parallel()

	if got := errs.WithNext(nil, "messq doctor"); got != nil {
		t.Fatalf("WithNext(nil) = %v, want nil", got)
	}
	base := errs.E(errs.ErrPaused, "cli.sub", "consumer is paused")
	// errors.As rather than a type assertion, so a wrapping WithNext would still be found
	// and would still fail the pointer comparison below.
	var same *errs.Error
	if !errors.As(errs.WithNext(base), &same) || same != base {
		t.Fatalf("WithNext with no suggestions returned a different error: %v", errs.WithNext(base))
	}
	if got := errs.NextOf(nil); got != nil {
		t.Fatalf("NextOf(nil) = %v, want nil", got)
	}
	if got := errs.NextOf(errs.ErrPaused); got != nil {
		t.Fatalf("NextOf on a bare sentinel = %v, want nil", got)
	}

	// The returned slice is a copy: a caller that rewrites it must not rewrite the error.
	resume := errs.WithNext(base, "messq consumer resume orders w")
	next := errs.NextOf(resume)
	next[0] = "rm -rf /"
	if got := errs.NextOf(resume); got[0] == "rm -rf /" {
		t.Fatal("NextOf handed out the error's own slice")
	}
}

type sentinelDecl struct{ name, message string }

// declaredSentinels reads errs.go and returns every package-level Err* variable in
// declaration order, with the message literal it was constructed from.
func declaredSentinels(t *testing.T) []sentinelDecl {
	t.Helper()

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "errs.go", nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse errs.go: %v", err)
	}

	var out []sentinelDecl
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.VAR {
			continue
		}
		for _, spec := range gen.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for i, name := range value.Names {
				if !strings.HasPrefix(name.Name, "Err") || i >= len(value.Values) {
					continue
				}
				out = append(out, sentinelDecl{name: name.Name, message: messageLiteral(t, name.Name, value.Values[i])})
			}
		}
	}
	return out
}

// messageLiteral extracts the string a sentinel was declared with. A sentinel built any other
// way is a failure rather than a skip: the scan is only evidence while it sees everything.
func messageLiteral(t *testing.T, name string, expr ast.Expr) string {
	t.Helper()

	call, ok := expr.(*ast.CallExpr)
	if !ok || len(call.Args) != 1 {
		t.Fatalf("%s is not declared as a single-argument call; teach this test the new shape", name)
	}
	lit, ok := call.Args[0].(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		t.Fatalf("%s is not declared with a string literal; teach this test the new shape", name)
	}
	unquoted, err := strconv.Unquote(lit.Value)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return unquoted
}

func messages(all []error) []string {
	out := make([]string, 0, len(all))
	for _, err := range all {
		out = append(out, err.Error())
	}
	return out
}
