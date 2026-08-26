// SPDX-License-Identifier: Apache-2.0

package client_test

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// updateDoc rewrites the API-surface golden instead of comparing against it.
var updateDoc = flag.Bool("update", false, "rewrite pkg/client/testdata/go_doc_all.golden")

// TestGoDocAllIsCurrent freezes the exported surface the way a reader actually
// sees it: the rendered output of `go doc -all` for this package. The golden is
// the compatibility promise made reviewable (issue #22) — an added or changed
// exported name shows up as a diff in CI, not as a silent widening. The real
// tool is exec'd rather than go/doc re-rendered so the snapshot can never
// drift from what godoc prints; the output is source-derived and stable (the
// package carries no build-tagged files), so plain runs are deterministic.
//
// After changing the exported surface, run `go test ./pkg/client -update` and
// read the diff before committing: the diff IS the API review.
func TestGoDocAllIsCurrent(t *testing.T) {
	var stdout, stderr bytes.Buffer
	// t.Context() cancels at test end, killing a wedged `go doc` instead of hanging CI.
	cmd := exec.CommandContext(t.Context(), "go", "doc", "-all", ".") // test binary cwd IS the package directory
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("go doc -all . failed: %v\nstderr: %s", err, stderr.String())
	}
	want := stdout.Bytes()

	golden := filepath.Join("testdata", "go_doc_all.golden")
	if *updateDoc {
		// MkdirAll BEFORE the write: a fresh checkout without the golden must
		// not die writing into a missing directory on the first -update run.
		if err := os.MkdirAll(filepath.Dir(golden), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(golden, want, 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote %s (%d bytes)", golden, len(want))
		return
	}

	got, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("%v\nrun: go test ./pkg/client -update", err)
	}
	if !bytes.Equal(got, want) {
		at := firstDiff(got, want)
		t.Fatalf("%s is stale: first difference at byte %d\n%s\nrun: go test ./pkg/client -update",
			golden, at, diffWindow(got, want, at))
	}
}

// firstDiff returns the offset of the first differing byte, or the length of
// the shorter side when one output is a prefix of the other.
func firstDiff(a, b []byte) int {
	n := min(len(a), len(b))
	for i := range n {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

// diffWindow quotes ±60 bytes around the first difference from both sides, so
// the failure names the drifted declaration without dumping two full renders.
func diffWindow(got, want []byte, at int) string {
	lo := max(at-60, 0)
	return fmt.Sprintf("--- got ---\n%q\n--- golden ---\n%q",
		got[lo:min(at+60, len(got))], want[lo:min(at+60, len(want))])
}
