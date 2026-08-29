// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestSourceDateHonoursEpoch pins the reproducibility contract: with
// SOURCE_DATE_EPOCH set, the man header's date is that instant, UTC — two
// builds of one commit render identical bytes.
func TestSourceDateHonoursEpoch(t *testing.T) {
	t.Setenv("SOURCE_DATE_EPOCH", "1700000000")
	got := sourceDate()
	if got.Unix() != 1700000000 || got.Location().String() != "UTC" {
		t.Errorf("sourceDate() = %s, want the epoch instant in UTC", got)
	}
}

// TestGenerateIsReproducibleAndHasInclude drives the real generator twice over
// a scratch root with a fixed epoch and pins: (1) two runs are byte-identical,
// (2) man/man8/messq.8 is exactly the one-line roff include of man1 — the page
// #17's systemd unit cites as man:messq(8).
func TestGenerateIsReproducibleAndHasInclude(t *testing.T) {
	t.Setenv("SOURCE_DATE_EPOCH", "1700000000")
	dirs := make([]map[string]string, 0, 2)
	var soBody string
	for range 2 {
		out := t.TempDir()
		if err := generate(out); err != nil {
			t.Fatalf("generate: %v", err)
		}
		files := map[string]string{}
		walkErr := filepath.Walk(out, func(path string, info os.FileInfo, wErr error) error {
			if wErr != nil || info.IsDir() {
				return wErr
			}
			raw, rErr := os.ReadFile(path)
			if rErr != nil {
				return rErr
			}
			sum := sha256.Sum256(raw)
			rel, _ := filepath.Rel(out, path)
			files[rel] = hex.EncodeToString(sum[:])
			if strings.HasSuffix(rel, filepath.Join("man", "man8", "messq.8")) {
				soBody = string(raw)
			}
			return nil
		})
		if walkErr != nil {
			t.Fatalf("hash tree: %v", walkErr)
		}
		dirs = append(dirs, files)
	}
	for name, hash := range dirs[0] {
		if dirs[1][name] != hash {
			t.Errorf("%s differs between two runs of one epoch — output is not reproducible", name)
		}
	}
	if len(dirs[1]) != len(dirs[0]) {
		t.Errorf("run generated %d files then %d files", len(dirs[0]), len(dirs[1]))
	}
	if want := ".so man1/messq.1\n"; soBody != want {
		t.Errorf("man/man8/messq.8 = %q, want the one-line include %q", soBody, want)
	}
	// The man1 section actually rendered the root page.
	if _, ok := dirs[0][filepath.Join("man", "man1", "messq.1")]; !ok {
		t.Error("man/man1/messq.1 missing from the generated tree")
	}
	if _, ok := dirs[0][filepath.Join("docs", "cli", "messq.md")]; !ok {
		t.Error("docs/cli/messq.md missing from the generated tree")
	}
}
