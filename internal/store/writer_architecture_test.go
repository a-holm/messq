package store

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestArchitectureSingleWriterSurface pins the structural half of the exactly-one-writer rule
// (PLAN §3.2): no package outside internal/store may obtain a read-write handle. The rw
// handle leaves #5's Store only through TakeWriter (Store.NewWriter takes it internally), so
// the guard is textual over the module tree: any TakeWriter reference, or any direct SQLite
// driver import that would allow opening a private writable handle, outside internal/store
// fails here — long before it becomes a second writer.
//
// database/sql itself stays legal elsewhere: read-pool consumers name its types for RO()
// results, and reading is not writing. internal/tools are build-time utilities that never
// link into the daemon.
func TestArchitectureSingleWriterSurface(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatalf("resolve module root: %v", err)
	}

	var violations []string
	walkErr := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "dist", "testdata", "node_modules", "tools", ".github":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		if strings.HasPrefix(rel, "internal/store/") {
			return nil // the owner of the rule
		}
		content, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		src := string(content)
		if strings.Contains(src, "TakeWriter(") {
			violations = append(violations, rel+": references TakeWriter outside internal/store")
		}
		for _, bad := range []string{`modernc.org/sqlite`, `github.com/mattn/go-sqlite3`} {
			if strings.Contains(src, bad) {
				violations = append(violations, rel+" imports "+bad+" outside internal/store")
			}
		}
		return nil
	})
	if walkErr != nil {
		t.Fatalf("walk module tree: %v", walkErr)
	}
	for _, v := range violations {
		t.Error(v)
	}
}

// TestModuleTreeLoadsUnderTheGuard makes sure the architecture walk above cannot pass
// vacuously on a broken tree: go list must succeed for the packages it claims to police.
func TestModuleTreeLoadsUnderTheGuard(t *testing.T) {
	ctx := context.Background()
	cmd := exec.CommandContext(ctx, "go", "list", "github.com/a-holm/messq/internal/...", "github.com/a-holm/messq/cmd/...")
	cmd.Dir = "../.."
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go list failed (%v): %s", err, out)
	}
}
