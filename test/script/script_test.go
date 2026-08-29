// SPDX-License-Identifier: Apache-2.0

// Package script runs the CLI's testscript (.txtar) golden suite (issue #26 §5).
//
// TestMain registers this test binary AS the messq CLI: testscript.RunMain copies
// the binary onto each script's PATH, and a script line saying `messq …` re-execs
// it — so every script drives the real command tree, the real client and the real
// transport, while the suite owns the clock and the daemon lifecycle. Coverage of
// the re-exec'd CLI counts through GOCOVERDIR, which testscript propagates, so
// the golden suite feeds the coverage floors instead of dodging them.
package script

import (
	"flag"
	"os"
	"testing"

	"github.com/rogpeppe/go-internal/testscript"

	"github.com/a-holm/messq/internal/cli"
	"github.com/a-holm/messq/internal/testutil/scriptenv"
)

var update = flag.Bool("update", false, "rewrite golden sections instead of failing on drift")

// TestMain is the dispatch point for both faces of this binary: `go test` runs
// the suite; a re-exec as `messq` runs one CLI invocation through the production
// entry point and exits with its code. The child MUST exit with cli.Run's code —
// the exit code is the contract the scripts assert — so os.Exit is the point of
// this function, exactly as it is in cmd/messq.
func TestMain(m *testing.M) {
	testscript.Main(m, map[string]func(){
		"messq": func() {
			os.Exit(cli.Run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr)) //nolint:forbidigo // the re-exec'd CLI IS a command entry point; its exit code is the contract
		},
	})
}

// TestScripts runs every .txtar under this directory through the shared
// scriptenv: the determinism environment, the custom command set and the inproc
// daemon. The coverage-binding tests live in bindings_test.go.
func TestScripts(t *testing.T) {
	t.Parallel()
	suite := &scriptenv.Suite{Dir: ".", Update: *update}
	testscript.Run(t, suite.Params())
}
