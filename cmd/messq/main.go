// SPDX-License-Identifier: Apache-2.0

// Command messq is the messq daemon and its client. All behaviour lives in internal/cli so it
// is testable without spawning a process.
package main

import (
	"os"

	"github.com/a-holm/messq/internal/cli"
)

func main() {
	os.Exit(cli.Run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}
