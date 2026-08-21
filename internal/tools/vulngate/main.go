// SPDX-License-Identifier: Apache-2.0

// Command vulngate turns a govulncheck scan into a build gate, and keeps the suppression list
// honest.
//
// govulncheck exits 0 whatever it finds when -format is sarif, json or vex, and it has no
// native suppression mechanism, so both decisions are made here.
//
// Two modes:
//
//	vulngate -allow .govulncheck-allow -check-expiry
//	    Validate the allow file and fail on any entry past its date. Needs no network, so it
//	    runs before the scan and fails fast.
//
//	govulncheck -format sarif ./... | vulngate -allow .govulncheck-allow [-strict]
//	    Fail on a vulnerability govulncheck reports as reachable from messq's own code, unless
//	    it is suppressed by a live allow entry. -strict also fails on an allow entry that
//	    matches nothing, which is what the nightly lane runs.
//
// Exit codes: 0 clean, 1 blocked, 2 bad input.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"time"
)

const (
	exitOK      = 0
	exitBlocked = 1
	exitUsage   = 2
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	var (
		allowPath   string
		checkExpiry bool
		strict      bool
		nowFlag     string
	)
	fs := flag.NewFlagSet("vulngate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&allowPath, "allow", ".govulncheck-allow", "suppression file to apply")
	fs.BoolVar(&checkExpiry, "check-expiry", false, "only validate the allow file and its expiry dates")
	fs.BoolVar(&strict, "strict", false, "also fail on an allow entry that matches no finding")
	fs.StringVar(&nowFlag, "now", "", "override today's date (YYYY-MM-DD), for tests")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "vulngate: unexpected argument %q\n", fs.Arg(0))
		return exitUsage
	}

	// The seam that makes this testable is the -now flag below, not internal/clock: the Clock
	// interface exists so daemon deadlines are testable, and a build-gate command is not the
	// daemon.
	now := time.Now().UTC() //nolint:forbidigo // -now is this command's clock seam.
	if nowFlag != "" {
		parsed, err := time.Parse(dateLayout, nowFlag)
		if err != nil {
			fmt.Fprintf(stderr, "vulngate: -now %q is not a %s date\n", nowFlag, dateLayout)
			return exitUsage
		}
		now = parsed
	}

	content, err := os.ReadFile(allowPath)
	if err != nil {
		fmt.Fprintf(stderr, "vulngate: %v\n", err)
		return exitUsage
	}
	allow, err := parseAllow(bytes.NewReader(content))
	if err != nil {
		fmt.Fprintf(stderr, "vulngate: %s: %v\n", allowPath, err)
		return exitUsage
	}

	if expired := expiredSuppressions(allow, now); len(expired) > 0 {
		for _, s := range expired {
			fmt.Fprintf(stdout, "vulngate: FAIL %s suppression expired on %s: %s\n",
				s.OSV, s.Expires.Format(dateLayout), s.Reason)
		}
		fmt.Fprintf(stdout, "next: fix the vulnerability, or extend the entry in %s with a fresh reason\n", allowPath)
		return exitBlocked
	}

	if checkExpiry {
		if len(allow) == 0 {
			fmt.Fprintf(stdout, "vulngate: %s is empty, which is the expected steady state\n", allowPath)
		} else {
			fmt.Fprintf(stdout, "vulngate: %d live suppressions in %s\n", len(allow), allowPath)
		}
		return exitOK
	}

	findings, err := parseSARIF(stdin)
	if err != nil {
		fmt.Fprintf(stderr, "vulngate: %v\n", err)
		return exitUsage
	}

	return report(judge(findings, allow, now), strict, stdout)
}

func report(v verdict, strict bool, stdout io.Writer) int {
	for _, f := range v.Blocking {
		fmt.Fprintf(stdout, "vulngate: FAIL %s is reachable from messq's own code: %s\n", f.OSV, f.Message)
	}
	for _, f := range v.Suppressed {
		fmt.Fprintf(stdout, "vulngate: suppressed %s\n", f.OSV)
	}
	for _, s := range v.Unused {
		fmt.Fprintf(stdout, "vulngate: %s %s suppresses nothing; delete the entry\n", unusedLabel(strict), s.OSV)
	}

	fmt.Fprintf(stdout, "vulngate: %d reachable, %d imported but not called, %d in required modules only\n",
		len(v.Blocking)+len(v.Suppressed), v.Imported, v.Required)

	switch {
	case len(v.Blocking) > 0:
		fmt.Fprintln(stdout, "next: go tool -modfile=tools/govulncheck.mod govulncheck -show verbose ./...")
		return exitBlocked
	case strict && len(v.Unused) > 0:
		return exitBlocked
	default:
		return exitOK
	}
}

func unusedLabel(strict bool) string {
	if strict {
		return "FAIL"
	}
	return "warning:"
}
