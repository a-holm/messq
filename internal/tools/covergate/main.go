// SPDX-License-Identifier: Apache-2.0

// Command covergate enforces the per-package statement-coverage floors in coverage.floors,
// and is the only thing allowed to move them.
//
// Three modes:
//
//	covergate -profile cover.out -floors coverage.floors
//	    Check every floor. Exit 1 on a violation.
//
//	covergate -floors coverage.floors -compare-floors base.floors [-commit-messages file]
//	    Refuse a floor that the proposed file sets below the merge base, unless a
//	    coverage-floor-lowered trailer in the branch's commit messages names that floor.
//
//	covergate -profile cover.out -floors coverage.floors -ratchet
//	    Raise the floors that measured coverage clears by a whole point. Run by a human,
//	    committed, reviewed. Never by CI: a bot that edits the gate is not a gate.
//
// Exit codes: 0 all floors met, 1 violation, 2 bad input.
package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	exitOK        = 0
	exitViolation = 1
	exitUsage     = 2
)

// maxUncoveredShown caps the uncovered ranges printed per failing package. The point is to
// name the next place to write a test, not to reproduce the profile.
const maxUncoveredShown = 8

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

type options struct {
	profile       string
	floors        string
	compareFloors string
	root          string
	commitMsgs    string
	doRatchet     bool
}

func run(args []string, stdout, stderr io.Writer) int {
	var opt options
	fs := flag.NewFlagSet("covergate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&opt.profile, "profile", "cover.out", "coverage profile to read")
	fs.StringVar(&opt.floors, "floors", "coverage.floors", "floors file to enforce")
	fs.StringVar(&opt.compareFloors, "compare-floors", "", "baseline floors file; refuse any floor set below it")
	fs.StringVar(&opt.root, "root", ".", "module root, used to find go.mod and the floored packages")
	// -commit-messages holds the concatenated bodies of every commit the branch adds, in the
	// format `git log --format=%B <merge-base>..HEAD` writes. It replaces the earlier
	// -allow-lower, which a single trailer anywhere on the branch set for every floor at
	// once: one explained move silently authorized every unrelated cut.
	fs.StringVar(&opt.commitMsgs, "commit-messages", "", "file of git commit bodies whose coverage-floor-lowered trailers may explain lowerings")
	fs.BoolVar(&opt.doRatchet, "ratchet", false, "rewrite the floors file upward to match measured coverage")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "covergate: unexpected argument %q\n", fs.Arg(0))
		return exitUsage
	}

	floors, err := readFloors(opt.floors)
	if err != nil {
		fmt.Fprintf(stderr, "covergate: %s: %v\n", opt.floors, err)
		return exitUsage
	}

	switch {
	case opt.compareFloors != "":
		return runCompare(opt, floors, stdout, stderr)
	case opt.doRatchet:
		return runRatchet(opt, floors, stdout, stderr)
	default:
		return runCheck(opt, floors, stdout, stderr)
	}
}

func runCheck(opt options, floors []floor, stdout, stderr io.Writer) int {
	profile, err := readProfile(opt)
	if err != nil {
		fmt.Fprintf(stderr, "covergate: %v\n", err)
		return exitUsage
	}

	state, err := scanPackages(opt.root, floors)
	if err != nil {
		fmt.Fprintf(stderr, "covergate: %v\n", err)
		return exitUsage
	}

	failed := false
	for _, r := range check(profile, floors, state) {
		switch r.Status {
		case statusOK:
			fmt.Fprintf(stdout, "covergate: OK      %-24s %6.2f%% >= %.1f%%\n", r.Floor.Pkg, r.Pct, r.Floor.Min)
		case statusPending:
			fmt.Fprintf(stdout, "covergate: PENDING %-24s %s (%s)\n", r.Floor.Pkg, r.Reason, r.Floor.Note)
		case statusFail:
			failed = true
			writeFailure(stdout, r)
		}
	}

	if failed {
		fmt.Fprintf(stdout, "next: go tool cover -html=%s\n", opt.profile)
		return exitViolation
	}
	return exitOK
}

func writeFailure(w io.Writer, r result) {
	if r.Reason != "" {
		fmt.Fprintf(w, "covergate: FAIL    %-24s %s (%s)\n", r.Floor.Pkg, r.Reason, r.Floor.Note)
		return
	}
	fmt.Fprintf(w, "covergate: FAIL    %-24s %6.2f%% < %.1f%% (%s)\n", r.Floor.Pkg, r.Pct, r.Floor.Min, r.Floor.Note)
	if len(r.Uncovered) == 0 {
		return
	}
	fmt.Fprintln(w, "                   uncovered hot spots:")
	for i, b := range r.Uncovered {
		if i == maxUncoveredShown {
			fmt.Fprintf(w, "                     ... and %d more\n", len(r.Uncovered)-maxUncoveredShown)
			break
		}
		fmt.Fprintf(w, "                     %s:%d-%d\n", b.File, b.StartLine, b.EndLine)
	}
}

func runCompare(opt options, proposed []floor, stdout, stderr io.Writer) int {
	base, err := readFloors(opt.compareFloors)
	if err != nil {
		fmt.Fprintf(stderr, "covergate: %s: %v\n", opt.compareFloors, err)
		return exitUsage
	}

	var trailers []string
	if opt.commitMsgs != "" {
		content, err := os.ReadFile(opt.commitMsgs)
		if err != nil {
			fmt.Fprintf(stderr, "covergate: %s: %v\n", opt.commitMsgs, err)
			return exitUsage
		}
		trailers = floorLoweredTrailers(content)
	}

	lowerings := compareFloors(base, proposed)
	if len(lowerings) == 0 {
		fmt.Fprintf(stdout, "covergate: %d floors, none lowered against %s\n", len(proposed), opt.compareFloors)
		return exitOK
	}

	// A trailer explains only the floors its reason names, so the acceptance is per floor:
	// one explained move no longer unlocks every unrelated cut on the same branch.
	var unexplained []lowering
	for _, l := range lowerings {
		ok := false
		for _, reason := range trailers {
			if namesFloor(reason, l.Pkg) {
				ok = true
				break
			}
		}
		if !ok {
			unexplained = append(unexplained, l)
		}
	}

	for _, l := range lowerings {
		fmt.Fprintf(stdout, "covergate: %s\n", l)
	}
	if len(unexplained) == 0 {
		fmt.Fprintln(stdout, "covergate: accepted, a commit on this branch explains the lowering")
		return exitOK
	}
	for _, l := range unexplained {
		fmt.Fprintf(stdout, "covergate: no commit on this branch explains the lowering of %s\n", l.Pkg)
	}
	fmt.Fprintln(stdout, "next: raise the coverage instead, or put 'coverage-floor-lowered: <package> <reason>' in a commit message on this branch")
	return exitViolation
}

func runRatchet(opt options, floors []floor, stdout, stderr io.Writer) int {
	profile, err := readProfile(opt)
	if err != nil {
		fmt.Fprintf(stderr, "covergate: %v\n", err)
		return exitUsage
	}

	measured := make(map[string]float64, len(profile))
	for pkg, cov := range profile {
		if cov.Total > 0 {
			measured[pkg] = cov.Pct()
		}
	}

	raises := ratchet(floors, measured)
	if len(raises) == 0 {
		fmt.Fprintf(stdout, "covergate: %s unchanged, no floor clears its value by %.1f points\n", opt.floors, ratchetSlack)
		return exitOK
	}

	content, err := os.ReadFile(opt.floors)
	if err != nil {
		fmt.Fprintf(stderr, "covergate: %v\n", err)
		return exitUsage
	}
	updated := applyRaises(string(content), raises)
	if err := os.WriteFile(opt.floors, []byte(updated), 0o644); err != nil {
		fmt.Fprintf(stderr, "covergate: %v\n", err)
		return exitUsage
	}

	for _, r := range raises {
		fmt.Fprintf(stdout, "covergate: raised %-24s %.1f -> %.1f\n", r.Pkg, r.From, r.To)
	}
	fmt.Fprintf(stdout, "covergate: %s rewritten, review and commit it\n", opt.floors)
	return exitOK
}

func readFloors(path string) ([]floor, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return parseFloors(bytes.NewReader(content))
}

func readProfile(opt options) (map[string]*pkgCover, error) {
	module, err := modulePath(opt.root)
	if err != nil {
		return nil, err
	}
	content, err := os.ReadFile(opt.profile)
	if err != nil {
		return nil, err
	}
	profile, err := parseProfile(bytes.NewReader(content), module)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", opt.profile, err)
	}
	return profile, nil
}

func scanPackages(root string, floors []floor) (map[string]pkgState, error) {
	state := make(map[string]pkgState, len(floors))
	for _, f := range floors {
		s, err := scanPackage(root, f.Pkg)
		if err != nil {
			return nil, err
		}
		state[f.Pkg] = s
	}
	return state, nil
}

// modulePath reads the module line of go.mod. Coverage profiles name files by import path, so
// stripping the module path is what turns them into the package paths coverage.floors uses.
func modulePath(root string) (string, error) {
	content, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		return "", err
	}
	for line := range strings.Lines(string(content)) {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(line), "module "); ok {
			return strings.TrimSpace(rest), nil
		}
	}
	return "", errors.New("go.mod has no module line")
}
