// SPDX-License-Identifier: Apache-2.0

// Package buildinfo reports how this binary was built. It lives outside package main so that
// the CLI, the HTTP surface and the metrics registry can all read the same values.
package buildinfo

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"strings"
)

// Injected at link time with -ldflags -X. Empty in `go install`, `go run` and `go test`
// builds, where Get falls back to runtime/debug.ReadBuildInfo. dirty is a string because
// -ldflags -X only sets strings; "true" means the build came from a modified worktree.
var (
	version string
	commit  string
	date    string
	dirty   string
)

// readBuildInfo is a seam so tests can exercise the branch where no build info is embedded.
var readBuildInfo = debug.ReadBuildInfo

// commitLen is the number of hex characters kept from a VCS revision.
const commitLen = 12

// Info describes the binary. The JSON field names are part of the CLI compatibility contract
// (PLAN.md section 8) and are frozen by TestInfoJSONKeys.
//
// Version takes whichever shape the build had information for, in descending order of
// precision: a tag ("v0.3.1"), a tag plus distance ("v0.3.1-4-gabc1234"), a bare short commit
// while the repository has no tag yet ("6516113"), a pseudo-version from a module download
// ("v0.0.0-20260821091252-6c40cfb51281"), "(devel)" from an unstamped `go run` or `go test`
// build, or "dev" when nothing at all was available. It is never empty.
//
// Dirty is reported here and only here. Version carries no dirty marker, so a modified
// worktree shows up once rather than in two fields.
type Info struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`     // 12 hex characters, or "" when unavailable
	Date      string `json:"date"`       // RFC3339 UTC, or "" when unavailable
	Dirty     bool   `json:"dirty"`      // built from a modified worktree
	GoVersion string `json:"go_version"` // runtime.Version()
	Platform  string `json:"platform"`   // "linux/amd64"
}

// Get reports the build information of the running binary. It never panics and never returns
// an empty Version. Commit and Date are empty when nothing recorded them.
func Get() Info {
	info := Info{
		Version:   version,
		Commit:    commit,
		Date:      date,
		Dirty:     dirty == "true",
		GoVersion: runtime.Version(),
		Platform:  runtime.GOOS + "/" + runtime.GOARCH,
	}

	if bi, ok := readBuildInfo(); ok && bi != nil {
		if info.Version == "" {
			// A VCS-stamped build of a modified worktree carries "+dirty" in Main.Version.
			// Dirty is the only field that reports it, so strip it here too.
			info.Version = strings.TrimSuffix(bi.Main.Version, "+dirty")
		}
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				if info.Commit == "" {
					info.Commit = s.Value
				}
			case "vcs.time":
				if info.Date == "" {
					info.Date = s.Value
				}
			case "vcs.modified":
				if dirty == "" {
					info.Dirty = s.Value == "true"
				}
			}
		}
	}

	if info.Version == "" {
		info.Version = "dev"
	}
	if len(info.Commit) > commitLen {
		info.Commit = info.Commit[:commitLen]
	}
	return info
}

// Short renders Info as the single line printed by `messq version`. Unknown fields render as
// "unknown" rather than as an empty gap.
func Short() string {
	i := Get()

	commit := i.Commit
	if commit == "" {
		commit = "unknown"
	}
	if i.Dirty {
		commit += "+dirty"
	}
	date := i.Date
	if date == "" {
		date = "unknown"
	}

	return fmt.Sprintf("messq %s (%s, %s, %s, %s)", i.Version, commit, date, i.GoVersion, i.Platform)
}
