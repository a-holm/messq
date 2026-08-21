// SPDX-License-Identifier: Apache-2.0

// Package buildinfo reports how this binary was built. It lives outside package main so that
// the CLI, the HTTP surface and the metrics registry can all read the same values.
package buildinfo

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

// Injected at link time with -ldflags -X. Empty in `go install`, `go run` and `go test`
// builds, where Get falls back to runtime/debug.ReadBuildInfo.
var (
	version string
	commit  string
	date    string
)

// readBuildInfo is a seam so tests can exercise the branch where no build info is embedded.
var readBuildInfo = debug.ReadBuildInfo

// commitLen is the number of hex characters kept from a VCS revision.
const commitLen = 12

// Info describes the binary. The JSON field names are part of the CLI compatibility contract
// (PLAN.md section 8) and are frozen by TestInfoJSONKeys.
type Info struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	Date      string `json:"date"`
	Dirty     bool   `json:"dirty"`
	GoVersion string `json:"go_version"`
	Platform  string `json:"platform"`
}

// Get reports the build information of the running binary. It never panics and never returns
// an empty Version. Commit and Date are empty when nothing recorded them.
func Get() Info {
	info := Info{
		Version:   version,
		Commit:    commit,
		Date:      date,
		GoVersion: runtime.Version(),
		Platform:  runtime.GOOS + "/" + runtime.GOARCH,
	}

	if bi, ok := readBuildInfo(); ok && bi != nil {
		if info.Version == "" {
			info.Version = bi.Main.Version
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
				info.Dirty = s.Value == "true"
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
