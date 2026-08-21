// SPDX-License-Identifier: Apache-2.0

package buildinfo

import (
	"encoding/json"
	"runtime"
	"runtime/debug"
	"slices"
	"strings"
	"testing"
)

// setLdflags installs ldflag-injected values for one test and restores them afterwards.
func setLdflags(t *testing.T, v, c, d, dy string) {
	t.Helper()
	oldV, oldC, oldD, oldDy := version, commit, date, dirty
	t.Cleanup(func() { version, commit, date, dirty = oldV, oldC, oldD, oldDy })
	version, commit, date, dirty = v, c, d, dy
}

// setBuildInfo replaces the debug.ReadBuildInfo seam for one test.
func setBuildInfo(t *testing.T, bi *debug.BuildInfo, ok bool) {
	t.Helper()
	old := readBuildInfo
	t.Cleanup(func() { readBuildInfo = old })
	readBuildInfo = func() (*debug.BuildInfo, bool) { return bi, ok }
}

func buildInfoWith(mainVersion string, settings map[string]string) *debug.BuildInfo {
	bi := &debug.BuildInfo{}
	bi.Main.Version = mainVersion
	for k, v := range settings {
		bi.Settings = append(bi.Settings, debug.BuildSetting{Key: k, Value: v})
	}
	return bi
}

func TestGet_LdflagsWin(t *testing.T) {
	setBuildInfo(t, buildInfoWith("(devel)", map[string]string{
		"vcs.revision": "ffffffffffffffffffffffffffffffffffffffff",
		"vcs.time":     "1999-01-01T00:00:00Z",
	}), true)
	setLdflags(t, "v0.1.0", "a1b2c3d4e5f6", "2026-08-24T09:00:00Z", "false")

	got := Get()
	if got.Version != "v0.1.0" {
		t.Errorf("Version = %q, want %q", got.Version, "v0.1.0")
	}
	if got.Commit != "a1b2c3d4e5f6" {
		t.Errorf("Commit = %q, want %q", got.Commit, "a1b2c3d4e5f6")
	}
	if got.Date != "2026-08-24T09:00:00Z" {
		t.Errorf("Date = %q, want %q", got.Date, "2026-08-24T09:00:00Z")
	}
	if got.GoVersion != runtime.Version() {
		t.Errorf("GoVersion = %q, want %q", got.GoVersion, runtime.Version())
	}
	if want := runtime.GOOS + "/" + runtime.GOARCH; got.Platform != want {
		t.Errorf("Platform = %q, want %q", got.Platform, want)
	}
}

func TestGet_BuildInfoFallback(t *testing.T) {
	tests := []struct {
		name string
		bi   *debug.BuildInfo
		ok   bool
		want Info
	}{
		{
			name: "vcs settings populate commit date and dirty",
			bi: buildInfoWith("(devel)", map[string]string{
				"vcs.revision": "0123456789abcdef0123456789abcdef01234567",
				"vcs.time":     "2026-08-21T10:00:00Z",
				"vcs.modified": "true",
			}),
			ok: true,
			want: Info{
				Version: "(devel)",
				Commit:  "0123456789ab",
				Date:    "2026-08-21T10:00:00Z",
				Dirty:   true,
			},
		},
		{
			name: "tagged install without vcs settings",
			bi:   buildInfoWith("v1.4.2", nil),
			ok:   true,
			want: Info{Version: "v1.4.2"},
		},
		{
			name: "empty main version falls back to dev",
			bi:   buildInfoWith("", nil),
			ok:   true,
			want: Info{Version: "dev"},
		},
		{
			name: "no build info at all",
			bi:   nil,
			ok:   false,
			want: Info{Version: "dev"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setLdflags(t, "", "", "", "")
			setBuildInfo(t, tt.bi, tt.ok)

			got := Get()
			if got.Version == "" {
				t.Fatal("Version is empty; Get must never report an empty version")
			}
			if got.Version != tt.want.Version {
				t.Errorf("Version = %q, want %q", got.Version, tt.want.Version)
			}
			if got.Commit != tt.want.Commit {
				t.Errorf("Commit = %q, want %q", got.Commit, tt.want.Commit)
			}
			if got.Date != tt.want.Date {
				t.Errorf("Date = %q, want %q", got.Date, tt.want.Date)
			}
			if got.Dirty != tt.want.Dirty {
				t.Errorf("Dirty = %v, want %v", got.Dirty, tt.want.Dirty)
			}
		})
	}
}

func TestGet_CommitIsTruncatedToTwelve(t *testing.T) {
	setBuildInfo(t, nil, false)
	setLdflags(t, "v0.1.0", "0123456789abcdef0123456789abcdef01234567", "", "")

	if got := Get().Commit; got != "0123456789ab" {
		t.Errorf("Commit = %q, want %q", got, "0123456789ab")
	}
}

// TestGet_Dirty pins the single-source rule: the build system reports a modified worktree
// through the dirty ldflag, never by decorating Version, and the vcs.modified setting is only
// consulted when no ldflag was injected.
func TestGet_Dirty(t *testing.T) {
	tests := []struct {
		name        string
		dirtyLdflag string
		vcsModified string
		want        bool
	}{
		{name: "ldflag true wins over an unmodified vcs record", dirtyLdflag: "true", vcsModified: "false", want: true},
		{name: "ldflag false wins over a modified vcs record", dirtyLdflag: "false", vcsModified: "true", want: false},
		{name: "no ldflag falls back to the vcs record", dirtyLdflag: "", vcsModified: "true", want: true},
		{name: "no ldflag and a clean vcs record", dirtyLdflag: "", vcsModified: "false", want: false},
		{name: "nothing recorded at all", dirtyLdflag: "", vcsModified: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			settings := map[string]string{}
			if tt.vcsModified != "" {
				settings["vcs.modified"] = tt.vcsModified
			}
			setBuildInfo(t, buildInfoWith("v0.1.0", settings), true)
			setLdflags(t, "v0.1.0", "", "", tt.dirtyLdflag)

			got := Get()
			if got.Dirty != tt.want {
				t.Errorf("Dirty = %v, want %v", got.Dirty, tt.want)
			}
			if strings.Contains(got.Version, "dirty") {
				t.Errorf("Version = %q, want no dirty marker: Dirty is the only place that reports it", got.Version)
			}
		})
	}
}

// TestInfoJSONKeys freezes the JSON field names of Info. PLAN.md section 8 makes CLI JSON
// field names part of the compatibility contract, so a rename must break this test.
func TestInfoJSONKeys(t *testing.T) {
	want := []string{"commit", "date", "dirty", "go_version", "platform", "version"}

	b, err := json.Marshal(Info{})
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(b, &fields); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	got := make([]string, 0, len(fields))
	for k := range fields {
		got = append(got, k)
	}
	slices.Sort(got)

	if !slices.Equal(got, want) {
		t.Errorf("JSON keys = %v, want %v", got, want)
	}
}

func TestShort(t *testing.T) {
	tests := []struct {
		name                         string
		version, commit, date, dirty string
		bi                           *debug.BuildInfo
		ok                           bool
		want                         string
	}{
		{
			name:    "fully injected",
			version: "v0.1.0", commit: "a1b2c3d4e5f6", date: "2026-08-24T09:00:00Z",
			want: "messq v0.1.0 (a1b2c3d4e5f6, 2026-08-24T09:00:00Z, " + runtime.Version() + ", " + runtime.GOOS + "/" + runtime.GOARCH + ")",
		},
		{
			name: "dirty devel build without a date",
			bi: buildInfoWith("(devel)", map[string]string{
				"vcs.revision": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
				"vcs.modified": "true",
			}),
			ok:   true,
			want: "messq (devel) (a1b2c3d4e5f6+dirty, unknown, " + runtime.Version() + ", " + runtime.GOOS + "/" + runtime.GOARCH + ")",
		},
		{
			name: "nothing known at all",
			bi:   nil,
			ok:   false,
			want: "messq dev (unknown, unknown, " + runtime.Version() + ", " + runtime.GOOS + "/" + runtime.GOARCH + ")",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setLdflags(t, tt.version, tt.commit, tt.date, tt.dirty)
			setBuildInfo(t, tt.bi, tt.ok)

			if got := Short(); got != tt.want {
				t.Errorf("Short() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestGet_RealBuildInfo exercises the production seam: a `go test` binary must still yield a
// usable Info without any ldflags.
func TestGet_RealBuildInfo(t *testing.T) {
	setLdflags(t, "", "", "", "")

	got := Get()
	if got.Version == "" {
		t.Error("Version is empty under go test")
	}
	if got.GoVersion == "" || !strings.HasPrefix(got.GoVersion, "go") {
		t.Errorf("GoVersion = %q, want a go... version string", got.GoVersion)
	}
	if len(got.Commit) > 12 {
		t.Errorf("Commit = %q, want at most 12 characters", got.Commit)
	}
}
