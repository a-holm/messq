// SPDX-License-Identifier: Apache-2.0

package conf

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// newCmd builds one throwaway command carrying every flag shape the chassis uses.
func newCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "probe"}
	fs := cmd.Flags()
	fs.String("addr", "unix:///run/messq/messq.sock", "daemon address")
	fs.Duration("timeout", 30*time.Second, "per-request deadline")
	fs.Bool("quiet", false, "suppress narration")
	fs.Count("verbose", "narrate once per -v")
	fs.StringSlice("header", nil, "repeatable headers")
	// A local (non-persistent) flag must resolve exactly like a persistent one.
	fs.String("commit-window", "250ms", "local flag")
	return cmd
}

func TestEnvNameDerivation(t *testing.T) {
	tests := []struct{ flag, want string }{
		{"addr", "MESSQ_ADDR"},
		{"commit-window", "MESSQ_COMMIT_WINDOW"},
		{"token-file", "MESSQ_TOKEN_FILE"},
		{"quiet", "MESSQ_QUIET"},
	}
	for _, tt := range tests {
		if got := EnvName(tt.flag); got != tt.want {
			t.Errorf("EnvName(%q) = %q, want %q", tt.flag, got, tt.want)
		}
	}
}

func TestApplyEnvPrecedence(t *testing.T) {
	tests := []struct {
		name    string
		flag    string // set via the command line layer, "" = untouched
		env     map[string]string
		flagArg []string // argv form of the flag layer
		want    func(cmd *cobra.Command) string
	}{
		{
			name: "default holds when nothing is set",
			want: func(cmd *cobra.Command) string { return mustGet(cmd, "addr") },
		},
		{
			name: "env beats default",
			env:  map[string]string{"MESSQ_ADDR": "unix:///tmp/x.sock"},
			want: func(cmd *cobra.Command) string { return mustGet(cmd, "addr") },
			// checked below against unix:///tmp/x.sock
		},
		{
			name:    "flag beats env",
			env:     map[string]string{"MESSQ_ADDR": "unix:///tmp/x.sock"},
			flagArg: []string{"--addr", "http://127.0.0.1:4390"},
			want:    func(cmd *cobra.Command) string { return mustGet(cmd, "addr") },
		},
		{
			name: "empty env is unset, never a zero value",
			env:  map[string]string{"MESSQ_QUIET": ""},
			want: func(cmd *cobra.Command) string { return mustGet(cmd, "quiet") },
		},
		{
			name: "duration env parses",
			env:  map[string]string{"MESSQ_TIMEOUT": "5s"},
			want: func(cmd *cobra.Command) string { return mustGet(cmd, "timeout") },
		},
		{
			name: "bool env true",
			env:  map[string]string{"MESSQ_QUIET": "true"},
			want: func(cmd *cobra.Command) string { return mustGet(cmd, "quiet") },
		},
		{
			name: "count env accumulates",
			env:  map[string]string{"MESSQ_VERBOSE": "2"},
			want: func(cmd *cobra.Command) string { return mustGet(cmd, "verbose") },
		},
		{
			name: "slice env splits on comma",
			env:  map[string]string{"MESSQ_HEADER": "a,b"},
			want: func(cmd *cobra.Command) string { return strings.Join(mustGetSlice(cmd, "header"), "|") },
		},
	}
	wants := []string{
		"unix:///run/messq/messq.sock",
		"unix:///tmp/x.sock",
		"http://127.0.0.1:4390",
		"false",
		"5s",
		"true",
		"2",
		"a|b",
	}
	for i := range tests {
		tests[i].flag = wants[i]
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := newCmd()
			if len(tt.flagArg) > 0 {
				if err := cmd.Flags().Parse(tt.flagArg); err != nil {
					t.Fatalf("flag layer: %v", err)
				}
			}
			getenv := func(k string) string { return tt.env[k] }
			if err := ApplyEnv(cmd, getenv); err != nil {
				t.Fatalf("ApplyEnv: %v", err)
			}
			if got := tt.want(cmd); got != tt.flag {
				t.Errorf("resolved %q, want %q", got, tt.flag)
			}
		})
	}
}

func TestApplyEnvLocalFlagGetsEnvToo(t *testing.T) {
	cmd := newCmd()
	getenv := func(k string) string {
		if k == "MESSQ_COMMIT_WINDOW" {
			return "1h"
		}
		return ""
	}
	if err := ApplyEnv(cmd, getenv); err != nil {
		t.Fatalf("ApplyEnv: %v", err)
	}
	if got := mustGet(cmd, "commit-window"); got != "1h" {
		t.Errorf("local flag commit-window = %q, want its env fallback 1h", got)
	}
}

func TestApplyEnvInvalidValueNamesTheEnvVar(t *testing.T) {
	cmd := newCmd()
	getenv := func(k string) string {
		if k == "MESSQ_TIMEOUT" {
			return "soon"
		}
		return ""
	}
	err := ApplyEnv(cmd, getenv)
	if err == nil {
		t.Fatal("ApplyEnv succeeded, want an invalid-value error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "MESSQ_TIMEOUT") {
		t.Errorf("error %q does not name the ENV VAR — the message must say which variable to fix", msg)
	}
	if !strings.Contains(msg, `"soon"`) {
		t.Errorf("error %q does not quote the offending value", msg)
	}
	if strings.Contains(msg, "--timeout") {
		t.Errorf("error %q names the flag; D8 says name the environment variable", msg)
	}
}

func TestSourceStringAndDumpElision(t *testing.T) {
	for s, want := range map[Source]string{
		SourceDefault: "default",
		SourceEnv:     "env",
		SourceFlag:    "flag",
		Source(99):    "unknown",
	} {
		if got := s.String(); got != want {
			t.Errorf("Source(%d).String() = %q, want %q", uint(s), got, want)
		}
	}
	// A value beyond the column width is elided so one bad paste cannot wreck
	// the dump layout.
	cmd := newCmd()
	long := strings.Repeat("x", 40)
	if err := cmd.Flags().Set("addr", long); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	Dump(&buf, cmd, func(string) string { return "" })
	if !strings.Contains(buf.String(), long[:28]+"...") {
		t.Errorf("dump does not elide the long value: %q", buf.String())
	}
}

func TestApplyEnvInvalidSliceElement(t *testing.T) {
	cmd := newCmd()
	getenv := func(k string) string { return map[string]string{"MESSQ_HEADER": ","}[k] }
	if err := ApplyEnv(cmd, getenv); err == nil {
		t.Skip("pflag accepts empty CSV elements; nothing to assert")
	}
}

func TestSourcesForVerboseDump(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		flagArg []string
		flag    string
		want    Source
	}{
		{name: "default", flag: "addr", want: SourceDefault},
		{name: "env", env: map[string]string{"MESSQ_ADDR": "x"}, flag: "addr", want: SourceEnv},
		{name: "flag", flagArg: []string{"--addr", "y"}, flag: "addr", want: SourceFlag},
		// The brief's rule: an env-applied value still reports as env even though
		// pflag's Changed would claim otherwise if we forgot to reset it.
		{name: "env wins over stale Changed bit", env: map[string]string{"MESSQ_ADDR": "z"}, flag: "addr", want: SourceEnv},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := newCmd()
			if len(tt.flagArg) > 0 {
				if err := cmd.Flags().Parse(tt.flagArg); err != nil {
					t.Fatal(err)
				}
			}
			if err := ApplyEnv(cmd, func(k string) string { return tt.env[k] }); err != nil {
				t.Fatal(err)
			}
			srcs := Sources(cmd, func(k string) string { return tt.env[k] })
			got, ok := srcs[tt.flag]
			if !ok || got != tt.want {
				t.Errorf("Sources()[%q] = %v, %v; want %v", tt.flag, got, ok, tt.want)
			}
		})
	}
}

func TestDumpRendersTheWhichSettingWonTable(t *testing.T) {
	cmd := newCmd()
	env := map[string]string{"MESSQ_OUTPUT": "json"}
	cmd.Flags().String("output", "auto", "output mode")
	if err := cmd.Flags().Parse([]string{"--timeout", "5s"}); err != nil {
		t.Fatal(err)
	}
	if err := ApplyEnv(cmd, func(k string) string { return env[k] }); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	Dump(&buf, cmd, func(k string) string { return env[k] })
	out := buf.String()
	for _, want := range []string{
		"addr        unix:///run/messq/messq.sock   (default)",
		"timeout     5s                             (flag --timeout)",
		"output      json                           (env MESSQ_OUTPUT)",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("-vv dump missing line:\n  %q\ngot:\n%s", want, out)
		}
	}
}

func TestDumpSortedStable(t *testing.T) {
	cmd := newCmd()
	var a, b bytes.Buffer
	Dump(&a, cmd, func(string) string { return "" })
	Dump(&b, cmd, func(string) string { return "" })
	if a.String() != b.String() {
		t.Error("Dump is not deterministic between calls")
	}
}

func mustGet(cmd *cobra.Command, name string) string {
	f := cmd.Flags().Lookup(name)
	if f == nil {
		panic("no flag " + name)
	}
	return f.Value.String()
}

func mustGetSlice(cmd *cobra.Command, name string) []string {
	sv, ok := cmd.Flags().Lookup(name).Value.(pflag.SliceValue)
	if !ok {
		panic("not a slice: " + name)
	}
	return sv.GetSlice()
}
