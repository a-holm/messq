// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"strings"
	"testing"

	"github.com/a-holm/messq/pkg/client"
)

func FuzzBuildEnv(f *testing.F) {
	f.Add("x-tenant", "acme", "")
	f.Add("a.b/c", "weird value", "MESSQ_SEQ=stale\n")
	f.Add("9lives", "\x00nul", "")
	f.Add("", "", "")
	f.Fuzz(func(t *testing.T, name, value, parentBlock string) {
		m := &client.Delivered{
			Stream:     "s",
			Consumer:   "c",
			Seq:        7,
			ID:         "01J8ZQ4K2M9V0X7Y3B5N6C8D1E",
			Subject:    "s.created",
			Header:     map[string]string{name: value},
			AckToken:   "t/1",
			TraceID:    traceHex,
			MaxDeliver: 3,
			Attempt:    1,
		}
		var parent []string
		for _, line := range strings.Split(parentBlock, "\n") {
			if line != "" {
				parent = append(parent, line)
			}
		}
		env, _, err := BuildEnv(m, parent, EnvOptions{ExtraEnv: []string{"X=" + value}})
		if err != nil {
			return
		}

		seen := map[string]int{}
		for _, kv := range env {
			i := strings.IndexByte(kv, '=')
			if i < 0 {
				t.Fatalf("entry %q lacks '='", kv)
			}
			n := kv[:i]
			v := kv[i+1:]
			if n == "" {
				t.Fatal("empty variable name in output")
			}
			if strings.ContainsAny(n, "=\x00") {
				t.Fatalf("forbidden byte in name %q", n)
			}
			if strings.ContainsRune(v, '\x00') {
				t.Fatalf("NUL survived into value of %s (%q)", n, v)
			}
			seen[n]++
			if seen[n] > 1 {
				t.Fatalf("duplicate variable %s produced twice", n)
			}
		}
	})
}

// The table rows of §4 beyond TestBuildEnvTableAndHeaderMangling's spot checks:
// full ordering never matters to correctness, but every documented variable
// must exist with the documented rendering.
func TestBuildEnvEveryDocumentedVariable(t *testing.T) {
	m := sampleMsg()
	env, warns, err := BuildEnv(m, []string{"PATH=/bin"}, EnvOptions{Traceparent: tpOf()})
	if err != nil {
		t.Fatal(err)
	}
	if len(warns) != 0 {
		t.Fatalf("clean sample must warn nothing, got %v", warns)
	}
	wantDeadline := "2025-10-22T09:14:32.190Z" // epoch 1761124472190
	checks := map[string]string{
		"MESSQ_DEADLINE":    wantDeadline,
		"MESSQ_REDELIVERED": "1",
	}
	for k, v := range checks {
		got, ok := lookup(env, k)
		if !ok || got != v {
			t.Fatalf("%s = %q ok=%v, want %q", k, got, ok, v)
		}
	}
	m2 := sampleMsg()
	m2.Attempt = 1
	env2, _, berr := BuildEnv(m2, nil, EnvOptions{})
	if berr != nil {
		t.Fatal(berr)
	}
	if v, _ := lookup(env2, "MESSQ_REDELIVERED"); v != "0" {
		t.Fatalf("first attempt must not be marked redelivered: %q", v)
	}
}

// Header names composed only of separators still mangle somewhere meaningful;
// empty-name headers are skipped with a warning and live on in JSON.
func TestBuildEnvDegenerateHeaderNames(t *testing.T) {
	m := sampleMsg()
	m.Header = map[string]string{"": "nameless", "-": "dashy"}
	env, warns, err := BuildEnv(m, nil, EnvOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := lookup(env, "MESSQ_HEADER_"); ok {
		t.Fatal("empty/underscore-mangling header should not yield its own var")
	}
	if got, ok := lookup(env, nameHeadersJSON); !ok || !strings.Contains(got, `"-"`) {
		t.Fatalf("degenerate headers lost from JSON: %q", got)
	}
	if len(warns) < 2 {
		t.Fatalf("expected warnings for both degenerate headers, got %v", warns)
	}
}

// Extra K=V overriding a parent survivor keeps exactly one entry.
func TestBuildEnvNoShadowingAfterOverride(t *testing.T) {
	env, _, err := BuildEnv(sampleMsg(), []string{"LANG=C.UTF-8", "PATH=/bin"},
		EnvOptions{ExtraEnv: []string{"PATH=/override/bin"}})
	if err != nil {
		t.Fatal(err)
	}
	if n := countNamed(t, env, "PATH"); n != 1 {
		t.Fatalf("PATH appears %d times after override; libc getenv would read the stale one", n)
	}
	if v, _ := lookup(env, "PATH"); v != "/override/bin" {
		t.Fatalf("override did not win: %q", v)
	}
}
