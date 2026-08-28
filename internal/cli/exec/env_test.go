// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"strings"
	"testing"

	"github.com/a-holm/messq/pkg/client"
)

const (
	spanHex  = "1a2b3c4d5e6f7a8b"
	traceHex = "4bf92f3577b34da6a3ce929d0e0e4736"
)

func sampleMsg() *client.Delivered {
	return &client.Delivered{
		Stream:      "orders",
		Consumer:    "worker",
		Seq:         10493,
		ID:          "01J8ZQ4K2M9V0X7Y3B5N6C8D1E",
		Subject:     "orders.eu.created",
		Header:      map[string]string{"x-tenant": "acme"},
		Size:        1234,
		Attempt:     2,
		MaxDeliver:  5,
		AckToken:    "orders/worker/10493/2/1",
		DeadlineMS:  1761124472190,
		PublishedAt: 1761124442114,
		TraceID:     traceHex,
	}
}

// lookup extracts one VAR=value entry's value; second return reports presence.
func lookup(env []string, name string) (string, bool) {
	prefix := name + "="
	for _, kv := range env {
		if strings.HasPrefix(kv, prefix) {
			return kv[len(prefix):], true
		}
	}
	return "", false
}

func countNamed(t *testing.T, env []string, name string) int {
	t.Helper()
	n := 0
	for _, kv := range env {
		if strings.HasPrefix(kv, name+"=") {
			n++
		}
	}
	return n
}

// Red killer S3-a: a stale MESSQ_SEQ in the operator's shell must never reach
// the child pretending to be this message's metadata.
func TestBuildEnvStripsStaleParentSeq(t *testing.T) {
	parent := []string{
		"MESSQ_SEQ=999999",
		"MESSQ_HEADER_X_TENANT=sneaky",
		"MESSQ_SUBJECT=fake/stream",
		"PATH=/usr/bin",
	}
	env, _, err := BuildEnv(sampleMsg(), parent, EnvOptions{Traceparent: tpOf()})
	if err != nil {
		t.Fatalf("BuildEnv: %v", err)
	}
	for name, want := range map[string]string{
		// Presence alone proves nothing — BuildEnv computes fresh values for
		// these names. The STALE ones must be gone.
		"MESSQ_SEQ":             "10493",
		"MESSQ_HEADER_X_TENANT": "acme",
		"MESSQ_SUBJECT":         sampleMsg().Subject,
	} {
		got, ok := lookup(env, name)
		if !ok || got != want {
			t.Fatalf("%s = %q ok=%v, want %q: parent state leaked or fresh value lost", name, got, ok, want)
		}
	}
	if strings.Contains(strings.Join(env, "\n"), "999999") {
		t.Fatal("stale parent value 999999 survived somewhere in the child env")
	}
}

// The two deliberate keepers survive the strip so children can call messq back.
func TestBuildEnvKeepsAddrAndTokenFile(t *testing.T) {
	parent := []string{"MESSQ_ADDR=unix:///run/messq.sock", "MESSQ_TOKEN_FILE=/etc/messq/token", "HOME=/root"}
	env, _, err := BuildEnv(sampleMsg(), parent, EnvOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, keep := range []string{"MESSQ_ADDR", "MESSQ_TOKEN_FILE", "HOME"} {
		if _, ok := lookup(env, keep); !ok {
			t.Fatalf("%s was dropped; children call `messq` back through these", keep)
		}
	}
}

// Red killer S3-b: header names get mangled to the strict charset, the §4 table
// lands verbatim, and headers surface BOTH ways (mangled var + lossless JSON).
func TestBuildEnvTableAndHeaderMangling(t *testing.T) {
	msg := sampleMsg()
	msg.Header = map[string]string{
		"x-tenant": "acme",
		"num-9":    "n",
		"a.b/c":    "weird",
		"with nul": "not\x00allowed",
	}
	opts := EnvOptions{Traceparent: "00-" + traceHex + "-" + spanHex + "-01"}
	env, warns, err := BuildEnv(msg, []string{"PATH=/bin"}, opts)
	if err != nil {
		t.Fatalf("BuildEnv: %v", err)
	}
	checks := map[string]string{
		"MESSQ_STREAM":          "orders",
		"MESSQ_CONSUMER":        "worker",
		"MESSQ_MSG_ID":          msg.ID,
		"MESSQ_SEQ":             "10493",
		"MESSQ_SUBJECT":         msg.Subject,
		"MESSQ_ATTEMPT":         "2",
		"MESSQ_MAX_DELIVER":     "5",
		"MESSQ_REDELIVERED":     "1", // attempt 2
		"MESSQ_TRACE_ID":        traceHex,
		"MESSQ_DEADLINE_MS":     "1761124472190",
		"MESSP_SIZE_SENTINEL":   "never-set",
		"MESSQ_SIZE":            "1234",
		"MESSQ_ACK_TOKEN":       msg.AckToken,
		"MESSQ_HEADER_X_TENANT": "acme",
		"MESSQ_HEADER_NUM_9":    "n",
		"MESSQ_HEADER_A_B_C":    "weird",
		"traceparent":           "00-" + traceHex + "-" + spanHex + "-01",
	}
	for name, want := range checks {
		if name == "MESSP_SIZE_SENTINEL" {
			continue
		}
		got, ok := lookup(env, name)
		if !ok {
			t.Fatalf("child env lacks %s (full: %v)", name, env)
		}
		if got != want {
			t.Fatalf("%s = %q, want %q", name, got, want)
		}
	}
	if got, ok := lookup(env, "MESSQ_HEADERS_JSON"); !ok || got != `{"a.b/c":"weird","num-9":"n","with nul":"not\u0000allowed","x-tenant":"acme"}` {
		t.Fatalf("MESSQ_HEADERS_JSON = %q ok=%v (lossless contract)", got, ok)
	}
	if _, ok := lookup(env, "MESSQ_HEADER_WITH_NUL"); ok {
		t.Fatal("NUL-valued header leaked its own variable; must ride JSON only")
	}
	foundWarn := false
	for _, w := range warns {
		if strings.Contains(w, "with nul") {
			foundWarn = true
		}
	}
	if !foundWarn {
		t.Fatalf("skipped-NUL-value header produced no warning; warnings were %v", warns)
	}

	vars := make(map[string]bool)
	for _, kv := range env {
		eq := strings.IndexByte(kv, '=')
		if eq <= 0 {
			t.Fatalf("env entry %q is not name=value", kv)
		}
		vars[kv[:eq]] = true
	}
}

// Red killer S3-c: equal mangling targets collide — SORTED header names decide,
// first wins, loser dropped WITH a warning naming both originals.
func TestBuildEnvCollisionSortedFirstWins(t *testing.T) {
	msg := sampleMsg()
	msg.Header = map[string]string{"x-a": "second?", "X.A": "first!"}
	env, warns, err := BuildEnv(msg, nil, EnvOptions{})
	if err != nil {
		t.Fatal(err)
	}
	v, _ := lookup(env, "MESSQ_HEADER_X_A")
	if v != "first!" { // "X.A" sorts BEFORE "x-a" (ASCII '.'=46 < '-'45? '-'=45 <'.'=46 → x-a first!)
		t.Fatalf("collision winner %q — expected sort order to pick deterministically", v)
	}
	dropped := false
	for _, w := range warns {
		if strings.Contains(w, "x-a") && strings.Contains(w, "X.A") && strings.Contains(w, "keeping") {
			dropped = true
		}
	}
	if !dropped {
		t.Fatalf("collision must WARN naming both names and the kept one, got %v", warns)
	}
}

// Red killer S3-d: -clean-env reduces the parent world to the allowlist while
// keeping messq's own passes-through.
func TestBuildEnvCleanEnvAllowlist(t *testing.T) {
	parent := []string{
		"MALLOC_ARENA_MAX=2", "SECRET_TOKEN=hush", "PATH=/opt/bin",
		"MESSQ_ADDR=unix:///sock", "MESSQ_NOISE=yes",
	}
	env, _, err := BuildEnv(sampleMsg(), parent, EnvOptions{CleanEnv: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, banned := range []string{"MALLOC_ARENA_MAX", "SECRET_TOKEN", "MESSQ_NOISE"} {
		if _, ok := lookup(env, banned); ok {
			t.Fatalf("--exec-clean-env allowed %s through", banned)
		}
	}
	if v, _ := lookup(env, "PATH"); v != "/opt/bin" {
		t.Fatalf("PATH lost under clean-env: %q", v)
	}
	if v, _ := lookup(env, "MESSQ_ADDR"); v != "unix:///sock" {
		t.Fatalf("MESSQ_ADDR lost under clean-env: %q", v)
	}
}

// Extras: validated, placed last, able to override survivors, refusing the
// reserved namespace outright.
func TestBuildEnvExtrasRules(t *testing.T) {
	msg := sampleMsg()
	opts := EnvOptions{ExtraEnv: []string{
		"APP_MODE=careful",  // lands
		"PATH=/custom/path", // overrides parent survivor
		"MESSQ_SEQ=forged",  // refused: reserved namespace
	}, Traceparent: tpOf()}
	env, warns, err := BuildEnv(msg, []string{"PATH=/bin"}, opts)
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := lookup(env, "APP_MODE"); v != "careful" {
		t.Fatal("--exec-env APP_MODE did not reach the child")
	}
	if v, _ := lookup(env, "PATH"); v != "/custom/path" {
		t.Fatalf("--exec-env PATH should override parent PATH, got %q", v)
	}
	if v, _ := lookup(env, "MESSQ_SEQ"); v != "10493" {
		t.Fatalf("extra forged MESSQ_SEQ beat computed value: %q", v)
	}
	refused := false
	for _, w := range warns {
		if strings.Contains(w, "MESSQ_SEQ") {
			refused = true
		}
	}
	if !refused {
		t.Fatalf("reserved extra produced no refusal note; warns=%v", warns)
	}
}

func TestBuildEnvRejectsGarbage(t *testing.T) {
	base := []string{}
	cases := []struct {
		name  string
		extra []string
	}{
		{"missing equals", []string{"BROKEN"}},
		{"empty name", []string{"=value"}},
		{"nul in value", []string{"APP=nul\x00byte"}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, err := BuildEnv(sampleMsg(), base, EnvOptions{ExtraEnv: tt.extra}); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestBuildEnvValidationOnMessage(t *testing.T) {
	m := sampleMsg()
	m.Subject = ""
	if _, _, err := BuildEnv(m, nil, EnvOptions{}); err == nil {
		t.Fatal("expected error delivering a subject-less message to a child")
	}
}

// helpers -------------------------------------------------------------------

func tpOf() string { return "00-" + traceHex + "-" + spanHex + "-01" }
