// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/a-holm/messq/pkg/client"
)

// The child-environment contract of issue #25 §4: metadata in MESSQ_* variables,
// user headers namespaced under MESSQ_HEADER_* with strict mangling, and the W3C
// traceparent. Three laws hold everywhere below:
//
//   - parent MESSQ_* state never masquerades as this message's metadata: a stale
//     MESSQ_SEQ inherited from an operator shell is STRIPPED before building;
//     MESSQ_ADDR and MESSQ_TOKEN_FILE are deliberately exempt so the child can
//     call `messq` itself;
//   - the payload NEVER enters argv or env (§10): BuildEnv only ever handles
//     metadata, and the hermetic canary test greps the child's /proc files;
//   - output is unambiguous: exactly one entry per variable name, every value
//     free of NUL — FuzzBuildEnv holds both properties against arbitrary input.

const (
	prefixReserved    = "MESSQ_"
	nameAddrKept      = "MESSQ_ADDR"
	nameTokenKept     = "MESSQ_TOKEN_FILE" //nolint:gosec // a path to the credential file, not the credential itself; G101 matches on the word "TOKEN" only
	envVarTraceparent = "traceparent"
	nameHeadersJSON   = prefixReserved + "HEADERS_JSON"
)

// cleanEnvAllowlist is the --exec-clean-env set: PATH, HOME, TZ, LANG plus
// messq's own two pass-throughs.
var cleanEnvAllowlist = map[string]bool{
	"PATH":        true,
	"HOME":        true,
	"TZ":          true,
	"LANG":        true,
	nameAddrKept:  true,
	nameTokenKept: true,
}

// EnvOptions carries what BuildEnv needs besides the message itself. Traceparent
// arrives pre-minted (crypto randomness lives with the runner) so this builder
// stays pure.
type EnvOptions struct {
	ExtraEnv    []string // --exec-env entries, flag order, later duplicates win
	CleanEnv    bool     // --exec-clean-env: allowlist parent instead of full world
	Traceparent string   // W3C value, empty skips the variable
}

// BuildEnv assembles the child environment for one delivered message.
//
// Composition order is fixed and final: surviving parent entries in their
// original order, the computed MESSQ_* block in §4 order (per-header vars
// sorted), the W3C traceparent, then --exec-env extras in flag order. Any
// variable appears at most once: extras override parents (position moved to the
// end so libc getenv cannot read the stale first match), computed values cannot
// be forged through extras (reserved namespace refuses them with a warning).
func BuildEnv(m *client.Delivered, parent []string, opts EnvOptions) ([]string, []string, error) {
	warns := []string{}
	if m == nil {
		return nil, warns, fmt.Errorf("no delivered message")
	}
	if m.Subject == "" || m.ID == "" {
		return nil, warns, fmt.Errorf("message %s lacks subject/id; refusing to deliver metadata-free work", shortID(m.ID))
	}
	extraOK, extraWarns, err := validatedExtras(opts.ExtraEnv)
	if err != nil {
		return nil, append(warns, extraWarns...), err
	}
	warns = append(warns, extraWarns...)

	out := filterParent(parent, opts.CleanEnv)

	add := func(name, val string) { out = append(out, name+"="+val) }
	add("MESSQ_STREAM", m.Stream)
	add("MESSQ_CONSUMER", m.Consumer)
	add("MESSQ_MSG_ID", m.ID)
	add("MESSQ_SEQ", strconv.FormatInt(m.Seq, 10))
	add("MESSQ_SUBJECT", m.Subject)
	add("MESSQ_ATTEMPT", strconv.Itoa(m.Attempt))
	add("MESSQ_MAX_DELIVER", strconv.Itoa(m.MaxDeliver))
	add("MESSQ_REDELIVERED", bool01(m.Attempt > 1))
	add("MESSQ_TRACE_ID", m.TraceID)
	add("MESSQ_DEADLINE", msRFC3339(m.DeadlineMS))
	add("MESSQ_DEADLINE_MS", strconv.FormatInt(m.DeadlineMS, 10))
	add("MESSQ_PUBLISHED_AT", msRFC3339(m.PublishedAt))
	add("MESSQ_SIZE", strconv.FormatInt(m.Size, 10))
	add("MESSQ_ACK_TOKEN", m.AckToken)
	for _, hv := range headerVars(m.Header, &warns) {
		add(hv.name, hv.value)
	}
	j, jerr := json.Marshal(m.Header) // map[string]string: sorted keys, \u0000-safe
	if jerr != nil {
		// Impossible for map[string]string today; a warning beats a panic if a
		// future field type breaks that assumption mid-release.
		warns = append(warns, "MESSQ_HEADERS_JSON marshal failed: "+jerr.Error())
	} else {
		add(nameHeadersJSON, string(j))
	}
	if opts.Traceparent != "" && !strings.ContainsRune(opts.Traceparent, '\x00') {
		add(envVarTraceparent, opts.Traceparent)
	}

	for _, x := range extraOK {
		name, val, _ := splitKv(x)
		removeNamed(&out, name)
		add(name, val)
	}
	return out, warns, nil
}

// headerVars mangles user headers into MESSQ_HEADER_* variables:
//
//   - uppercase ASCII letters kept, EVERY other rune → '_' after uppercasing,
//     so `x-tenant` arrives as MESSQ_HEADER_X_TENANT;
//   - a leading digit gets a '_' prefix;
//   - names that collapse onto each other collide deterministically: sorted by
//     ORIGINAL name ascending ("X.A" < "x-a"), first wins, loser dropped WITH a
//     warning naming both originals — MESSQ_HEADERS_JSON still holds both;
//   - NUL-valued headers and empty names are skipped here (warning), lossless
//     in JSON.
func headerVars(h map[string]string, warns *[]string) []kvPair {
	names := make([]string, 0, len(h))
	for n := range h {
		names = append(names, n)
	}
	sort.Strings(names)

	var out []kvPair
	taken := map[string]string{} // mangled -> original winner
	warnedDrop := map[string]bool{}
	for _, original := range names {
		switch {
		case original == "":
			*warns = append(*warns, "header with empty name skipped (kept in MESSQ_HEADERS_JSON)")
			continue
		case strings.ContainsRune(h[original], '\x00'):
			*warns = append(*warns, fmt.Sprintf("header %q: NUL value cannot reach an env var; it stays in MESSQ_HEADERS_JSON", original))
			continue
		}
		mangled := mangleHeaderName(original)
		if mangled == "" || strings.Trim(mangled, "_") == "" {
			*warns = append(*warns, fmt.Sprintf("header %q mangles to nothing meaningful; skipped (kept in JSON)", original))
			continue
		}
		prev, collided := taken[mangled]
		if collided {
			if !warnedDrop[mangled] {
				*warns = append(*warns, fmt.Sprintf(
					"header collision: %q and %q both map to %s%s; keeping %q — MESSQ_HEADERS_JSON keeps both",
					prev, original, prefixReserved+"HEADER_", mangled, prev))
				warnedDrop[mangled] = true
			}
			continue
		}
		taken[mangled] = original
		out = append(out, kvPair{name: prefixReserved + "HEADER_" + mangled, value: h[original]})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

// mangleHeaderName uppercases, replaces every rune outside [A-Z0-9] with '_',
// and prefixes a lone leading digit.
func mangleHeaderName(name string) string {
	up := strings.ToUpper(name)
	var b strings.Builder
	b.Grow(len(up) + 1)
	if up != "" && up[0] >= '0' && up[0] <= '9' {
		b.WriteByte('_')
	}
	for i := 0; i < len(up); i++ {
		c := up[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
			b.WriteByte(c)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// filterParent returns parent entries that may survive: under --exec-clean-env
// only the allowlist; otherwise everything EXCEPT the perishable MESSQ_*
// metadata namespace (ADDR/TOKEN_FILE are the deliberate keepers). Malformed
// entries without '=' die here too: an exec environment entry must parse.
func filterParent(parent []string, cleanEnv bool) []string {
	var out []string
	for _, kv := range parent {
		name, val, ok := splitKv(kv)
		if !ok || name == "" {
			continue
		}
		if strings.ContainsAny(val, "\x00") || strings.ContainsAny(name, "\x00") {
			continue // unrepresentable state can only arrive corrupted
		}
		if cleanEnv && !cleanEnvAllowlist[name] {
			continue
		}
		if isReserved(name) && name != nameAddrKept && name != nameTokenKept {
			continue
		}
		out = append(out, name+"="+val)
	}
	return out
}

// validatedExtras type-checks --exec-env entries: NAME=value, non-empty name,
// no NUL anywhere. Reserved-namespace refusals degrade to warnings so the run
// proceeds without forging message identity.
func validatedExtras(extras []string) ([]string, []string, error) {
	var ok []string
	var warns []string
	for _, x := range extras {
		name, _, present := splitKv(x)
		switch {
		case !present:
			return nil, warns, fmt.Errorf("--exec-env %q: want NAME=value", x)
		case name == "":
			return nil, warns, fmt.Errorf("--exec-env %q: empty NAME", x)
		case strings.ContainsRune(x, '\x00'):
			return nil, warns, fmt.Errorf("--exec-env %q: NUL byte is not representable", x)
		case isReserved(name):
			warns = append(warns, "--exec-env "+name+"=… refused: the MESSQ_/traceparent namespace belongs to messq")
		default:
			ok = append(ok, x)
		}
	}
	return ok, warns, nil
}

// splitKv splits at the FIRST '='; ok=false when none exists.
func splitKv(s string) (name, val string, ok bool) {
	i := strings.IndexByte(s, '=')
	if i < 0 {
		return "", "", false
	}
	return s[:i], s[i+1:], true
}

func isReserved(name string) bool {
	return strings.HasPrefix(name, prefixReserved) || name == envVarTraceparent
}

// removeNamed drops every existing entry for name before an override is
// re-appended: execve does not dedupe, libc getenv reads FIRST match, so a
// surviving stale pair would shadow the fresh value otherwise.
func removeNamed(env *[]string, name string) {
	prefix := name + "="
	kept := (*env)[:0]
	for _, kv := range *env {
		if !strings.HasPrefix(kv, prefix) {
			kept = append(kept, kv)
		}
	}
	*env = kept
}

func bool01(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

// msRFC3339 renders unix milliseconds in the documented shape
// (2026-10-22T09:14:32.190Z): always UTC, always millisecs.
func msRFC3339(ms int64) string {
	return time.UnixMilli(ms).UTC().Format("2006-01-02T15:04:05.000Z")
}

func shortID(id string) string { return id }

type kvPair struct{ name, value string }
