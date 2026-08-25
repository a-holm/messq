// SPDX-License-Identifier: Apache-2.0

package obs

import (
	"log/slog"
	"slices"
	"testing"
)

// wantVocabNames is the frozen vocabulary of PLAN §9.2 / SEMANTICS S2.4, positionally
// aligned with the Kind enum: index i is the name Kind(i) must render. The empty string
// at index 0 is KindInvalid, which is not a member of the closed set — it exists so the
// zero value of Kind is never a valid event. Renaming a member, adding one, removing one
// or reordering them is a breaking change under S16 and fails this golden.
var wantVocabNames = [...]string{
	"", // KindInvalid
	"server.start", "server.stop", "server.reload",
	"recovery.unclean", "recovery.reclaimed", "storage.fatal",
	"stream.create", "stream.update", "stream.delete", "stream.purge",
	"retention.expire", "retention.blocked",
	"consumer.create", "consumer.update", "consumer.delete", "consumer.seek",
	"consumer.pause", "consumer.lag",
	"msg.publish", "msg.dup", "msg.deliver", "msg.ack", "msg.ack_dup", "msg.ack_stale",
	"msg.nak", "msg.term", "msg.extend", "msg.timeout", "msg.dead",
	"dlq.redrive",
	"flow.blocked", "disk.degraded", "auth.denied", "api.error", "admin.action",
}

// TestVocabularyNames pins the closed set character-for-character against the literal
// list above (G1): the DB column, the log field and the metric label all derive from
// Kind.String(), so a rename here is a three-surface breaking change and must be a
// failing golden. It also pins ParseKind as the exact inverse of String over the set,
// with empty and unknown names refused.
func TestVocabularyNames(t *testing.T) {
	if len(wantVocabNames) != numKinds {
		t.Fatalf("vocabulary size drifted: table has %d rows, enum declares numKinds=%d",
			len(wantVocabNames), numKinds)
	}
	const realKinds = 35 // PLAN §9.2: the closed set, excluding KindInvalid.
	if got := numKinds - 1; got != realKinds {
		t.Fatalf("closed set changed size: %d kinds, want %d — adding or removing a kind is a breaking change under S16",
			got, realKinds)
	}
	for i, want := range wantVocabNames {
		k := Kind(i)
		if got := k.String(); got != want {
			t.Errorf("kind %d: String() = %q, want %q", i, got, want)
		}
	}
	if t.Failed() {
		t.Fatal("vocabulary names diverged from the PLAN §9.2 golden")
	}

	for i := 1; i < numKinds; i++ {
		want := wantVocabNames[i]
		got, perr := ParseKind(want)
		if perr != nil {
			t.Errorf("ParseKind(%q) failed: %v", want, perr)
			continue
		}
		if got != Kind(i) {
			t.Errorf("ParseKind(%q) = kind %d, want %d — parse must invert String exactly", want, got, i)
		}
	}

	for _, bad := range []string{"", "msg.publsh", "MSG.PUBLISH", "stream.purge "} {
		got, derr := ParseKind(bad)
		if derr == nil {
			t.Errorf("ParseKind(%q) = %v, want an error — only members of the closed set parse", bad, got)
		}
	}

	// Out-of-range values stay total: they render empty, parse to nothing, carry the
	// zero level and answer false everywhere, but must not panic — Validate owns
	// rejecting them loudly.
	const stray = Kind(255)
	if got := stray.String(); got != "" {
		t.Errorf("Kind(255).String() = %q, want %q", got, "")
	}
	if got := stray.Level(); got != 0 {
		t.Errorf("Kind(255).Level() = %s, want the zero level", got)
	}
	if stray.Sampleable() || stray.Repeatable() || stray.Fold() {
		t.Errorf("Kind(255) answered true for a metadata flag — out-of-range kinds have none")
	}
}

// wantSampleable is every kind --log-sample may ever drop (PLAN §9.3: hot-path events at
// DEBUG). Pinned by name, not by constant, so an enum reorder cannot silently satisfy it.
var wantSampleable = []string{
	"msg.publish", "msg.dup", "msg.deliver", "msg.ack", "msg.ack_dup", "msg.extend",
}

// wantRepeatable is every kind subject to --event-repeat-interval row limiting (§8).
// State-change kinds are absent by construction: 10 000 timeouts ⇒ 10 000 rows, and a
// stream.create is never collapsed into its predecessor.
var wantRepeatable = []string{
	"consumer.lag", "msg.ack_dup", "msg.ack_stale",
	"flow.blocked", "disk.degraded", "auth.denied", "api.error",
}

// wantFold is every kind #13's fold model must have a rule for (§5.2 I10): replaying the
// folded journal reproduces persisted state.
var wantFold = []string{
	"recovery.reclaimed",
	"stream.create", "stream.update", "stream.delete", "stream.purge",
	"retention.expire",
	"consumer.create", "consumer.update", "consumer.delete", "consumer.seek", "consumer.pause",
	"msg.publish", "msg.deliver", "msg.ack", "msg.nak", "msg.term", "msg.timeout", "msg.dead",
	"dlq.redrive",
}

// wantLevels pins the baseline level of every kind against issue #19's vocabulary table.
// msg.extend's "WARN if capped" and api.error's "ERROR for 5xx" are runtime raises via
// Event.Level — Validate (slice 2) enforces raise-only — so the baselines here are DEBUG
// and WARN respectively.
var wantLevels = []struct {
	name string
	lvl  slog.Level
}{
	{"server.start", slog.LevelInfo},
	{"server.stop", slog.LevelInfo},
	{"server.reload", slog.LevelInfo},
	{"recovery.unclean", slog.LevelWarn},
	{"recovery.reclaimed", slog.LevelInfo},
	{"storage.fatal", slog.LevelError},
	{"stream.create", slog.LevelInfo},
	{"stream.update", slog.LevelInfo},
	{"stream.delete", slog.LevelWarn},
	{"stream.purge", slog.LevelWarn},
	{"retention.expire", slog.LevelInfo},
	{"retention.blocked", slog.LevelWarn},
	{"consumer.create", slog.LevelInfo},
	{"consumer.update", slog.LevelInfo},
	{"consumer.delete", slog.LevelWarn},
	{"consumer.seek", slog.LevelWarn},
	{"consumer.pause", slog.LevelInfo},
	{"consumer.lag", slog.LevelInfo},
	{"msg.publish", slog.LevelDebug},
	{"msg.dup", slog.LevelDebug},
	{"msg.deliver", slog.LevelDebug},
	{"msg.ack", slog.LevelDebug},
	{"msg.ack_dup", slog.LevelDebug},
	{"msg.ack_stale", slog.LevelWarn},
	{"msg.nak", slog.LevelWarn},
	{"msg.term", slog.LevelWarn},
	{"msg.extend", slog.LevelDebug},
	{"msg.timeout", slog.LevelWarn},
	{"msg.dead", slog.LevelWarn},
	{"dlq.redrive", slog.LevelInfo},
	{"flow.blocked", slog.LevelWarn},
	{"disk.degraded", slog.LevelWarn},
	{"auth.denied", slog.LevelWarn},
	{"api.error", slog.LevelWarn},
	{"admin.action", slog.LevelInfo},
}

// TestVocabConsistency enforces the rules the metadata table must satisfy (G1): sampling
// implies DEBUG (an operator cannot lose a WARN by turning --log-sample on), the
// sampleable / repeatable / fold subsets are exactly the pinned sets, no two kinds share
// a name, and every baseline level matches the issue-#19 table verbatim.
func TestVocabConsistency(t *testing.T) {
	seen := make(map[string]bool, numKinds)
	var sampled, repeatable, folded []string
	for i := 1; i < numKinds; i++ {
		k := Kind(i)
		name := k.String()
		if seen[name] {
			t.Errorf("duplicate vocabulary name %q", name)
		}
		seen[name] = true

		if k.Sampleable() && k.Level() != slog.LevelDebug {
			t.Errorf("%s: sampleable at level %s — sampling only ever removes DEBUG records", name, k.Level())
		}
		switch {
		case k.Sampleable():
			sampled = append(sampled, name)
		case k.Level() == slog.LevelDebug:
			t.Errorf("%s: DEBUG kind is not marked sampleable — hot-path events log at DEBUG and may be sampled", name)
		}
		if k.Repeatable() {
			repeatable = append(repeatable, name)
		}
		if k.Fold() {
			folded = append(folded, name)
		}
	}

	slices.Sort(sampled)
	assertNameSet(t, "sampleable", sampled, wantSampleable)
	slices.Sort(repeatable)
	assertNameSet(t, "repeatable", repeatable, wantRepeatable)
	slices.Sort(folded)
	assertNameSet(t, "fold", folded, wantFold)

	for _, row := range wantLevels {
		k, kerr := ParseKind(row.name)
		if kerr != nil {
			t.Errorf("wantLevels row %q is not a member of the vocabulary: %v", row.name, kerr)
			continue
		}
		if got := k.Level(); got != row.lvl {
			t.Errorf("%s: Level() = %s, want %s", row.name, got, row.lvl)
		}
	}
}

// assertNameSet requires got and want to hold the same names. got arrives sorted; want
// is sorted here so the pinned sets stay honest regardless of listing order.
func assertNameSet(t *testing.T, what string, got, want []string) {
	t.Helper()
	sorted := slices.Clone(want)
	slices.Sort(sorted)
	for j := 1; j < len(sorted); j++ {
		if sorted[j] == sorted[j-1] {
			t.Fatalf("%s pin itself lists %q twice", what, sorted[j])
		}
	}
	if !slices.Equal(got, sorted) {
		t.Errorf("%s set mismatch:\n got: %v\nwant: %v", what, got, sorted)
	}
}
