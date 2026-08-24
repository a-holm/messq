package wirecheck

import (
	"strings"
	"testing"

	"pgregory.net/rapid"
)

// fataler is satisfied by both *testing.T and *rapid.T.
type fataler interface {
	Helper()
	Fatalf(format string, args ...any)
}

func mustDigest(t fataler, v any) Digest {
	t.Helper()
	d, err := DigestOf(v)
	if err != nil {
		t.Fatalf("DigestOf: %v", err)
	}
	return d
}

// TestClassifyVerdictMatrix walks the whole §3 table. Each row is the exact mutant
// class the classifier must catch — a wrong verdict here launders a wire break.
func TestClassifyVerdictMatrix(t *testing.T) {
	type base struct {
		A string `json:"a"`
		B int    `json:"b,omitempty"`
	}
	baseD := mustDigest(t, base{})

	cases := []struct {
		name string
		old  Digest
		new  Digest
		side Side
		want Verdict
	}{
		{
			name: "add optional is additive",
			old:  baseD,
			new: mustDigest(t, struct {
				A string `json:"a"`
				B int    `json:"b,omitempty"`
				C bool   `json:"c,omitempty"`
			}{}),
			want: Additive,
		},
		{
			name: "add always to response is additive",
			old:  baseD,
			new: mustDigest(t, struct {
				A string `json:"a"`
				B int    `json:"b,omitempty"`
				C bool   `json:"c"`
			}{}),
			side: Response,
			want: Additive,
		},
		{
			name: "add always to request is breaking",
			old:  baseD,
			new: mustDigest(t, struct {
				A string `json:"a"`
				B int    `json:"b,omitempty"`
				C bool   `json:"c"`
			}{}),
			side: Request,
			want: Breaking,
		},
		{
			name: "remove field is breaking",
			old:  baseD,
			new: mustDigest(t, struct {
				A string `json:"a"`
			}{}),
			want: Breaking,
		},
		{
			name: "retype is breaking",
			old:  baseD,
			new: mustDigest(t, struct {
				A string `json:"a"`
				B string `json:"b,omitempty"`
			}{}),
			want: Breaking,
		},
		{
			name: "always to optional is breaking",
			old: mustDigest(t, struct {
				A string `json:"a"`
				B int    `json:"b"`
			}{}),
			new:  baseD,
			want: Breaking,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			side := tc.side
			if side == 0 {
				side = Response
			}
			got := Classify(tc.old, tc.new, side)
			if got.Verdict != tc.want {
				t.Fatalf("Classify verdict = %s (%v), want %s", got.Verdict, got.Changes, tc.want)
			}
		})
	}

	t.Run("rename is removal plus addition and stays breaking", func(t *testing.T) {
		renamed := mustDigest(t, struct {
			A string `json:"a"`
			Z int    `json:"z,omitempty"`
		}{})
		got := Classify(baseD, renamed, Response)
		if got.Verdict != Breaking {
			t.Fatalf("rename classified %s, want BREAKING", got.Verdict)
		}
	})

	t.Run("optional to always on response is additive", func(t *testing.T) {
		tightened := mustDigest(t, struct {
			A string `json:"a"`
			B int    `json:"b"`
		}{})
		if got := Classify(baseD, tightened, Response); got.Verdict != Additive {
			t.Fatalf("optional→always classified %s, want ADDITIVE", got.Verdict)
		}
	})
}

// TestEnumDiff pins the closed-set half of the matrix: members are added freely
// (clients tolerate unknown members), never silently removed.
func TestEnumDiff(t *testing.T) {
	old := []string{"ok", "stale", "unknown"}
	if got := ClassifyEnum(old, append(append([]string{}, old...), "wrong_generation")); got != Additive {
		t.Errorf("enum add = %s, want ADDITIVE", got)
	}
	shrunk := []string{"ok", "stale"}
	if got := ClassifyEnum(old, shrunk); got != Breaking {
		t.Errorf("enum remove = %s, want BREAKING", got)
	}
	swapped := []string{"ok", "stale", "bogus"}
	if got := ClassifyEnum(old, swapped); got != Breaking {
		t.Errorf("enum rename = %s, want BREAKING", got)
	}
}

// TestStatusRowChangeIsBreaking: a code's HTTP status is frozen (§7); moving one is a
// break even though no field changed.
func TestStatusRowChangeIsBreaking(t *testing.T) {
	old := map[string]int{"stale_ack": 409, "not_found": 404}
	if ClassifyStatusTable(old, map[string]int{"stale_ack": 400, "not_found": 404}) != Breaking {
		t.Error("status change not flagged BREAKING")
	}
	if ClassifyStatusTable(old, old) != Additive {
		t.Error("identical tables flagged non-ADDITIVE")
	}
}

// TestClassifierSoundnessProperty: generated single-step mutations classify exactly as
// the matrix says — every removal/rename/retype BREAKING, every pure addition ADDITIVE.
func TestClassifierSoundnessProperty(t *testing.T) {
	type doc struct {
		K string `json:"k"`
		N int    `json:"n,omitempty"`
	}
	d := mustDigest(t, doc{})
	rapid.Check(t, func(t *rapid.T) {
		switch rapid.IntRange(0, 2).Draw(t, "mutation") {
		case 0: // remove a random field → always BREAKING
			which := rapid.SampledFrom([]string{"k", "n"}).Draw(t, "field")
			var less Digest
			if which == "k" {
				less = mustDigest(t, struct {
					N int `json:"n,omitempty"`
				}{})
			} else {
				less = mustDigest(t, struct {
					K string `json:"k"`
				}{})
			}
			if v := Classify(d, less, Response); v.Verdict != Breaking {
				t.Fatalf("removal of %s classified %s", which, v.Verdict)
			}
		case 1: // add an optional field → always ADDITIVE
			more := mustDigest(t, struct {
				K string `json:"k"`
				N int    `json:"n,omitempty"`
				X bool   `json:"x,omitempty"`
			}{})
			if v := Classify(d, more, Response); v.Verdict != Additive {
				t.Fatalf("addition classified %s (%v)", v.Verdict, v.Changes)
			}
		default: // retype a random field's JSON kind → always BREAKING
			which := rapid.SampledFrom([]string{"k", "n"}).Draw(t, "field")
			var retyped Digest
			if which == "k" {
				retyped = mustDigest(t, struct {
					K bool `json:"k"`
					N int  `json:"n,omitempty"`
				}{})
			} else {
				retyped = mustDigest(t, struct {
					K string `json:"k"`
					N bool   `json:"n,omitempty"`
				}{})
			}
			if v := Classify(d, retyped, Response); v.Verdict != Breaking {
				t.Fatalf("retype of %s classified %s", which, v.Verdict)
			}
		}
	})
}

// TestVerdictString keeps the CI log greppable for the two contract words.
func TestVerdictString(t *testing.T) {
	if Additive.String() != "ADDITIVE" || Breaking.String() != "BREAKING" {
		t.Fatalf("verdict strings wrong: %q %q", Additive.String(), Breaking.String())
	}
	if !strings.Contains(Classify(mustDigest(t, struct {
		A string `json:"a"`
	}{}), mustDigest(t, struct{}{}), Response).Summary(), "removed") {
		t.Error("summary should name the removal")
	}
}
