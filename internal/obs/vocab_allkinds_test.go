package obs

import "testing"

// TestAllKindsCoversClosedSet pins the exported iterator over the closed vocabulary
// that #21's metrics exhaustiveness test consumes: every member of the §9.2 set is
// yielded exactly once, KindInvalid is never yielded, order follows declaration order,
// and every yielded kind renders its frozen name. A new vocabulary member must appear
// here automatically — that is the whole point of iterating instead of listing.
func TestAllKindsCoversClosedSet(t *testing.T) {
	kinds := AllKinds()

	const realKinds = 35 // PLAN §9.2: the closed set, excluding KindInvalid.
	if len(kinds) != realKinds {
		t.Fatalf("AllKinds() returned %d kinds, want %d", len(kinds), realKinds)
	}

	seen := make(map[Kind]bool, len(kinds))
	for i, k := range kinds {
		if k == KindInvalid {
			t.Fatalf("AllKinds()[%d] = KindInvalid: the zero value is not a member of the closed set", i)
		}
		if seen[k] {
			t.Fatalf("AllKinds() yielded %v (%q) twice", k, k.String())
		}
		seen[k] = true
		if k.String() == "" {
			t.Fatalf("AllKinds()[%d] renders an empty name", i)
		}
		if i > 0 && kinds[i-1] >= k {
			t.Fatalf("AllKinds() out of declaration order at index %d: %v after %v", i, k, kinds[i-1])
		}
	}

	// ParseKind must invert the iteration for every member: the names the metrics
	// projection dispatches on are exactly the names the events table stores.
	for _, k := range kinds {
		back, err := ParseKind(k.String())
		if err != nil || back != k {
			t.Fatalf("ParseKind(%q) = %v, %v; want kind %d", k.String(), back, err, k)
		}
	}
}
