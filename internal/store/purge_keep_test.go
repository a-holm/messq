// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"errors"
	"testing"

	"github.com/a-holm/messq/internal/errs"
)

// Regression guard for the keep-only purge scan path (issue #15 §5): a PurgeSpec with
// Keep > 0 and no subject filter is the documented "keep the newest K, delete the
// rest" shape, and it routes to the scan path with a nil pattern. The pattern match
// must degrade to "every row is a candidate", not panic on the nil receiver.
func TestPurgeKeepWithoutSubjectKeepsNewestTail(t *testing.T) {
	st := newConsumerStream(t)
	publishSubjs(t, st, "orders.a", "orders.b", "orders.c", "orders.d", "orders.e")
	ctx := context.Background()

	dry, err := st.Purge(ctx, "orders", PurgeSpec{Keep: 2}, true, "test")
	if err != nil {
		t.Fatalf("dry keep purge: %v", err)
	}
	if dry.Impact.Messages != 3 {
		t.Fatalf("dry impact messages = %d, want 3 (newest two kept)", dry.Impact.Messages)
	}
	if n := countEvent(t, st, "stream.purge"); n != 0 {
		t.Fatalf("dry run wrote %d purge events, want 0", n)
	}

	res, err := st.Purge(ctx, "orders", PurgeSpec{Keep: 2}, false, "test")
	if err != nil {
		t.Fatalf("keep purge: %v", err)
	}
	if res.Impact.Messages != 3 {
		t.Fatalf("impact messages = %d, want 3", res.Impact.Messages)
	}
	if res.Impact.FirstSeqAfter != 4 {
		t.Fatalf("first seq after = %d, want 4 (kept tail starts at 4)", res.Impact.FirstSeqAfter)
	}
	for _, seq := range []int64{1, 2, 3} {
		if _, err := st.PeekSeq(ctx, "orders", seq); !errors.Is(err, errs.ErrNotFound) {
			t.Fatalf("peek seq %d after keep-purge = %v, want ErrNotFound", seq, err)
		}
	}
	for _, seq := range []int64{4, 5} {
		if _, err := st.PeekSeq(ctx, "orders", seq); err != nil {
			t.Fatalf("kept seq %d must survive: %v", seq, err)
		}
	}
}

// Keep at or above the existing count selects nothing: nothing to delete is a clean
// zero-impact answer on both the dry and the real path.
func TestPurgeKeepMeetsOrExceedsExisting(t *testing.T) {
	st := newConsumerStream(t)
	publishSubjs(t, st, "orders.a", "orders.b")
	ctx := context.Background()

	dry, err := st.Purge(ctx, "orders", PurgeSpec{Keep: 5}, true, "test")
	if err != nil {
		t.Fatalf("dry keep-all purge: %v", err)
	}
	if dry.Impact.Messages != 0 {
		t.Fatalf("dry impact messages = %d, want 0", dry.Impact.Messages)
	}
	if _, err := st.Purge(ctx, "orders", PurgeSpec{Keep: 5}, false, "test"); err != nil {
		t.Fatalf("keep-all purge: %v", err)
	}
	for _, seq := range []int64{1, 2} {
		if _, err := st.PeekSeq(ctx, "orders", seq); err != nil {
			t.Fatalf("seq %d must survive a keep-all purge: %v", seq, err)
		}
	}
	if n := countEvent(t, st, "stream.purge"); n != 0 {
		t.Fatalf("no-op purges wrote %d events, want 0", n)
	}
}
