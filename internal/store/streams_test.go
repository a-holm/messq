// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/queue"
)

// openWithStore opens a fresh store wired to a fake clock and returns it with its dir.
func openWithStore(t *testing.T) (*Store, string) {
	t.Helper()
	dir := testDataDir(t)
	st, _, err := Open(context.Background(), testOptions(dir, fakeClock(), &logCapture{}))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if cerr := st.Close(context.Background()); cerr != nil {
			t.Logf("close: %v", cerr)
		}
	})
	return st, dir
}

func wantErrIs(t *testing.T, err, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("err = %v, want %v", err, target)
	}
}

func TestCreateStreamRoundTrip(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)

	cfg := queue.DefaultConfig("orders")
	info, existed, err := st.CreateStream(ctx, cfg, "test")
	if err != nil || existed {
		t.Fatalf("create = %v, existed=%v, want fresh", err, existed)
	}
	if info.Name != "orders" || info.Msgs != 0 || info.Bytes != 0 ||
		info.FirstSeq != 0 || info.LastSeq != 0 {
		t.Errorf("fresh info = %+v", info)
	}
	if info.CreatedAt != fakeStartMillis {
		t.Errorf("CreatedAt = %d, want %d", info.CreatedAt, fakeStartMillis)
	}

	got, err := st.GetStream(ctx, "orders")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Name != info.Name || got.MaxMsgSize != info.MaxMsgSize ||
		got.DedupWindowMS != cfg.DedupWindow.Milliseconds() {
		t.Errorf("round trip mismatch: %+v vs %+v", got, info)
	}
}

func TestCreateStreamIdempotentAndConflict(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)

	cfg := queue.DefaultConfig("orders")
	if _, _, err := st.CreateStream(ctx, cfg, "test"); err != nil {
		t.Fatalf("first create: %v", err)
	}

	identical, existed, err := st.CreateStream(ctx, queue.DefaultConfig("orders"), "test")
	if err != nil || !existed {
		t.Fatalf("re-create = %v, existed=%v, want idempotent success", err, existed)
	}
	if identical.Name != "orders" {
		t.Errorf("re-create info = %+v", identical)
	}

	other := queue.DefaultConfig("orders")
	other.MaxMsgSize = 4096
	_, _, err = st.CreateStream(ctx, other, "test")
	var see *StreamExistsError
	if !errors.As(err, &see) {
		t.Fatalf("different config = %v, want StreamExistsError", err)
	}
	if !slices.Contains(see.Diff, "max_msg_size") {
		t.Errorf("diff = %v, want max_msg_size named", see.Diff)
	}
	wantErrIs(t, err, errs.ErrConflict)

	caseClash := queue.DefaultConfig("ORDERS")
	_, _, err = st.CreateStream(ctx, caseClash, "test")
	var ncc *NameCaseCollisionError
	if !errors.As(err, &ncc) || ncc.Existing != "orders" {
		t.Fatalf("case clash = %v, want NameCaseCollisionError{orders}", err)
	}
}

func TestCreateStreamValidationRefusalsLeaveNoRows(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)

	reserved := queue.DefaultConfig("orders.dlq")
	if _, _, err := st.CreateStream(ctx, reserved, "test"); !errors.Is(err, queue.ErrReservedName) {
		t.Fatalf(".dlq create = %v, want ErrReservedName", err)
	}
	bad := queue.DefaultConfig("ok")
	bad.Subjects = []string{"a..b"}
	if _, _, err := st.CreateStream(ctx, bad, "test"); !errors.Is(err, errs.ErrBadSubject) {
		t.Fatalf("bad pattern = %v, want bad subject", err)
	}

	list, err := st.ListStreams(ctx)
	if err != nil || len(list) != 0 {
		t.Fatalf("refused creates leaked rows: list=%v err=%v", list, err)
	}
}

func TestListStreamsSorted(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	for _, name := range []string{"zeta", "alpha", "mid"} {
		if _, _, err := st.CreateStream(ctx, queue.DefaultConfig(name), "test"); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}
	list, err := st.ListStreams(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	want := []string{"alpha", "mid", "zeta"}
	if len(list) != len(want) {
		t.Fatalf("list = %d streams, want %d", len(list), len(want))
	}
	for i, si := range list {
		if si.Name != want[i] {
			t.Errorf("list[%d] = %q, want %q", i, si.Name, want[i])
		}
	}
}

func TestGetStreamMissingIsTypedNotFound(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	_, err := st.GetStream(ctx, "nope")
	wantErrIs(t, err, errs.ErrNotFound)
}

func TestCreateStreamWritesEventRow(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	if _, _, err := st.CreateStream(ctx, queue.DefaultConfig("orders"), "tester"); err != nil {
		t.Fatalf("create: %v", err)
	}
	var n int
	if err := st.ro.QueryRowContext(ctx,
		`SELECT count(*) FROM events WHERE event='stream.create' AND stream='orders' AND actor='tester'`,
	).Scan(&n); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if n != 1 {
		t.Fatalf("stream.create event rows = %d, want 1", n)
	}
}
