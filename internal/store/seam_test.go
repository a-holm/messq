// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/queue"
)

// The writer seam's refusals: a read-only store has no rw handle, so every
// state-changing command must fail with the shutdown sentinel instead of panicking
// on a nil handle.
func TestWriteCommandsRefuseOnReadOnlyStore(t *testing.T) {
	ctx := context.Background()
	dir := testDataDir(t)
	open := testOptions(dir, fakeClock(), &logCapture{})
	st, _, err := Open(ctx, open)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	mustCreate(t, st, queue.DefaultConfig("orders"))
	if cerr := st.Close(ctx); cerr != nil {
		t.Fatalf("close: %v", cerr)
	}

	roOpts := testOptions(dir, fakeClock(), &logCapture{})
	roOpts.ReadOnly = true
	roStore, _, oErr := Open(ctx, roOpts)
	if oErr != nil {
		t.Fatalf("reopen ro: %v", oErr)
	}
	defer func() {
		if cerr := roStore.Close(ctx); cerr != nil {
			t.Logf("close ro: %v", cerr)
		}
	}()

	_, pErr := roStore.Publish(ctx, pub("orders.a", []byte("x")))
	if !errors.Is(pErr, errs.ErrShuttingDown) || !strings.Contains(pErr.Error(), "read-write handle") {
		t.Errorf("ro publish = %v, want ErrShuttingDown naming the missing handle", pErr)
	}
	if _, _, cErr := roStore.CreateStream(ctx, queue.DefaultConfig("more"), "test"); !errors.Is(cErr, errs.ErrShuttingDown) {
		t.Errorf("ro create = %v, want ErrShuttingDown", cErr)
	}
	if _, uErr := roStore.UpdateStream(ctx, "orders", StreamPatch{MaxMsgs: i64(3)}, false, "t"); !errors.Is(uErr, errs.ErrShuttingDown) {
		t.Errorf("ro update = %v, want ErrShuttingDown", uErr)
	}
	if _, dErr := roStore.DeleteStream(ctx, "orders", "orders", "t"); !errors.Is(dErr, errs.ErrShuttingDown) {
		t.Errorf("ro delete = %v, want ErrShuttingDown", dErr)
	}
	if _, sErr := roStore.SweepDedup(ctx, "orders"); !errors.Is(sErr, errs.ErrShuttingDown) {
		t.Errorf("ro sweep = %v, want ErrShuttingDown", sErr)
	}
	// Reads keep working while writes refuse (issue §6).
	if _, gErr := roStore.GetStream(ctx, "orders"); gErr != nil {
		t.Errorf("ro get = %v, want reads alive", gErr)
	}
}

func TestUpdateStreamFullDiffNamesEveryField(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	mustCreate(t, st, queue.DefaultConfig("orders"))

	retention := queue.RetentionWorkQueue
	discard := queue.DiscardNew
	subjects := []string{"a.b", "c.d"}
	res, err := st.UpdateStream(ctx, "orders", StreamPatch{
		Subjects:      &subjects,
		Retention:     &retention,
		MaxMsgs:       i64(10),
		MaxBytes:      i64(1000),
		MaxAgeMS:      i64(5000),
		MaxMsgSize:    i64(2048),
		Discard:       &discard,
		DedupWindowMS: i64(0),
	}, true, "test") // allow_loss: workqueue + narrowed everything with rows absent is fine anyway
	if err != nil {
		t.Fatalf("full patch: %v", err)
	}
	want := []string{
		"subjects", "retention", "max_msgs", "max_bytes",
		"max_age", "max_msg_size", "discard", "dedup_window",
	}
	if !slices.Equal(res.Fields, want) {
		t.Errorf("fields = %v, want %v", res.Fields, want)
	}
	got, gErr := st.GetStream(ctx, "orders")
	if gErr != nil || got.MaxMsgs != 10 || got.MaxBytes != 1000 ||
		got.MaxAgeMS != 5000 || got.MaxMsgSize != 2048 ||
		string(got.Retention) != string(queue.RetentionWorkQueue) ||
		string(got.Discard) != string(queue.DiscardNew) ||
		got.DedupWindowMS != 0 || !slices.Equal(got.Subjects, []string{"a.b", "c.d"}) {
		t.Errorf("full patch persisted wrong: %+v err=%v", got, gErr)
	}
}

func TestPublishMissingSeqCounterIsNotFound(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	mustCreate(t, st, queue.DefaultConfig("orders"))
	if _, xErr := st.rw.ExecContext(ctx,
		`DELETE FROM stream_seq WHERE stream='orders'`); xErr != nil {
		t.Fatalf("drop counter: %v", xErr)
	}
	_, err := st.Publish(ctx, pub("orders.a", []byte("x")))
	wantErrIs(t, err, errs.ErrNotFound)
}
