// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"errors"
	"testing"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/queue"
)

// seedMessages writes message rows directly (white-box): UpdateStream's data-loss
// measurement and subjects-narrowing count read whatever is stored, independent of
// how it got there — the publish path does not exist yet.
func seedMessages(t *testing.T, st *Store, stream string, n int, size int64) {
	t.Helper()
	ctx := context.Background()
	for i := 1; i <= n; i++ {
		if _, err := st.rw.ExecContext(ctx,
			`INSERT INTO messages (stream, seq, id, subject, body, size, published_at, trace_id)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			stream, i, "id-"+stream+"-"+string(rune('0'+i)), "orders.a", []byte("x"), size,
			fakeStartMillis+int64(i)*1000, "trace"); err != nil {
			t.Fatalf("seed message %d: %v", i, err)
		}
	}
	if _, err := st.rw.ExecContext(ctx,
		`INSERT INTO stream_stats (stream, msgs, bytes) VALUES (?, ?, ?)
		 ON CONFLICT (stream) DO UPDATE SET msgs = excluded.msgs, bytes = excluded.bytes`,
		stream, n, int64(n)*size); err != nil {
		t.Fatalf("seed stats: %v", err)
	}
}

func i64(v int64) *int64 { return &v }

func TestUpdateStreamSparsePatch(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	cfg := queue.DefaultConfig("orders")
	cfg.MaxMsgSize = 1 << 20
	if _, _, err := st.CreateStream(ctx, cfg, "test"); err != nil {
		t.Fatalf("create: %v", err)
	}

	res, err := st.UpdateStream(ctx, "orders", StreamPatch{MaxMsgSize: i64(262144)}, false, "tester")
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if res.Info.MaxMsgSize != 262144 {
		t.Errorf("MaxMsgSize = %d, want 262144", res.Info.MaxMsgSize)
	}
	// Sparse: everything absent keeps its stored value.
	if res.Info.MaxAgeMS != cfg.MaxAge.Milliseconds() || res.Info.DedupWindowMS != cfg.DedupWindow.Milliseconds() ||
		res.Info.Retention != string(queue.RetentionLimits) || res.Info.Discard != string(queue.DiscardOld) {
		t.Errorf("absent fields drifted: %+v", res.Info)
	}
	got, gErr := st.GetStream(ctx, "orders")
	if gErr != nil || got.MaxMsgSize != 262144 {
		t.Errorf("persisted MaxMsgSize = %+v err=%v, want 262144", got, gErr)
	}
	var fields string
	if err := st.ro.QueryRowContext(ctx,
		`SELECT detail FROM events WHERE event='stream.update' AND stream='orders'`).Scan(&fields); err != nil {
		t.Fatalf("update event: %v", err)
	}
	want := `{"fields":["max_msg_size"]}`
	if fields != want {
		t.Errorf("event detail = %s, want %s", fields, want)
	}
}

func TestUpdateStreamWouldLoseData(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	if _, _, err := st.CreateStream(ctx, queue.DefaultConfig("orders"), "test"); err != nil {
		t.Fatalf("create: %v", err)
	}
	// 10 messages x 100 bytes, all published "now" per the fake clock.
	seedMessages(t, st, "orders", 10, 100)

	patch := StreamPatch{MaxMsgs: i64(5)}
	_, err := st.UpdateStream(ctx, "orders", patch, false, "test")
	var wld *queue.WouldLoseDataError
	if !errors.As(err, &wld) || wld.Field != "max_msgs" {
		t.Fatalf("lowering max_msgs = %v, want WouldLoseDataError{max_msgs}", err)
	}
	if wld.AtRiskMsgs != 5 || wld.AtRiskBytes != 500 {
		t.Errorf("at risk = %d msgs / %d bytes, want 5 / 500", wld.AtRiskMsgs, wld.AtRiskBytes)
	}
	wantErrIs(t, err, errs.ErrConflict)

	// Refused update leaves the stored config alone.
	got, gErr := st.GetStream(ctx, "orders")
	if gErr != nil || got.MaxMsgs != 0 {
		t.Fatalf("refused update leaked: %+v err=%v", got, gErr)
	}

	// allow_data_loss applies it.
	res, err := st.UpdateStream(ctx, "orders", patch, true, "test")
	if err != nil || res.Info.MaxMsgs != 5 {
		t.Fatalf("allow_loss update = %+v err=%v, want max_msgs=5", res, err)
	}

	// Shortening max_age below the oldest stored message refuses likewise.
	shortAge := StreamPatch{MaxAgeMS: i64(5_000)} // rows are 0..9 s old
	_, err = st.UpdateStream(ctx, "orders", shortAge, false, "test")
	if !errors.As(err, &wld) || wld.Field != "max_age" {
		t.Fatalf("shortening max_age = %v, want WouldLoseDataError{max_age}", err)
	}

	// limits → workqueue with stored rows refuses.
	wq := queue.RetentionWorkQueue
	_, err = st.UpdateStream(ctx, "orders", StreamPatch{Retention: &wq}, false, "test")
	if !errors.As(err, &wld) || wld.Field != "retention" {
		t.Fatalf("limits→workqueue = %v, want WouldLoseDataError{retention}", err)
	}
}

func TestUpdateStreamSubjectsNarrowingReportsMismatch(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	cfg := queue.DefaultConfig("orders")
	cfg.Subjects = []string{"orders.>"}
	if _, _, err := st.CreateStream(ctx, cfg, "test"); err != nil {
		t.Fatalf("create: %v", err)
	}
	seedMessages(t, st, "orders", 3, 10)
	// One row outside the future filter.
	if _, err := st.rw.ExecContext(ctx,
		`UPDATE messages SET subject='other.b' WHERE seq=3`); err != nil {
		t.Fatalf("relabel: %v", err)
	}

	narrowed := []string{"other.>"}
	res, err := st.UpdateStream(ctx, "orders", StreamPatch{Subjects: &narrowed}, false, "test")
	if err != nil {
		t.Fatalf("narrowing must not refuse, got %v", err)
	}
	if res.NarrowedMsgs != 2 {
		t.Errorf("NarrowedMsgs = %d, want 2", res.NarrowedMsgs)
	}
}

func TestUpdateStreamValidationAndMissing(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	if _, _, err := st.CreateStream(ctx, queue.DefaultConfig("orders"), "test"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := st.UpdateStream(ctx, "nope", StreamPatch{MaxMsgs: i64(9)}, false, "test"); !errors.Is(err, errs.ErrNotFound) {
		t.Fatalf("missing stream = %v, want ErrNotFound", err)
	}
	tooBig := i64(int64(1)<<23 + 1) // above the 8 MiB ceiling
	if _, err := st.UpdateStream(ctx, "orders", StreamPatch{MaxMsgSize: tooBig}, false, "test"); !errors.Is(err, errs.ErrBadRequest) {
		t.Fatalf("over-ceiling patch = %v, want bad_request", err)
	}
	// An empty patch is a no-op that writes no event row.
	res, err := st.UpdateStream(ctx, "orders", StreamPatch{}, false, "test")
	if err != nil {
		t.Fatalf("empty patch: %v", err)
	}
	if res.NarrowedMsgs != 0 {
		t.Errorf("empty patch narrowed = %d, want 0", res.NarrowedMsgs)
	}
	var n int
	if err := st.ro.QueryRowContext(ctx,
		`SELECT count(*) FROM events WHERE event='stream.update'`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("empty patch wrote %d events (err=%v), want 0", n, err)
	}
}
