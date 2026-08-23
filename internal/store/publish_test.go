// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/queue"
)

func pub(subject string, body []byte) PublishCmd {
	return PublishCmd{Stream: "orders", Req: queue.PublishReq{Subject: subject, Body: body}}
}

func mustCreate(t *testing.T, st *Store, cfg queue.StreamConfig) {
	t.Helper()
	if _, _, err := st.CreateStream(context.Background(), cfg, "test"); err != nil {
		t.Fatalf("create %s: %v", cfg.Name, err)
	}
}

func TestPublishRoundTrip(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	mustCreate(t, st, queue.DefaultConfig("orders"))

	ack, err := st.Publish(ctx, PublishCmd{Stream: "orders", Req: queue.PublishReq{
		Subject: "orders.a",
		Body:    []byte(`{"id":40}`),
		Headers: map[string]string{"Tenant": "acme", "A-Header": "v"},
	}})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if ack.Stream != "orders" || ack.Seq != 1 || ack.Duplicate || ack.PublishedAt != fakeStartMillis {
		t.Errorf("ack = %+v", ack)
	}
	if len(ack.ID) != 26 { // ULID canonical form
		t.Errorf("id %q is not a 26-char ULID", ack.ID)
	}
	if len(ack.TraceID) != 32 { // minted hex
		t.Errorf("trace_id %q is not 32 hex bytes", ack.TraceID)
	}

	var hdr, subject2 string
	var size int64
	if err := st.ro.QueryRowContext(ctx,
		`SELECT subject, hdr, size FROM messages WHERE stream='orders' AND seq=1`,
	).Scan(&subject2, &hdr, &size); err != nil {
		t.Fatalf("read row: %v", err)
	}
	if subject2 != "orders.a" || size != int64(len(`{"id":40}`)) {
		t.Errorf("row = %q size=%d", subject2, size)
	}
	if hdr != `{"A-Header":"v","Tenant":"acme"}` { // canonical keys, sorted JSON
		t.Errorf("hdr = %s", hdr)
	}

	info, gErr := st.GetStream(ctx, "orders")
	if gErr != nil || info.Msgs != 1 || info.Bytes != size || info.FirstSeq != 1 ||
		info.LastSeq != 1 {
		t.Errorf("stats after publish = %+v err=%v", info, gErr)
	}
	var next int64
	if err := st.ro.QueryRowContext(ctx,
		`SELECT next FROM stream_seq WHERE stream='orders'`).Scan(&next); err != nil || next != 2 {
		t.Fatalf("stream_seq.next = %d err=%v, want 2", next, err)
	}

	var evSeq int64
	var detail string
	if err := st.ro.QueryRowContext(ctx, `
		SELECT seq, detail FROM events
		 WHERE event='msg.publish' AND stream='orders' AND msg_id=?`, ack.ID,
	).Scan(&evSeq, &detail); err != nil {
		t.Fatalf("event read: %v", err)
	}
	if evSeq != 1 {
		t.Errorf("event seq = %d, want 1", evSeq)
	}
	wantDetail := `{"size":9,"headers":2,"dedup":false}`
	if detail != wantDetail {
		t.Errorf("event detail = %s, want %s", detail, wantDetail)
	}

	// A second publish allocates the next sequence.
	ack2, pErr := st.Publish(ctx, pub("orders.b", []byte("x")))
	if pErr != nil || ack2.Seq != 2 {
		t.Fatalf("second publish = %+v err=%v, want seq 2", ack2, pErr)
	}
}

func TestPublishEmptyBodyStoresNullHeaders(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	mustCreate(t, st, queue.DefaultConfig("orders"))

	ack, err := st.Publish(ctx, pub("orders.a", nil))
	if err != nil {
		t.Fatalf("publish empty: %v", err)
	}
	var size int64
	var hdr *string
	var body []byte
	if err := st.ro.QueryRowContext(ctx,
		`SELECT size, hdr, body FROM messages WHERE seq=1`).Scan(&size, &hdr, &body); err != nil {
		t.Fatalf("read row: %v", err)
	}
	if size != 0 || hdr != nil || len(body) != 0 {
		t.Errorf("empty publish stored size=%d hdr=%v bodyLen=%d, want 0/NULL/0", size, hdr, len(body))
	}
	_ = ack
}

func TestPublishRejectionsAreTyped(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	cfg := queue.DefaultConfig("orders")
	cfg.Subjects = []string{"orders.*"} // wildcard pattern: only literal subjects match
	cfg.MaxMsgSize = 16
	mustCreate(t, st, cfg)

	_, err := st.Publish(ctx, pub("other.a", []byte("x")))
	var mm *queue.MismatchError
	if !errors.As(err, &mm) || !strings.Contains(err.Error(), "orders.*") {
		t.Fatalf("unmatched subject = %v, want MismatchError naming accepted patterns", err)
	}
	wantErrIs(t, err, errs.ErrBadSubject)

	wild := pub("orders.*", []byte("x")) // wildcards never publish
	if _, werr := st.Publish(ctx, wild); !errors.Is(werr, errs.ErrBadSubject) {
		t.Fatalf("wildcard publish = %v, want bad_subject", werr)
	}

	big := pub("orders.a", make([]byte, 17))
	_, err = st.Publish(ctx, big)
	var tl *queue.TooLargeError
	if !errors.As(err, &tl) || tl.Size != 17 || tl.Limit != 16 {
		t.Fatalf("oversized body = %v, want TooLargeError{17,16}", err)
	}
	wantErrIs(t, err, errs.ErrTooLarge)

	// Rejections allocate no sequence and store no rows (P1).
	var next int64
	if err := st.ro.QueryRowContext(ctx,
		`SELECT next FROM stream_seq WHERE stream='orders'`).Scan(&next); err != nil || next != 1 {
		t.Errorf("rejected publishes moved stream_seq to %d err=%v, want 1", next, err)
	}
	if n := countRows(t, st, `SELECT count(*) FROM messages`); n != 0 {
		t.Errorf("%d rows survived rejection", n)
	}

	if _, err := st.Publish(ctx, PublishCmd{
		Stream: "nope",
		Req:    queue.PublishReq{Subject: "a", Body: []byte("x")},
	}); !errors.Is(err, errs.ErrNotFound) {
		t.Fatalf("publish to missing stream = %v, want ErrNotFound", err)
	}
}

func TestPublishClampsBackwardsClock(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	mustCreate(t, st, queue.DefaultConfig("orders"))

	seedMessages(t, st, "orders", 1, 4)
	if _, err := st.rw.ExecContext(ctx,
		`UPDATE messages SET published_at = ?
		 WHERE stream='orders' AND seq=1`, fakeStartMillis+60_000); err != nil {
		t.Fatalf("rewind seed: %v", err)
	}
	// The seed bypasses the allocator, so move the counter past it by hand.
	if _, err := st.rw.ExecContext(ctx,
		`UPDATE stream_seq SET next = 2 WHERE stream='orders'`); err != nil {
		t.Fatalf("bump seq: %v", err)
	}

	ack, err := st.Publish(ctx, pub("orders.a", []byte("x")))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if ack.PublishedAt != fakeStartMillis+60_000 {
		t.Fatalf("published_at = %d, want clamped to %d", ack.PublishedAt, fakeStartMillis+60_000)
	}
	var detail string
	if err := st.ro.QueryRowContext(ctx,
		`SELECT detail FROM events WHERE event='msg.publish'`).Scan(&detail); err != nil {
		t.Fatalf("event: %v", err)
	}
	if !strings.Contains(detail, `"skew_ms":-60000`) {
		t.Errorf("clamped publish wrote detail %s, want the backwards jump recorded as skew_ms", detail)
	}
}

func TestPublishDedupHitAndGapFreeSeqs(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	mustCreate(t, st, queue.DefaultConfig("orders"))

	first := PublishCmd{Stream: "orders", Req: queue.PublishReq{
		Subject: "orders.a", Body: []byte("one"), MsgID: "o-40",
	}}
	ack1, err := st.Publish(ctx, first)
	if err != nil {
		t.Fatalf("first publish: %v", err)
	}

	dup, err := st.Publish(ctx, first)
	if err != nil {
		t.Fatalf("duplicate publish: %v", err)
	}
	if !dup.Duplicate || dup.Seq != ack1.Seq || dup.ID != ack1.ID ||
		dup.PublishedAt != ack1.PublishedAt {
		t.Fatalf("dup ack = %+v, want original %+v with duplicate=true", dup, ack1)
	}

	// Same key, different subject: still the original, flagged forensically.
	shifted := first
	shifted.Req.Subject = "orders.b"
	dup2, err := st.Publish(ctx, shifted)
	if err != nil || !dup2.Duplicate || dup2.Seq != ack1.Seq {
		t.Fatalf("cross-subject dup = %+v err=%v", dup2, err)
	}

	// No second row, no gap, exactly two msg.dup events.
	if n := countRows(t, st, `SELECT count(*) FROM messages`); n != 1 {
		t.Errorf("messages = %d, want 1", n)
	}
	var next int64
	if err := st.ro.QueryRowContext(ctx,
		`SELECT next FROM stream_seq WHERE stream='orders'`).Scan(&next); err != nil || next != 2 {
		t.Errorf("stream_seq.next = %d err=%v, want 2 (no gap)", next, err)
	}
	var details []string
	rows, qErr := st.ro.QueryContext(ctx,
		`SELECT detail FROM events WHERE event='msg.dup' ORDER BY id`)
	if qErr != nil {
		t.Fatalf("dup events: %v", qErr)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			t.Errorf("close dup rows: %v", cerr)
		}
	}()
	for rows.Next() {
		var d string
		if sErr := rows.Scan(&d); sErr != nil {
			t.Fatalf("scan dup detail: %v", sErr)
		}
		details = append(details, d)
	}
	if rErr := rows.Err(); rErr != nil {
		t.Fatalf("iterate dup events: %v", rErr)
	}
	if len(details) != 2 {
		t.Fatalf("msg.dup events = %d, want 2", len(details))
	}
	if !strings.Contains(details[0], `"original_seq":1`) ||
		strings.Contains(details[0], `"subject_differs":true`) {
		t.Errorf("plain dup detail = %s", details[0])
	}
	if !strings.Contains(details[1], `"subject_differs":true`) {
		t.Errorf("cross-subject dup detail = %s, want subject_differs:true", details[1])
	}
}

func TestPublishDedupWindowZeroDisablesDedup(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	cfg := queue.DefaultConfig("orders")
	cfg.DedupWindow = 0
	mustCreate(t, st, cfg)

	req := queue.PublishReq{Subject: "orders.a", Body: []byte("x"), MsgID: "o-40"}
	if _, err := st.Publish(ctx, PublishCmd{Stream: "orders", Req: req}); err != nil {
		t.Fatalf("first: %v", err)
	}
	ack2, err := st.Publish(ctx, PublishCmd{Stream: "orders", Req: req})
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if ack2.Duplicate || ack2.Seq != 2 {
		t.Fatalf("window=0 second ack = %+v, want fresh seq 2", ack2)
	}
	var keys int
	if err := st.ro.QueryRowContext(ctx,
		`SELECT count(*) FROM messages WHERE dedup_key IS NOT NULL`).Scan(&keys); err != nil || keys != 0 {
		t.Fatalf("non-NULL dedup keys = %d err=%v, want 0", keys, err)
	}
}

func TestPublishTraceIDPrecedence(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	mustCreate(t, st, queue.DefaultConfig("orders"))

	explicit := "4bf92f3577b34da6a3ce929d0e0e4736"
	ack, err := st.Publish(ctx, PublishCmd{Stream: "orders", Req: queue.PublishReq{
		Subject: "orders.a", Body: []byte("x"), TraceID: explicit,
	}})
	if err != nil || ack.TraceID != explicit {
		t.Fatalf("explicit trace = %q err=%v, want %q", ack.TraceID, err, explicit)
	}
}
