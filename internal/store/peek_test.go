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

func TestPeekSeqRoundTripByteIdentical(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	mustCreate(t, st, queue.DefaultConfig("orders"))

	body := []byte{0x00, 0x01, 0xff, 0xfe, 'x'} // NUL + invalid-UTF-8 bytes
	ack, err := st.Publish(ctx, PublishCmd{Stream: "orders", Req: queue.PublishReq{
		Subject: "orders.a", Body: body,
		Headers: map[string]string{"Content-Type": "application/octet-stream"},
	}})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	before := dumpCounts(t, st)
	msg, err := st.PeekSeq(ctx, "orders", ack.Seq)
	if err != nil {
		t.Fatalf("peek: %v", err)
	}
	if !slices.Equal(msg.Body, body) {
		t.Errorf("body = %v, want byte-identical %v", msg.Body, body)
	}
	if msg.Headers["Content-Type"] != "application/octet-stream" || len(msg.Headers) != 1 {
		t.Errorf("headers = %v", msg.Headers)
	}
	if msg.Stream != "orders" || msg.Seq != ack.Seq || msg.ID != ack.ID ||
		msg.Subject != "orders.a" || msg.Size != int64(len(body)) ||
		msg.PublishedAt != fakeStartMillis || msg.TraceID != ack.TraceID {
		t.Errorf("message fields drifted: %+v", msg)
	}
	after := dumpCounts(t, st)
	for table, n := range before {
		if after[table] != n {
			t.Errorf("peek mutated %s: %d → %d", table, n, after[table])
		}
	}
}

// dumpCounts snapshots the row count of every table (the side-effect-free check).
func dumpCounts(t *testing.T, st *Store) map[string]int {
	t.Helper()
	out := map[string]int{}
	for _, table := range []string{"messages", "events", "stream_stats", "stream_seq", "meta"} {
		out[table] = countRows(t, st, `SELECT count(*) FROM `+table)
	}
	return out
}

func TestPeekSeqMissesExplainThemselves(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	mustCreate(t, st, queue.DefaultConfig("orders"))

	_, err := st.PeekSeq(ctx, "orders", 5) // nothing ever published
	var miss *PeekMissError
	if !errors.As(err, &miss) || miss.Reason != "never_published" || miss.Boundary != 0 {
		t.Fatalf("empty peek = %v (%+v), want never_published boundary 0", err, miss)
	}
	wantErrIs(t, err, errs.ErrNotFound)
	if msg := miss.Error(); !strings.Contains(msg, "never_published") || !strings.Contains(msg, "0") {
		t.Errorf("PeekMissError.Error() = %q, want reason and boundary rendered", msg)
	}

	seedMessages(t, st, "orders", 3, 4)
	// Retire the two oldest rows the way retention will; first_seq becomes 3.
	if _, dErr := st.rw.ExecContext(ctx,
		`DELETE FROM messages WHERE seq <= 2`); dErr != nil {
		t.Fatalf("retire rows: %v", dErr)
	}
	_, err = st.PeekSeq(ctx, "orders", 2)
	if !errors.As(err, &miss) || miss.Reason != "expired" || miss.Boundary != 3 {
		t.Fatalf("expired peek = %v (%+v), want expired boundary 3", err, miss)
	}

	if _, err := st.PeekSeq(ctx, "nope", 1); !errors.Is(err, errs.ErrNotFound) {
		t.Fatalf("missing stream = %v, want ErrNotFound", err)
	}
}

func TestPeekSeqCorruptHeaderIsReported(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	mustCreate(t, st, queue.DefaultConfig("orders"))
	ack, pErr := st.Publish(ctx, pub("orders.a", []byte("x")))
	if pErr != nil {
		t.Fatalf("publish: %v", pErr)
	}
	if _, uErr := st.rw.ExecContext(ctx,
		`UPDATE messages SET hdr='{bad json' WHERE seq=?`, ack.Seq); uErr != nil {
		t.Fatalf("corrupt hdr: %v", uErr)
	}
	_, err := st.PeekSeq(ctx, "orders", ack.Seq)
	if err == nil || !strings.Contains(err.Error(), "header JSON") {
		t.Fatalf("corrupt hdr peek = %v, want reported corruption", err)
	}
}

func TestPeekIDFindsAcrossStreamsAndJoinsOnExistence(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	mustCreate(t, st, queue.DefaultConfig("orders"))
	mustCreate(t, st, queue.DefaultConfig("billing"))

	ackA, pErr := st.Publish(ctx, PublishCmd{
		Stream: "orders",
		Req:    queue.PublishReq{Subject: "orders.a", Body: []byte("a")},
	})
	if pErr != nil {
		t.Fatalf("publish a: %v", pErr)
	}
	ackB, pErr := st.Publish(ctx, PublishCmd{
		Stream: "billing",
		Req:    queue.PublishReq{Subject: "billing.a", Body: []byte("b")},
	})
	if pErr != nil {
		t.Fatalf("publish b: %v", pErr)
	}

	for _, ack := range []Ack{ackA, ackB} {
		msg, err := st.PeekID(ctx, ack.ID)
		if err != nil {
			t.Fatalf("peek %s: %v", ack.ID, err)
		}
		if msg.ID != ack.ID || msg.Body == nil {
			t.Errorf("peek by id = %+v, want body included", msg)
		}
	}

	// An orphan row whose stream is gone is invisible to reads.
	if _, xErr := st.rw.ExecContext(ctx,
		`DELETE FROM streams WHERE name='billing'`); xErr != nil {
		t.Fatalf("drop stream row: %v", xErr)
	}
	if _, err := st.PeekID(ctx, ackB.ID); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("orphan peek = %v, want ErrNotFound", err)
	}

	if _, err := st.PeekID(ctx, "01JZZZZZZZZZZZZZZZZZZZZZZZ"); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("unknown id = %v, want ErrNotFound", err)
	}
}

func TestListMessagesRangeClampAndResume(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	mustCreate(t, st, queue.DefaultConfig("orders"))
	for i := 1; i <= 7; i++ {
		if _, err := st.Publish(ctx, pub("orders.a", []byte{byte('0' + i)})); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	page, err := st.ListMessages(ctx, ListQuery{Stream: "orders", Limit: 3})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(page.Messages) != 3 || !page.Complete || page.Limit != 3 ||
		page.Messages[0].Seq != 1 || page.Messages[2].Seq != 3 {
		t.Fatalf("page = %+v", page)
	}
	if page.Messages[0].Body != nil {
		t.Errorf("listing carried bodies by default (I11)")
	}
	if page.Messages[0].Size != 1 {
		t.Errorf("metadata size = %d, want 1", page.Messages[0].Size)
	}

	resume, err := st.ListMessages(ctx, ListQuery{Stream: "orders", FromSeq: 6, Limit: 10})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if len(resume.Messages) != 2 || resume.Messages[0].Seq != 6 || !resume.Complete {
		t.Fatalf("resume page = %+v", resume)
	}

	clamped, err := st.ListMessages(ctx, ListQuery{Stream: "orders", Limit: 50_000})
	if err != nil || clamped.Limit != 1000 {
		t.Fatalf("clamp = limit %d err=%v, want effective 1000", clamped.Limit, err)
	}

	withBody, err := st.ListMessages(ctx, ListQuery{Stream: "orders", Limit: 500, IncludeBody: true})
	if err != nil || withBody.Limit != 100 { // body pages clamp harder
		t.Fatalf("body clamp = limit %d err=%v, want 100", withBody.Limit, err)
	}
	if len(withBody.Messages) == 0 || len(withBody.Messages[0].Body) != 1 {
		t.Fatalf("body page = %+v, want bodies carried", withBody.Messages)
	}
	// A listed row WITH headers exercises the parsed-header scan of listings.
	hdrAck, hErr := st.Publish(ctx, PublishCmd{
		Stream: "orders",
		Req: queue.PublishReq{
			Subject: "orders.a", Body: []byte("h"),
			Headers: map[string]string{"Tenant": "acme"},
		},
	})
	if hErr != nil {
		t.Fatalf("publish hdr: %v", hErr)
	}
	listed, lErr := st.ListMessages(ctx, ListQuery{
		Stream:  "orders",
		FromSeq: hdrAck.Seq, Limit: 5,
	})
	if lErr != nil || len(listed.Messages) != 1 ||
		listed.Messages[0].Headers["Tenant"] != "acme" {
		t.Fatalf("listed headers = %+v err=%v, want Tenant parsed", listed.Messages, lErr)
	}
}

func TestListMessagesDescFromHead(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	mustCreate(t, st, queue.DefaultConfig("orders"))
	for i := 1; i <= 5; i++ {
		if _, err := st.Publish(ctx, pub("orders.a", []byte("x"))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	page, err := st.ListMessages(ctx, ListQuery{Stream: "orders", Limit: 2, Order: "desc"})
	if err != nil {
		t.Fatalf("desc list: %v", err)
	}
	got := []int64{page.Messages[0].Seq, page.Messages[1].Seq}
	want := []int64{5, 4}
	if !slices.Equal(got, want) || !page.Complete {
		t.Fatalf("desc page seqs = %v complete=%v, want %v", got, page.Complete, want)
	}
}

func TestListMessagesWildcardScanIsBoundedAndHonest(t *testing.T) {
	ctx := context.Background()
	dir := testDataDir(t)
	opt := testOptions(dir, fakeClock(), &logCapture{})
	opt.PeekScanLimit = 4 // shrink the bound so the partial case is reachable
	st, _, oErr := Open(ctx, opt)
	if oErr != nil {
		t.Fatalf("open: %v", oErr)
	}
	defer func() {
		if cerr := st.Close(ctx); cerr != nil {
			t.Logf("close: %v", cerr)
		}
	}()
	cfg := queue.DefaultConfig("orders")
	mustCreate(t, st, cfg)
	// Three matching rows first, then five others: a full page fits inside the
	// shrunken bound, while an oversized request starves honestly.
	subjects := []string{
		"orders.a", "orders.b", "orders.c",
		"other.z", "other.z", "other.z", "other.z", "other.z",
	}
	for _, s := range subjects {
		req := pub(s, []byte("x"))
		if _, err := st.Publish(ctx, req); err != nil {
			t.Fatalf("publish %s: %v", s, err)
		}
	}

	// Full page fits inside the scan bound: complete.
	page, err := st.ListMessages(ctx, ListQuery{
		Stream:  "orders",
		Subject: "orders.*", Limit: 3,
	})
	if err != nil {
		t.Fatalf("wildcard list: %v", err)
	}
	if len(page.Messages) != 3 || !page.Complete {
		t.Fatalf("page = %d msgs complete=%v, want 3/true", len(page.Messages), page.Complete)
	}
	for i, m := range page.Messages {
		if want := int64(1 + i); m.Seq != want { // rows 1..3 hold orders.*
			t.Errorf("page[%d].seq = %d, want %d", i, m.Seq, want)
		}
	}
	if page.ScannedToSeq != 0 {
		t.Errorf("complete page reported scanned_to_seq=%d, want 0", page.ScannedToSeq)
	}

	// Unfilled page that exhausts its scan bound: honest partial answer.
	partial, err := st.ListMessages(ctx, ListQuery{
		Stream:  "orders",
		Subject: "orders.*", Limit: 10,
	})
	if err != nil {
		t.Fatalf("partial list: %v", err)
	}
	if partial.Complete || partial.ScannedToSeq != 4 || len(partial.Messages) != 3 {
		t.Fatalf("partial = complete=%v scanned_to=%d msgs=%d, want false/4/3",
			partial.Complete, partial.ScannedToSeq, len(partial.Messages))
	}

	// Resuming past the reported point sees the tail and reports it completely.
	resumed, err := st.ListMessages(ctx, ListQuery{
		Stream:  "orders",
		Subject: "orders.*", Limit: 10, FromSeq: partial.ScannedToSeq + 1,
	})
	if err != nil {
		t.Fatalf("resumed list: %v", err)
	}
	if len(resumed.Messages) != 0 || !resumed.Complete {
		t.Fatalf("resumed = %+v, want empty and complete", resumed)
	}

	// A literal filter takes the messages_subj index path and returns the same row.
	literal, lErr := st.ListMessages(ctx, ListQuery{
		Stream:  "orders",
		Subject: "orders.b", Limit: 10,
	})
	if lErr != nil || len(literal.Messages) != 1 || !literal.Complete ||
		literal.Messages[0].Seq != 2 {
		t.Fatalf("literal list = %+v err=%v, want exactly seq 2", literal, lErr)
	}
}
