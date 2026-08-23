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

func TestPublishBatchContiguousAndOrdered(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	mustCreate(t, st, queue.DefaultConfig("orders"))

	batch := BatchCmd{Stream: "orders", Reqs: []queue.PublishReq{
		{Subject: "orders.b", Body: []byte("second")},
		{Subject: "orders.a", Body: []byte("first")},
		{Subject: "orders.c", Body: nil},
	}}
	ack, err := st.PublishBatch(ctx, batch)
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if ack.Stream != "orders" || len(ack.Results) != 3 {
		t.Fatalf("ack = %+v", ack)
	}
	for i, want := range []int64{1, 2, 3} {
		if got := ack.Results[i].Seq; got != want {
			t.Errorf("results[%d].seq = %d, want %d", i, got, want)
		}
		if dup := ack.Results[i].Duplicate; dup {
			t.Errorf("results[%d] marked duplicate", i)
		}
	}
	if subj := ack.Results[1].Seq; true { // input order kept regardless of subject sort
		rowSubj := ""
		if err := st.ro.QueryRowContext(ctx,
			`SELECT subject FROM messages WHERE stream='orders' AND seq=?`,
			subj).Scan(&rowSubj); err != nil || rowSubj != "orders.a" {
			t.Errorf("seq 2 holds %q err=%v, want orders.a", rowSubj, err)
		}
	}
	info, gErr := st.GetStream(ctx, "orders")
	if gErr != nil || info.Msgs != 3 || info.Bytes != 11 { // 6+5+0 bodies, empty legal
		t.Errorf("stats after batch = %+v err=%v", info, gErr)
	}
}

func TestPublishBatchAllOrNothingNamesTheLine(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	mustCreate(t, st, queue.DefaultConfig("orders"))
	if _, _, err := st.CreateStream(ctx, queue.DefaultConfig("other"), "test"); err != nil {
		t.Fatalf("create other: %v", err)
	}

	batch := BatchCmd{Stream: "orders", Reqs: []queue.PublishReq{
		{Subject: "orders.a", Body: []byte("ok")},
		{Subject: "orders.*", Body: []byte("wildcards never publish")}, // line 2
		{Subject: "orders.a", Body: []byte("never reached")},
	}}
	before := dumpCounts(t, st)
	_, err := st.PublishBatch(ctx, batch)
	wantErrIs(t, err, errs.ErrBadSubject)
	if !strings.Contains(err.Error(), "line 2") {
		t.Errorf("error %q does not name line 2", err.Error())
	}
	after := dumpCounts(t, st)
	for table, n := range before {
		if after[table] != n {
			t.Errorf("failed batch mutated %s: %d → %d", table, n, after[table])
		}
	}
}

func TestPublishBatchDuplicateInsideBatch(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	mustCreate(t, st, queue.DefaultConfig("orders"))

	ack, err := st.PublishBatch(ctx, BatchCmd{Stream: "orders", Reqs: []queue.PublishReq{
		{Subject: "orders.a", Body: []byte("x"), MsgID: "o-40"},
		{Subject: "orders.a", Body: []byte("y"), MsgID: "o-40"}, // retry mid-batch
	}})
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	first, second := ack.Results[0], ack.Results[1]
	if first.Duplicate || second.Seq != first.Seq || !second.Duplicate {
		t.Fatalf("results = %+v / %+v, want second a duplicate of first", first, second)
	}
	if n := countRows(t, st, `SELECT count(*) FROM messages`); n != 1 {
		t.Errorf("messages = %d, want 1", n)
	}
	var next int64
	if err := st.ro.QueryRowContext(ctx,
		`SELECT next FROM stream_seq WHERE stream='orders'`).Scan(&next); err != nil || next != 2 {
		t.Errorf("stream_seq.next = %d err=%v, want 2 (no gap)", next, err)
	}
	if dups := countRows(t, st, `SELECT count(*) FROM events WHERE event='msg.dup'`); dups != 1 {
		t.Errorf("msg.dup events = %d, want 1", dups)
	}
}

func TestPublishBatchBounds(t *testing.T) {
	ctx := context.Background()
	dir := testDataDir(t)
	opt := testOptions(dir, fakeClock(), &logCapture{})
	opt.MaxBatchMessages = 4
	st, _, oErr := Open(ctx, opt)
	if oErr != nil {
		t.Fatalf("open: %v", oErr)
	}
	defer func() {
		if cerr := st.Close(ctx); cerr != nil {
			t.Logf("close: %v", cerr)
		}
	}()
	mustCreate(t, st, queue.DefaultConfig("orders"))

	if _, err := st.PublishBatch(ctx, BatchCmd{Stream: "orders"}); !errors.Is(err, errs.ErrBadRequest) {
		t.Fatalf("empty batch = %v, want bad_request", err)
	}
	five := make([]queue.PublishReq, 5)
	for i := range five {
		five[i] = queue.PublishReq{Subject: "orders.a", Body: []byte("x")}
	}
	_, err := st.PublishBatch(ctx, BatchCmd{Stream: "orders", Reqs: five})
	if !errors.Is(err, errs.ErrBadRequest) || !strings.Contains(err.Error(), "4") {
		t.Fatalf("oversized batch = %v, want bad_request naming the limit", err)
	}
}

func TestCmdBytesEstimatesFeedBudget(t *testing.T) {
	single := PublishCmd{Req: queue.PublishReq{
		Body:    []byte(strings.Repeat("b", 1000)),
		Headers: map[string]string{"A": strings.Repeat("h", 50)},
	}}
	if got := single.Bytes(); got < 1050 || got > 1400 {
		t.Errorf("PublishCmd.Bytes() = %d, want body+headers+overhead", got)
	}
	batch := BatchCmd{Reqs: []queue.PublishReq{
		{Body: make([]byte, 10)},
		{Body: make([]byte, 20)},
	}}
	if got, want := batch.Bytes(), single.Bytes()*0+30+512; got < want-200 || got > want+200 {
		t.Errorf("BatchCmd.Bytes() = %d, want roughly the summed bodies plus overhead (~%d)", got, want)
	}
	var metaOnly BatchCmd // stream CRUD commands report zero cost
	if metaOnly.Bytes() != 0 {
		t.Errorf("metadata-only Bytes() = %d, want 0", metaOnly.Bytes())
	}
}
