// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"testing"
)

// The read layer of issue #20 slice 1: Events(EventFilter) over the fenced read pool
// with honest pagination (AfterID is an EXCLUSIVE cursor in scan direction), the
// MaxEventID/EventHorizon journal probes, and the shared `since` parser. Every test
// seeds the events table directly through the rw handle — the read side must work on
// whatever rows are durable, independent of which command produced them.

// evRow is one seeded audit row. Empty strings become SQL NULL via the production
// nullStr/nullI64 helpers, matching what insertEvent writes for optional fields.
type evRow struct {
	id, ts                    int64
	name                      string
	stream, consumer, subject string
	msgID, traceID            string
	seq, attempt              int64
	actor, detail             string
}

func seedEvents(t *testing.T, st *Store, rows []evRow) {
	t.Helper()
	// Open itself commits audit rows (startup/recovery); the read layer must work on
	// whatever rows are durable, so these tests reset to a known journal first.
	clearEvents(t, st)
	const q = `INSERT INTO events
		(id, ts, event, stream, consumer, subject, msg_id, seq, attempt, trace_id, actor, detail)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	for _, r := range rows {
		if _, err := st.rw.ExecContext(context.Background(), q,
			r.id, r.ts, r.name, nullStr(r.stream), nullStr(r.consumer),
			nullStr(r.subject), nullStr(r.msgID), nullI64(r.seq),
			nullI64(r.attempt), nullStr(r.traceID), nullStr(r.actor),
			nullStr(r.detail)); err != nil {
			t.Fatalf("seed event row %d: %v", r.id, err)
		}
	}
}

// clearEvents empties the journal through the rw handle (test-only reset).
func clearEvents(t *testing.T, st *Store) {
	t.Helper()
	if _, err := st.rw.ExecContext(context.Background(), `DELETE FROM events`); err != nil {
		t.Fatalf("clear events: %v", err)
	}
}

// sevenRowJournal seeds ids 1..7 with strictly increasing ts so a page's identity can
// be asserted through both the id cursors and the returned TS values.
func sevenRowJournal() []evRow {
	rows := make([]evRow, 0, 7)
	for i := int64(1); i <= 7; i++ {
		rows = append(rows, evRow{
			id: i, ts: 1_700_000_000_000 + i*10, name: "msg.publish",
			stream: "orders", msgID: "01MSG" + string(rune('A'+i-1)), traceID: "tr-1",
		})
	}
	return rows
}

func eventTSList(p EventPage) []int64 {
	out := make([]int64, 0, len(p.Events))
	for _, e := range p.Events {
		out = append(out, e.TS)
	}
	return out
}

func int64Equal(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestEventsAfterIDPaginationHasNoSkipOrDup walks the whole journal through
// limit-sized pages and demands the concatenation be exactly the seeded sequence:
// a cursor that skips or duplicates a row reds here (issue #20 slice 1 named red).
func TestEventsAfterIDPaginationHasNoSkipOrDup(t *testing.T) {
	ctx := context.Background()
	st := openEventStore(t, nil)
	seedEvents(t, st, sevenRowJournal())

	var gotTS []int64
	wantComplete := []bool{false, false, true}
	wantNext := []int64{3, 6, 0}
	wantScanned := []int64{3, 6, 7}
	after := int64(0)
	for page := 0; ; page++ {
		if page > 100 {
			t.Fatal("pagination did not terminate in 100 pages")
		}
		p, err := st.Events(ctx, EventFilter{AfterID: after, Limit: 3})
		if err != nil {
			t.Fatalf("page %d: %v", page, err)
		}
		if len(p.Events) == 0 && !p.Complete {
			t.Fatalf("page %d: empty but Complete=false (NextAfterID=%d)", page, p.NextAfterID)
		}
		gotTS = append(gotTS, eventTSList(p)...)
		if p.Complete != wantComplete[page] {
			t.Errorf("page %d: Complete = %v, want %v", page, p.Complete, wantComplete[page])
		}
		if p.NextAfterID != wantNext[page] {
			t.Errorf("page %d: NextAfterID = %d, want %d", page, p.NextAfterID, wantNext[page])
		}
		if p.ScannedToID != wantScanned[page] {
			t.Errorf("page %d: ScannedToID = %d, want %d", page, p.ScannedToID, wantScanned[page])
		}
		after = p.NextAfterID
		if after == 0 {
			break
		}
	}
	want := []int64{
		1_700_000_000_010, 1_700_000_000_020, 1_700_000_000_030,
		1_700_000_000_040, 1_700_000_000_050, 1_700_000_000_060,
		1_700_000_000_070,
	}
	if !int64Equal(gotTS, want) {
		t.Errorf("paged walk = %v, want each row exactly once in id order %v", gotTS, want)
	}
}

// TestEventsOrderDescWalksTheJournalBackwards pins the Order contract: desc returns
// the newest id first and the AfterID cursor stays exclusive in scan direction, so
// desc pagination must also be gap-free.
func TestEventsOrderDescWalksTheJournalBackwards(t *testing.T) {
	ctx := context.Background()
	st := openEventStore(t, nil)
	seedEvents(t, st, sevenRowJournal())

	p, err := st.Events(ctx, EventFilter{Limit: 4, Order: OrderDesc})
	if err != nil {
		t.Fatalf("desc head page: %v", err)
	}
	wantHead := []int64{
		1_700_000_000_070, 1_700_000_000_060, 1_700_000_000_050, 1_700_000_000_040,
	}
	if !int64Equal(eventTSList(p), wantHead) {
		t.Errorf("desc head = %v, want newest-first %v", eventTSList(p), wantHead)
	}
	if p.Complete || p.NextAfterID != 4 {
		t.Errorf("desc head: Complete=%v NextAfterID=%d, want false/4", p.Complete, p.NextAfterID)
	}

	tail, err := st.Events(ctx, EventFilter{AfterID: p.NextAfterID, Limit: 4, Order: OrderDesc})
	if err != nil {
		t.Fatalf("desc tail page: %v", err)
	}
	wantTail := []int64{
		1_700_000_000_030, 1_700_000_000_020, 1_700_000_000_010,
	}
	if !int64Equal(eventTSList(tail), wantTail) {
		t.Errorf("desc tail = %v, want %v", eventTSList(tail), wantTail)
	}
	if !tail.Complete || tail.NextAfterID != 0 {
		t.Errorf("desc tail: Complete=%v NextAfterID=%d, want true/0", tail.Complete, tail.NextAfterID)
	}
}

// TestEventsFiltersPinExactRows drives every residual predicate of the anchor→index
// rule and one combined query, each pinned to the exact expected rows.
func TestEventsFiltersPinExactRows(t *testing.T) {
	ctx := context.Background()
	st := openEventStore(t, nil)
	seedEvents(t, st, []evRow{
		{id: 1, ts: 1000, name: "stream.create", stream: "orders"},
		{
			id: 2, ts: 1010, name: "msg.publish", stream: "orders", subject: "orders.a",
			msgID: "m1", traceID: "trA",
		},
		{
			id: 3, ts: 1020, name: "msg.deliver", stream: "orders", consumer: "workers",
			subject: "orders.a", msgID: "m1", traceID: "trA", seq: 1, attempt: 1,
		},
		{
			id: 4, ts: 1030, name: "msg.timeout", stream: "orders", consumer: "workers",
			subject: "orders.a", msgID: "m1", traceID: "trA", seq: 1, attempt: 1,
		},
		{
			id: 5, ts: 1040, name: "msg.nak", stream: "orders", consumer: "other",
			subject: "orders.a", msgID: "m1", traceID: "trA", seq: 1, attempt: 2,
		},
		{
			id: 6, ts: 1050, name: "msg.dead", stream: "orders", subject: "orders.a",
			msgID: "m1", traceID: "trA", seq: 1,
		},
		{id: 7, ts: 1060, name: "stream.create", stream: "billing"},
		{
			id: 8, ts: 1070, name: "msg.publish", stream: "billing", subject: "billing.b",
			msgID: "m2", traceID: "trB",
		},
	})

	cases := []struct {
		name   string
		filter EventFilter
		want   []int64
	}{
		{
			"by msg_id anchors on events_msg",
			EventFilter{MsgID: "m1"},
			[]int64{1010, 1020, 1030, 1040, 1050},
		},
		{
			"by trace_id anchors on events_trace",
			EventFilter{TraceID: "trB"},
			[]int64{1070},
		},
		{
			"by stream is residual",
			EventFilter{Stream: "billing"},
			[]int64{1060, 1070},
		},
		{
			"by consumer is residual",
			EventFilter{Consumer: "workers"},
			[]int64{1020, 1030},
		},
		{
			"exact event name",
			EventFilter{Events: []string{"msg.dead"}},
			[]int64{1050},
		},
		{
			"one-level glob covers the area",
			EventFilter{Events: []string{"stream.*"}},
			[]int64{1000, 1060},
		},
		{
			"since is inclusive until exclusive",
			EventFilter{Since: 1030, Until: 1050},
			[]int64{1030, 1040},
		},
		{
			"combined anchor and residual",
			EventFilter{TraceID: "trA", Consumer: "workers", Since: 1025},
			[]int64{1030},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := st.Events(ctx, tc.filter)
			if err != nil {
				t.Fatalf("events: %v", err)
			}
			if !p.Complete {
				t.Errorf("Complete = false with no limit pressure, want true")
			}
			got := eventTSList(p)
			if !int64Equal(got, tc.want) {
				t.Errorf("filter matched ts %v, want %v", got, tc.want)
			}
		})
	}
}

// TestEventsClampsLimitToEventQueryMaxLimit proves the ceiling is applied silently
// never: the caller asking above it gets the clamped page and an honest
// Complete=false resume cursor, and a zero limit means the ceiling itself.
func TestEventsClampsLimitToEventQueryMaxLimit(t *testing.T) {
	ctx := context.Background()
	st := openEventStore(t, func(o *Options) { o.EventQueryMaxLimit = 3 })
	seedEvents(t, st, sevenRowJournal())

	p, err := st.Events(ctx, EventFilter{Limit: 99})
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	if len(p.Events) != 3 {
		t.Errorf("len(Events) = %d, want clamped to 3", len(p.Events))
	}
	if p.Complete || p.NextAfterID != 3 {
		t.Errorf("clamped page: Complete=%v NextAfterID=%d, want false/3",
			p.Complete, p.NextAfterID)
	}

	zero, err := st.Events(ctx, EventFilter{})
	if err != nil {
		t.Fatalf("zero-limit events: %v", err)
	}
	if len(zero.Events) != 3 {
		t.Errorf("zero limit resolved to %d rows, want the 3-row ceiling", len(zero.Events))
	}
}

// TestMaxEventIDAndHorizonTrackTheJournal pins the two scalar probes: MaxEventID is
// the follow handoff point; HorizonTS is the OLDEST RETAINED ts (the trim horizon),
// which on this seed differs from the highest-id row's ts by design.
func TestMaxEventIDAndHorizonTrackTheJournal(t *testing.T) {
	ctx := context.Background()
	st := openEventStore(t, nil)
	clearEvents(t, st)

	maxID, err := st.MaxEventID(ctx)
	if err != nil {
		t.Fatalf("MaxEventID empty: %v", err)
	}
	horizon, err := st.EventHorizon(ctx)
	if err != nil {
		t.Fatalf("EventHorizon empty: %v", err)
	}
	if maxID != 0 || horizon != 0 {
		t.Errorf("empty journal: MaxEventID=%d Horizon=%d, want 0/0", maxID, horizon)
	}

	seedEvents(t, st, []evRow{
		{id: 5, ts: 2000, name: "msg.publish", stream: "orders"},
		{id: 9, ts: 1000, name: "msg.publish", stream: "orders"},
	})
	maxID, err = st.MaxEventID(ctx)
	if err != nil {
		t.Fatalf("MaxEventID: %v", err)
	}
	horizon, err = st.EventHorizon(ctx)
	if err != nil {
		t.Fatalf("EventHorizon: %v", err)
	}
	if maxID != 9 {
		t.Errorf("MaxEventID = %d, want 9", maxID)
	}
	if horizon != 1000 {
		t.Errorf("EventHorizon = %d, want 1000 (min ts, not last id)", horizon)
	}
}

// TestParseSinceAcceptsThreeForms pins the shared parser contract: RFC3339 instants,
// bare unix-ms integers, and signed relative durations off an explicit now — no wall
// clock inside the parser. Unsigned durations are rejected so "15m" cannot silently
// mean something different across callers.
func TestParseSinceAcceptsThreeForms(t *testing.T) {
	now := int64(1_700_000_000_000) // 2023-11-14T22:13:20Z
	cases := []struct {
		spec string
		want int64
	}{
		{"", 0},
		{"1700000000000", 1_700_000_000_000},
		{"2023-11-14T22:13:20Z", 1_700_000_000_000},
		{"2023-11-14T22:13:19.5Z", 1_700_000_000_000 - 500},
		{"-15m", now - 900_000},
		{"+90s", now + 90_000},
		{"-1h30m", now - 5_400_000},
	}
	for _, tc := range cases {
		got, err := ParseSince(tc.spec, now)
		if err != nil {
			t.Errorf("ParseSince(%q): %v", tc.spec, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ParseSince(%q) = %d, want %d", tc.spec, got, tc.want)
		}
	}
	for _, bad := range []string{"15m", "yesterday", "1700000000000x", "-abc"} {
		if got, err := ParseSince(bad, now); err == nil {
			t.Errorf("ParseSince(%q) = %d, want error", bad, got)
		}
	}
}

// openEventStore opens a fresh store with a fake clock; tweak runs before defaults
// apply so tests can set ceilings like EventQueryMaxLimit.
func openEventStore(t *testing.T, tweak func(o *Options)) *Store {
	t.Helper()
	dir := testDataDir(t)
	opts := testOptions(dir, fakeClock(), &logCapture{})
	if tweak != nil {
		tweak(&opts)
	}
	st, _, err := Open(context.Background(), opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := st.Close(context.Background()); closeErr != nil {
			t.Logf("close: %v", closeErr)
		}
	})
	return st
}
