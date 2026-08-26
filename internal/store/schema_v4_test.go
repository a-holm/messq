// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"testing"

	"github.com/a-holm/messq/internal/queue"
)

// TestSchemaV4AddsExpiryWatermarks pins migration 0004 (#27 §4 Decision 2): the #7
// counter table gains the retention watermark columns expired_seq / expired_at as an
// ALTER with zero defaults — never a re-CREATE of a frozen artefact, so an existing data
// directory keeps its counts. The watermark is what lets peek/trace answer "seq N was
// removed by retention" (#28 consumes it) and what discard=new's O(1) check rides on.
func TestSchemaV4AddsExpiryWatermarks(t *testing.T) {
	ctx := context.Background()
	dir := testDataDir(t)
	st, _, err := Open(ctx, testOptions(dir, fakeClock(), &logCapture{}))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		if cerr := st.Close(ctx); cerr != nil {
			t.Logf("close store: %v", cerr)
		}
	}()
	if got := st.SchemaVersion(); got != 4 {
		t.Fatalf("SchemaVersion() = %d, want 4", got)
	}

	// The columns exist on the STRICT table with NOT NULL DEFAULT 0.
	type col struct {
		name    string
		typ     string
		notNull int
		dflt    *string
		pk      int
	}
	var cols []col
	rows, err := st.ro.QueryContext(ctx, `PRAGMA table_info(stream_stats)`)
	if err != nil {
		t.Fatalf("table_info: %v", err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			t.Logf("close table_info rows: %v", cerr)
		}
	}()
	for rows.Next() {
		var c col
		if err := rows.Scan(&c.pk, &c.name, &c.typ, &c.notNull, &c.dflt, &c.pk); err != nil {
			t.Fatalf("scan table_info: %v", err)
		}
		cols = append(cols, c)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("table_info rows: %v", err)
	}
	wanted := map[string]bool{"expired_seq": false, "expired_at": false}
	for _, c := range cols {
		if _, ok := wanted[c.name]; ok {
			wanted[c.name] = true
			if c.typ != "INTEGER" || c.notNull != 1 || c.dflt == nil || *c.dflt != "0" {
				t.Fatalf("column %s = (%s, notnull=%d, dflt=%v), want INTEGER NOT NULL DEFAULT 0",
					c.name, c.typ, c.notNull, c.dflt)
			}
		}
	}
	for name, found := range wanted {
		if !found {
			t.Fatalf("stream_stats is missing column %s after migration 0004", name)
		}
	}

	// A fresh publish reads back the zero watermarks: the writer's INSERT names only
	// the original three columns, so the defaults must carry the rest.
	if _, _, err := st.CreateStream(ctx, queue.DefaultConfig("orders"), "test"); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	var expiredSeq, expiredAt int64
	if err := st.ro.QueryRowContext(ctx,
		`SELECT expired_seq, expired_at FROM stream_stats WHERE stream = 'orders'`,
	).Scan(&expiredSeq, &expiredAt); err != nil {
		t.Fatalf("read watermarks: %v", err)
	}
	if expiredSeq != 0 || expiredAt != 0 {
		t.Fatalf("watermarks = (%d,%d), want (0,0)", expiredSeq, expiredAt)
	}
}
