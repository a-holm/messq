// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"testing"
)

// TestSchemaV2HasStreamStats pins migration 0002: the stream counters table exists,
// is strict, starts empty, and the ladder reports the current version. The table is
// what keeps GET /v1/streams/{s} constant-time (issue §5): msgs/bytes are maintained
// by every insert instead of counted per request. 0004 (#27) later ALTERed on the
// expired_seq/expired_at watermarks; the version pin tracks the whole ladder.
func TestSchemaV2HasStreamStats(t *testing.T) {
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
	var n int
	if err := st.ro.QueryRowContext(ctx,
		`SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='stream_stats'`,
	).Scan(&n); err != nil {
		t.Fatalf("inspect sqlite_schema: %v", err)
	}
	if n != 1 {
		t.Fatalf("stream_stats table missing after migration")
	}
}
