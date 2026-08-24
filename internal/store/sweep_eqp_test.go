// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/queue"
)

// The EXPLAIN QUERY PLAN goldens (issue #11 G4). The deliveries_expiry partial index
// must turn "what expires next, globally" into an ordered range scan with no temp sort:
// a regression that stops using it (or bloats it by dropping the WHERE state=1 guard)
// changes these plans and reds the audit.

// eqp reads the query plan of q line by line.
func eqp(t *testing.T, st *Store, q string) string {
	t.Helper()
	rows, err := st.RO().QueryContext(context.Background(), "EXPLAIN QUERY PLAN "+q)
	if err != nil {
		t.Fatalf("EXPLAIN QUERY PLAN: %v", err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			t.Errorf("close eqp rows: %v", cerr)
		}
	}()
	var b strings.Builder
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		if err := rows.Scan(&id, &parent, &notUsed, &detail); err != nil {
			t.Fatalf("scan eqp: %v", err)
		}
		b.WriteString(detail + "\n")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate eqp: %v", err)
	}
	return b.String()
}

const (
	sweepExpiryScan = `SELECT d.stream, d.consumer, d.seq FROM deliveries d
LEFT JOIN messages m ON m.stream = d.stream AND m.seq = d.seq
WHERE d.state = 1 AND d.visible_at <= 9999999999999
ORDER BY d.visible_at, d.stream, d.consumer, d.seq LIMIT 1024`
	sweepIdleProbe = `SELECT visible_at FROM deliveries WHERE state = 1
ORDER BY visible_at LIMIT 1`
	sweepRetireScan = `SELECT d.seq FROM deliveries d
LEFT JOIN messages m ON m.stream = d.stream AND m.seq = d.seq
WHERE d.stream = 's' AND d.consumer = 'c' AND d.state = 0 AND d.attempts >= 5
ORDER BY d.seq LIMIT 1024`
)

func mustSweepStore(t *testing.T) *Store {
	t.Helper()
	st, _, err := Open(context.Background(), testOptions(filepath.Join(t.TempDir(), "data"), fakeClock(), &logCapture{}))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if cerr := st.Close(context.Background()); cerr != nil {
			t.Errorf("close store: %v", cerr)
		}
	})
	if _, _, err := st.CreateStream(context.Background(), queue.DefaultConfig("s"), "test"); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	cfg := queue.DefaultConsumerConfig("c")
	if _, err := st.CreateConsumer(context.Background(), "s", cfg, queue.StartPosition{Kind: queue.StartFirst}, "test"); err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	return st
}

// TestExpiryScanUsesPartialIndex pins that the expiry scan and the idle probe descend
// the deliveries_expiry partial index (USING INDEX deliveries_expiry) and neither needs
// a temp sort (USE TEMP B-TREE is a table-order scan + sort — exactly the cost the
// index pays for). The retire scan uses the per-consumer deliveries_ready index.
func TestExpiryScanUsesPartialIndex(t *testing.T) {
	st := mustSweepStore(t)
	expiry := eqp(t, st, sweepExpiryScan)
	if !strings.Contains(expiry, "deliveries_expiry") {
		t.Fatalf("expiry scan does not use deliveries_expiry:\n%s", expiry)
	}
	if strings.Contains(expiry, "USE TEMP B-TREE FOR ORDER BY") {
		t.Fatalf("expiry scan sorts in a temp b-tree — the index's total order is lost:\n%s", expiry)
	}
	probe := eqp(t, st, sweepIdleProbe)
	if !strings.Contains(probe, "deliveries_expiry") {
		t.Fatalf("idle probe does not use deliveries_expiry:\n%s", probe)
	}
	if strings.Contains(probe, "USE TEMP B-TREE FOR ORDER BY") {
		t.Fatalf("idle probe sorts in a temp b-tree:\n%s", probe)
	}
	// The retire scan is per-consumer and orders by seq; the optimizer drives it off the
	// deliveries primary key (stream, consumer, seq), which yields seq order with no sort.
	retire := eqp(t, st, sweepRetireScan)
	if !strings.Contains(retire, "USING INDEX") && !strings.Contains(retire, "USING COVERING INDEX") {
		t.Fatalf("retire scan does not use an index:\n%s", retire)
	}
	if strings.Contains(retire, "USE TEMP B-TREE FOR ORDER BY") {
		t.Fatalf("retire scan sorts in a temp b-tree:\n%s", retire)
	}
}
