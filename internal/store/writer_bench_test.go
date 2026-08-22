package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/a-holm/messq/internal/clock"
)

// BenchmarkWriterCommit is the pre-#8 baseline for the D1 throughput gate: commands/s across
// the two durability modes and four concurrency levels. The real ≥ 5000 msg/s gate is
// measured end-to-end through serve in #8; these numbers feed #31's trend tracking. Run:
//
//	go test ./internal/store/ -run '^$' -bench BenchmarkWriterCommit -benchtime 2s
//
// Results are host-specific (ecryptfs over NVMe on the dev box): record them as such, never
// as absolute claims. Mean batch size falls out of sum/count on messq_commit_batch_size once
// a CommitMetrics observer is wired into the writer.
func BenchmarkWriterCommit(b *testing.B) {
	for _, mode := range []Durability{DurabilityFull, DurabilityRelaxed} {
		for _, conc := range []int{1, 8, 64, 256} {
			b.Run(fmt.Sprintf("%s/%d_concurrent", mode, conc), func(b *testing.B) {
				benchmarkWriterCommit(b, mode, conc)
			})
		}
	}
}

func benchmarkWriterCommit(b *testing.B, mode Durability, concurrency int) {
	ctx := context.Background()
	// b.TempDir is group-readable; §10 wants messq to own the 0700 creation, so add our own
	// level like testDataDir does for tests.
	dir := filepath.Join(b.TempDir(), "data")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		b.Fatalf("make data dir: %v", err)
	}
	opts := testOptions(dir, clock.System{}, &logCapture{})
	opts.Durability = mode
	st, _, err := Open(ctx, opts)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer func() {
		if closeErr := st.Close(ctx); closeErr != nil {
			b.Errorf("close store: %v", closeErr)
		}
	}()
	w, err := st.NewWriter(Config{Durability: mode}) // window=0: batches self-clock under load
	if err != nil {
		b.Fatalf("NewWriter: %v", err)
	}

	var seq atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, doErr := w.Do(ctx, &probeCmd{
				key: seq.Add(1),
				val: "x",
			}); doErr != nil {
				b.Errorf("Do: %v", doErr)
				return
			}
		}
	})
	b.StopTimer()

	if err := w.Close(ctx); err != nil {
		b.Fatalf("close: %v", err)
	}
	if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed, "cmds/s")
	}
}
