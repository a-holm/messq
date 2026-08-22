// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/clock"
)

// Recovery and startup benchmarks. What is measured is Open and nothing else: the timer
// stops while each iteration's directory is reset from a pristine template, so the number
// is the §4.4 startup path (unclean detection, quick_check, the N-row reclaim, the dedup
// trim, the TRUNCATE checkpoint), not test scaffolding. Durability is the default full
// mode (synchronous=FULL) throughout — the published numbers carry that fact, and the
// hardware line, with them.
//
// The 1k and 10k cases use b.TempDir. The 100k case honours MESSQ_BENCH_DIR because the
// reference numbers are measured on the owner-designated directory (ecryptfs over NVMe on
// the development host — slower through the crypto layer than bare ext4, stated honestly
// wherever the numbers are published); /home/johan/bench-messq is that directory.

const (
	benchRows1k   = 1_000
	benchRows10k  = 10_000
	benchRows100k = 100_000
)

// benchLogger keeps recovery log lines out of the benchmark output.
func benchLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// benchOptions is the Open configuration every benchmark shares: default durability
// (full), deterministic reclaim (jitter 0), quiet logger.
func benchOptions(dir string) Options {
	return Options{
		DataDir:       dir,
		ReclaimJitter: 0,
		Clock:         clock.System{},
		Logger:        benchLogger(),
	}
}

// seedTemplate builds a pristine template database: schema v1, marker dirty (clean_shutdown
// = 0, exactly the on-disk shape a SIGKILL leaves), n INFLIGHT delivery rows committed, and
// the WAL checkpointed into the file so a single-file copy is a complete directory state.
// The writer handle is released kill-style — no optimize, no marker flip. The directory
// itself is left for Open to create at 0700: §10 refuses anything broader, including the
// modes testing frameworks hand out for temp roots.
func seedTemplate(b *testing.B, root string, n int) string {
	b.Helper()
	ctx := context.Background()
	dir := filepath.Join(root, "template")
	st, report, err := Open(ctx, benchOptions(dir))
	if err != nil {
		b.Fatalf("open template: %v", err)
	}
	if report.SchemaTo != 1 {
		b.Fatalf("template schema = %d, want 1", report.SchemaTo)
	}
	w, err := st.TakeWriter()
	if err != nil {
		b.Fatalf("take writer: %v", err)
	}
	if n > 0 {
		if err := seedInflightRows(ctx, w, n); err != nil {
			b.Fatalf("seed %d inflight rows: %v", n, err)
		}
	}
	if _, _, err := checkpointTruncate(ctx, w); err != nil {
		b.Fatalf("checkpoint template: %v", err)
	}
	if err := w.Close(); err != nil {
		b.Fatalf("close template writer: %v", err)
	}
	killSimulate(b, st, nil) // dirty on-disk state, flock released, no marker write
	return filepath.Join(dir, dbFileName)
}

// seedInflightRows inserts n INFLIGHT deliveries in batched multi-row statements, one
// transaction per batch — the fastest honest way to load the pending set.
func seedInflightRows(ctx context.Context, w *sql.DB, n int) error {
	const batch = 200
	const prefix = `INSERT INTO deliveries (stream, consumer, seq, subject, state, attempts, visible_at, generation, delivered_at, last_reason) VALUES `
	for base := 1; base <= n; base += batch {
		var sb strings.Builder
		sb.WriteString(prefix)
		args := make([]any, 0, batch*4)
		for i := base; i < base+batch && i <= n; i++ {
			if i != base {
				sb.WriteByte(',')
			}
			sb.WriteString(`('bench', 'c0', ?, ?, 1, 2, 0, 1, NULL, NULL)`)
			args = append(args, i, "bench.subject."+strconv.Itoa(i))
		}

		tx, txErr := w.BeginTx(ctx, nil)
		if txErr != nil {
			return txErr
		}
		_, execErr := tx.ExecContext(ctx, sb.String(), args...)
		if execErr != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				return errors.Join(execErr, rbErr)
			}
			return execErr
		}
		if commitErr := tx.Commit(); commitErr != nil {
			return commitErr
		}
	}
	return nil
}

// resetFromTemplate gives the next iteration a pristine copy of the template state.
func resetFromTemplate(b *testing.B, template, dir string) {
	b.Helper()
	if err := os.RemoveAll(dir); err != nil {
		b.Fatalf("reset %s: %v", dir, err)
	}
	if err := os.Mkdir(dir, 0o700); err != nil {
		b.Fatalf("mkdir %s: %v", dir, err)
	}
	src, err := os.Open(template)
	if err != nil {
		b.Fatalf("open template: %v", err)
	}
	dst, err := os.OpenFile(filepath.Join(dir, dbFileName), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		b.Fatalf("create db copy: %v", err)
	}
	if _, err := io.Copy(dst, src); err != nil {
		b.Fatalf("copy template: %v", err)
	}
	if err := src.Close(); err != nil {
		b.Fatalf("close template: %v", err)
	}
	if err := dst.Close(); err != nil {
		b.Fatalf("close db copy: %v", err)
	}
}

// assertRecoveryMeasuredPath guards the benchmark against silently measuring the wrong
// path: the report must say unclean, quick_check, and exactly the seeded reclaim.
func assertRecoveryMeasuredPath(b *testing.B, report *RecoveryReport, wantReclaimed int64) {
	b.Helper()
	if !report.Unclean {
		b.Fatal("measured open was clean — the benchmark is not measuring recovery")
	}
	if report.CheckKind != checkQuickCheck {
		b.Fatalf("CheckKind = %q, want quick_check", report.CheckKind)
	}
	if report.Reclaimed != wantReclaimed {
		b.Fatalf("Reclaimed = %d, want %d", report.Reclaimed, wantReclaimed)
	}
}

func BenchmarkRecovery(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{benchRows1k, benchRows10k, benchRows100k} {
		b.Run(strings.ToLower(strconv.Itoa(n/1000))+"k_inflight", func(b *testing.B) {
			root := b.TempDir()
			if n == benchRows100k {
				if d := os.Getenv("MESSQ_BENCH_DIR"); d != "" {
					root = d
					if err := os.MkdirAll(root, 0o700); err != nil {
						b.Fatalf("prepare %s: %v", root, err)
					}
				}
			}
			template := seedTemplate(b, root, n)
			work := filepath.Join(root, "work")

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				resetFromTemplate(b, template, work)
				b.StartTimer()

				st, report, err := Open(ctx, benchOptions(work))
				if err != nil {
					b.Fatalf("open: %v", err)
				}
				assertRecoveryMeasuredPath(b, report, int64(n))

				b.StopTimer()
				if err := st.Close(ctx); err != nil {
					b.Fatalf("close: %v", err)
				}
			}
		})
	}
}

// BenchmarkOpenFresh measures the empty-directory path: schema creation, bookkeeping mint,
// the zero-row recovery tail.
func BenchmarkOpenFresh(b *testing.B) {
	ctx := context.Background()
	root := b.TempDir()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := filepath.Join(root, "fresh-"+strconv.Itoa(i))
		if err := os.Mkdir(dir, 0o700); err != nil {
			b.Fatalf("mkdir: %v", err)
		}
		b.StartTimer()

		st, report, err := Open(ctx, benchOptions(dir))
		if err != nil {
			b.Fatalf("open: %v", err)
		}
		if report.Unclean || report.Reclaimed != 0 {
			b.Fatalf("fresh open reported %+v", report)
		}

		b.StopTimer()
		if err := st.Close(ctx); err != nil {
			b.Fatalf("close: %v", err)
		}
	}
}

// BenchmarkOpenClean measures the steady restart path on a gracefully closed directory:
// marker clean, no check, no reclaim — the number #6 and #8 compare against.
func BenchmarkOpenClean(b *testing.B) {
	ctx := context.Background()
	root := b.TempDir()
	template := filepath.Join(root, "clean-template.db")

	// The clean template is a directory that was opened and gracefully closed: marker 1.
	cleanDir := filepath.Join(root, "clean-src")
	if err := os.Mkdir(cleanDir, 0o700); err != nil {
		b.Fatalf("mkdir: %v", err)
	}
	st, _, err := Open(ctx, benchOptions(cleanDir))
	if err != nil {
		b.Fatalf("open clean template: %v", err)
	}
	if err := st.Close(ctx); err != nil {
		b.Fatalf("close clean template: %v", err)
	}
	if err := os.Rename(filepath.Join(cleanDir, dbFileName), template); err != nil {
		b.Fatalf("stage clean template: %v", err)
	}

	work := filepath.Join(root, "work")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		resetFromTemplate(b, template, work)
		b.StartTimer()

		st, report, err := Open(ctx, benchOptions(work))
		if err != nil {
			b.Fatalf("open: %v", err)
		}
		if report.Unclean || report.CheckKind != checkSkipped || report.Reclaimed != 0 {
			b.Fatalf("clean open reported %+v, want clean skip", report)
		}

		b.StopTimer()
		if err := st.Close(ctx); err != nil {
			b.Fatalf("close: %v", err)
		}
	}
}
