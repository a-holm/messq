// SPDX-License-Identifier: Apache-2.0

package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
)

// The lite crash harness: a helper subprocess commits delivery rows one transaction each,
// then SIGKILLs itself at a seeded-random point. The parent reopens the directory and
// asserts what §4.4 promises after a real power-cut-shaped death: quick_check ran and came
// back clean, every COMMITTED row is present exactly once, and nothing torn slipped in
// between. This is deliberately small — #8 owns the full sweep against the ledger oracle.
//
// The child is the same test binary re-executing itself under an environment gate, handled
// in TestMain below; the seed travels through the environment so any failure reproduces
// bit-for-bit on rerun (no wall-clock seeding, no sleeps).

// Environment gates for the crash-child role and its parameters. Exported nowhere; only
// this file and the re-executed copy of itself see them.
const (
	envCrashRole = "MESSQ_CRASH_TEST_CHILD"
	envCrashDir  = "MESSQ_CRASH_TEST_DIR"
	envCrashSeed = "MESSQ_CRASH_TEST_SEED"
	envCrashRows = "MESSQ_CRASH_TEST_ROWS"
)

func TestMain(m *testing.M) {
	if os.Getenv(envCrashRole) == "writer" {
		// Never returns: the child SIGKILLs itself mid-loop, and any failure along the
		// way panics — the stack lands in the stderr buffer the parent shows. A test
		// binary must not call os.Exit directly (forbidigo): returning from TestMain
		// lets the testing package exit with m.Run's own code.
		runCrashChild()
		return
	}
	// The property machine's actions are real SQLite opens with durability=full fsyncs, so
	// rapid's library defaults (100 checks × ~30 steps) cost minutes per run. The PR lane
	// gets a deliberately light machine (3 × 6) — trimmed three times (20×15 → 8×10 →
	// 4×8 → 3×6) so the FULL parallel `make test` still finishes with margin inside
	// TEST_TIMEOUT; the nightly property job (#13) is where depth belongs, via
	// -rapid.checks/-rapid.steps on the command line — an explicit flag always wins over
	// this preset, because Parse runs after it.
	mustFlagSet("rapid.checks", "3")
	mustFlagSet("rapid.steps", "6")
	m.Run() // the testing package exits with m.Run's code when TestMain returns
}

func mustFlagSet(name, value string) {
	if err := flag.Set(name, value); err != nil {
		panic("set " + name + "=" + value + ": " + err.Error())
	}
}

// crashKillPoint mirrors the child's RNG consumption exactly: the seed's first and only
// draw decides how many rows commit before the kill, so the parent knows the ground truth
// without trusting anything the child said.
func crashKillPoint(seed string, rows int) (int, error) {
	s64, err := strconv.ParseInt(seed, 10, 64)
	if err != nil {
		return 0, err
	}
	return rand.New(rand.NewSource(s64)).Intn(rows + 1), nil
}

// runCrashChild opens the store, inserts <rows> deliveries committed one tx each, and
// SIGKILLs itself immediately after committing the seeded kill point's row. Killing between
// transactions is the honest shape for SQLite: the engine owns atomicity, so the interesting
// question is whether the boundary is where SQLite says it is. Any failure panics: the
// parent requires death-by-SIGKILL and prints this process's stderr, so a panic is exactly
// the loud, diagnosable signal wanted — no exit codes to invent.
func runCrashChild() {
	dir := os.Getenv(envCrashDir)
	rows, pErr := strconv.Atoi(os.Getenv(envCrashRows))
	if pErr != nil {
		panic("crash child: bad row count: " + pErr.Error())
	}
	killAfter := rand.New(rand.NewSource(mustSeed(os.Getenv(envCrashSeed)))).Intn(rows + 1)

	st, _, openErr := Open(context.Background(), Options{
		DataDir: dir,
		Clock:   clock.System{},
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if openErr != nil {
		panic("crash child: open: " + openErr.Error())
	}
	w, takeErr := st.TakeWriter()
	if takeErr != nil {
		panic("crash child: take writer: " + takeErr.Error())
	}
	ctx := context.Background()
	for i := 1; i <= rows; i++ {
		tx, txErr := w.BeginTx(ctx, nil)
		if txErr != nil {
			panic(fmt.Sprintf("crash child: begin tx %d: %v", i, txErr))
		}
		if _, insErr := tx.ExecContext(ctx,
			`INSERT INTO deliveries (stream, consumer, seq, subject, state, attempts, visible_at, generation, delivered_at, last_reason)
			 VALUES ('crash', 'c0', ?, ?, 0, ?, 0, 1, NULL, NULL)`,
			i, "crash.subject."+strconv.Itoa(i), i%4); insErr != nil {
			panic(fmt.Sprintf("crash child: insert row %d: %v", i, insErr))
		}
		if cErr := tx.Commit(); cErr != nil {
			panic(fmt.Sprintf("crash child: commit row %d: %v", i, cErr))
		}
		if i == killAfter {
			// SIGKILL: no defers run, no handles close, the flock dies with the process.
			if kErr := syscall.Kill(os.Getpid(), syscall.SIGKILL); kErr != nil {
				panic("crash child: self-SIGKILL failed: " + kErr.Error())
			}
		}
	}
	panic(fmt.Sprintf("crash child: reached end of insert loop; killAfter=%d rows=%d — the SIGKILL never landed", killAfter, rows))
}

func mustSeed(s string) int64 {
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		panic("crash child: bad seed " + s + ": " + err.Error())
	}
	return v
}

// TestCrashSubprocessCommitPrefix runs the child ten times, each with its own directory,
// row count band and deterministic seed, and holds every reopened directory to the §4.4
// contract. Ten bounded iterations, not a soak: the point here is the shape of the proof,
// which the nightly lane can scale.
func TestCrashSubprocessCommitPrefix(t *testing.T) {
	ctx := context.Background()

	for iter := 0; iter < 10; iter++ {
		dir := filepath.Join(t.TempDir(), "data")
		rows := 25 + iter*5
		seed := strconv.FormatInt(int64(900+iter), 10)

		want, err := crashKillPoint(seed, rows)
		if err != nil {
			t.Fatalf("derive kill point: %v", err)
		}

		var stderr bytes.Buffer
		launchCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		cmd := exec.CommandContext(launchCtx, os.Args[0], "-test.run=^$")
		cmd.Env = append(os.Environ(),
			envCrashRole+"=writer",
			envCrashDir+"="+dir,
			envCrashSeed+"="+seed,
			envCrashRows+"="+strconv.Itoa(rows),
		)
		cmd.Stderr = &stderr
		runErr := cmd.Run()
		cancel()
		var exitErr *exec.ExitError
		if runErr == nil || !errors.As(runErr, &exitErr) {
			t.Fatalf("iter %d: child did not die: %v\nchild log:\n%s", iter, runErr, stderr.String())
		}
		if errors.Is(runErr, context.DeadlineExceeded) {
			// The launcher's own SIGKILL must never pass for the child's: a hung child is
			// a failure, not a crash sample.
			t.Fatalf("iter %d: child hung past the 2m launch budget\nchild log:\n%s", iter, stderr.String())
		}
		status, ok := exitErr.Sys().(syscall.WaitStatus)
		if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
			t.Fatalf("iter %d: child died wrong: %v\nchild log:\n%s", iter, runErr, stderr.String())
		}

		st, report, openErr := Open(ctx, Options{
			DataDir: dir,
			Clock:   clock.System{},
			Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		})
		if openErr != nil {
			t.Fatalf("iter %d (want %d committed): reopen refused: %v\nchild log:\n%s",
				iter, want, openErr, stderr.String())
		}

		if !report.Unclean {
			t.Errorf("iter %d: RecoveryReport.Unclean = false, want true after SIGKILL", iter)
		}
		if report.CheckKind != checkQuickCheck {
			t.Errorf("iter %d: CheckKind = %q, want %q (the unclean stop must force quick_check)",
				iter, report.CheckKind, checkQuickCheck)
		}
		if report.Reclaimed != 0 {
			t.Errorf("iter %d: Reclaimed = %d, want 0 (every seeded row is READY)", iter, report.Reclaimed)
		}

		assertCrashRows(t, iter, st.RO(), want)

		if closeErr := st.Close(ctx); closeErr != nil {
			t.Errorf("iter %d: close reopened store: %v", iter, closeErr)
		}
	}
}

// assertCrashRows checks the commit-prefix property: exactly the first `want` rows exist,
// numbered 1..want without gaps or duplicates, and each carries its complete column payload
// — a torn insert would surface as a missing tail row or a mismatched field, never as a
// half-written row SQLite let through.
func assertCrashRows(t *testing.T, iter int, ro *sql.DB, want int) {
	t.Helper()

	var n int
	if err := ro.QueryRowContext(context.Background(),
		`SELECT count(*) FROM deliveries WHERE stream = 'crash'`).Scan(&n); err != nil {
		t.Fatalf("iter %d: count deliveries: %v", iter, err)
	}
	if n != want {
		t.Errorf("iter %d: %d committed rows survived, want %d", iter, n, want)
	}

	rows, err := ro.QueryContext(context.Background(),
		`SELECT seq, subject, state, attempts, generation FROM deliveries WHERE stream = 'crash' ORDER BY seq`)
	if err != nil {
		t.Fatalf("iter %d: read deliveries: %v", iter, err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			t.Errorf("iter %d: close rows: %v", iter, cerr)
		}
	}()

	next := 1
	for rows.Next() {
		var seq, state, attempts, generation int64
		var subject string
		if scanErr := rows.Scan(&seq, &subject, &state, &attempts, &generation); scanErr != nil {
			t.Fatalf("iter %d: scan delivery: %v", iter, scanErr)
		}
		if seq != int64(next) {
			t.Errorf("iter %d: found seq %d at position %d — commit prefix has a gap or duplicate", iter, seq, next)
			return
		}
		if subject != "crash.subject."+strconv.Itoa(next) || state != 0 || attempts != int64(next)%4 || generation != 1 {
			t.Errorf("iter %d: row %d torn: subject=%q state=%d attempts=%d generation=%d",
				iter, seq, subject, state, attempts, generation)
		}
		next++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iter %d: iterate deliveries: %v", iter, err)
	}
	if next-1 != want {
		t.Errorf("iter %d: scanned %d rows, want %d", iter, next-1, want)
	}
}
