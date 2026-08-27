// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/queue"
)

// This file tops up the #27 retention/reap/trim/housekeeping code-path coverage
// (issue #27): the command vocabulary carried by the new janitor jobs, the Solo
// marker contract of the housekeeping PRAGMAs, the data-directory lock-holder
// diagnostics, dead-letter policy resolution and header-merge degradation, and
// the spent-transaction refusals every writer command inherits. Everything is
// deterministic: fixed clocks, real files, committed-transaction cascade errors,
// and OS failures that are state-based (a read-only fd), never timing-based.

const cov27BaseMS = int64(1700005000000) // the fixed `now` every subtest reasons against

// TestHousekeepingCommandVocabulary pins the closed writer-command vocabulary the
// janitor jobs (#27) enqueue through: labels unique, bytes metadata-only.
func TestHousekeepingCommandVocabulary(t *testing.T) {
	cases := []struct {
		name string
		cmd  Cmd
		want CmdKind
	}{
		{"reap-resume", reapResumeCmd{}, kindReapResume},
		{"trim-events", TrimEventsCmd{}, kindTrimEvents},
		{"retention", RetentionCmd{}, kindRetention},
	}
	for _, tc := range cases {
		if got := tc.cmd.Kind(); got != tc.want {
			t.Errorf("%s Kind = %q, want %q", tc.name, got, tc.want)
		}
		if b := tc.cmd.Bytes(); b != 0 {
			t.Errorf("%s Bytes = %d, want 0 (metadata-only)", tc.name, b)
		}
	}
}

// TestDSNPoolRoleNames covers poolRole.String fully, including the unknown-role
// default that mirrors synchronousWord's fail-safe direction.
func TestDSNPoolRoleNames(t *testing.T) {
	cases := []struct {
		role poolRole
		want string
	}{
		{poolWriter, "writer"},
		{poolReader, "reader"},
		{poolReadOnly, "read-only"},
		{poolRole(42), "read-only"},
	}
	for _, tc := range cases {
		if got := tc.role.String(); got != tc.want {
			t.Errorf("poolRole(%d).String() = %q, want %q", uint8(tc.role), got, tc.want)
		}
	}
}

// TestSoloHousekeepingCommandsRejectBatching pins the Solo marker contract:
// both housekeeping commands carry the marker and refuse the batch path loudly —
// running a PRAGMA checkpoint or vacuum inside a batch transaction is impossible
// by design, so reaching this refusal would mean a misuse slipped past the type
// system.
func TestSoloHousekeepingCommandsRejectBatching(t *testing.T) {
	st := openCommandPathStore(t, fakeClock())
	ctx := context.Background()

	for _, c := range []struct {
		name string
		cmd  Cmd
	}{
		{"checkpoint", CheckpointCmd{Mode: CheckpointPassive}},
		{"vacuum", VacuumCmd{Pages: 1}},
	} {
		soloer, ok := c.cmd.(interface{ Solo() })
		if !ok {
			t.Fatalf("%s: does not implement Solo", c.name)
		}
		if !isSolo(c.cmd) {
			t.Errorf("%s: isSolo = false, want true", c.name)
		}
		soloer.Solo()
		_, _, err := c.cmd.Apply(ctx, nil, time.UnixMilli(cov27BaseMS))
		if err == nil || !IsCmdError(err) {
			t.Errorf("%s.Apply = %v, want a marked business rejection", c.name, err)
			continue
		}
		if !strings.Contains(err.Error(), "never run inside a batch") {
			t.Errorf("%s.Apply message %v, want the batch-refusal wording", c.name, err)
		}
	}

	// A malformed mode is refused before any PRAGMA runs.
	if _, _, err := (CheckpointCmd{Mode: "SHARK"}).ApplySolo(ctx, st.rw, time.UnixMilli(cov27BaseMS)); err == nil ||
		!strings.Contains(err.Error(), "neither PASSIVE nor TRUNCATE") {
		t.Errorf("bogus checkpoint mode = %v, want the mode refusal", err)
	}
}

// lockDataDirMustFile opens a writable scratch file under dir for the diagnostics
// tests to break on purpose.
func lockDataDirMustFile(t *testing.T, path string) *os.File {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatalf("open scratch file: %v", err)
	}
	return f
}

// TestLockHolderLineDiagnostics covers the LOCK-file diagnostics arms of the data
// directory: a stamped holder line round-trips through readHolderLine, an
// unreadable source degrades to "" (both the seek failure and the read failure),
// and stamping onto a read-only fd reports its truncate failure.
func TestLockHolderLineDiagnostics(t *testing.T) {
	dir := t.TempDir()
	lock, err := lockDataDir(dir, lockExclusive)
	if err != nil {
		t.Fatalf("lockDataDir: %v", err)
	}
	t.Cleanup(func() {
		if ulErr := lock.unlock(); ulErr != nil && !strings.Contains(ulErr.Error(), "not held") {
			t.Errorf("unlock: %v", ulErr)
		}
	})
	if line := readHolderLine(lock.f); !strings.Contains(line, "pid=") {
		t.Fatalf("readHolderLine = %q, want the pid= holder line", line)
	}

	// Seek failure: an already-closed file degrades to an empty line.
	closed := lockDataDirMustFile(t, filepath.Join(dir, "closed"))
	if closeErr := closed.Close(); closeErr != nil {
		t.Fatalf("close scratch file: %v", closeErr)
	}
	if line := readHolderLine(closed); line != "" {
		t.Errorf("readHolderLine(closed) = %q, want \"\"", line)
	}

	// ...and so does an unreadable one: a directory fd seeks fine but reads EISDIR.
	dirFD, oErr := os.Open(dir)
	if oErr != nil {
		t.Fatalf("open dir fd: %v", oErr)
	}
	t.Cleanup(func() {
		if cErr := dirFD.Close(); cErr != nil {
			t.Errorf("close dir fd: %v", cErr)
		}
	})
	if line := readHolderLine(dirFD); line != "" {
		t.Errorf("readHolderLine(dirfd) = %q, want \"\"", line)
	}

	// Truncate failure: a read-only fd cannot be restamped.
	scratch := lockDataDirMustFile(t, filepath.Join(dir, "ro"))
	if cErr := scratch.Close(); cErr != nil {
		t.Fatalf("close scratch file: %v", cErr)
	}
	ro, rErr := os.Open(filepath.Join(dir, "ro"))
	if rErr != nil {
		t.Fatalf("open read-only file: %v", rErr)
	}
	t.Cleanup(func() {
		if cErr := ro.Close(); cErr != nil {
			t.Errorf("close read-only file: %v", cErr)
		}
	})
	if wErr := writeHolderLine(ro); wErr == nil || !strings.Contains(wErr.Error(), "truncate") {
		t.Errorf("writeHolderLine(read-only) = %v, want a truncate failure", wErr)
	}
}

// TestUnlockReportsUnknownState drives unlock over a manually closed descriptor:
// both the LOCK_UN flock release and the close themselves fail, and the returned
// error must surface BOTH rather than swallow one half of the lost guarantee.
func TestUnlockReportsUnknownState(t *testing.T) {
	l, err := lockDataDir(t.TempDir(), lockExclusive)
	if err != nil {
		t.Fatalf("lockDataDir: %v", err)
	}
	if cErr := l.f.Close(); cErr != nil {
		t.Fatalf("pre-close lock fd: %v", cErr)
	}
	uErr := l.unlock()
	if uErr == nil ||
		!strings.Contains(uErr.Error(), "flock LOCK_UN") ||
		!strings.Contains(uErr.Error(), "close lock file") {
		t.Fatalf("unlock after close = %v, want joined flock+close failures", uErr)
	}
}

// spentTxDirect hands back a runner that applies one command against an
// ALREADY-COMMITTED transaction: every statement fails with database/sql's
// ErrTxDone, which is precisely how the unmarked infrastructure-error propagations
// (as opposed to the domain refusals callers may mark) surface when the wrapper
// transaction outlives its work.
func spentTxDirect(t *testing.T) func(cmd Cmd) error {
	t.Helper()
	const base = cov27BaseMS
	path := filepath.Join(t.TempDir(), dbFileName)
	migrateFresh(t, path, newStepClock(time.UnixMilli(base)))
	db := openTestDB(t, path)
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit empty tx: %v", err)
	}
	return func(cmd Cmd) error {
		_, _, applyErr := cmd.Apply(context.Background(), tx, time.UnixMilli(base))
		return applyErr
	}
}

// TestSpentTransactionRefusals walks five writer commands over the committed
// transaction: each must come back as an UNMARKED wrapped error naming its first
// SQL step and chaining database/sql's own refusal message.
func TestSpentTransactionRefusals(t *testing.T) {
	call := spentTxDirect(t)

	refusals := []struct {
		name string
		cmd  Cmd
		want string
	}{
		{"create-consumer", createConsumerCmd{
			stream: "orders", cfg: queue.DefaultConsumerConfig("worker"),
			start: queue.StartPosition{Kind: queue.StartNew}, limits: queue.ConsumerLimits{},
		}, "read reap marker"},
		{"update-consumer", updateConsumerCmd{
			stream: "orders", name: "worker", limits: queue.ConsumerLimits{},
		}, "read consumer"},
		{
			"delete-consumer",
			deleteConsumerCmd{stream: "orders", name: "worker"},
			"read consumer",
		},
		{"set-paused", setPausedCmd{stream: "orders", name: "worker"}, "read consumer"},
		{"reap-resume", reapResumeCmd{}, "find reap marker"},
	}
	for _, tc := range refusals {
		err := call(tc.cmd)
		if err == nil || IsCmdError(err) || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s on a spent tx = %v, want unmarked %q propagation", tc.name, err, tc.want)
		}
		if !errors.Is(err, sql.ErrTxDone) {
			t.Errorf("%s: error should wrap database/sql's ErrTxDone, got %v", tc.name, err)
		}
	}
}

// TestDeadSinkResolvesMissingConsumerAsDrop covers loadDeadPolicy's NoRows arm:
// a dead-letter transition without a live consumer row resolves the policy to
// drop — nothing may be copied into a DLQ for a configuration that no longer
// exists — and the msg.dead event records the dropped outcome.
func TestDeadSinkResolvesMissingConsumerAsDrop(t *testing.T) {
	st := openCommandPathStore(t, fakeClock())
	ctx := context.Background()
	if _, _, err := st.CreateStream(ctx, queue.DefaultConfig("orders"), "tester"); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	pr, pErr := st.Publish(ctx, PublishCmd{
		Stream: "orders",
		Req:    queue.PublishReq{Subject: "orders.new", Body: []byte("body"), MsgID: "d1"},
	})
	if pErr != nil {
		t.Fatalf("publish: %v", pErr)
	}

	tx, bErr := st.rw.BeginTx(ctx, nil)
	if bErr != nil {
		t.Fatalf("begin: %v", bErr)
	}
	t.Cleanup(func() {
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			t.Errorf("rollback seed tx: %v", rbErr)
		}
	})

	ev, dErr := st.newDeadSink().Dead(ctx, tx, queue.DeadCtx{
		Stream:     "orders",
		Subject:    "orders.new",
		Seq:        uint64(pr.Seq),
		MsgID:      "d1",
		TraceID:    "trace-1",
		Attempts:   3,
		Generation: 1,
		Cause:      queue.DeadCauseTerminated,
	}, time.UnixMilli(cov27BaseMS))
	if dErr != nil {
		t.Fatalf("Dead(ghost consumer) = %v, want dropped dead-letter", dErr)
	}
	if ev.Event != "msg.dead" || ev.Detail["dlq"] != string(DeadOutcomeDropped) {
		t.Fatalf("event = %+v, want msg.dead with dlq=dropped", ev)
	}
}

// TestDeadSinkRefusesCorruptOriginHeader plants a non-JSON origin header and runs
// one copy-transition over it: mergeCopyHeaders must refuse the whole death —
// provenance survives degradation, corruption does not get copied.
func TestDeadSinkRefusesCorruptOriginHeader(t *testing.T) {
	st := openCommandPathStore(t, fakeClock())
	ctx := context.Background()
	if _, _, err := st.CreateStream(ctx, queue.DefaultConfig("orders"), "tester"); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	pr, pErr := st.Publish(ctx, PublishCmd{
		Stream: "orders",
		Req:    queue.PublishReq{Subject: "orders.new", Body: []byte("body"), MsgID: "d2"},
	})
	if pErr != nil {
		t.Fatalf("publish: %v", pErr)
	}
	if _, cErr := st.CreateConsumer(ctx, "orders", queue.DefaultConsumerConfig("worker"),
		queue.StartPosition{Kind: queue.StartFirst}, "tester"); cErr != nil {
		t.Fatalf("create consumer: %v", cErr)
	}
	if _, xErr := st.rw.ExecContext(ctx,
		`UPDATE messages SET hdr = 'junk' WHERE stream = 'orders' AND seq = ?`, pr.Seq); xErr != nil {
		t.Fatalf("plant corrupt header: %v", xErr)
	}

	tx, bErr := st.rw.BeginTx(ctx, nil)
	if bErr != nil {
		t.Fatalf("begin: %v", bErr)
	}
	t.Cleanup(func() {
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			t.Errorf("rollback plant tx: %v", rbErr)
		}
	})

	_, dErr := st.newDeadSink().Dead(ctx, tx, queue.DeadCtx{
		Stream:     "orders",
		Consumer:   "worker",
		Subject:    "orders.new",
		Seq:        uint64(pr.Seq),
		MsgID:      "d2",
		Attempts:   4,
		Generation: 1,
		Cause:      queue.DeadCauseMaxDeliver,
		Policy:     queue.DeadPolicyDLQ,
	}, time.UnixMilli(cov27BaseMS))
	if dErr == nil || IsCmdError(dErr) || !strings.Contains(dErr.Error(), "origin hdr not JSON") {
		t.Fatalf("Dead(corrupt hdr) = %v, want unmarked origin-header refusal", dErr)
	}
}

// TestCreateConsumerStartResolutionRefusals drives resolveStartPosition's
// unknown-kind refusal end to end (the applyInsert maybeCmdErr wrap included)
// plus the immutable-start rule on re-create: cursor movement belongs to seek,
// never to a re-POST.
func TestCreateConsumerStartResolutionRefusals(t *testing.T) {
	st := openCommandPathStore(t, fakeClock())
	ctx := context.Background()
	if _, _, err := st.CreateStream(ctx, queue.DefaultConfig("orders"), "tester"); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	if _, cErr := st.CreateConsumer(ctx, "orders", queue.DefaultConsumerConfig("worker"),
		queue.StartPosition{Kind: "warp"}, "tester"); cErr == nil ||
		!IsCmdError(cErr) || !strings.Contains(cErr.Error(), `start kind "warp" is not known`) {
		t.Fatalf("unknown start kind = %v, want marked bad-request", cErr)
	}

	if _, cErr := st.CreateConsumer(ctx, "orders", queue.DefaultConsumerConfig("worker"),
		queue.StartPosition{Kind: queue.StartNew}, "tester"); cErr != nil {
		t.Fatalf("create consumer: %v", cErr)
	}
	_, reErr := st.CreateConsumer(ctx, "orders", queue.DefaultConsumerConfig("worker"),
		queue.StartPosition{Kind: queue.StartSeq, Seq: 7}, "tester")
	var imm *ImmutableFieldError
	if reErr == nil || !errors.As(reErr, &imm) || imm.Field != "start" {
		t.Fatalf("re-create with moved start = %v, want ImmutableFieldError", reErr)
	}
}
