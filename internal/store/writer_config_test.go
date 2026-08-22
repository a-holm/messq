package store

import (
	"errors"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/errs"
)

// TestWriterConfigDefaults pins the zero-value and negative-value behaviour of the writer
// configuration: unset budgets fall back to the PLAN §4.3 defaults, a zero commit window is
// meaningful (commit whatever is queued now) and therefore kept, and only a negative window
// is rejected — there is no interpretation of "linger minus one second" that survives review.
func TestWriterConfigDefaults(t *testing.T) {
	t.Parallel()

	var zero Config
	if err := zero.fillDefaults(); err != nil {
		t.Fatalf("zero config refused: %v", err)
	}
	if zero.CommitWindow != 0 {
		t.Errorf("CommitWindow = %v, want 0 (immediate; the 2ms default lives at the flag layer)", zero.CommitWindow)
	}
	if zero.CommitMaxBatch != 256 {
		t.Errorf("CommitMaxBatch = %d, want 256", zero.CommitMaxBatch)
	}
	if zero.CommitMaxBytes != int64(8)<<20 {
		t.Errorf("CommitMaxBytes = %d, want 8 MiB", zero.CommitMaxBytes)
	}
	if zero.QueueDepth != 2048 {
		t.Errorf("QueueDepth = %d, want 2048", zero.QueueDepth)
	}
	if zero.FatalDrain != 2*time.Second {
		t.Errorf("FatalDrain = %v, want 2s", zero.FatalDrain)
	}

	negative := Config{
		CommitWindow:   -time.Second,
		CommitMaxBatch: -1,
		CommitMaxBytes: -1,
		QueueDepth:     -1,
		FatalDrain:     -time.Millisecond,
	}
	err := negative.fillDefaults()
	if err == nil {
		t.Fatal("negative CommitWindow accepted")
	}
	// The other fields are still defaulted despite the error, so a caller that ignores the
	// error never runs with unbounded budgets.
	if negative.CommitMaxBatch != 256 || negative.QueueDepth != 2048 {
		t.Errorf("fields left undefaulted after refusal: %+v", negative)
	}

	chosen := Config{CommitWindow: time.Millisecond, CommitMaxBatch: 7, QueueDepth: 3}
	if err := chosen.fillDefaults(); err != nil {
		t.Fatalf("chosen config refused: %v", err)
	}
	if chosen.CommitMaxBatch != 7 || chosen.QueueDepth != 3 || chosen.CommitWindow != time.Millisecond {
		t.Errorf("fillDefaults overwrote caller choices: %+v", chosen)
	}
}

// TestClassifyStorageError pins the fsyncgate error classes: errno-bearing failures map to
// their class through errors.Is (so wrapping layers do not hide them), SQLite's errno-less
// textual signatures map by message, and everything else is unknown. The class is a log field
// and a metric label, never a control-flow decision.
func TestClassifyStorageError(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, "unknown"},
		{"eio direct", syscall.EIO, "eio"},
		{"eio wrapped", fmt.Errorf("commit: %w", syscall.EIO), "eio"},
		{"enospc wrapped", fmt.Errorf("commit: %w", syscall.ENOSPC), "enospc"},
		{"ioerr by sqlite text", errors.New("disk I/O error"), "eio"},
		{"corrupt by sqlite text", errors.New("database disk image is malformed"), "corrupt"},
		{"foreign file by sqlite text", errors.New("file is not a database"), "corrupt"},
		{"plain infra", errors.New("some driver failure"), "unknown"},
	}
	for _, tc := range cases {
		if got := classify(tc.err); got != tc.want {
			t.Errorf("%s: classify(%v) = %q, want %q", tc.name, tc.err, got, tc.want)
		}
	}
}

// TestCmdErrorMarksBusinessRejection pins the two-class rule every command author depends
// on: a sentinel wrapped with CmdErr stays matching its original sentinel for callers, is
// reported as a business rejection to the engine, and a plain error is not.
func TestCmdErrorMarksBusinessRejection(t *testing.T) {
	t.Parallel()

	business := CmdErr(errs.ErrTooLarge)
	if !IsCmdError(business) {
		t.Error("CmdErr-wrapped error not recognised as a business rejection")
	}
	if !errors.Is(business, errs.ErrTooLarge) {
		t.Error("CmdErr broke errors.Is to the wrapped sentinel")
	}
	if IsCmdError(errs.ErrTooLarge) {
		t.Error("unwrapped sentinel classified as business rejection — wrap it first")
	}
	if IsCmdError(nil) {
		t.Error("nil classified as business rejection")
	}
	if business.Error() != errs.ErrTooLarge.Error() {
		t.Errorf("CmdErr rewrote the message: %q vs %q", business.Error(), errs.ErrTooLarge.Error())
	}
}
