// SPDX-License-Identifier: Apache-2.0

package obs

import (
	"errors"
	"strings"
	"syscall"
	"time"
)

// CommitObserver is the metrics seam of the group-commit engine (#6). The writer calls it;
// an implementation turns the calls into instruments. The Prometheus implementation lives in
// the internal/obs/prommetrics subpackage, not here: an import of client_golang in this
// package would drag net/http into every importer of obs, and internal/store depends on obs
// while layers.sh forbids it from reaching net/http at all. #21 owns registry wiring, naming
// policy sign-off and the golden exposition test.
//
// Implementations must be safe for concurrent use and must never block: every method runs on
// or adjacent to the writer goroutine's commit path.
//
// Cardinality rule (PLAN §D11): none of these observations may introduce stream, consumer,
// subject or identifier labels. The only label in the set is class on commit errors.
type CommitObserver interface {
	// ObserveCommit records one finished transaction: batch is the number of commands in
	// it, d its wall duration (dominated by fsync under durability=full), err the commit
	// outcome. On success the batch size feeds messq_commit_batch_size and d feeds
	// messq_commit_duration_seconds; on failure only d is observed (no commands were
	// applied, so a batch-size observation would poison sum/count) and the error class
	// increments messq_commit_errors_total{class}.
	ObserveCommit(batch int, d time.Duration, err error)

	// ObserveQueueDepth samples len(command channel) once per batch cycle: the saturation
	// signal behind messq_writer_queue_depth.
	ObserveQueueDepth(n int)

	// SetReadOnly flips messq_readonly between 0 and 1. Exactly one true transition ever
	// happens per writer: the fsyncgate latch.
	SetReadOnly(ro bool)
}

// NopCommitObserver discards everything. It is the default when no observer is injected.
type NopCommitObserver struct{}

// ObserveCommit does nothing.
func (NopCommitObserver) ObserveCommit(int, time.Duration, error) {}

// ObserveQueueDepth does nothing.
func (NopCommitObserver) ObserveQueueDepth(int) {}

// SetReadOnly does nothing.
func (NopCommitObserver) SetReadOnly(bool) {}

// ClassifyStorageError names the storage-fault family of err for storage.fatal logs and the
// messq_commit_errors_total{class} label. Errno-bearing errors are matched through errors.Is
// so arbitrary wrapping does not hide them; SQLite's textual signatures cover the errno-less
// spellings modernc emits; anything else is unknown. The class is a label and a log field,
// never a control-flow decision.
func ClassifyStorageError(err error) string {
	if err == nil {
		return "unknown"
	}
	if errors.Is(err, syscall.EIO) {
		return "eio"
	}
	if errors.Is(err, syscall.ENOSPC) {
		return "enospc"
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "i/o error"), strings.Contains(msg, "input/output error"):
		return "eio"
	case strings.Contains(msg, "no space left"), strings.Contains(msg, "disk is full"):
		return "enospc"
	case strings.Contains(msg, "corrupt"), strings.Contains(msg, "malformed"),
		strings.Contains(msg, "not a database"), strings.Contains(msg, "encrypted"):
		return "corrupt"
	}
	return "unknown"
}
