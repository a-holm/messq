// SPDX-License-Identifier: Apache-2.0

package obs

import "time"

// CommitObserver is the metrics seam of the group-commit engine (#6). The writer calls it;
// an implementation turns the calls into instruments. The Prometheus adapter lives here next
// to the interface because metric registration is this package's job alone (see the forbidigo
// rules); #21 owns registry wiring, naming policy sign-off and the golden exposition test.
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
