// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"fmt"
	"io"
	"strings"
	"sync"
)

// The one-time exit-code hint (issue #25 §9): printed to stderr on the FIRST
// non-zero child exit of a run, exactly once, suppressed by --no-hints. Text is
// the generated source bound to docs/exit-codes.md — TestHintMatchesDocs fails
// the moment they drift apart (G9, executed claim).
const hintText = `hint: with --exec, the child's exit code is the ack decision:
        0    ack    done
        75   nak    EX_TEMPFAIL — retry with the consumer's backoff  [1s 5s 30s 2m 10m]
        65   term   EX_DATAERR  — permanent, straight to <stream>.dlq
        else nak    treated like 75 and reported as an unexpected failure
      the child's stderr becomes the failure reason: messq trace <msg-id>
      this hint prints once per run.  messq help exit-codes`

// HintPrinter prints the §9 explainer once per RUN (not per message), guarded
// against concurrent settles racing for the honour.
type HintPrinter struct {
	w          io.Writer // may be nil: printing becomes a no-op
	suppressed bool
	once       sync.Once
}

func NewHintPrinter(w io.Writer, suppressed bool) *HintPrinter {
	return &HintPrinter{w: w, suppressed: suppressed}
}

// MarkFailing marks that a failing child was seen; the next safe point prints.
// Splitting mark/print keeps Handle free of write-error policy while never
// losing the once-per-run guarantee.
func (h *HintPrinter) PrintOnce() {
	if h == nil || h.suppressed || h.w == nil {
		return
	}
	h.once.Do(func() {
		fmt.Fprintln(h.w, strings.TrimRight(hintText, "\n"))
	})
}

var _ = fmt.Sprintf
