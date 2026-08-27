// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/a-holm/messq/internal/cli/render"
	"github.com/a-holm/messq/pkg/client"
)

// Per-message records (issue #25 §9): human/table lines, ndjson frames and one
// end-of-run summary object under --output json. Field names are compatibility
// surface pinned at 1.0 by TestNDJSONRecordSchema.

// Record is the frozen NDJSON shape; JSON tags ARE the wire.
type Record struct {
	TS              string `json:"ts"`
	Stream          string `json:"stream"`
	Consumer        string `json:"consumer"`
	Seq             int64  `json:"seq"`
	MsgID           string `json:"msg_id"`
	Subject         string `json:"subject"`
	Attempt         int    `json:"attempt"`
	MaxDeliver      int    `json:"max_deliver"`
	ExitCode        int    `json:"exit_code"`
	Signal          string `json:"signal"`
	Outcome         string `json:"outcome"`
	DurationMS      int64  `json:"duration_ms"`
	RetryInMS       int64  `json:"retry_in_ms"`
	Reason          string `json:"reason"`
	ReasonTruncated bool   `json:"reason_truncated"`
	TraceID         string `json:"trace_id"`
}

// Summary closes the run under --output json ("a stream of records is what
// ndjson is for").
type Summary struct {
	Messages    int64  `json:"messages"`
	Acks        int64  `json:"acks"`
	Naks        int64  `json:"naks"`
	Terms       int64  `json:"terms"`
	LeaseLost   int64  `json:"lease_lost"`
	SpawnFailed int64  `json:"spawn_failed"`
	DurationMS  int64  `json:"duration_ms"`
	Stream      string `json:"stream"`
	Consumer    string `json:"consumer"`
}

// Emitter renders per-message records and the end-of-run summary through the
// resolved --output face. Safe under concurrency: the collector behind it is
// mutex-guarded because Worker handlers fan out.
type Emitter struct {
	w    io.Writer
	mode render.Format

	mu      sync.Mutex
	sum     Summary
	started time.Time
}

func NewEmitter(w io.Writer, mode render.Format) *Emitter {
	if w == nil {
		w = io.Discard
	}
	return &Emitter{w: w, mode: mode, started: time.UnixMilli(0)}
}

// Start anchors summary durations (Clock-seam injected by callers who care).
func (e *Emitter) Start(now time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.started = now
}

// Emit appends the classifier's verdict onto the counters and renders the
// record per the resolved face. Outcomes update counts BEFORE any formatting,
// so a formatting failure can never corrupt accounting.
func (e *Emitter) Emit(rec Record) error {
	e.mu.Lock()
	e.sum.Messages++
	switch rec.Outcome {
	case OutcomeAck.String():
		e.sum.Acks++
	case OutcomeTerm.String():
		e.sum.Terms++
	case OutcomeAbandon.String():
		e.sum.LeaseLost++
	default:
		e.sum.Naks++
	}
	e.mu.Unlock()

	switch e.mode {
	case render.FormatAuto:
		return errors.New("render: format was not resolved (call render.Resolve first)")
	case render.FormatTable:
		return e.emitHuman(rec)
	case render.FormatJSON:
		return json.NewEncoder(e.w).Encode(rec)
	case render.FormatNDJSON:
		b, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		_, err = e.w.Write(append(b, '\n'))
		return err
	default:
		return errors.New("render: format was not resolved (call render.Resolve first)")
	}
}

// Human face mirrors the §9 mock: ULID prefix, subject, attempt/x, the exit or
// signal column, the transition word, duration, and the quoted reason.
func (e *Emitter) emitHuman(r Record) error {
	id := r.MsgID
	if len(id) > 11 {
		id = id[:11] + "…"
	}
	exitCol := "exit " + strconv.Itoa(r.ExitCode)
	if r.Signal != "" {
		exitCol = r.Signal
	}
	line := fmt.Sprintf("%s  %s  attempt %d/%d  %-8s %-4s %dms",
		id, r.Subject, r.Attempt, r.MaxDeliver, exitCol, r.Outcome, r.DurationMS)
	if r.Reason != "" {
		line += "  \"" + r.Reason + "\""
	}
	if _, err := fmt.Fprintln(e.w, line); err != nil {
		return err
	}
	return nil
}

// SummaryNow returns a snapshot copy (tests assert counts between emits).
func (e *Emitter) SummaryNow() Summary {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.sum
}

// WriteSummary renders the end-of-run object: only --output json prints it;
// ndjson runs stay a stream of records, table runs close with the prose line.
func (e *Emitter) WriteSummary(elapsedMS int64) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.sum.DurationMS = elapsedMS
	switch e.mode {
	case render.FormatAuto:
		return errors.New("render: format was not resolved (call render.Resolve first)")
	case render.FormatJSON:
		return json.NewEncoder(e.w).Encode(e.sum)
	case render.FormatTable:
		_, err := fmt.Fprintf(e.w,
			"worker stopped: %d messages · %d ack · %d nak · %d term · %d lease-lost · %dms\n",
			e.sum.Messages, e.sum.Acks, e.sum.Naks, e.sum.Terms, e.sum.LeaseLost, elapsedMS)
		return err
	case render.FormatNDJSON:
		return nil // a stream of records is what ndjson is for (§9)
	default:
		return errors.New("render: format was not resolved (call render.Resolve first)")
	}
}

// recordFromResult translates one settled child into the frozen shape.
func recordFromResult(m *client.Delivered, res Result, durMS, retryMS int64, ts time.Time) Record {
	sig := ""
	if res.Signal != 0 {
		sig = signalName(res.Signal)
	}
	return Record{
		TS:              ts.UTC().Format("2006-01-02T15:04:05.000Z"),
		Stream:          m.Stream,
		Consumer:        m.Consumer,
		Seq:             m.Seq,
		MsgID:           m.ID,
		Subject:         m.Subject,
		Attempt:         m.Attempt,
		MaxDeliver:      m.MaxDeliver,
		ExitCode:        res.ExitCode,
		Signal:          sig,
		Outcome:         res.Outcome.String(),
		DurationMS:      durMS,
		RetryInMS:       retryMS,
		Reason:          res.Reason,
		ReasonTruncated: res.Truncated,
		TraceID:         m.TraceID,
	}
}
