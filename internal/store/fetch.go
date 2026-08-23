// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/obs"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/subject"
)

// The delivery command (issue #9 §3–§5): one Fetch is one writer command that runs the
// top-up (T1), the claim (T2) and the co-committed msg.deliver/flow.blocked events in
// ONE transaction. Payload bodies are read from the read pool only after the commit
// returned, so an 8 MiB BLOB never sits inside the writer's commit window.

// kindFetch labels the fetch command in logs. Carried, never interpreted.
const kindFetch CmdKind = "consumer.fetch"

// HoldReason names why a fetch's batch came back short or empty. The empty value means
// "no hold" (a full batch); the closed set is frozen here so #14's long-poll and the
// wire enum cannot drift. "backoff" is reserved for #11 and never emitted by this issue.
type HoldReason string

// The hold reasons this issue emits (plus the reserved "backoff" for #11).
const (
	HoldNone        HoldReason = ""
	HoldPaused      HoldReason = "paused"
	HoldFlowControl HoldReason = "flow_control"
	HoldBackoff     HoldReason = "backoff"
	HoldCatchingUp  HoldReason = "catching_up"
	HoldEmpty       HoldReason = "empty"
)

// FetchReq is one single-shot fetch request (issue §3). Batch and MaxBytes are clamped
// to the process limits; wait_ms is a #14 concern and does not appear here.
type FetchReq struct {
	Stream   string
	Consumer string
	Batch    int   // 1..--max-fetch-batch
	MaxBytes int64 // 0 = --fetch-max-bytes
}

// Delivered is one claimed message's wire shape. Body holds the raw bytes; the
// encoding/json package marshals []byte to a base64 string, so the wire field body_b64
// is exactly that encoding. Headers is nil when the message has none.
type Delivered struct {
	Stream      string            `json:"stream"`
	Consumer    string            `json:"consumer"`
	Seq         int64             `json:"seq"`
	ID          string            `json:"id"`
	Subject     string            `json:"subject"`
	Headers     map[string]string `json:"headers,omitempty"`
	Body        []byte            `json:"body_b64"`
	Size        int64             `json:"size"`
	Attempt     int32             `json:"attempt"`
	MaxDeliver  int32             `json:"max_deliver"`
	AckToken    string            `json:"ack_token"`
	DeadlineMS  int64             `json:"deadline_ms"`
	AckWaitMS   int64             `json:"ack_wait_ms"`
	PublishedAt int64             `json:"published_at"`
	TraceID     string            `json:"trace_id"`
}

// FetchResult is the outcome of one Fetch (issue §3 §9).
type FetchResult struct {
	Messages      []Delivered `json:"messages"`
	Hold          HoldReason  `json:"hold"`
	CursorSeq     int64       `json:"cursor_seq"`
	Pending       int64       `json:"pending"`
	Inflight      int64       `json:"inflight"`
	Backlog       int64       `json:"backlog"`
	MaxAckPending int64       `json:"max_ack_pending,omitempty"`
}

// claimedDelivery is the metadata of one claimed row, enough to mint its token and read
// its body after the commit.
type claimedDelivery struct {
	seq        int64
	attempt    int32
	deadlineMS int64
	ackWaitMS  int64
}

// fetchResult is the command's internal outcome. Bodies are filled afterwards.
type fetchResult struct {
	stream        string
	consumer      string
	claimed       []claimedDelivery
	hold          HoldReason
	cursorSeq     int64
	pending       int64
	inflight      int64
	backlog       int64
	maxAckPending int64 // populated when hold == flow_control
	generation    int64
	maxDeliver    int32
	ackWaitMS     int64
	conflicts     int64 // ON CONFLICT count from top-up; non-zero is a verify C3 signal
}

// flowKey is the flow.blocked rate-limit map key for one consumer.
func flowKey(stream, consumer string) string { return stream + "\x00" + consumer }

// Fetch runs one single-shot delivery command and then reads the claimed payloads from
// the read pool. It never blocks, never sleeps and never registers a waiter.
func (s *Store) Fetch(ctx context.Context, req FetchReq) (FetchResult, error) {
	if err := queue.ValidateExistingStreamName(req.Stream); err != nil {
		return FetchResult{}, err
	}
	if err := queue.ValidateConsumerName(req.Consumer); err != nil {
		return FetchResult{}, err
	}
	if req.Batch <= 0 {
		return FetchResult{}, errs.E(errs.ErrBadRequest, "store.Fetch",
			"batch is %d, want >= 1", req.Batch)
	}
	batch := req.Batch
	if batch > s.consumerLimits.MaxFetchBatch {
		batch = s.consumerLimits.MaxFetchBatch
	}
	maxBytes := req.MaxBytes
	if maxBytes <= 0 {
		maxBytes = s.consumerLimits.FetchMaxBytes
	}

	res, err := s.enqueue(ctx, "store.Fetch", fetchCmd{
		stream: req.Stream, consumer: req.Consumer, batch: batch, maxBytes: maxBytes,
		limits: s.consumerLimits, flow: s.flowBlocked,
	})
	if err != nil {
		return FetchResult{}, err
	}
	fr, ok := res.(fetchResult)
	if !ok {
		return FetchResult{}, fmt.Errorf("store.Fetch: engine returned %T, want fetchResult", res)
	}

	msgs := s.readClaimedPayloads(ctx, fr.stream, fr.consumer, fr.claimed, fr.generation, fr.maxDeliver, fr.ackWaitMS)
	return FetchResult{
		Messages: msgs, Hold: fr.hold, CursorSeq: fr.cursorSeq,
		Pending: fr.pending, Inflight: fr.inflight, Backlog: fr.backlog,
		MaxAckPending: fr.maxAckPending,
	}, nil
}

// fetchCmd is the single writer command behind Fetch.
type fetchCmd struct {
	stream   string
	consumer string
	batch    int
	maxBytes int64
	limits   queue.ConsumerLimits
	flow     map[string]int64
}

func (c fetchCmd) Kind() CmdKind { return kindFetch }
func (c fetchCmd) Bytes() int    { return c.batch * 256 } // row + event per claimed message

// consumerState is the authoritative consumers-row slice a fetch needs.
type consumerState struct {
	filters       []string
	ackWaitMS     int64
	maxDeliver    int32
	maxAckPending int64
	paused        bool
	cursorSeq     int64
	generation    int64
}

func (c fetchCmd) Apply(ctx context.Context, tx *sql.Tx, now time.Time) (Result, []obs.Event, error) {
	nowMS := now.UnixMilli()
	st, err := loadConsumerTx(ctx, tx, c.stream, c.consumer)
	if err != nil {
		return nil, nil, maybeCmdErr(err)
	}
	if st.paused {
		return fetchResult{
			stream: c.stream, consumer: c.consumer, hold: HoldPaused,
			cursorSeq: st.cursorSeq, generation: st.generation,
			maxDeliver: st.maxDeliver, ackWaitMS: st.ackWaitMS,
		}, nil, nil
	}

	var streamNext int64
	if snErr := tx.QueryRowContext(ctx,
		`SELECT next FROM stream_seq WHERE stream = ?`, c.stream).Scan(&streamNext); snErr != nil {
		if errors.Is(snErr, sql.ErrNoRows) {
			return nil, nil, maybeCmdErr(errs.E(errs.ErrNotFound, "store.Fetch",
				"stream %q does not exist", c.stream))
		}
		return nil, nil, fmt.Errorf("read head of %q: %w", c.stream, snErr)
	}

	pending, _, err := countDeliveries(ctx, tx, c.stream, c.consumer)
	if err != nil {
		return nil, nil, err
	}

	var events []obs.Event
	cursorSeq := st.cursorSeq
	conflicts := int64(0)

	if pending >= st.maxAckPending {
		if flowBlockedDue(c.flow, flowKey(c.stream, c.consumer), nowMS, c.limits.FlowBlockedInterval.Milliseconds()) {
			ev, evErr := commitEvent(ctx, tx, event{
				ts: nowMS, name: "flow.blocked",
				stream: nullStr(c.stream), consumer: nullStr(c.consumer),
				detail: nullStr(fmt.Sprintf(`{"pending":%d,"max_ack_pending":%d}`, pending, st.maxAckPending)),
			})
			if evErr != nil {
				return nil, nil, evErr
			}
			events = append(events, ev)
		}
	} else {
		cursorSeq, conflicts, _, err = topUp(ctx, tx, c.stream, c.consumer, st, pending, streamNext, c.limits.ScanLimit)
		if err != nil {
			return nil, nil, err
		}
	}

	claimed, err := claimReady(ctx, tx, c.stream, c.consumer, nowMS, st.ackWaitMS, c.batch, c.maxBytes)
	if err != nil {
		return nil, nil, err
	}

	// msg.deliver per claimed row, same transaction (D11).
	for _, d := range claimed {
		ev, evErr := commitEvent(ctx, tx, event{
			ts: nowMS, name: "msg.deliver",
			stream: nullStr(c.stream), consumer: nullStr(c.consumer),
			seq: nullI64(d.seq), attempt: nullI64(int64(d.attempt)),
			detail: nullStr(fmt.Sprintf(`{"attempt":%d,"deadline_ms":%d}`, d.attempt, d.deadlineMS)),
		})
		if evErr != nil {
			return nil, nil, evErr
		}
		events = append(events, ev)
	}

	var inflight int64
	pending, inflight, err = countDeliveries(ctx, tx, c.stream, c.consumer)
	if err != nil {
		return nil, nil, err
	}
	backlog, _ := consumerBacklog(ctx, tx, c.stream, cursorSeq, streamNext, st.filters)

	hold := fetchHold(st.paused, len(claimed) == c.batch, pending, cursorSeq, streamNext, st.maxAckPending)
	res := fetchResult{
		stream: c.stream, consumer: c.consumer, claimed: claimed, hold: hold,
		cursorSeq: cursorSeq, pending: pending, inflight: inflight, backlog: backlog,
		conflicts: conflicts, generation: st.generation, maxDeliver: st.maxDeliver, ackWaitMS: st.ackWaitMS,
	}
	if hold == HoldFlowControl {
		res.maxAckPending = st.maxAckPending
	}
	return res, events, nil
}

// loadConsumerTx reads the authoritative consumers-row slice inside the transaction.
func loadConsumerTx(ctx context.Context, tx *sql.Tx, stream, name string) (consumerState, error) {
	var st consumerState
	var filtersJSON string
	var paused int64
	err := tx.QueryRowContext(ctx, `
		SELECT filters, ack_wait_ms, max_deliver, max_ack_pending, paused, cursor_seq, generation
		  FROM consumers WHERE stream = ? AND name = ?`, stream, name).
		Scan(&filtersJSON, &st.ackWaitMS, &st.maxDeliver, &st.maxAckPending, &paused, &st.cursorSeq, &st.generation)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return st, errs.E(errs.ErrNotFound, "store.Fetch",
			"consumer %q/%q does not exist", stream, name)
	case err != nil:
		return st, fmt.Errorf("read consumer %q/%q: %w", stream, name, err)
	}
	st.filters = unmarshalStringList(filtersJSON)
	st.paused = paused != 0
	return st, nil
}

// countDeliveries returns pending (all rows) and inflight (state = 1) for one consumer.
func countDeliveries(ctx context.Context, tx *sql.Tx, stream, consumer string) (pending, inflight int64, err error) {
	err = tx.QueryRowContext(ctx,
		`SELECT count(*), coalesce(sum(state), 0) FROM deliveries WHERE stream = ? AND consumer = ?`,
		stream, consumer).Scan(&pending, &inflight)
	if err != nil {
		return 0, 0, fmt.Errorf("count deliveries of %q/%q: %w", stream, consumer, err)
	}
	return pending, inflight, nil
}

// topUp runs transition T1: it scans messages from the cursor, inserts a READY delivery
// row for every matching subject while pending < max_ack_pending, and advances the
// cursor past everything decided. It returns the new cursor, the ON CONFLICT count, and
// whether a flow-control-declined match pinned the cursor (so the caller knows to
// re-read the pending set for the hold reason).
func topUp(ctx context.Context, tx *sql.Tx, stream, consumer string, st consumerState, pending, streamNext int64, scanLimit int) (int64, int64, bool, error) {
	set, err := subject.ParseSet(st.filters)
	if err != nil {
		return 0, 0, false, err // validated at creation; unreachable
	}
	cursor := st.cursorSeq
	bound := st.maxAckPending
	conflicts := int64(0)
	declined := false
	declinedSeq := int64(0)

	rows, qErr := tx.QueryContext(ctx,
		`SELECT seq, subject FROM messages WHERE stream = ? AND seq >= ? ORDER BY seq LIMIT ?`,
		stream, cursor, scanLimit)
	if qErr != nil {
		return 0, 0, false, fmt.Errorf("scan messages for %q/%q: %w", stream, consumer, qErr)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			_ = cerr // best-effort: the caller's error already carries the story
		}
	}()

	scanned := 0
	lastExamined := int64(0)
	for rows.Next() {
		var seq int64
		var subj string
		if sErr := rows.Scan(&seq, &subj); sErr != nil {
			return 0, 0, false, fmt.Errorf("scan message of %q: %w", stream, sErr)
		}
		scanned++
		lastExamined = seq
		if !set.Match(subj) {
			continue
		}
		if pending >= bound {
			declined = true
			declinedSeq = seq
			break
		}
		ins, iErr := tx.ExecContext(ctx, `
			INSERT INTO deliveries (stream, consumer, seq, subject, state, attempts, visible_at, generation, delivered_at)
			VALUES (?, ?, ?, ?, 0, 0, 0, ?, NULL)
			ON CONFLICT (stream, consumer, seq) DO NOTHING`,
			stream, consumer, seq, subj, st.generation)
		if iErr != nil {
			return 0, 0, false, fmt.Errorf("admit delivery %q/%q seq %d: %w", stream, consumer, seq, iErr)
		}
		if affected, aErr := ins.RowsAffected(); aErr != nil {
			return 0, 0, false, fmt.Errorf("count admit of %q/%q seq %d: %w", stream, consumer, seq, aErr)
		} else if affected == 0 {
			conflicts++
		} else {
			pending++
		}
	}
	if rErr := rows.Err(); rErr != nil {
		return 0, 0, false, fmt.Errorf("iterate messages for %q/%q: %w", stream, consumer, rErr)
	}

	var nextCursor int64
	switch {
	case declined:
		nextCursor = declinedSeq // flow control must delay, never drop
	case scanned == scanLimit:
		nextCursor = lastExamined + 1
	default:
		nextCursor = streamNext
	}
	if nextCursor < cursor {
		nextCursor = cursor // never rewind (C4: cursor <= next, and the short-scan jump is at most head)
	}

	if _, uErr := tx.ExecContext(ctx,
		`UPDATE consumers SET cursor_seq = ? WHERE stream = ? AND name = ?`, nextCursor, stream, consumer); uErr != nil {
		return 0, 0, false, fmt.Errorf("advance cursor of %q/%q: %w", stream, consumer, uErr)
	}

	return nextCursor, conflicts, declined, nil
}

// claimReady runs transition T2: it selects the READY rows in ascending seq, applies
// the byte budget (always returning at least one row even if it alone exceeds the
// budget), then claims them with post-increment attempts in one statement.
func claimReady(ctx context.Context, tx *sql.Tx, stream, consumer string, nowMS, ackWaitMS int64, batch int, maxBytes int64) ([]claimedDelivery, error) {
	candidates, err := readClaimCandidates(ctx, tx, stream, consumer, nowMS, batch)
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return nil, nil
	}

	// Byte budget: stop before exceeding, but always return at least one row.
	var chosen []int64
	var budget int64
	for _, cd := range candidates {
		if len(chosen) > 0 && budget+cd.size > maxBytes {
			break
		}
		chosen = append(chosen, cd.seq)
		budget += cd.size
	}

	attemptBySeq, err := applyClaim(ctx, tx, stream, consumer, nowMS, ackWaitMS, chosen)
	if err != nil {
		return nil, err
	}
	out := make([]claimedDelivery, 0, len(chosen))
	for _, s := range chosen {
		out = append(out, claimedDelivery{
			seq: s, attempt: attemptBySeq[s],
			deadlineMS: nowMS + ackWaitMS, ackWaitMS: ackWaitMS,
		})
	}
	return out, nil
}

// readClaimCandidates lists the claimable READY rows in ascending seq.
func readClaimCandidates(ctx context.Context, tx *sql.Tx, stream, consumer string, nowMS int64, batch int) (out []claimCandidate, err error) {
	rows, qErr := tx.QueryContext(ctx, `
		SELECT d.seq, m.size
		  FROM deliveries d JOIN messages m ON m.stream = d.stream AND m.seq = d.seq
		 WHERE d.stream = ? AND d.consumer = ? AND d.state = 0 AND d.visible_at <= ?
		 ORDER BY d.seq LIMIT ?`, stream, consumer, nowMS, batch)
	if qErr != nil {
		return nil, fmt.Errorf("list claim candidates of %q/%q: %w", stream, consumer, qErr)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("close candidates of %q/%q: %w", stream, consumer, cerr)
		}
	}()
	for rows.Next() {
		var cd claimCandidate
		if sErr := rows.Scan(&cd.seq, &cd.size); sErr != nil {
			return nil, fmt.Errorf("scan candidate of %q/%q: %w", stream, consumer, sErr)
		}
		out = append(out, cd)
	}
	if rErr := rows.Err(); rErr != nil {
		return nil, fmt.Errorf("iterate candidates of %q/%q: %w", stream, consumer, rErr)
	}
	return out, nil
}

// claimCandidate is one READY row eligible for a claim.
type claimCandidate struct {
	seq  int64
	size int64
}

// applyClaim flips the chosen rows to INFLIGHT with post-increment attempts in one
// statement, returning each row's post-increment attempt count.
func applyClaim(ctx context.Context, tx *sql.Tx, stream, consumer string, nowMS, ackWaitMS int64, chosen []int64) (attemptBySeq map[int64]int32, err error) {
	ph := strings.TrimSuffix(strings.Repeat("?,", len(chosen)), ",")
	args := []any{nowMS + ackWaitMS, nowMS, stream, consumer}
	for _, s := range chosen {
		args = append(args, s)
	}
	//nolint:gosec // G202: the only interpolated fragment is a compile-time-generated
	// placeholder list ("?,?,…"); caller data reaches SQLite through bound args only.
	rows, qErr := tx.QueryContext(ctx, `
		UPDATE deliveries SET state = 1, attempts = attempts + 1, visible_at = ?, delivered_at = ?
		 WHERE stream = ? AND consumer = ? AND seq IN (`+ph+`)
		 RETURNING seq, attempts`, args...)
	if qErr != nil {
		return nil, fmt.Errorf("claim deliveries of %q/%q: %w", stream, consumer, qErr)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("close claims of %q/%q: %w", stream, consumer, cerr)
		}
	}()
	attemptBySeq = make(map[int64]int32, len(chosen))
	for rows.Next() {
		var seq int64
		var attempts int32
		if sErr := rows.Scan(&seq, &attempts); sErr != nil {
			return nil, fmt.Errorf("scan claim of %q/%q: %w", stream, consumer, sErr)
		}
		attemptBySeq[seq] = attempts
	}
	if rErr := rows.Err(); rErr != nil {
		return nil, fmt.Errorf("iterate claims of %q/%q: %w", stream, consumer, rErr)
	}
	return attemptBySeq, nil
}

// fetchHold names why a fetch's batch came back short (issue §9 §3): full batch → none;
// paused → paused; at the bound → flow_control; nothing pending and still behind the
// head → catching_up; nothing pending at the head → empty; otherwise none.
func fetchHold(paused bool, full bool, pending, cursorSeq, next, bound int64) HoldReason {
	if full {
		return HoldNone
	}
	if paused {
		return HoldPaused
	}
	if bound > 0 && pending >= bound {
		return HoldFlowControl
	}
	if pending == 0 {
		if cursorSeq < next {
			return HoldCatchingUp
		}
		return HoldEmpty
	}
	return HoldNone
}

// flowBlockedDue reports whether a flow.blocked event is due for this consumer now and
// records the emission. Single-writer-goroutine only: the map needs no lock.
func flowBlockedDue(m map[string]int64, key string, nowMS, intervalMS int64) bool {
	if m == nil {
		return true
	}
	last, ok := m[key]
	if ok && nowMS-last < intervalMS {
		return false
	}
	m[key] = nowMS
	return true
}

// readClaimedPayloads reads the bodies and identities of the claimed seqs from the read
// pool, after the commit. A claimed row whose message is gone (an orphan from a purge
// that did not drop delivery rows) is omitted and logged; verify C2 catches the purge.
func (s *Store) readClaimedPayloads(ctx context.Context, stream, consumer string, claimed []claimedDelivery, generation int64, maxDeliver int32, ackWaitMS int64) []Delivered {
	if len(claimed) == 0 {
		return nil
	}
	seqs := make([]int64, len(claimed))
	for i, c := range claimed {
		seqs[i] = c.seq
	}
	ph := strings.TrimSuffix(strings.Repeat("?,", len(seqs)), ",")
	args := []any{stream}
	for _, s := range seqs {
		args = append(args, s)
	}
	ro := s.readPool()
	//nolint:gosec // G202: placeholder list only; caller data reaches SQLite via bound args.
	rows, err := ro.QueryContext(ctx, `
		SELECT seq, id, subject, hdr, body, size, published_at, trace_id
		  FROM messages WHERE stream = ? AND seq IN (`+ph+`)`, args...)
	if err != nil {
		s.logger.Warn("store.fetch", "error", fmt.Sprintf("read payloads of %q: %v", stream, err))
		return nil
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			s.logger.Warn("store.fetch", "error", cerr.Error())
		}
	}()
	bySeq := make(map[int64]Message, len(seqs))
	for rows.Next() {
		var m Message
		var hdr sql.Null[string]
		if sErr := rows.Scan(&m.Seq, &m.ID, &m.Subject, &hdr, &m.Body, &m.Size, &m.PublishedAt, &m.TraceID); sErr != nil {
			s.logger.Warn("store.fetch", "error", fmt.Sprintf("scan payload of %q: %v", stream, sErr))
			continue
		}
		if hdr.Valid {
			var headers map[string]string
			if err := json.Unmarshal([]byte(hdr.V), &headers); err != nil {
				s.logger.Error("store.fetch", "error", fmt.Sprintf("bad hdr on %q seq %d: %v", stream, m.Seq, err))
				continue
			}
			m.Headers = headers
		}
		bySeq[m.Seq] = m
	}
	if rErr := rows.Err(); rErr != nil {
		s.logger.Warn("store.fetch", "error", fmt.Sprintf("iterate payloads of %q: %v", stream, rErr))
	}

	out := make([]Delivered, 0, len(claimed))
	for _, c := range claimed {
		m, ok := bySeq[c.seq]
		if !ok {
			s.logger.Warn("store.fetch", "stream", stream, "seq", c.seq, "error", "orphan claimed row: message gone")
			continue
		}
		//nolint:gosec // G115: generation is a small monotone counter, far below int32 max.
		tok := queue.Token{Stream: stream, Consumer: consumer, Seq: c.seq, Attempt: c.attempt, Generation: int32(generation)}
		out = append(out, Delivered{
			Stream: stream, Consumer: consumer, Seq: m.Seq, ID: m.ID, Subject: m.Subject,
			Headers: m.Headers, Body: m.Body, Size: m.Size,
			Attempt: c.attempt, MaxDeliver: maxDeliver,
			AckToken: tok.String(), DeadlineMS: c.deadlineMS, AckWaitMS: c.ackWaitMS,
			PublishedAt: m.PublishedAt, TraceID: m.TraceID,
		})
	}
	return out
}
