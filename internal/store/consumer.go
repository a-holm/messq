// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/obs"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/subject"
)

// The consumer half of the delivery engine (issue #9 §2): configuration CRUD, the
// durable cursor and start-position resolution. Every state change rides the writer
// engine as a [Cmd] with its co-committed event row; every read runs on the fenced
// read pool. Claim/top-up/flow-control live in fetch.go; this file owns configuration
// and lifecycle.
//
// The consumers table has no FOREIGN KEY on stream, so existence and the mid-reap
// refusal are checked here, exactly like stream creation does.

// kind labels for the consumer commands. Carried, never switched on.
const (
	kindCreateConsumer CmdKind = "consumer.create"
	kindUpdateConsumer CmdKind = "consumer.update"
	kindDeleteConsumer CmdKind = "consumer.delete"
	kindSetPaused      CmdKind = "consumer.pause"
)

// metaConsumerStartPrefix names the meta keys holding each consumer's creation-time
// start position. start is immutable (issue §2 decision): it is recorded here so a
// differing re-supply can be refused, without a schema migration.
const metaConsumerStartPrefix = "cstart."

// consumerStartKey renders the meta key for one consumer's recorded start. The NUL
// separator cannot appear in either name (rule S11 bans control characters), so a
// stream name containing a '.' can never collide with a different (stream, name) pair.
func consumerStartKey(stream, name string) string {
	return metaConsumerStartPrefix + stream + "\x00" + name
}

// ConsumerInfo is the read shape of one consumer: its configuration, cursor and
// generation, plus the live derived statistics (§9 §8). The JSON field names are the
// CLI/HTTP contract and are golden-tested from the day they exist.
type ConsumerInfo struct {
	Stream         string   `json:"stream"`
	Name           string   `json:"name"`
	Filters        []string `json:"filters"`
	AckWaitMS      int64    `json:"ack_wait_ms"`
	MaxDeliver     int32    `json:"max_deliver"`
	MaxAckPending  int64    `json:"max_ack_pending"`
	BackoffMS      []int64  `json:"backoff_ms"`
	DeadPolicy     string   `json:"dead_policy"`
	Paused         bool     `json:"paused"`
	CursorSeq      int64    `json:"cursor_seq"`
	Generation     int64    `json:"generation"`
	CreatedAt      int64    `json:"created_at"`
	RetryHorizonMS int64    `json:"retry_horizon_ms"`

	// Derived statistics (§9 §8), defined once here so metrics (#21) and lag (#24)
	// can never disagree with the fetch path.
	Pending            int64  `json:"pending"`
	Inflight           int64  `json:"inflight"`
	ReadyNow           int64  `json:"ready_now"`
	InBackoff          int64  `json:"in_backoff"`
	Backlog            int64  `json:"backlog"`
	BacklogExact       bool   `json:"backlog_exact"`
	OldestPendingAgeMS int64  `json:"oldest_pending_age_ms"`
	BlockedBy          string `json:"blocked_by"`
}

// Config renders the stored configuration as the pure layer's value type.
func (i ConsumerInfo) Config() queue.ConsumerConfig {
	return queue.ConsumerConfig{
		Name:          i.Name,
		Filters:       i.Filters,
		AckWait:       msDuration(i.AckWaitMS),
		MaxDeliver:    i.MaxDeliver,
		MaxAckPending: i.MaxAckPending,
		Backoff:       msList(i.BackoffMS),
		DeadPolicy:    queue.DeadPolicy(i.DeadPolicy),
		Paused:        i.Paused,
	}
}

// ConsumerPatch carries the present fields of a PATCH /v1/streams/{s}/consumers/{c}
// body. A nil pointer leaves the stored value untouched; name, start, generation and
// paused are immutable here — paused changes through SetPaused, start through seek
// (#28), and name never.
type ConsumerPatch struct {
	Filters       *[]string         `json:"filters"`
	AckWaitMS     *int64            `json:"ack_wait_ms"`
	MaxDeliver    *int32            `json:"max_deliver"`
	MaxAckPending *int64            `json:"max_ack_pending"`
	BackoffMS     *[]int64          `json:"backoff_ms"`
	DeadPolicy    *queue.DeadPolicy `json:"dead_policy"`
}

// ConsumerCreateResult reports what one CreateConsumer did: a fresh insert, an
// idempotent no-op, or an update of a differing configuration.
type ConsumerCreateResult struct {
	Info     ConsumerInfo
	Warnings queue.Warnings
	Created  bool // new row inserted
	Updated  bool // existing row, configuration updated
}

// ConsumerDeleteResult reports what a confirmed consumer deletion removed.
type ConsumerDeleteResult struct {
	Pending  int64 `json:"pending"`
	Inflight int64 `json:"inflight"`
}

// ImmutableFieldError refuses an attempt to move an existing consumer's cursor via the
// creation-time start field. Moving a cursor is seek (#28), never a re-POST.
type ImmutableFieldError struct {
	Field string
}

func (e *ImmutableFieldError) Error() string {
	return fmt.Sprintf("%s is immutable after creation; use seek to move the cursor", e.Field)
}
func (e *ImmutableFieldError) Unwrap() error { return errs.ErrConflict }

// consumerCols is the projection every consumers-row reader uses, in scan order.
const consumerCols = `stream, name, filters, ack_wait_ms, max_deliver, max_ack_pending,` +
	` backoff_ms, dead_policy, cursor_seq, generation, paused, created_at`

// scanConsumerInfo scans one consumers row from any row source. The derived statistics
// are not filled here; call fillConsumerStats for the live numbers.
func scanConsumerInfo(row interface{ Scan(dest ...any) error }) (ConsumerInfo, error) {
	var info ConsumerInfo
	var filtersJSON, backoffJSON, deadPolicy string
	var paused int64
	if err := row.Scan(&info.Stream, &info.Name, &filtersJSON, &info.AckWaitMS, &info.MaxDeliver,
		&info.MaxAckPending, &backoffJSON, &deadPolicy, &info.CursorSeq, &info.Generation,
		&paused, &info.CreatedAt); err != nil {
		return ConsumerInfo{}, err
	}
	info.Filters = unmarshalStringList(filtersJSON)
	info.BackoffMS = unmarshalInt64List(backoffJSON)
	info.DeadPolicy = deadPolicy
	info.Paused = paused != 0
	info.RetryHorizonMS = queue.RetryHorizon(info.Config()).Milliseconds()
	return info, nil
}

// CreateConsumer validates and stores one consumer, idempotently: a fresh name inserts
// (cursor resolved from start, generation 1); an identical re-create returns the row
// with no write and no event; a differing configuration is applied as an update; a
// start that differs from the recorded creation value refuses with
// [ImmutableFieldError].
func (s *Store) CreateConsumer(ctx context.Context, stream string, cfg queue.ConsumerConfig, start queue.StartPosition, actor string) (ConsumerCreateResult, error) {
	if err := queue.ValidateExistingStreamName(stream); err != nil {
		return ConsumerCreateResult{}, err
	}
	if err := queue.ValidateDeadPolicyForStream(stream, cfg.DeadPolicy); err != nil {
		return ConsumerCreateResult{}, err
	}
	warnings, err := queue.ValidateConsumerConfig(cfg, s.consumerLimits)
	if err != nil {
		return ConsumerCreateResult{}, err
	}
	if start.Kind == "" {
		return ConsumerCreateResult{}, errs.E(errs.ErrBadRequest, "store.CreateConsumer",
			"start position is required")
	}
	res, err := s.enqueue(ctx, "store.CreateConsumer", createConsumerCmd{
		stream: stream, cfg: cfg, start: start, actor: actor, limits: s.consumerLimits,
	})
	if err != nil {
		return ConsumerCreateResult{}, err
	}
	cr, ok := res.(consumerCreateResult)
	if !ok {
		return ConsumerCreateResult{}, fmt.Errorf("store.CreateConsumer: engine returned %T, want consumerCreateResult", res)
	}
	cr.info, err = s.GetConsumer(ctx, stream, cfg.Name)
	if err != nil {
		return ConsumerCreateResult{}, err
	}
	return ConsumerCreateResult{Info: cr.info, Warnings: warnings, Created: cr.created, Updated: cr.updated}, nil
}

// UpdateConsumer applies a sparse patch to one consumer. Changing a field re-validates
// the composed configuration inside the transaction; lowering max_ack_pending below the
// current pending set runs the shrink (drop undelivered tail rows, rewind the cursor)
// described in §5. paused is not patchable here — SetPaused owns it.
func (s *Store) UpdateConsumer(ctx context.Context, stream, name string, p ConsumerPatch, actor string) (ConsumerInfo, error) {
	if err := queue.ValidateExistingStreamName(stream); err != nil {
		return ConsumerInfo{}, err
	}
	if err := queue.ValidateConsumerName(name); err != nil {
		return ConsumerInfo{}, err
	}
	res, err := s.enqueue(ctx, "store.UpdateConsumer", updateConsumerCmd{
		stream: stream, name: name, p: p, actor: actor, limits: s.consumerLimits,
	})
	if err != nil {
		return ConsumerInfo{}, err
	}
	if _, ok := res.(ConsumerInfo); !ok {
		return ConsumerInfo{}, fmt.Errorf("store.UpdateConsumer: engine returned %T, want ConsumerInfo", res)
	}
	return s.GetConsumer(ctx, stream, name)
}

// DeleteConsumer removes one consumer and all its delivery rows in one transaction,
// reporting how many of those rows were pending versus in flight. confirm semantics
// live in the HTTP layer; the store deletes what it is told.
func (s *Store) DeleteConsumer(ctx context.Context, stream, name, actor string) (ConsumerDeleteResult, error) {
	if err := queue.ValidateExistingStreamName(stream); err != nil {
		return ConsumerDeleteResult{}, err
	}
	if err := queue.ValidateConsumerName(name); err != nil {
		return ConsumerDeleteResult{}, err
	}
	res, err := s.enqueue(ctx, "store.DeleteConsumer", deleteConsumerCmd{stream: stream, name: name, actor: actor})
	if err != nil {
		return ConsumerDeleteResult{}, err
	}
	dr, ok := res.(ConsumerDeleteResult)
	if !ok {
		return ConsumerDeleteResult{}, fmt.Errorf("store.DeleteConsumer: engine returned %T, want ConsumerDeleteResult", res)
	}
	return dr, nil
}

// SetPaused flips a consumer's paused flag. Both directions emit consumer.pause with
// detail.paused carrying the new value (the event vocabulary is closed and has no
// consumer.resume). An unchanged flag is a no-op with no event.
func (s *Store) SetPaused(ctx context.Context, stream, name string, paused bool, actor string) (ConsumerInfo, error) {
	if err := queue.ValidateExistingStreamName(stream); err != nil {
		return ConsumerInfo{}, err
	}
	if err := queue.ValidateConsumerName(name); err != nil {
		return ConsumerInfo{}, err
	}
	res, err := s.enqueue(ctx, "store.SetPaused", setPausedCmd{stream: stream, name: name, paused: paused, actor: actor})
	if err != nil {
		return ConsumerInfo{}, err
	}
	if _, ok := res.(setPausedResult); !ok {
		return ConsumerInfo{}, fmt.Errorf("store.SetPaused: engine returned %T, want setPausedResult", res)
	}
	return s.GetConsumer(ctx, stream, name)
}

// GetConsumer reads one consumer's configuration and derived statistics from the
// read-only pool. A missing consumer is errs.ErrNotFound.
func (s *Store) GetConsumer(ctx context.Context, stream, name string) (ConsumerInfo, error) {
	if err := queue.ValidateExistingStreamName(stream); err != nil {
		return ConsumerInfo{}, err
	}
	if err := queue.ValidateConsumerName(name); err != nil {
		return ConsumerInfo{}, err
	}
	ro := s.readPool()
	info, err := scanConsumerInfo(ro.QueryRowContext(ctx,
		`SELECT `+consumerCols+` FROM consumers WHERE stream = ? AND name = ?`, stream, name))
	if errors.Is(err, sql.ErrNoRows) {
		return ConsumerInfo{}, errs.E(errs.ErrNotFound, "store.GetConsumer",
			"consumer %q/%q does not exist", stream, name)
	}
	if err != nil {
		return ConsumerInfo{}, fmt.Errorf("read consumer %q/%q: %w", stream, name, err)
	}
	if err := s.fillConsumerStats(ctx, ro, &info); err != nil {
		return ConsumerInfo{}, err
	}
	return info, nil
}

// ListConsumers reads every consumer of one stream in name order, statistics filled.
func (s *Store) ListConsumers(ctx context.Context, stream string) ([]ConsumerInfo, error) {
	if err := queue.ValidateExistingStreamName(stream); err != nil {
		return nil, err
	}
	ro := s.readPool()
	rows, err := ro.QueryContext(ctx,
		`SELECT `+consumerCols+` FROM consumers WHERE stream = ? ORDER BY name`, stream)
	if err != nil {
		return nil, fmt.Errorf("list consumers of %q: %w", stream, err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			s.logger.Warn("store.ListConsumers", "error", cerr.Error())
		}
	}()
	var out []ConsumerInfo
	for rows.Next() {
		info, scanErr := scanConsumerInfo(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scan consumer of %q: %w", stream, scanErr)
		}
		out = append(out, info)
	}
	if rErr := rows.Err(); rErr != nil {
		return nil, fmt.Errorf("iterate consumers of %q: %w", stream, rErr)
	}
	for i := range out {
		if err := s.fillConsumerStats(ctx, ro, &out[i]); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// createConsumerCmd stores one consumer, resolving the start position to a cursor and
// co-committing consumer.create. Idempotency and the immutability refusal run here,
// against the authoritative row.
type createConsumerCmd struct {
	stream string
	cfg    queue.ConsumerConfig
	start  queue.StartPosition
	actor  string
	limits queue.ConsumerLimits
}

type consumerCreateResult struct {
	info    ConsumerInfo
	created bool
	updated bool
}

func (c createConsumerCmd) Kind() CmdKind { return kindCreateConsumer }
func (c createConsumerCmd) Bytes() int    { return 0 }

func (c createConsumerCmd) Apply(ctx context.Context, tx *sql.Tx, now time.Time) (Result, []obs.Event, error) {
	ts := now.UnixMilli()
	if err := refuseDuringReap(ctx, tx, c.stream); err != nil {
		return nil, nil, maybeCmdErr(err)
	}
	var streamExists int
	if err := tx.QueryRowContext(ctx, `SELECT 1 FROM streams WHERE name = ?`, c.stream).Scan(&streamExists); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil, maybeCmdErr(errs.E(errs.ErrNotFound, "store.CreateConsumer",
				"stream %q does not exist", c.stream))
		}
		return nil, nil, fmt.Errorf("read stream %q: %w", c.stream, err)
	}

	existing, err := scanConsumerInfo(tx.QueryRowContext(ctx,
		`SELECT `+consumerCols+` FROM consumers WHERE stream = ? AND name = ? COLLATE NOCASE`,
		c.stream, c.cfg.Name))
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return c.applyInsert(ctx, tx, ts)
	case err != nil:
		return nil, nil, fmt.Errorf("read existing consumer: %w", err)
	}

	// Existing name. start is immutable: compare the supplied start against the one
	// recorded at creation, refusing a move (that is seek's job).
	if immErr := c.checkStartImmutable(ctx, tx); immErr != nil {
		return nil, nil, maybeCmdErr(immErr)
	}

	old := existing.Config()
	diff := consumerConfigDiff(old, c.cfg)
	if len(diff) == 0 {
		return consumerCreateResult{info: existing}, nil, nil // idempotent, no event
	}
	updated, _, ev, err := c.applyUpdate(ctx, tx, existing, diff, ts)
	if err != nil {
		return nil, nil, err
	}
	return consumerCreateResult{info: updated, updated: true}, []obs.Event{ev}, nil
}

// applyInsert writes a fresh consumer row and its consumer.create event.
func (c createConsumerCmd) applyInsert(ctx context.Context, tx *sql.Tx, ts int64) (Result, []obs.Event, error) {
	cursor, err := resolveStartPosition(ctx, tx, c.stream, c.start)
	if err != nil {
		return nil, nil, maybeCmdErr(err)
	}
	if _, xErr := tx.ExecContext(ctx, `INSERT INTO consumers
		(stream, name, filters, ack_wait_ms, max_deliver, max_ack_pending, backoff_ms,
		 dead_policy, cursor_seq, generation, paused, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)`,
		c.stream, c.cfg.Name, marshalStringList(c.cfg.Filters), c.cfg.AckWait.Milliseconds(),
		c.cfg.MaxDeliver, c.cfg.MaxAckPending, marshalInt64List(msDurations(c.cfg.Backoff)),
		string(c.cfg.DeadPolicy), cursor, boolInt(c.cfg.Paused), ts); xErr != nil {
		return nil, nil, fmt.Errorf("insert consumer %q/%q: %w", c.stream, c.cfg.Name, xErr)
	}
	if _, xErr := tx.ExecContext(ctx,
		`INSERT INTO meta (k, v) VALUES (?, ?)`, consumerStartKey(c.stream, c.cfg.Name), c.start.String()); xErr != nil {
		return nil, nil, fmt.Errorf("record start of %q/%q: %w", c.stream, c.cfg.Name, xErr)
	}
	ev, evErr := commitEvent(ctx, tx, event{
		ts:       ts,
		name:     "consumer.create",
		stream:   nullStr(c.stream),
		actor:    nullStr(c.actor),
		consumer: nullStr(c.cfg.Name),
		detail:   nullStr(fmt.Sprintf(`{"cursor_seq":%d,"start":%q}`, cursor, c.start.String())),
	})
	if evErr != nil {
		return nil, nil, evErr
	}
	info := ConsumerInfo{
		Stream: c.stream, Name: c.cfg.Name, Filters: c.cfg.Filters,
		AckWaitMS: c.cfg.AckWait.Milliseconds(), MaxDeliver: c.cfg.MaxDeliver,
		MaxAckPending: c.cfg.MaxAckPending, BackoffMS: msDurations(c.cfg.Backoff),
		DeadPolicy: string(c.cfg.DeadPolicy), Paused: c.cfg.Paused,
		CursorSeq: cursor, Generation: 1, CreatedAt: ts,
		RetryHorizonMS: queue.RetryHorizon(c.cfg).Milliseconds(),
	}
	return consumerCreateResult{info: info, created: true}, []obs.Event{ev}, nil
}

// checkStartImmutable refuses a re-POST whose start differs from the recorded creation
// value. The event.subject convention is not used; the consumer name rides in subject
// because §9.2's consumer events name the consumer there.
func (c createConsumerCmd) checkStartImmutable(ctx context.Context, tx *sql.Tx) error {
	var recorded string
	err := tx.QueryRowContext(ctx,
		`SELECT v FROM meta WHERE k = ?`, consumerStartKey(c.stream, c.cfg.Name)).Scan(&recorded)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil // no recorded start (e.g. a consumer created before this rule): nothing to move
	case err != nil:
		return fmt.Errorf("read recorded start of %q/%q: %w", c.stream, c.cfg.Name, err)
	}
	if recorded != c.start.String() {
		return &ImmutableFieldError{Field: "start"}
	}
	return nil
}

// applyUpdate applies a differing configuration to an existing consumer and returns the
// new read shape, the changed-field list and the co-committed consumer.update event.
func (c createConsumerCmd) applyUpdate(ctx context.Context, tx *sql.Tx, existing ConsumerInfo, diff []string, ts int64) (ConsumerInfo, []string, obs.Event, error) {
	next := c.cfg
	if err := queue.ValidateDeadPolicyForStream(c.stream, next.DeadPolicy); err != nil {
		return ConsumerInfo{}, nil, obs.Event{}, maybeCmdErr(err)
	}
	next.Paused = existing.Paused // paused is operational state, not patchable config
	if _, err := queue.ValidateConsumerConfig(next, c.limits); err != nil {
		return ConsumerInfo{}, nil, obs.Event{}, maybeCmdErr(err)
	}
	if _, xErr := tx.ExecContext(ctx, `UPDATE consumers SET
		filters = ?, ack_wait_ms = ?, max_deliver = ?, max_ack_pending = ?, backoff_ms = ?, dead_policy = ?
		WHERE stream = ? AND name = ?`,
		marshalStringList(next.Filters), next.AckWait.Milliseconds(), next.MaxDeliver,
		next.MaxAckPending, marshalInt64List(msDurations(next.Backoff)), string(next.DeadPolicy),
		c.stream, c.cfg.Name); xErr != nil {
		return ConsumerInfo{}, nil, obs.Event{}, fmt.Errorf("update consumer %q/%q: %w", c.stream, c.cfg.Name, xErr)
	}
	raw, jErr := jsonMarshal(map[string]any{"fields": diff})
	if jErr != nil {
		raw = []byte(`{}`)
	}
	ev, evErr := commitEvent(ctx, tx, event{
		ts:       ts,
		name:     "consumer.update",
		stream:   nullStr(c.stream),
		consumer: nullStr(c.cfg.Name),
		actor:    nullStr(c.actor),
		detail:   nullStr(string(raw)),
	})
	if evErr != nil {
		return ConsumerInfo{}, nil, obs.Event{}, evErr
	}
	info := existing
	info.Filters = next.Filters
	info.AckWaitMS = next.AckWait.Milliseconds()
	info.MaxDeliver = next.MaxDeliver
	info.MaxAckPending = next.MaxAckPending
	info.BackoffMS = msDurations(next.Backoff)
	info.DeadPolicy = string(next.DeadPolicy)
	info.RetryHorizonMS = queue.RetryHorizon(next).Milliseconds()
	return info, diff, ev, nil
}

// updateConsumerCmd applies a sparse PATCH inside the transaction.
type updateConsumerCmd struct {
	stream string
	name   string
	p      ConsumerPatch
	actor  string
	limits queue.ConsumerLimits
}

func (c updateConsumerCmd) Kind() CmdKind { return kindUpdateConsumer }
func (c updateConsumerCmd) Bytes() int    { return 0 }

func (c updateConsumerCmd) Apply(ctx context.Context, tx *sql.Tx, now time.Time) (Result, []obs.Event, error) {
	ts := now.UnixMilli()
	existing, err := scanConsumerInfo(tx.QueryRowContext(ctx,
		`SELECT `+consumerCols+` FROM consumers WHERE stream = ? AND name = ?`, c.stream, c.name))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, maybeCmdErr(errs.E(errs.ErrNotFound, "store.UpdateConsumer",
			"consumer %q/%q does not exist", c.stream, c.name))
	}
	if err != nil {
		return nil, nil, fmt.Errorf("read consumer %q/%q: %w", c.stream, c.name, err)
	}

	next, fields := applyConsumerPatch(existing.Config(), c.p)
	if len(fields) == 0 {
		return existing, nil, nil // empty patch: no write, no event
	}
	if dpErr := queue.ValidateDeadPolicyForStream(c.stream, next.DeadPolicy); dpErr != nil {
		return nil, nil, maybeCmdErr(dpErr)
	}
	if _, vErr := queue.ValidateConsumerConfig(next, c.limits); vErr != nil {
		return nil, nil, maybeCmdErr(vErr)
	}

	// Shrink first (G6): lowering max_ack_pending below the current pending set drops
	// only READY∧attempts=0 tail rows and rewinds the cursor, before the new bound is
	// stored, so the row never observes pending > max_ack_pending beyond a deliberate
	// shrink's residue.
	var overshoot int64
	if c.p.MaxAckPending != nil && *c.p.MaxAckPending < existing.MaxAckPending {
		overshoot, err = shrinkDeliveries(ctx, tx, c.stream, c.name, *c.p.MaxAckPending)
		if err != nil {
			return nil, nil, err
		}
	}

	if _, xErr := tx.ExecContext(ctx, `UPDATE consumers SET
		filters = ?, ack_wait_ms = ?, max_deliver = ?, max_ack_pending = ?, backoff_ms = ?, dead_policy = ?
		WHERE stream = ? AND name = ?`,
		marshalStringList(next.Filters), next.AckWait.Milliseconds(), next.MaxDeliver,
		next.MaxAckPending, marshalInt64List(msDurations(next.Backoff)), string(next.DeadPolicy),
		c.stream, c.name); xErr != nil {
		return nil, nil, fmt.Errorf("update consumer %q/%q: %w", c.stream, c.name, xErr)
	}

	detail := map[string]any{"fields": fields}
	if c.p.MaxAckPending != nil && *c.p.MaxAckPending < existing.MaxAckPending {
		detail["overshoot"] = overshoot
	}
	raw, jErr := jsonMarshal(detail)
	if jErr != nil {
		raw = []byte(`{}`)
	}
	ev, evErr := commitEvent(ctx, tx, event{
		ts:       ts,
		name:     "consumer.update",
		stream:   nullStr(c.stream),
		consumer: nullStr(c.name),
		actor:    nullStr(c.actor),
		detail:   nullStr(string(raw)),
	})
	if evErr != nil {
		return nil, nil, evErr
	}

	info := existing
	info.Filters = next.Filters
	info.AckWaitMS = next.AckWait.Milliseconds()
	info.MaxDeliver = next.MaxDeliver
	info.MaxAckPending = next.MaxAckPending
	info.BackoffMS = msDurations(next.Backoff)
	info.DeadPolicy = string(next.DeadPolicy)
	info.RetryHorizonMS = queue.RetryHorizon(next).Milliseconds()
	return info, []obs.Event{ev}, nil
}

// applyConsumerPatch lays the present patch fields over old and names what moved.
func applyConsumerPatch(old queue.ConsumerConfig, p ConsumerPatch) (queue.ConsumerConfig, []string) {
	next := old
	var fields []string
	if p.Filters != nil {
		next.Filters = *p.Filters
		fields = append(fields, "filters")
	}
	if p.AckWaitMS != nil {
		next.AckWait = msDuration(*p.AckWaitMS)
		fields = append(fields, "ack_wait")
	}
	if p.MaxDeliver != nil {
		next.MaxDeliver = *p.MaxDeliver
		fields = append(fields, "max_deliver")
	}
	if p.MaxAckPending != nil {
		next.MaxAckPending = *p.MaxAckPending
		fields = append(fields, "max_ack_pending")
	}
	if p.BackoffMS != nil {
		next.Backoff = msList(*p.BackoffMS)
		fields = append(fields, "backoff")
	}
	if p.DeadPolicy != nil {
		next.DeadPolicy = *p.DeadPolicy
		fields = append(fields, "dead_policy")
	}
	return next, fields
}

// deleteConsumerCmd removes one consumer and its deliveries atomically.
type deleteConsumerCmd struct {
	stream string
	name   string
	actor  string
}

func (c deleteConsumerCmd) Kind() CmdKind { return kindDeleteConsumer }
func (c deleteConsumerCmd) Bytes() int    { return 0 }

func (c deleteConsumerCmd) Apply(ctx context.Context, tx *sql.Tx, now time.Time) (Result, []obs.Event, error) {
	ts := now.UnixMilli()
	var exists int
	if err := tx.QueryRowContext(ctx,
		`SELECT 1 FROM consumers WHERE stream = ? AND name = ?`, c.stream, c.name).Scan(&exists); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil, maybeCmdErr(errs.E(errs.ErrNotFound, "store.DeleteConsumer",
				"consumer %q/%q does not exist", c.stream, c.name))
		}
		return nil, nil, fmt.Errorf("read consumer %q/%q: %w", c.stream, c.name, err)
	}
	var pending, inflight int64
	if err := tx.QueryRowContext(ctx,
		`SELECT count(*), coalesce(sum(state), 0) FROM deliveries WHERE stream = ? AND consumer = ?`,
		c.stream, c.name).Scan(&pending, &inflight); err != nil {
		return nil, nil, fmt.Errorf("count deliveries of %q/%q: %w", c.stream, c.name, err)
	}
	if _, xErr := tx.ExecContext(ctx,
		`DELETE FROM deliveries WHERE stream = ? AND consumer = ?`, c.stream, c.name); xErr != nil {
		return nil, nil, fmt.Errorf("delete deliveries of %q/%q: %w", c.stream, c.name, xErr)
	}
	if _, xErr := tx.ExecContext(ctx,
		`DELETE FROM consumers WHERE stream = ? AND name = ?`, c.stream, c.name); xErr != nil {
		return nil, nil, fmt.Errorf("delete consumer %q/%q: %w", c.stream, c.name, xErr)
	}
	if _, xErr := tx.ExecContext(ctx,
		`DELETE FROM meta WHERE k = ?`, consumerStartKey(c.stream, c.name)); xErr != nil {
		return nil, nil, fmt.Errorf("drop start record of %q/%q: %w", c.stream, c.name, xErr)
	}
	raw, jErr := jsonMarshal(map[string]int64{"pending": pending, "inflight": inflight})
	if jErr != nil {
		raw = []byte(`{}`)
	}
	ev, evErr := commitEvent(ctx, tx, event{
		ts:       ts,
		name:     "consumer.delete",
		stream:   nullStr(c.stream),
		consumer: nullStr(c.name),
		actor:    nullStr(c.actor),
		detail:   nullStr(string(raw)),
	})
	if evErr != nil {
		return nil, nil, evErr
	}
	return ConsumerDeleteResult{Pending: pending, Inflight: inflight}, []obs.Event{ev}, nil
}

// setPausedCmd flips a consumer's paused flag. Both directions emit consumer.pause
// with detail.paused carrying the new value.
type setPausedCmd struct {
	stream string
	name   string
	paused bool
	actor  string
}

type setPausedResult struct{}

func (c setPausedCmd) Kind() CmdKind { return kindSetPaused }
func (c setPausedCmd) Bytes() int    { return 0 }

func (c setPausedCmd) Apply(ctx context.Context, tx *sql.Tx, now time.Time) (Result, []obs.Event, error) {
	ts := now.UnixMilli()
	var paused int64
	if err := tx.QueryRowContext(ctx,
		`SELECT paused FROM consumers WHERE stream = ? AND name = ?`, c.stream, c.name).Scan(&paused); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil, maybeCmdErr(errs.E(errs.ErrNotFound, "store.SetPaused",
				"consumer %q/%q does not exist", c.stream, c.name))
		}
		return nil, nil, fmt.Errorf("read consumer %q/%q: %w", c.stream, c.name, err)
	}
	if (paused != 0) == c.paused {
		return setPausedResult{}, nil, nil // no change: no write, no event
	}
	if _, xErr := tx.ExecContext(ctx,
		`UPDATE consumers SET paused = ? WHERE stream = ? AND name = ?`,
		boolInt(c.paused), c.stream, c.name); xErr != nil {
		return nil, nil, fmt.Errorf("pause consumer %q/%q: %w", c.stream, c.name, xErr)
	}
	ev, evErr := commitEvent(ctx, tx, event{
		ts:       ts,
		name:     "consumer.pause",
		stream:   nullStr(c.stream),
		consumer: nullStr(c.name),
		actor:    nullStr(c.actor),
		detail:   nullStr(fmt.Sprintf(`{"paused":%t}`, c.paused)),
	})
	if evErr != nil {
		return nil, nil, evErr
	}
	return setPausedResult{}, []obs.Event{ev}, nil
}

// resolveStartPosition maps a creation-time start position to a cursor_seq, clamped to
// [first_seq, stream_seq.next]. first_seq is the stream's oldest live message (its
// stream_seq.next when empty).
func resolveStartPosition(ctx context.Context, tx *sql.Tx, stream string, start queue.StartPosition) (int64, error) {
	first, next, err := streamBounds(ctx, tx, stream)
	if err != nil {
		return 0, err
	}
	switch start.Kind {
	case queue.StartFirst:
		return first, nil
	case queue.StartNew:
		return next, nil
	case queue.StartSeq:
		return clampCursor(start.Seq, first, next), nil
	case queue.StartTime:
		return resolveTimeStart(ctx, tx, stream, start.Time, next)
	default:
		return 0, errs.E(errs.ErrBadRequest, "", "start kind %q is not known", start.Kind)
	}
}

// streamBounds reads the two anchors a start position resolves against: the oldest
// live message sequence and stream_seq.next.
func streamBounds(ctx context.Context, tx *sql.Tx, stream string) (first, next int64, err error) {
	var firstN sql.Null[int64]
	if err := tx.QueryRowContext(ctx,
		`SELECT min(seq) FROM messages WHERE stream = ?`, stream).Scan(&firstN); err != nil {
		return 0, 0, fmt.Errorf("read first_seq of %q: %w", stream, err)
	}
	if err := tx.QueryRowContext(ctx,
		`SELECT next FROM stream_seq WHERE stream = ?`, stream).Scan(&next); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, 0, errs.E(errs.ErrNotFound, "store.resolveStart", "stream %q does not exist", stream)
		}
		return 0, 0, fmt.Errorf("read seq counter of %q: %w", stream, err)
	}
	if firstN.Valid {
		return firstN.V, next, nil
	}
	return next, next, nil // empty stream: first and head coincide at next
}

// clampCursor bounds seq into [first, next].
func clampCursor(seq, first, next int64) int64 {
	if seq < first {
		return first
	}
	if seq > next {
		return next
	}
	return seq
}

// resolveTimeStart finds the first sequence published at or after t via the
// messages_age index; a t past the head yields stream_seq.next.
func resolveTimeStart(ctx context.Context, tx *sql.Tx, stream string, t, next int64) (int64, error) {
	var seq sql.Null[int64]
	if err := tx.QueryRowContext(ctx,
		`SELECT seq FROM messages WHERE stream = ? AND published_at >= ?
		 ORDER BY published_at, seq LIMIT 1`, stream, t).Scan(&seq); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("resolve time start of %q: %w", stream, err)
	}
	if !seq.Valid {
		return next, nil
	}
	return seq.V, nil
}

// consumerConfigDiff names every config field where two configurations disagree.
// Paused is excluded: it is operational state changed through SetPaused, never a
// configuration diff.
func consumerConfigDiff(old, next queue.ConsumerConfig) []string {
	var diff []string
	if marshalStringList(old.Filters) != marshalStringList(next.Filters) {
		diff = append(diff, "filters")
	}
	if old.AckWait != next.AckWait {
		diff = append(diff, "ack_wait")
	}
	if old.MaxDeliver != next.MaxDeliver {
		diff = append(diff, "max_deliver")
	}
	if old.MaxAckPending != next.MaxAckPending {
		diff = append(diff, "max_ack_pending")
	}
	if !slices.Equal(old.Backoff, next.Backoff) {
		diff = append(diff, "backoff")
	}
	if old.DeadPolicy != next.DeadPolicy {
		diff = append(diff, "dead_policy")
	}
	return diff
}

// fillConsumerStats loads the derived statistics for one consumer on any querier (the
// read pool for reads, the transaction for fetch). All values match the §9 §8
// definitions; blocked_by follows the precedence list.
func (s *Store) fillConsumerStats(ctx context.Context, q querier, info *ConsumerInfo) error {
	now := nowMS(s.clk)
	var pending, inflight, readyNow, oldest sql.Null[int64]
	if err := q.QueryRowContext(ctx, `
		SELECT count(*), coalesce(sum(d.state), 0),
		       coalesce(sum(d.state = 0 AND d.visible_at <= ?3), 0),
		       coalesce(min(m.published_at), 0)
		  FROM deliveries d JOIN messages m ON m.stream = d.stream AND m.seq = d.seq
		 WHERE d.stream = ?1 AND d.consumer = ?2`,
		info.Stream, info.Name, now).Scan(&pending, &inflight, &readyNow, &oldest); err != nil {
		return fmt.Errorf("stats of %q/%q: %w", info.Stream, info.Name, err)
	}
	info.Pending = pending.V
	info.Inflight = inflight.V
	info.ReadyNow = readyNow.V
	info.InBackoff = info.Pending - info.Inflight - info.ReadyNow

	var next int64
	if err := q.QueryRowContext(ctx,
		`SELECT next FROM stream_seq WHERE stream = ?`, info.Stream).Scan(&next); err != nil &&
		!errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read head of %q: %w", info.Stream, err)
	}
	info.Backlog, info.BacklogExact = consumerBacklog(ctx, q, info.Stream, info.CursorSeq, next, info.Filters)
	if info.Pending == 0 {
		info.OldestPendingAgeMS = 0
	} else {
		info.OldestPendingAgeMS = now - oldest.V
	}
	info.BlockedBy = consumerBlockedBy(info.Paused, info.Pending, info.ReadyNow,
		info.CursorSeq, next, info.MaxAckPending)
	return nil
}

// consumerBacklog computes pending + the count of not-yet-admitted matching messages.
// It is exact for the default [">"] filter (stream_seq.next - cursor_seq) and for
// literal-only filters (summed over the messages_subj index); otherwise it is an upper
// bound (all messages at or above the cursor) with exact=false.
func consumerBacklog(ctx context.Context, q querier, stream string, cursorSeq, next int64, filters []string) (int64, bool) {
	remaining := next - cursorSeq
	if remaining < 0 {
		remaining = 0
	}
	if len(filters) == 1 && filters[0] == ">" {
		return remaining, true
	}
	set, err := subject.ParseSet(filters)
	if err != nil {
		return remaining, false
	}
	var literals []string
	for _, f := range set.Strings() {
		p, pErr := subject.ParsePattern(f)
		if pErr != nil || !p.IsLiteral() {
			return remaining, false // upper bound
		}
		literals = append(literals, f)
	}
	var matched int64
	for _, lit := range literals {
		var n int64
		if qErr := q.QueryRowContext(ctx,
			`SELECT count(*) FROM messages WHERE stream = ? AND subject = ? AND seq >= ?`,
			stream, lit, cursorSeq).Scan(&n); qErr != nil {
			return remaining, false
		}
		matched += n
	}
	return matched, true
}

// consumerBlockedBy returns why a consumer is not making progress, first match wins
// (issue §9 §8): paused → flow_control → backoff → catching_up → empty → none.
func consumerBlockedBy(paused bool, pending, readyNow, cursorSeq, next, bound int64) string {
	switch {
	case paused:
		return "paused"
	case bound > 0 && pending >= bound:
		return "flow_control"
	case pending > 0 && readyNow == 0:
		return "backoff"
	case pending == 0 && cursorSeq < next:
		return "catching_up"
	case pending == 0:
		return "empty"
	default:
		return "none"
	}
}

// shrinkDeliveries converges a lowered max_ack_pending (G6): it drops READY∧attempts=0
// rows from the tail (highest seq first) until the pending set fits the new bound, then
// rewinds cursor_seq to the lowest dropped seq so the dropped rows — never delivered,
// carrying no attempts or backoff state — are re-admitted later. The returned overshoot
// is the residue (INFLIGHT or in-backoff rows) still above the bound; those drain as
// work settles and are reported as an advisory, never a violation (I5).
func shrinkDeliveries(ctx context.Context, tx *sql.Tx, stream, name string, bound int64) (int64, error) {
	var pending, droppable int64
	if err := tx.QueryRowContext(ctx,
		`SELECT count(*) FROM deliveries WHERE stream = ? AND consumer = ?`, stream, name).Scan(&pending); err != nil {
		return 0, fmt.Errorf("count deliveries of %q/%q: %w", stream, name, err)
	}
	if pending <= bound {
		return 0, nil
	}
	excess := pending - bound
	if err := tx.QueryRowContext(ctx,
		`SELECT count(*) FROM deliveries WHERE stream = ? AND consumer = ? AND state = 0 AND attempts = 0`,
		stream, name).Scan(&droppable); err != nil {
		return 0, fmt.Errorf("count droppable deliveries of %q/%q: %w", stream, name, err)
	}
	dropped := excess
	if dropped > droppable {
		dropped = droppable
	}
	if dropped == 0 {
		return excess, nil
	}
	// The lowest dropped seq is the rewind target: after dropping the highest `dropped`
	// droppable rows, it is the (droppable - dropped)-th lowest, i.e. offset that far in
	// ascending order.
	var lowest int64
	if err := tx.QueryRowContext(ctx, `
		SELECT seq FROM deliveries
		 WHERE stream = ? AND consumer = ? AND state = 0 AND attempts = 0
		 ORDER BY seq ASC LIMIT 1 OFFSET ?`,
		stream, name, droppable-dropped).Scan(&lowest); err != nil {
		return 0, fmt.Errorf("read rewind seq of %q/%q: %w", stream, name, err)
	}
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM deliveries
		 WHERE stream = ? AND consumer = ? AND state = 0 AND attempts = 0
		   AND seq IN (SELECT seq FROM deliveries
		                WHERE stream = ? AND consumer = ? AND state = 0 AND attempts = 0
		                ORDER BY seq DESC LIMIT ?)`,
		stream, name, stream, name, dropped); err != nil {
		return 0, fmt.Errorf("shrink deliveries of %q/%q: %w", stream, name, err)
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE consumers SET cursor_seq = ? WHERE stream = ? AND name = ?`, lowest, stream, name); err != nil {
		return 0, fmt.Errorf("rewind cursor of %q/%q: %w", stream, name, err)
	}
	return excess - dropped, nil
}

// JSON-list helpers: filters marshal sorted+deduplicated (order is not configuration,
// equal sets compare equal and goldens stay stable); backoff preserves order (it is a
// sequence, order is semantic).
func marshalStringList(s []string) string {
	sorted := slices.Clone(s)
	slices.Sort(sorted)
	sorted = slices.Compact(sorted)
	raw, err := json.Marshal(sorted)
	if err != nil {
		return "[]"
	}
	return string(raw)
}

func unmarshalStringList(raw string) []string {
	var out []string
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return []string{"<corrupt: " + raw + ">"}
	}
	return out
}

func marshalInt64List(s []int64) string {
	raw, err := json.Marshal(s)
	if err != nil {
		return "[]"
	}
	return string(raw)
}

func unmarshalInt64List(raw string) []int64 {
	var out []int64
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil
	}
	return out
}

func msDurations(ds []time.Duration) []int64 {
	out := make([]int64, len(ds))
	for i, d := range ds {
		out[i] = d.Milliseconds()
	}
	return out
}

func msList(ms []int64) []time.Duration {
	out := make([]time.Duration, len(ms))
	for i, v := range ms {
		out[i] = time.Duration(v) * time.Millisecond
	}
	return out
}

func boolInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
