// SPDX-License-Identifier: Apache-2.0

package ledger

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/a-holm/messq/internal/clock"
)

// The group-commit half of the ledger. It mirrors internal/store's single-writer engine in
// exactly one respect — a 2 ms commit window and a 256-record batch cap, so a concurrent
// load does not pay one fsync per attempt — and is commented here as such because the
// harness is forbidden to look like the SUT anywhere else. The intent record is durable
// BEFORE the request is sent: without that ordering an in-flight publish is
// indistinguishable from one never attempted, and the reconciler's GHOST rule loses its
// meaning (issue §Design-2).

// ErrClosed is returned by every Ledger method after Close has begun its shutdown.
var ErrClosed = errors.New("ledger is closed")

const (
	defaultInterval = 2 * time.Millisecond
	defaultMaxBatch = 256
)

// Outcome carries the fields a resolved attempt contributes back to its record: the
// server's receipt on OK, or the status/code of a rejection.
type Outcome struct {
	Seq       uint64
	ID        string
	Duplicate bool
	Status    int    // HTTP status; 0 = transport failure
	Code      string // error-envelope code, or transport error
}

// Config tunes the group commit. Zero values mean the defaults.
type Config struct {
	// Interval is the commit window: a partial batch lingers this long for more arrivals
	// before it is flushed. 0 = 2 ms.
	Interval time.Duration
	// MaxBatch caps records per flush. 0 = 256.
	MaxBatch int
}

func (c *Config) applyDefaults() {
	if c.Interval <= 0 {
		c.Interval = defaultInterval
	}
	if c.MaxBatch <= 0 {
		c.MaxBatch = defaultMaxBatch
	}
}

// appendReq is one unit of work for the committer goroutine. wait is nil for a Resolve
// (fire-and-forget, durable by the next Sync) and non-nil for an Attempt or Sync, which
// block until their flush returns. sync marks a flush-now request that writes no record.
type appendReq struct {
	rec  Record
	wait chan error
	sync bool
}

// Ledger is an append-only, group-fsynced NDJSON oracle file, safe for concurrent use.
type Ledger struct {
	f         *os.File
	filePath  string
	w         *bufio.Writer
	syncFn    func() error // the fsync seam: f.Sync in production, a fake in tests
	cfg       Config
	clk       clock.Clock
	reqs      chan appendReq
	stop      chan struct{}
	committed chan struct{}

	mu     sync.Mutex
	latest map[string]Record // last full record per key, for Resolve reconstruction

	closeOnce sync.Once
	closeErr  error
}

// Open creates (or appends to) the ledger file at path and starts its committer.
func Open(path string, cfg Config, clk clock.Clock) (*Ledger, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open ledger %s: %w", path, err)
	}
	return newLedger(f, cfg, clk, f.Sync), nil
}

// newLedger assembles a Ledger over an already-open file with an injected fsync seam.
func newLedger(f *os.File, cfg Config, clk clock.Clock, syncFn func() error) *Ledger {
	cfg.applyDefaults()
	l := &Ledger{
		f:         f,
		filePath:  f.Name(),
		w:         bufio.NewWriter(f),
		syncFn:    syncFn,
		cfg:       cfg,
		clk:       clk,
		reqs:      make(chan appendReq),
		stop:      make(chan struct{}),
		committed: make(chan struct{}),
		latest:    make(map[string]Record),
	}
	go l.run()
	return l
}

// path reports the ledger file's path, so tests can Replay it. Not part of the public
// API surface: the driver owns the path and never asks the ledger for it.
func (l *Ledger) path() string { return l.filePath }

// Attempt appends an intent record and returns only once it is durably flushed. The caller
// sends the request only after Attempt returns, which is the ordering rule.
func (l *Ledger) Attempt(r Record) error {
	l.setLatest(r)
	wait := make(chan error, 1)
	if err := l.send(appendReq{rec: r, wait: wait}); err != nil {
		return err
	}
	return <-wait
}

// Resolve appends the outcome for key (whose intent Attempt already recorded) and does not
// wait for a flush: the outcome is durable by the next Sync. It fails for a key with no
// recorded intent.
func (l *Ledger) Resolve(key string, v Verdict, o Outcome) error {
	l.mu.Lock()
	rec, ok := l.latest[key]
	if !ok {
		l.mu.Unlock()
		return fmt.Errorf("ledger: resolve %q: no intent record", key)
	}
	rec.Verdict = v
	rec.Seq = o.Seq
	rec.ID = o.ID
	rec.Duplicate = o.Duplicate
	rec.Status = o.Status
	rec.Code = o.Code
	l.latest[key] = rec
	l.mu.Unlock()
	return l.send(appendReq{rec: rec})
}

// Sync flushes any pending records and fsyncs, returning once the file is durable. It is
// called before every kill and before every reconciliation.
func (l *Ledger) Sync() error {
	wait := make(chan error, 1)
	if err := l.send(appendReq{sync: true, wait: wait}); err != nil {
		return err
	}
	return <-wait
}

// setLatest records the full intent record in memory so a later Resolve can reconstruct
// the complete record (stream, subject, size, cycle, sent-at) rather than only the key.
func (l *Ledger) setLatest(r Record) {
	l.mu.Lock()
	l.latest[r.Key] = r
	l.mu.Unlock()
}

// send delivers a request to the committer, or fails with ErrClosed once Close has begun.
func (l *Ledger) send(req appendReq) error {
	select {
	case l.reqs <- req:
		return nil
	case <-l.stop:
		return ErrClosed
	}
}

// run is the committer goroutine: it assembles batches under the window/batch closing
// rules, flushes them, and delivers each waiter its result. A batch is never empty — it
// blocks for the first request — and a Sync request closes a batch immediately.
func (l *Ledger) run() {
	defer close(l.committed)
	for {
		first, ok := l.receive()
		if !ok {
			return
		}
		if first.sync {
			l.flush([]appendReq{first})
			continue
		}
		batch := []appendReq{first}
		timer := l.clk.NewTimer(l.cfg.Interval)
		flushNow := false
		for !flushNow && len(batch) < l.cfg.MaxBatch {
			select {
			case r := <-l.reqs:
				batch = append(batch, r)
				if r.sync {
					flushNow = true
				}
			case <-l.stop:
				flushNow = true
			case <-timer.C():
				flushNow = true
			}
		}
		timer.Stop()
		l.flush(batch)
	}
}

// receive blocks for the next request until the committer is stopped.
func (l *Ledger) receive() (appendReq, bool) {
	select {
	case r := <-l.reqs:
		return r, true
	case <-l.stop:
		return appendReq{}, false
	}
}

// flush writes every record in the batch, flushes the buffer and fsyncs, then delivers the
// shared result to every waiter. A sync-only request writes nothing but still forces the
// flush, so an empty ledger Sync still fsyncs.
func (l *Ledger) flush(batch []appendReq) {
	var err error
	for _, r := range batch {
		if r.sync {
			continue
		}
		if _, werr := l.w.Write(encode(r.rec)); werr != nil && err == nil {
			err = werr
		}
	}
	if err == nil {
		if ferr := l.w.Flush(); ferr != nil {
			err = ferr
		} else {
			err = l.syncFn()
		}
	}
	for _, r := range batch {
		if r.wait != nil {
			r.wait <- err
		}
	}
}

// Close flushes pending records durably, stops the committer and closes the file. It is
// idempotent and safe to call concurrently with a final Sync.
func (l *Ledger) Close() error {
	l.closeOnce.Do(func() {
		syncErr := l.Sync()
		close(l.stop)
		<-l.committed
		l.closeErr = errors.Join(syncErr, l.f.Close())
	})
	return l.closeErr
}
