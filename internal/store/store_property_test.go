// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/a-holm/messq/internal/clock"
	"pgregory.net/rapid"
)

// slogDiscard keeps the machine's recovery log lines out of test output.
func slogDiscard() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// A light rapid state machine over the storage lifecycle {open fresh, write deliveries,
// close clean, die and reopen}, holding every transition to the invariants #13 will scale
// to a full reference model: schema_version constant at 1, node_id immutable forever,
// attempts never decreasing across a reopen (D6/T9), no INFLIGHT row surviving an open,
// and RecoveryReport counts agreeing with the tables they describe. Real files, real
// fsyncs, deterministic clock — the storage-shaped subset of #13 that can exist today.

// propStore is one machine instance: a single data directory walked through as many
// open/write/close/die generations as rapid cares to draw.
type propStore struct {
	ctx      context.Context
	dir      string
	st       *Store
	w        *sql.DB          // the writer handle while this generation holds it (nil until taken)
	took     bool             // TakeWriter succeeded on the current generation
	dirty    bool             // last generation ended other than by an untaken-writer clean Close
	inflight int              // INFLIGHT rows the database currently holds (machine-side count)
	attempts map[string]int64 // delivery key -> attempts as written; never rewritten
	nextSeq  int
	nodeID   string
}

func (m *propStore) opts() Options {
	return Options{
		DataDir: m.dir,
		Clock:   clock.System{},
		Logger:  slogDiscard(),
		NewID:   nil, // default generator over Clock; node_id immutability is what we assert
	}
}

// open opens the directory (fresh or after any prior ending), asserts the report against
// the machine's expectations, and claims the writer handle for the generation.
func (m *propStore) open(t *rapid.T) {
	if m.st != nil {
		t.Skip("store already open")
	}
	st, report, err := Open(m.ctx, m.opts())
	if err != nil {
		t.Fatalf("open %s: %v", m.dir, err)
	}
	m.st = st

	if report.SchemaTo != 2 || report.SchemaFrom > report.SchemaTo {
		t.Errorf("schema moved %d -> %d, want monotone into 2", report.SchemaFrom, report.SchemaTo)
	}
	if got := st.SchemaVersion(); got != 2 {
		t.Errorf("SchemaVersion() = %d, want 2", got)
	}
	if report.Unclean != m.dirty {
		t.Errorf("Unclean = %v, want %v (previous generation ended dirty=%v)", report.Unclean, m.dirty, m.dirty)
	}
	if m.dirty && report.CheckKind != checkQuickCheck {
		t.Errorf("dirty reopen CheckKind = %q, want quick_check", report.CheckKind)
	}
	if !m.dirty && report.CheckKind != checkSkipped {
		t.Errorf("clean reopen CheckKind = %q, want skipped", report.CheckKind)
	}
	if report.Reclaimed != int64(m.inflight) {
		t.Errorf("Reclaimed = %d, want %d (the INFLIGHT rows this directory held)", report.Reclaimed, m.inflight)
	}
	if report.DedupExpired != 0 {
		t.Errorf("DedupExpired = %d, want 0 (machine publishes no dedup keys)", report.DedupExpired)
	}

	if id := st.NodeID(); m.nodeID == "" {
		m.nodeID = id
	} else if id != m.nodeID {
		t.Errorf("node_id changed from %s to %s across a reopen", m.nodeID, id)
	}

	w, err := st.TakeWriter()
	if err != nil {
		t.Fatalf("take writer: %v", err)
	}
	m.w, m.took = w, true

	// The core post-open invariant: nothing INFLIGHT survives recovery.
	var inflightLeft int64
	if scanErr := m.st.RO().QueryRowContext(m.ctx,
		`SELECT count(*) FROM deliveries WHERE state = 1`).Scan(&inflightLeft); scanErr != nil {
		t.Fatalf("count surviving INFLIGHT rows: %v", scanErr)
	}
	if inflightLeft != 0 {
		t.Fatalf("%d INFLIGHT rows survived an open — T9 reclaim failed", inflightLeft)
	}
	m.inflight = 0
	m.dirty = false // an Open always re-dirties via the marker write; tracked at close instead
	m.dirty = true
}

// writeDeliveries inserts a few delivery rows committed one transaction each, mixing READY
// and INFLIGHT states and assorted attempt counts.
func (m *propStore) writeDeliveries(t *rapid.T) {
	if m.st == nil || !m.took {
		t.Skip("no writer handle held")
	}
	n := rapid.IntRange(1, 4).Draw(t, "n")
	for i := 0; i < n; i++ {
		seq := m.nextSeq
		state := int64(rapid.IntRange(0, 1).Draw(t, "state"))
		attempts := int64(rapid.IntRange(0, 3).Draw(t, "attempts"))

		tx, err := m.w.BeginTx(m.ctx, nil)
		if err != nil {
			t.Fatalf("begin insert tx: %v", err)
		}
		_, err = tx.ExecContext(m.ctx,
			`INSERT INTO deliveries (stream, consumer, seq, subject, state, attempts, visible_at, generation, delivered_at, last_reason)
			 VALUES ('prop', 'c0', ?, ?, ?, ?, 0, 1, NULL, NULL)`,
			seq, "prop.subject."+strconv.Itoa(seq), state, attempts)
		if err == nil {
			err = tx.Commit()
		} else {
			if rbErr := tx.Rollback(); rbErr != nil {
				t.Fatalf("rollback after failed insert: %v", rbErr)
			}
		}
		if err != nil {
			t.Fatalf("insert delivery %d: %v", seq, err)
		}

		m.attempts["prop/c0/"+strconv.Itoa(seq)] = attempts
		if state == 1 {
			m.inflight++
		}
		m.nextSeq++
	}
}

// closeClean closes a generation whose writer was never taken: only then does Close own
// the rw-dependent steps and get to stamp clean_shutdown=1.
func (m *propStore) closeClean(t *rapid.T) {
	if m.st == nil || m.took {
		t.Skip("clean close needs an untouched writer")
	}
	if err := m.st.Close(m.ctx); err != nil {
		t.Fatalf("close clean: %v", err)
	}
	m.st = nil
}

// closeAfterWrites closes a generation whose writer WAS taken. By the handed-off rule the
// owner closed the handle, so Close deliberately skips the marker write: the directory
// stays dirty, and the machine records exactly that instead of pretending otherwise.
func (m *propStore) closeAfterWrites(t *rapid.T) {
	if m.st == nil || !m.took {
		t.Skip("handed-off close needs a taken writer")
	}
	if err := m.w.Close(); err != nil {
		t.Fatalf("owner close of writer: %v", err)
	}
	if err := m.st.Close(m.ctx); err != nil {
		t.Fatalf("close after writes: %v", err)
	}
	m.st, m.w, m.took = nil, nil, false
	m.dirty = true
}

// killAndReopen drops the live store SIGKILL-style and reopens within the same step, so
// the crash-path invariants are checked even when rapid never draws another action again.
func (m *propStore) killAndReopen(t *rapid.T) {
	if m.st == nil {
		t.Skip("store not open")
	}
	killSimulate(t, m.st, m.w)
	m.st, m.w, m.took = nil, nil, false
	m.dirty = true
	m.open(t)
}

// check runs after every action: identity and history must hold at every point in time.
func (m *propStore) check(t *rapid.T) {
	if m.st == nil {
		return
	}
	if got := m.st.NodeID(); got != m.nodeID {
		t.Errorf("node_id drifted: %s != %s", got, m.nodeID)
	}
	rows, err := m.st.RO().QueryContext(m.ctx,
		`SELECT stream, consumer, seq, attempts FROM deliveries ORDER BY stream, consumer, seq`)
	if err != nil {
		t.Fatalf("read deliveries: %v", err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			t.Errorf("close rows: %v", cerr)
		}
	}()
	seen := 0
	for rows.Next() {
		var stream, consumer string
		var seq, attempts int64
		if scanErr := rows.Scan(&stream, &consumer, &seq, &attempts); scanErr != nil {
			t.Fatalf("scan delivery: %v", scanErr)
		}
		key := stream + "/" + consumer + "/" + strconv.FormatInt(seq, 10)
		want, ok := m.attempts[key]
		if !ok {
			t.Errorf("row %s exists but was never written by the machine", key)
			continue
		}
		if attempts < want {
			t.Errorf("attempts for %s decreased across a reopen: %d < %d (D6/T9 violated)", key, attempts, want)
		}
		seen++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate deliveries: %v", err)
	}
	if seen != len(m.attempts) {
		t.Errorf("deliveries table holds %d rows, machine wrote %d — rows were lost", seen, len(m.attempts))
	}
}

// TestStoreLifecycleProperties drives the machine.
func TestStoreLifecycleProperties(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		//nolint:usetesting // rapid.T predates testing.TB's TempDir and has no equivalent
		dir, mkErr := os.MkdirTemp("", "messq-prop-")
		if mkErr != nil {
			t.Fatalf("temp dir: %v", mkErr)
		}
		t.Cleanup(func() {
			if rmErr := os.RemoveAll(dir); rmErr != nil {
				t.Errorf("cleanup %s: %v", dir, rmErr)
			}
		})
		m := &propStore{
			ctx:      context.Background(),
			dir:      filepath.Join(dir, "data"),
			attempts: make(map[string]int64),
		}
		t.Repeat(map[string]func(*rapid.T){
			"open":             m.open,
			"writeDeliveries":  m.writeDeliveries,
			"closeClean":       m.closeClean,
			"closeAfterWrites": m.closeAfterWrites,
			"killAndReopen":    m.killAndReopen,
			"":                 m.check,
		})
	})
}
