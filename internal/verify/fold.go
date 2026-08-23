// SPDX-License-Identifier: Apache-2.0

package verify

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// The I10 event fold: replaying the events table from the beginning must reproduce the
// persisted state (log ≡ state, S15). The fold switches exhaustively over the closed S2.4
// vocabulary — adding an event in a later issue fails compilation until its fold arm exists,
// which is how I10 stays true instead of quietly decaying into a check of half the
// vocabulary. At M2 the fold covers msg.publish, msg.dup, stream.* and recovery.*; every
// later event arrives with its fold arm.

// foldState is the shadow state the fold replays into.
type foldState struct {
	streams map[string]bool  // live stream names
	lastSeq map[string]int64 // stream -> highest msg.publish seq seen (0 = none)
	count   map[string]int64 // stream -> msg.publish count
}

// fold applies one event to the shadow state. A violation is returned for an event that has
// no fold arm yet (impossible at M2, the compile-time guarantee) or for a seq gap. The
// switch is over the S2.4 enum so `exhaustive` keeps it complete.
func (f *foldState) fold(name string, stream string, seq int64) []Violation {
	switch eventName(name) {
	case evStreamCreate:
		f.streams[stream] = true
		return nil
	case evStreamUpdate:
		return nil // config change; the fold tracks existence, counts and sequences only
	case evStreamDelete:
		f.streams[stream] = false
		return nil
	case evMsgPublish:
		f.count[stream]++
		if last := f.lastSeq[stream]; last != 0 && seq != last+1 {
			return []Violation{{ID: I10, Detail: fmt.Sprintf("stream=%s seq=%d is not contiguous after %d", stream, seq, last)}}
		}
		f.lastSeq[stream] = seq
		return nil
	case evMsgDup:
		return nil // a dedup hit allocates nothing and changes no state
	case evRecoveryUnclean, evRecoveryReclaimed:
		return nil // audit markers; no state change at M2 (no consumers to reclaim)
	case evServerStart, evServerStop, evServerReload, evStorageFatal,
		evStreamPurge, evRetentionExpire, evRetentionBlocked,
		evConsumerCreate, evConsumerUpdate, evConsumerDelete, evConsumerSeek, evConsumerPause, evConsumerLag,
		evMsgDeliver, evMsgAck, evMsgAckDup, evMsgAckStale, evMsgNak, evMsgTerm, evMsgExtend, evMsgTimeout, evMsgDead,
		evDLQRedrive, evFlowBlocked, evDiskDegraded, evAuthDenied, evAPIError, evAdminAction:
		return []Violation{{ID: I10, Detail: fmt.Sprintf("event %q has no fold arm yet; its owning issue must add one when it ships the event", name)}}
	default:
		return []Violation{{ID: I10, Detail: fmt.Sprintf("event %q is not a member of the S2.4 vocabulary", name)}}
	}
}

// checkI10 replays the journal and diffs it against the tables.
func checkI10(ctx context.Context, tx *sql.Tx, _ bool) ([]Violation, error) {
	fs := &foldState{
		streams: make(map[string]bool),
		lastSeq: make(map[string]int64),
		count:   make(map[string]int64),
	}
	rows, err := tx.QueryContext(ctx, `SELECT event, stream, seq FROM events ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			err = cerr
		}
	}()
	var vs []Violation
	for rows.Next() {
		var name string
		var stream sql.Null[string]
		var seq sql.Null[int64]
		if scanErr := rows.Scan(&name, &stream, &seq); scanErr != nil {
			return nil, scanErr
		}
		vs = append(vs, fs.fold(name, stream.V, seq.V)...)
	}
	if iterErr := rows.Err(); iterErr != nil {
		return nil, iterErr
	}
	diff, diffErr := fs.diff(ctx, tx)
	if diffErr != nil {
		return nil, diffErr
	}
	return append(vs, diff...), nil
}

// diff compares the shadow state against the persisted tables: stream names, message counts
// and the sequence allocator per stream.
func (f *foldState) diff(ctx context.Context, tx *sql.Tx) (vs []Violation, retErr error) {
	actual := make(map[string]bool)
	srows, err := tx.QueryContext(ctx, `SELECT name FROM streams`)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := srows.Close(); cerr != nil {
			retErr = errors.Join(retErr, fmt.Errorf("close streams rows: %w", cerr))
		}
	}()
	for srows.Next() {
		var n string
		if scanErr := srows.Scan(&n); scanErr != nil {
			return nil, scanErr
		}
		actual[n] = true
	}
	if iterErr := srows.Err(); iterErr != nil {
		return nil, iterErr
	}

	for name, live := range f.streams {
		if live && !actual[name] {
			vs = append(vs, Violation{ID: I10, Detail: fmt.Sprintf("stream %s was created but is not in the streams table", name)})
		}
		if !live && actual[name] {
			vs = append(vs, Violation{ID: I10, Detail: fmt.Sprintf("stream %s was deleted but is still in the streams table", name)})
		}
	}
	for name := range actual {
		if !f.streams[name] {
			vs = append(vs, Violation{ID: I10, Detail: fmt.Sprintf("stream %s exists but has no stream.create event", name)})
		}
	}

	for name, last := range f.lastSeq {
		if last == 0 {
			continue
		}
		var count int64
		if cErr := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages WHERE stream = ?`, name).Scan(&count); cErr != nil {
			return nil, cErr
		}
		if count != f.count[name] {
			vs = append(vs, Violation{ID: I10, Detail: fmt.Sprintf("stream %s has %d messages, fold predicts %d", name, count, f.count[name])})
		}
		var next int64
		if nErr := tx.QueryRowContext(ctx, `SELECT next FROM stream_seq WHERE stream = ?`, name).Scan(&next); nErr != nil {
			return nil, nErr
		}
		if next != last+1 {
			vs = append(vs, Violation{ID: I10, Detail: fmt.Sprintf("stream %s has stream_seq.next=%d, fold predicts %d", name, next, last+1)})
		}
	}
	return vs, nil
}
