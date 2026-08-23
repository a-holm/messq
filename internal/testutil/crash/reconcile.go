// SPDX-License-Identifier: Apache-2.0

package crash

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"sort"

	"github.com/a-holm/messq/internal/testutil/ledger"
	"github.com/a-holm/messq/internal/testutil/loadgen"
	"github.com/a-holm/messq/internal/verify"
)

// The reconciler is the three-valued oracle that joins the external ledger against the
// recovered state. It asserts nothing about an individual UNKNOWN record — that is the
// whole point of the three-valued design — but every OK record must be present and intact,
// every FAILED record must be absent, and the state must contain nothing the ledger never
// recorded. The vacuity guards (a later slice) make sure the UNKNOWN set is the right size
// for those assertions to mean anything.

// Violation is one reconciler rule firing, with enough detail to reproduce the defect.
type Violation struct {
	Rule   string // "OK-LOST", "OK-CORRUPT", "FAILED-PRESENT", "GHOST", …
	Key    string
	Detail string
}

// seqKey identifies one message by its stream and sequence number.
type seqKey struct {
	stream string
	seq    int64
}

// Message is one recovered message's reconciler-relevant fields. The body is reduced to a
// SHA-256 at load time, so the snapshot costs O(rows) metadata, not the bodies themselves.
type Message struct {
	ID       string
	Size     int
	BodySHA  [sha256.Size]byte
	DedupKey string
}

// StateSnapshot is the recovered state the reconciler joins against, loaded in chunks from
// a read-only handle on the data dir.
type StateSnapshot struct {
	msgAt  map[seqKey]Message
	dedup  map[string]seqKey // dedup_key -> owning (stream, seq)
	next   map[string]int64  // stream -> stream_seq.next
	maxSeq map[string]int64  // stream -> MAX(messages.seq)
}

// LoadState opens the data dir read-only and loads the reconciler's snapshot: every
// message's id/size/body-hash/dedup-key, plus the seq allocator and per-stream maximum.
func LoadState(ctx context.Context, dataDir string) (st *StateSnapshot, retErr error) {
	db, openErr := verify.Open(dataDir)
	if openErr != nil {
		return nil, openErr
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			retErr = errors.Join(retErr, fmt.Errorf("close read-only handle: %w", closeErr))
		}
	}()
	if pingErr := db.PingContext(ctx); pingErr != nil {
		return nil, fmt.Errorf("ping %s: %w", dataDir, pingErr)
	}

	st = &StateSnapshot{
		msgAt:  make(map[seqKey]Message),
		dedup:  make(map[string]seqKey),
		next:   make(map[string]int64),
		maxSeq: make(map[string]int64),
	}
	rows, queryErr := db.QueryContext(ctx, `SELECT stream, seq, id, size, body, dedup_key FROM messages`)
	if queryErr != nil {
		return nil, fmt.Errorf("load messages: %w", queryErr)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			retErr = errors.Join(retErr, fmt.Errorf("close messages rows: %w", cerr))
		}
	}()
	for rows.Next() {
		var stream string
		var seq int64
		var id string
		var size int
		var body []byte
		var dedup sql.Null[string]
		if scanErr := rows.Scan(&stream, &seq, &id, &size, &body, &dedup); scanErr != nil {
			return nil, fmt.Errorf("scan message: %w", scanErr)
		}
		sk := seqKey{stream: stream, seq: seq}
		st.msgAt[sk] = Message{ID: id, Size: size, BodySHA: sha256.Sum256(body), DedupKey: dedup.V}
		if dedup.Valid {
			st.dedup[dedup.V] = sk
		}
		if seq > st.maxSeq[stream] {
			st.maxSeq[stream] = seq
		}
	}
	if iterErr := rows.Err(); iterErr != nil {
		return nil, fmt.Errorf("iterate messages: %w", iterErr)
	}

	nextRows, nextErr := db.QueryContext(ctx, `SELECT stream, next FROM stream_seq`)
	if nextErr != nil {
		return nil, fmt.Errorf("load stream_seq: %w", nextErr)
	}
	defer func() {
		if cerr := nextRows.Close(); cerr != nil {
			retErr = errors.Join(retErr, fmt.Errorf("close stream_seq rows: %w", cerr))
		}
	}()
	for nextRows.Next() {
		var stream string
		var next int64
		if scanErr := nextRows.Scan(&stream, &next); scanErr != nil {
			return nil, fmt.Errorf("scan stream_seq: %w", scanErr)
		}
		st.next[stream] = next
	}
	if iterErr := nextRows.Err(); iterErr != nil {
		return nil, fmt.Errorf("iterate stream_seq: %w", iterErr)
	}
	return st, nil
}

// Reconcile joins the folded ledger against the recovered state and returns every violation
// of the seven rules. probeSeq is the sequence number a post-restart probe publish received;
// it must exceed every pre-crash sequence, which is the durable, gap-free allocator's
// promise (SEQ-REGRESSION). stream names the probe's target stream.
func Reconcile(state *StateSnapshot, recs map[string]ledger.Record, stream string, probeSeq int64) []Violation {
	var vs []Violation
	seen := make(map[seqKey]string, len(recs)) // (stream,seq) -> ledger key, for SEQ-COLLISION

	for key, rec := range recs {
		switch rec.Verdict {
		case ledger.OK:
			sk := seqKey{stream: rec.Stream, seq: rec.Seq}
			m, ok := state.msgAt[sk]
			if !ok {
				vs = append(vs, Violation{
					Rule: "OK-LOST", Key: key,
					Detail: fmt.Sprintf("stream=%s seq=%d absent after recovery", rec.Stream, rec.Seq),
				})
				continue
			}
			if m.ID != rec.ID {
				vs = append(vs, Violation{
					Rule: "OK-LOST", Key: key,
					Detail: fmt.Sprintf("stream=%s seq=%d id=%s, want %s", rec.Stream, rec.Seq, m.ID, rec.ID),
				})
			}
			if m.Size != rec.Size {
				vs = append(vs, Violation{
					Rule: "OK-CORRUPT", Key: key,
					Detail: fmt.Sprintf("stream=%s seq=%d size=%d, want %d", rec.Stream, rec.Seq, m.Size, rec.Size),
				})
			} else if m.BodySHA != sha256.Sum256(loadgen.Payload(key, rec.Size)) {
				vs = append(vs, Violation{
					Rule: "OK-CORRUPT", Key: key,
					Detail: fmt.Sprintf("stream=%s seq=%d body differs from Payload(key,%d)", rec.Stream, rec.Seq, rec.Size),
				})
			}
			if prev, dup := seen[sk]; dup {
				vs = append(vs, Violation{
					Rule: "SEQ-COLLISION", Key: key,
					Detail: fmt.Sprintf("keys %q and %q both map to stream=%s seq=%d", prev, key, rec.Stream, rec.Seq),
				})
			} else {
				seen[sk] = key
			}
			if rec.Duplicate && m.DedupKey != key {
				vs = append(vs, Violation{
					Rule: "DUP-INCONSISTENT", Key: key,
					Detail: fmt.Sprintf("duplicate returned stream=%s seq=%d whose dedup key is %q, want %q", rec.Stream, rec.Seq, m.DedupKey, key),
				})
			}
		case ledger.Failed:
			if sk, present := state.dedup[key]; present {
				vs = append(vs, Violation{
					Rule: "FAILED-PRESENT", Key: key,
					Detail: fmt.Sprintf("FAILED key found in state at stream=%s seq=%d", sk.stream, sk.seq),
				})
			}
		case ledger.Unknown:
			// The whole point of the three-valued ledger: an UNKNOWN record asserts nothing.
		}
	}

	for dk, sk := range state.dedup {
		if _, known := recs[dk]; !known {
			vs = append(vs, Violation{
				Rule: "GHOST", Key: dk,
				Detail: fmt.Sprintf("stream=%s seq=%d carries a dedup key the ledger never recorded", sk.stream, sk.seq),
			})
		}
	}

	if probeSeq > 0 && probeSeq <= state.maxSeq[stream] {
		vs = append(vs, Violation{
			Rule: "SEQ-REGRESSION", Key: stream,
			Detail: fmt.Sprintf("probe seq %d does not exceed pre-crash max %d", probeSeq, state.maxSeq[stream]),
		})
	}

	sort.Slice(vs, func(i, j int) bool {
		if vs[i].Rule != vs[j].Rule {
			return vs[i].Rule < vs[j].Rule
		}
		return vs[i].Key < vs[j].Key
	})
	return vs
}
