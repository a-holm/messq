// SPDX-License-Identifier: Apache-2.0

// Package ledger records, durably and outside the system under test, what the crash
// harness's load generator asked for and what it was told — the external three-valued
// oracle for at-least-once under crashes (PLAN.md §11 layer 3, D13).
//
// The encoding is NDJSON with a per-record crc32c, chosen so a human can read the file
// with jq during a failure investigation. The CRC exists for exactly one purpose: to
// recognise a torn final line from a killed driver (or an fsync hole) and truncate it,
// while a corrupt record anywhere else is a hard error. The verdicts are the closed set
// of the design: Unknown (in flight at the kill), OK (a 2xx is a durability promise) and
// Failed (rejected before the command could reach a commit).
package ledger

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
)

// Verdict is the three-valued outcome of one publish attempt, per PLAN §2/D13: OK must
// exist after recovery, FAILED must not, UNKNOWN may be either.
type Verdict uint8

const (
	// Unknown means the attempt was in flight when the daemon was killed: either outcome
	// is legal, and the reconciler asserts nothing about the record individually.
	Unknown Verdict = iota
	// OK means the server answered 2xx: the message MUST exist after recovery (I1).
	OK
	// Failed means the server rejected the publish before it could enter a commit batch:
	// the message MUST NOT exist.
	Failed
)

// String renders the verdict for the report. An out-of-range value renders as its number
// rather than masquerading as a real verdict.
func (v Verdict) String() string {
	switch v {
	case Unknown:
		return "UNKNOWN"
	case OK:
		return "OK"
	case Failed:
		return "FAILED"
	default:
		return fmt.Sprintf("Verdict(%d)", v)
	}
}

// castagnoli is the CRC-32C polynomial table, computed once. CRC-32C (Castagnoli) is the
// project's checksum: hardware-accelerated on x86 and stable across runs.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Record is one ledger entry: the intent the driver recorded before sending a request,
// folded by Replay into the latest outcome per key. The outcome fields (Seq, ID,
// Duplicate, Status, Code) are zero for an intent and populated by Resolve.
type Record struct {
	Key       string  `json:"key"`                 // ULID minted by the driver; also the Messq-Msg-Id
	Stream    string  `json:"stream"`              // harness stream name
	Subject   string  `json:"subject"`             // publish subject, deterministic per key
	Size      int     `json:"size"`                // body length; body itself is Payload(Key, Size)
	Cycle     int     `json:"cycle"`               // kill/restart cycle this attempt ran in
	SentAt    int64   `json:"sent_at_ms"`          // unix ms the intent was recorded
	Verdict   Verdict `json:"verdict"`             // Unknown=0, OK=1, Failed=2
	Seq       uint64  `json:"seq,omitempty"`       // set on OK
	ID        string  `json:"id,omitempty"`        // server ULID, set on OK
	Duplicate bool    `json:"duplicate,omitempty"` // dedup hit: returned the first record's seq
	Status    int     `json:"status,omitempty"`    // HTTP status; 0 = transport failure
	Code      string  `json:"code,omitempty"`      // error-envelope code, or transport error
	CRC       uint32  `json:"crc"`                 // crc32c over the record with CRC zeroed
}

// encode renders r as one NDJSON line: a complete, newline-terminated record whose CRC is
// computed over the record with its own CRC field zeroed. JSON object key order is stable
// for a fixed struct, so re-marshalling the same record for verification is byte-identical.
func encode(r Record) []byte {
	r.CRC = 0
	b, err := json.Marshal(r)
	if err != nil {
		// Unreachable: Record is a fixed, map-free struct of marshallable scalars.
		panic(fmt.Sprintf("ledger: marshal record: %v", err))
	}
	r.CRC = crc32.Checksum(b, castagnoli)
	out, err := json.Marshal(r)
	if err != nil {
		panic(fmt.Sprintf("ledger: marshal record with crc: %v", err))
	}
	return append(out, '\n')
}

// validCRC re-checks the record's checksum by zeroing CRC, re-marshalling and comparing.
// It is a method on *Record so the replay path can verify without re-encoding.
func (r *Record) validCRC() bool {
	saved := r.CRC
	r.CRC = 0
	b, err := json.Marshal(r)
	r.CRC = saved
	if err != nil {
		return false
	}
	return crc32.Checksum(b, castagnoli) == saved
}

// decode parses one NDJSON line into a record. A blank or all-NUL line is reported as
// invalid rather than a JSON error: it is the fsync-hole shape, not a real record.
func decode(line []byte) (Record, bool) {
	if len(line) == 0 || isNulBlock(line) {
		return Record{}, false
	}
	var r Record
	if err := json.Unmarshal(line, &r); err != nil {
		return Record{}, false
	}
	if !r.validCRC() {
		return Record{}, false
	}
	return r, true
}

// isNulBlock reports whether every byte of line is a NUL — the shape a torn append leaves
// when the file's size advanced past a data page that never reached disk.
func isNulBlock(line []byte) bool {
	if len(line) == 0 {
		return false
	}
	for _, b := range line {
		if b != 0 {
			return false
		}
	}
	return true
}

// Stats summarises one Replay pass for the report.
type Stats struct {
	// Records is the number of intact records folded.
	Records int
	// TornTail is the number of trailing records discarded as a torn write (0 or 1).
	TornTail int
}

// Replay folds ledger.jsonl into the latest record per key (last-writer-wins). A final
// line that fails to parse or fails its CRC is a torn tail from a killed driver: it is
// truncated and the pass continues, never an error. A bad line anywhere else — including
// a NUL block not at the very end — is a hard error, so a corrupt ledger can never be
// silently replayed as if it were empty.
func Replay(path string) (map[string]Record, Stats, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, Stats{}, fmt.Errorf("read ledger: %w", err)
	}
	if len(content) == 0 {
		return map[string]Record{}, Stats{}, nil
	}

	// A newline-terminated file ends with one empty element after the final '\n'; drop it
	// so the "last line" is the last record, not the terminator. A file that does not end
	// in '\n' keeps its final partial line, which is then judged as a torn tail.
	lines := bytes.Split(content, []byte{'\n'})
	if content[len(content)-1] == '\n' {
		lines = lines[:len(lines)-1]
	}

	out := make(map[string]Record, len(lines))
	var stats Stats
	for i, line := range lines {
		last := i == len(lines)-1
		r, ok := decode(line)
		if !ok {
			if last {
				// Torn tail: partial write, CRC failure, or fsync-hole NUL block at EOF.
				stats.TornTail++
				break
			}
			return nil, Stats{}, fmt.Errorf("ledger: corrupt record at line %d", i+1)
		}
		out[r.Key] = r
		stats.Records++
	}
	return out, stats, nil
}
