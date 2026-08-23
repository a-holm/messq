// SPDX-License-Identifier: Apache-2.0

package ledger

import (
	"os"
	"path/filepath"
	"testing"
)

// TestReplayRoundTrip proves a Record survives encode -> file -> Replay byte-for-byte on
// every field the reconciler reads, and that the CRC is recomputed and matches. It is the
// contract the whole oracle leans on: what the driver recorded is what the reconciler sees.
func TestReplayRoundTrip(t *testing.T) {
	rec := Record{
		Key:     "01J0ABCDEFGHJKMN0PQRSTVWXYZ",
		Stream:  "crash",
		Subject: "crash.a",
		Size:    1024,
		Cycle:   3,
		SentAt:  1729000000000,
		Verdict: OK,
		Seq:     42,
		ID:      "01J0BBBBBBBBBBBBBBBBBBBBBBBB",
	}
	path := filepath.Join(t.TempDir(), "ledger.jsonl")
	if err := os.WriteFile(path, encode(rec), 0o600); err != nil {
		t.Fatalf("write ledger: %v", err)
	}

	got, stats, err := Replay(path)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if stats.Records != 1 {
		t.Fatalf("stats.Records = %d, want 1", stats.Records)
	}
	out, ok := got[rec.Key]
	if !ok {
		t.Fatalf("Replay lost key %q", rec.Key)
	}
	if !out.validCRC() {
		t.Errorf("replayed record CRC does not verify: %d", out.CRC)
	}
	// Compare every field the reconciler reads, excluding the CRC (which Replay recomputes
	// rather than round-trips verbatim).
	gotRec, wantRec := out, rec
	gotRec.CRC, wantRec.CRC = 0, 0
	if gotRec != wantRec {
		t.Errorf("replayed record = %+v, want %+v", gotRec, wantRec)
	}
}

// TestReplayTornFinalLine proves a torn final line — the driver died mid-append — is
// truncated and replayed as the last intact record, never an error (G1, edge case 6).
func TestReplayTornFinalLine(t *testing.T) {
	good := Record{Key: "k1", Stream: "s", Verdict: OK, Size: 4}
	path := filepath.Join(t.TempDir(), "ledger.jsonl")
	data := append(encode(good), []byte(`{"key":"k2","stream":"s","ver`+"\x00")...)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write ledger: %v", err)
	}

	got, stats, err := Replay(path)
	if err != nil {
		t.Fatalf("Replay of a torn tail must not error, got %v", err)
	}
	if stats.TornTail != 1 {
		t.Errorf("stats.TornTail = %d, want 1", stats.TornTail)
	}
	if len(got) != 1 {
		t.Fatalf("Replay returned %d records, want the single intact one", len(got))
	}
	if _, ok := got["k1"]; !ok {
		t.Errorf("the intact record k1 was lost to the torn tail")
	}
}

// TestReplayMidFileCorruptionIsHardError proves a CRC/parse failure on a line that is not
// the last is a hard error — a corrupt record anywhere but the tail must not be silently
// dropped, or the oracle could "pass" by discarding evidence (G1).
func TestReplayMidFileCorruptionIsHardError(t *testing.T) {
	good := Record{Key: "k1", Stream: "s", Verdict: OK, Size: 4}
	path := filepath.Join(t.TempDir(), "ledger.jsonl")
	// Corrupt the middle record's stored CRC (flip the final digit before the closing
	// brace): its contents are intact, so re-marshalling reproduces the true CRC and the
	// mismatch is a hard error, never a silent drop.
	middle := encode(Record{Key: "k2", Stream: "s", Verdict: Unknown, Size: 8})
	middle[len(middle)-3] ^= 0x01
	data := append(append(encode(good), middle...), encode(Record{Key: "k3", Stream: "s", Verdict: OK, Size: 2})...)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write ledger: %v", err)
	}

	if _, _, err := Replay(path); err == nil {
		t.Fatal("Replay of a corrupt mid-file record must error, got nil")
	}
}

// TestReplayZeroLength proves an empty ledger (driver died before its first record) replays
// to an empty map, not an error.
func TestReplayZeroLength(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.jsonl")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("write ledger: %v", err)
	}
	got, stats, err := Replay(path)
	if err != nil {
		t.Fatalf("Replay of an empty ledger must not error, got %v", err)
	}
	if len(got) != 0 {
		t.Errorf("Replay returned %d records from an empty file, want 0", len(got))
	}
	if stats.Records != 0 || stats.TornTail != 0 {
		t.Errorf("stats = %+v, want all-zero", stats)
	}
}

// TestReplayNulBlockTail proves a file ending in a block of NUL bytes (the fsync-hole
// shape: the file's size advanced but the data page never made it to disk) replays to the
// last intact record instead of hard-erroring.
func TestReplayNulBlockTail(t *testing.T) {
	good := Record{Key: "k1", Stream: "s", Verdict: OK, Size: 4}
	path := filepath.Join(t.TempDir(), "ledger.jsonl")
	data := append(encode(good), make([]byte, 4096)...) // trailing block of NULs, no newline
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write ledger: %v", err)
	}

	got, stats, err := Replay(path)
	if err != nil {
		t.Fatalf("Replay of a NUL-block tail must not error, got %v", err)
	}
	if stats.TornTail != 1 {
		t.Errorf("stats.TornTail = %d, want 1", stats.TornTail)
	}
	if _, ok := got["k1"]; !ok {
		t.Errorf("the intact record k1 was lost to the NUL-block tail")
	}
}

// TestReplayLastWriterWins proves folding keeps the final record per key: an Attempt
// followed by two Resolves leaves only the last verdict visible to the reconciler.
func TestReplayLastWriterWins(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.jsonl")
	lines := [][]byte{
		encode(Record{Key: "k", Stream: "s", Verdict: Unknown, Size: 4, Cycle: 1}),
		encode(Record{Key: "k", Stream: "s", Verdict: OK, Size: 4, Cycle: 1, Seq: 7}),
		encode(Record{Key: "k", Stream: "s", Verdict: Failed, Size: 4, Cycle: 1, Status: 413, Code: "too_large"}),
	}
	var data []byte
	for _, l := range lines {
		data = append(data, l...)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write ledger: %v", err)
	}

	got, stats, err := Replay(path)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if stats.Records != 3 {
		t.Errorf("stats.Records = %d, want 3 (every line is intact)", stats.Records)
	}
	out := got["k"]
	if out.Verdict != Failed {
		t.Errorf("folded verdict = %v, want Failed (last writer wins)", out.Verdict)
	}
	if out.Status != 413 || out.Code != "too_large" {
		t.Errorf("folded outcome = status %d code %q, want 413 too_large", out.Status, out.Code)
	}
}
