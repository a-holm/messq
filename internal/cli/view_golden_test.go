// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var update = flag.Bool("update", false, "rewrite the view golden files")

// The fixture views below are the recorded shapes issue #24's command faces render,
// transcribed from the issue's Design-proposal transcripts (which PLAN §11 adopts as
// the goldens). Ids and seqs are fixed, so every golden is deterministic. As later
// slices land real commands, their views register next to these.

type fxPeekItem struct {
	Seq     int64  `json:"seq"`
	ID      string `json:"id"`
	Subject string `json:"subject"`
	Bytes   int    `json:"bytes"`
	AgeMS   int64  `json:"age_ms"`
}

type fxPeekListDoc struct {
	Items        []fxPeekItem `json:"items"`
	Complete     bool         `json:"complete"`
	ScannedToSeq int64        `json:"scanned_to_seq"`
	Next         []Hint       `json:"next,omitempty"`
}

func fxPeekListDocument() fxPeekListDoc {
	return fxPeekListDoc{
		Items: []fxPeekItem{
			{Seq: 10496, ID: "01J8ZQ4P0000000000000000", Subject: "orders.eu.created", Bytes: 1229, AgeMS: 12000},
			{Seq: 10495, ID: "01J8ZQ4M0000000000000000", Subject: "orders.us.created", Bytes: 922, AgeMS: 101000},
			{Seq: 10494, ID: "01J8ZQ4K2M9V0X7Y3B5N6C8D1E", Subject: "orders.eu.created", Bytes: 1229, AgeMS: 134000},
		},
		Complete:     true,
		ScannedToSeq: 10496,
		Next: []Hint{
			{Cmd: "messq peek orders --seq 10496 --raw"},
			{Cmd: "messq trace 01J8ZQ4P0000000000000000"},
		},
	}
}

type peekListView struct{}

func (peekListView) Table(w io.Writer) error {
	_, err := fmt.Fprintf(w, ""+
		"SEQ     ID          SUBJECT            SIZE     AGE\n"+
		"10496   01J8ZQ4P…   orders.eu.created  1.2 KiB  12s\n"+
		"10495   01J8ZQ4M…   orders.us.created  0.9 KiB  1m41s\n"+
		"10494   01J8ZQ4K…   orders.eu.created  1.2 KiB  2m14s\n")
	return err
}

func (peekListView) JSON() any { return fxPeekListDocument() }

// NDJSON yields one record per item: the streaming face jq users pipe.
func (peekListView) NDJSON() []any {
	doc := fxPeekListDocument()
	out := make([]any, len(doc.Items))
	for i := range doc.Items {
		out[i] = doc.Items[i]
	}
	return out
}

func (peekListView) Hints() []Hint { return fxPeekListDocument().Next }

func (peekListView) ExitCode() int { return exitOK }

type fxPendingEmptyDoc struct {
	Stream      string `json:"stream"`
	Consumer    string `json:"consumer"`
	Items       []any  `json:"items"`
	Filtered    int    `json:"filtered"`
	OlderThanMS int64  `json:"older_than_ms"`
	Next        []Hint `json:"next,omitempty"`
}

func fxPendingEmptyDocument() fxPendingEmptyDoc {
	return fxPendingEmptyDoc{
		Stream:      "orders",
		Consumer:    "worker",
		Items:       []any{},
		Filtered:    37,
		OlderThanMS: 60000,
		Next:        []Hint{{Cmd: "messq pending orders worker --older-than 60s", Why: "widen the filter"}},
	}
}

type pendingEmptyView struct{}

func (pendingEmptyView) Table(w io.Writer) error {
	_, err := fmt.Fprintln(w, "no pending messages older than 1m0s")
	return err
}

func (pendingEmptyView) JSON() any { return fxPendingEmptyDocument() }

// NDJSON returns nil: an empty listing is a scalar outcome, so the ndjson face is the
// document itself as a stream of one.
func (pendingEmptyView) NDJSON() []any { return nil }
func (pendingEmptyView) Hints() []Hint { return fxPendingEmptyDocument().Next }
func (pendingEmptyView) ExitCode() int { return exitEmpty }

type fxPubReceiptDoc struct {
	Op      string `json:"op"`
	Stream  string `json:"stream"`
	Subject string `json:"subject"`
	Seq     int64  `json:"seq"`
	ID      string `json:"id"`
	Bytes   int    `json:"bytes"`
	Dup     bool   `json:"duplicate"`
	Next    []Hint `json:"next,omitempty"`
}

func fxPubReceiptDocument() fxPubReceiptDoc {
	return fxPubReceiptDoc{
		Op: "publish", Stream: "orders", Subject: "orders.eu.created",
		Seq: 10494, ID: "01J8ZQ4K2M9V0X7Y3B5N6C8D1E", Bytes: 14,
		Next: []Hint{
			{Cmd: "messq trace 01J8ZQ4K2M9V0X7Y3B5N6C8D1E"},
			{Cmd: "messq peek orders --seq 10494"},
		},
	}
}

type pubReceiptView struct{}

func (pubReceiptView) Table(w io.Writer) error {
	_, err := fmt.Fprintln(w, "published  seq=10494  01J8ZQ4K2M9V0X7Y3B5N6C8D1E  14 B")
	return err
}

func (pubReceiptView) JSON() any { return fxPubReceiptDocument() }

func (pubReceiptView) NDJSON() []any { return nil }
func (pubReceiptView) Hints() []Hint { return fxPubReceiptDocument().Next }
func (pubReceiptView) ExitCode() int { return exitOK }

type fxAckResult struct {
	Token  string `json:"token"`
	Status string `json:"status"`
}

type fxAckStaleDoc struct {
	Command  string        `json:"command"`
	Results  []fxAckResult `json:"results"`
	OkCount  int           `json:"ok"`
	Stale    int           `json:"stale"`
	StaleAck int           `json:"stale_ack"`
	Next     []Hint        `json:"next,omitempty"`
}

// ackStaleView renders #10's stale-ack transcript: explanation, likely cause, fix —
// the "ran twice" mystery is never papered over.
func fxAckStaleDocument() fxAckStaleDoc {
	return fxAckStaleDoc{
		Command:  "ack",
		Results:  []fxAckResult{{Token: "orders/worker/10493/1/1", Status: "stale_ack"}},
		StaleAck: 1,
		Next: []Hint{
			{Cmd: "messq trace 01J8ZQ4K2M9V0X7Y3B5N6C8D1E", Why: "see whether the work ran twice"},
			{Cmd: "messq consumer edit orders worker --ack-wait 2m"},
		},
	}
}

// ackStaleView renders #10's stale-ack transcript: explanation, likely cause, fix —
// the "ran twice" mystery is never papered over.
type ackStaleView struct{}

func (ackStaleView) Table(w io.Writer) error {
	_, err := fmt.Fprintf(w, ""+
		"ack  orders/worker/10493/1/1  stale_ack\n"+
		"\n"+
		"stale ack: this token names attempt 1, but orders/worker/10493 is on attempt 2\n"+
		"  likely cause: ack_wait (30s) is shorter than your handler\n"+
		"  fix: messq consumer edit orders worker --ack-wait 2m\n")
	return err
}

func (ackStaleView) JSON() any {
	return fxAckStaleDoc{
		Command:  "ack",
		Results:  []fxAckResult{{Token: "orders/worker/10493/1/1", Status: "stale_ack"}},
		StaleAck: 1,
		Next: []Hint{
			{Cmd: "messq trace 01J8ZQ4K2M9V0X7Y3B5N6C8D1E", Why: "see whether the work ran twice"},
			{Cmd: "messq consumer edit orders worker --ack-wait 2m"},
		},
	}
}

func (ackStaleView) NDJSON() []any { return nil }
func (ackStaleView) Hints() []Hint { return fxAckStaleDocument().Next }
func (ackStaleView) ExitCode() int { return exitConflict }

// viewCase pairs each registered fixture view with its documented exit code, so a
// command that silently changes its outcome fails here before it ships.
type viewCase struct {
	name      string
	view      View
	wantExit  int
	wantHints int // every inspect command emits at least one hint (§8); receipts too
}

// goldenViews is the registry TestEveryViewHasThreeFaces and TestViewGoldenFiles walk.
// Later slices append their command views here; a view that skips a face fails there,
// which is the mechanised form of "never a third mode".
func goldenViews() []viewCase {
	return []viewCase{
		{name: "peek-list", view: peekListView{}, wantExit: exitOK, wantHints: 2},
		{name: "pending-empty", view: pendingEmptyView{}, wantExit: exitEmpty, wantHints: 1},
		{name: "pub-receipt", view: pubReceiptView{}, wantExit: exitOK, wantHints: 2},
		{name: "ack-stale", view: ackStaleView{}, wantExit: exitConflict, wantHints: 2},
	}
}

// TestEveryViewHasThreeFaces walks every registered view and enforces the View
// contract: a non-empty frozen JSON document, a table face that renders, NDJSON
// records that agree with the JSON items (or fall back to the single-line scalar
// form), hints confined to the structured next[] array, and the documented exit code.
func TestEveryViewHasThreeFaces(t *testing.T) {
	for _, tc := range goldenViews() {
		t.Run(tc.name, func(t *testing.T) {
			docValue := tc.view.JSON()
			if docValue == nil {
				t.Fatalf("%s: json face is empty — every command must freeze one machine document", tc.name)
			}
			jb, err := json.MarshalIndent(docValue, "", "  ")
			if err != nil || len(jb) == 0 {
				t.Fatalf("%s: json face does not marshal (%v)", tc.name, err)
			}

			var tab bytes.Buffer
			if terr := tc.view.Table(&tab); terr != nil {
				t.Fatalf("table face failed: %v", terr)
			}
			if herr := WriteHints(&tab, tc.view.Hints()); herr != nil {
				t.Fatalf("hints did not render on the table face: %v", herr)
			}

			var doc map[string]any
			if err := json.Unmarshal(jb, &doc); err != nil {
				t.Fatalf("json face is not valid JSON: %v", err)
			}

			// NDJSON agreement: one record per item, elementwise equal to the JSON
			// document's items; nil means scalar, rendered as the document itself.
			if records := tc.view.NDJSON(); records != nil {
				items, ok := doc["items"].([]any)
				if !ok {
					t.Fatalf("%s: NDJSON records but no items array in the json face", tc.name)
				}
				if len(records) != len(items) {
					t.Fatalf("%s: %d ndjson records vs %d json items", tc.name, len(records), len(items))
				}
				for i := range records {
					rec, rerr := json.Marshal(records[i])
					item, ierr := json.Marshal(items[i])
					if rerr != nil || ierr != nil || !jsonEqual(rec, item) {
						t.Fatalf("%s: ndjson record %d disagrees with json item %d:\n%s\n%s",
							tc.name, i, i, rec, item)
					}
				}
			}

			// The next[] array mirrors Hints() exactly.
			nextRaw, hasNext := doc["next"]
			hints := tc.view.Hints()
			switch {
			case len(hints) > 0 && !hasNext:
				t.Fatalf("%s: %d hints but no next[] array in the json face", tc.name, len(hints))
			case len(hints) > 0:
				want, werr := json.Marshal(hints)
				got, gerr := json.Marshal(nextRaw)
				if werr != nil || gerr != nil || !jsonEqual(want, got) {
					t.Fatalf("%s: next[] in the json face disagrees with Hints():\n%s\n%s", tc.name, got, want)
				}
			}

			// Hints are data in next[], never loose text: strip the structured array
			// and no hinted command line may survive anywhere in the document.
			scrubbed, serr := json.Marshal(docWithoutNext(doc))
			if serr != nil {
				t.Fatalf("re-marshal without next[]: %v", serr)
			}
			for _, h := range hints {
				if strings.Contains(string(scrubbed), h.Cmd) {
					t.Fatalf("%s: hint %q leaks into the json document outside next[]", tc.name, h.Cmd)
				}
			}

			if got := tc.view.ExitCode(); got != tc.wantExit {
				t.Fatalf("%s: ExitCode() = %d, want %d", tc.name, got, tc.wantExit)
			}
			switch tc.view.ExitCode() {
			case exitOK, exitError, exitUsage, exitNotFound, exitConflict, exitEmpty, 6, exitPermission:
			default:
				t.Fatalf("%s: ExitCode() = %d is not in the documented set 0..7", tc.name, tc.view.ExitCode())
			}
			if got := len(tc.view.Hints()); got < tc.wantHints {
				t.Fatalf("%s: %d hints, want at least %d (§8: every inspect command ends with the next useful command)",
					tc.name, got, tc.wantHints)
			}
		})
	}
}

// TestHintWireShape pins the frozen machine form of one hint inside next[]
// (PLAN §8 freezes JSON field names at 1.0): lowercase snake_case keys, and a
// hint without a reason carries no why key at all instead of an empty string.
func TestHintWireShape(t *testing.T) {
	withWhy, err := json.Marshal(Hint{Cmd: "messq trace X", Why: "see whether the work ran twice"})
	if err != nil {
		t.Fatalf("marshal Hint with Why: %v", err)
	}
	if want := `{"cmd":"messq trace X","why":"see whether the work ran twice"}`; string(withWhy) != want {
		t.Fatalf("Hint{Cmd,Why} marshals as %s, want %s", withWhy, want)
	}
	noWhy, err := json.Marshal(Hint{Cmd: "messq peek orders --seq 10494"})
	if err != nil {
		t.Fatalf("marshal Hint without Why: %v", err)
	}
	if want := `{"cmd":"messq peek orders --seq 10494"}`; string(noWhy) != want {
		t.Fatalf("hint without a reason marshals as %s, want %s (empty why omitted)", noWhy, want)
	}
}

// docWithoutNext returns the document map with its top-level next key removed.
func docWithoutNext(doc map[string]any) map[string]any {
	out := make(map[string]any, len(doc))
	for k, v := range doc {
		out[k] = v
	}
	delete(out, "next")
	return out
}

// jsonEqual compares two marshalled values by their canonical re-marshalling.
func jsonEqual(a, b []byte) bool {
	var av, bv any
	if uaerr := json.Unmarshal(a, &av); uaerr != nil {
		return false
	}
	if uberr := json.Unmarshal(b, &bv); uberr != nil {
		return false
	}
	aj, aerr := json.Marshal(av)
	if aerr != nil {
		return false
	}
	bj, berr := json.Marshal(bv)
	if berr != nil {
		return false
	}
	return bytes.Equal(aj, bj)
}

// TestViewGoldenFiles freezes all three faces of every registered view under
// internal/cli/testdata/goldens/. Run `go test ./internal/cli -update` to rewrite
// them after a deliberate rendering change; a diff here is a contract change and gets
// reviewed like one.
func TestViewGoldenFiles(t *testing.T) {
	dir := filepath.Join("testdata", "goldens")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create %s: %v", dir, err)
	}
	for _, tc := range goldenViews() {
		var tab bytes.Buffer
		if err := tc.view.Table(&tab); err != nil {
			t.Fatalf("%s: table face failed: %v", tc.name, err)
		}
		if err := WriteHints(&tab, tc.view.Hints()); err != nil {
			t.Fatalf("%s: hints did not render: %v", tc.name, err)
		}
		jb, err := json.MarshalIndent(tc.view.JSON(), "", "  ")
		if err != nil {
			t.Fatalf("%s: json face failed: %v", tc.name, err)
		}
		jb = append(jb, '\n')

		var nd bytes.Buffer
		if records := tc.view.NDJSON(); records != nil {
			for _, rec := range records {
				line, merr := json.Marshal(rec)
				if merr != nil {
					t.Fatalf("%s: ndjson record does not marshal: %v", tc.name, merr)
				}
				if _, werr := nd.Write(line); werr != nil {
					t.Fatalf("%s: buffer write failed: %v", tc.name, werr)
				}
				if _, werr := nd.WriteString("\n"); werr != nil {
					t.Fatalf("%s: buffer write failed: %v", tc.name, werr)
				}
			}
		} else if _, werr := nd.Write(jb); werr != nil {
			t.Fatalf("%s: buffer write failed: %v", tc.name, werr)
		}

		faces := map[string][]byte{
			"table":  tab.Bytes(),
			"json":   jb,
			"ndjson": nd.Bytes(),
		}
		for face, got := range faces {
			path := filepath.Join(dir, tc.name+"."+face+".golden")
			if *update {
				if werr := os.WriteFile(path, got, 0o644); werr != nil {
					t.Fatalf("update %s: %v", path, werr)
				}
				continue
			}
			want, rerr := os.ReadFile(path)
			if rerr != nil {
				if errors.Is(rerr, os.ErrNotExist) {
					t.Fatalf("%s is missing; run `go test ./internal/cli -update` to seed it", path)
				}
				t.Fatalf("read %s: %v", path, rerr)
			}
			if !bytes.Equal(got, want) {
				i := firstDiff(got, want)
				t.Errorf("%s changed.\ngot:  %q\nwant: %q\n(differ at byte %d; deliberate? re-run with -update and review the diff)",
					path, got, want, i)
			}
		}
	}
}

func firstDiff(a, b []byte) int {
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}
