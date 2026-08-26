// SPDX-License-Identifier: Apache-2.0

package render

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// view is a minimal Renderable used across the tests.
type view struct{ rows []string }

func (v view) Table(w *TableWriter) error {
	for _, r := range v.rows {
		if err := w.WriteLine(r); err != nil {
			return err
		}
	}
	return nil
}

func (v view) Data() any {
	return map[string]any{"items": []map[string]any{{"name": "orders"}}}
}

func TestParseRejectsAFourthMode(t *testing.T) {
	tests := []struct {
		in      string
		want    Format
		wantErr bool
	}{
		{"auto", FormatAuto, false},
		{"table", FormatTable, false},
		{"json", FormatJSON, false},
		{"ndjson", FormatNDJSON, false},
		{"yaml", 0, true},
		{"wide", 0, true},
		{"template={{.x}}", 0, true},
		{"", 0, true},
	}
	for _, tt := range tests {
		got, err := Parse(tt.in)
		if tt.wantErr {
			if err == nil {
				t.Errorf("Parse(%q) accepted a fourth mode", tt.in)
			} else if !strings.Contains(err.Error(), "auto|table|json|ndjson") {
				t.Errorf("Parse(%q) error %q does not list the four modes", tt.in, err.Error())
			}
			continue
		}
		if err != nil || got != tt.want {
			t.Errorf("Parse(%q) = %v, %v; want %d", tt.in, got, err, tt.want)
		}
	}
}

func TestResolveMatrix(t *testing.T) {
	tests := []struct {
		name   string
		format Format
		isTTY  bool
		stream bool
		want   Format
	}{
		{name: "auto on a tty is table", format: FormatAuto, isTTY: true, want: FormatTable},
		{name: "auto on a pipe is json", format: FormatAuto, want: FormatJSON},
		{name: "auto streaming on a tty is table", format: FormatAuto, isTTY: true, stream: true, want: FormatTable},
		{name: "auto streaming on a pipe is ndjson", format: FormatAuto, stream: true, want: FormatNDJSON},
		{name: "explicit beats detection", format: FormatNDJSON, isTTY: true, want: FormatNDJSON},
		{name: "explicit table on a pipe stays table", format: FormatTable, want: FormatTable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Resolve(tt.format, tt.isTTY, tt.stream); got != tt.want {
				t.Errorf("Resolve(%v, %v, %v) = %v, want %v", tt.format, tt.isTTY, tt.stream, got, tt.want)
			}
		})
	}
}

func TestEmitFaces(t *testing.T) {
	v := view{rows: []string{"NAME", "orders"}}
	t.Run("json emits one parseable document", func(t *testing.T) {
		var buf bytes.Buffer
		if err := Emit(&buf, FormatJSON, v); err != nil {
			t.Fatal(err)
		}
		var doc map[string]any
		if err := json.Unmarshal(buf.Bytes(), &doc); err != nil {
			t.Fatalf("stdout is not one JSON document: %v (%q)", err, buf.String())
		}
	})
	t.Run("ndjson emits one record per item", func(t *testing.T) {
		var buf bytes.Buffer
		st := streamer{items: []Renderable{scalar{"a"}, scalar{"b"}, scalar{"c"}}}
		if err := Emit(&buf, FormatNDJSON, st); err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
		if len(lines) != 3 {
			t.Fatalf("ndjson wrote %d lines, want 3: %q", len(lines), buf.String())
		}
	})
	t.Run("machine mode never emits an escape sequence", func(t *testing.T) {
		var buf bytes.Buffer
		dangerous := view{rows: []string{"NAME", "\x1b]0;pwned\x07"}}
		if err := EmitStyled(&buf, FormatJSON, dangerous, true); err != nil {
			t.Fatal(err)
		}
		if strings.ContainsRune(buf.String(), '\x1b') {
			t.Errorf("ESC reached machine-mode output: %q", buf.String())
		}
	})
	t.Run("table face flushes through tabwriter", func(t *testing.T) {
		var buf bytes.Buffer
		if err := Emit(&buf, FormatTable, view{rows: []string{"NAME	COUNT", "orders	7"}}); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(buf.String(), "NAME    COUNT") {
			t.Errorf("table face output wrong: %q", buf.String())
		}
	})
	t.Run("ndjson of a scalar emits one line", func(t *testing.T) {
		var buf bytes.Buffer
		if err := Emit(&buf, FormatNDJSON, scalar{"orders"}); err != nil {
			t.Fatal(err)
		}
		if n := strings.Count(buf.String(), "\n"); n != 1 {
			t.Errorf("scalar ndjson wrote %d lines, want 1: %q", n, buf.String())
		}
	})
	t.Run("empty results are not errors", func(t *testing.T) {
		var buf bytes.Buffer
		if err := Emit(&buf, FormatJSON, empty{}); err != nil {
			t.Fatal(err)
		}
		if buf.String() != "[]\n" && buf.String() != "[]\n\n" {
			t.Errorf("empty list rendered %q, want [] document", buf.String())
		}
		buf.Reset()
		if err := Emit(&buf, FormatNDJSON, empty{}); err != nil {
			t.Fatal(err)
		}
		if buf.Len() != 0 {
			t.Errorf("empty ndjson rendered %q, want nothing", buf.String())
		}
	})
}

type scalar struct{ name string }

func (s scalar) Table(w *TableWriter) error { return nil }
func (s scalar) Data() any                  { return map[string]any{"name": s.name} }

type streamer struct{ items []Renderable }

func (s streamer) Table(w *TableWriter) error { return nil }
func (s streamer) Data() any                  { return nil }
func (s streamer) Each(fn func(Renderable) error) error {
	for _, it := range s.items {
		if err := fn(it); err != nil {
			return err
		}
	}
	return nil
}

type empty struct{}

func (empty) Table(w *TableWriter) error           { return nil }
func (empty) Data() any                            { return []any{} }
func (empty) Each(fn func(Renderable) error) error { return nil }

func TestSafeEscapesControlsAndPreservesText(t *testing.T) {
	tests := []struct{ in, want string }{
		{"plain orders", "plain orders"},
		{"café latte", "café latte"}, // valid UTF-8 preserved untouched
		{"\x1b]0;title\x07", "\\x1b]0;title\\x07"},
		{"a\x07b", "a\\x07b"},
		{"a\x1bb", "a\\x1bb"},
		{"tab\tkept?", "tab\\x09kept?"},
		{"nl\nnl", "nl\\x0anl"},
		{"del\x7fchar", "del\\x7fchar"},
		{"c1\u0085next", "c1\\x85next"},
		{"", ""},
	}
	for _, tt := range tests {
		if got := Safe(tt.in); got != tt.want {
			t.Errorf("Safe(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
	for _, b := range []byte(Safe("\x1b]0;pwned\x07")) {
		if b < 0x20 || b == 0x7f {
			t.Errorf("Safe let control byte %#x survive", b)
		}
	}
}

func TestHumanisers(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"bytes kib", Bytes(1284), "1.3 KiB"},
		{"bytes mib", Bytes(1024 * 1024), "1.0 MiB"},
		{"bytes small", Bytes(999), "999 B"},
		{"count separator", Count(1284), "1,284"},
		{"count small", Count(42), "42"},
		{"relative minutes", RelTime(frozen(), frozen().Add(-4*time.Minute-12*time.Second)), "4m12s"},
		{"relative hours", RelTime(frozen(), frozen().Add(-2*time.Hour-7*time.Minute)), "2h07m"},
		{"future relative", RelTime(frozen(), frozen().Add(-90*time.Second)), "1m30s"},
		{"absent", RelTime(frozen(), time.Time{}), "-"},
	}
	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("%s: got %q, want %q", tt.name, tt.got, tt.want)
		}
	}
}

func frozen() time.Time { return time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC) }

func TestTableFaceAlignsColumns(t *testing.T) {
	var buf bytes.Buffer
	tw := NewTableWriter(&buf)
	for _, row := range []string{"NAME\tCOUNT", "orders\t1,284", "billing\t7"} {
		if err := tw.WriteLine(row); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Flush(); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "NAME     COUNT") && strings.Contains(out, "orders   1,284") {
		t.Errorf("columns are not padded: %q", out)
	}
	if strings.Contains(out, "\t") {
		t.Errorf("raw tab reached the output: %q", out)
	}
}

func TestFormatStringAndEmitGuards(t *testing.T) {
	for f, want := range map[Format]string{
		FormatAuto: "auto", FormatTable: "table", FormatJSON: "json",
		FormatNDJSON: "ndjson", Format(42): "format(42)",
	} {
		if got := f.String(); got != want {
			t.Errorf("Format(%d).String() = %q, want %q", int(f), got, want)
		}
	}
	// Emit refuses an unresolved mode rather than guessing a behaviour.
	if err := Emit(&bytes.Buffer{}, FormatAuto, view{}); err == nil {
		t.Error("Emit accepted FormatAuto; resolve the mode first")
	}
}

func TestHumaniserEdgeCases(t *testing.T) {
	if got, want := Count(-1284), "-1,284"; got != want {
		t.Errorf("Count(-1284) = %q, want %q", got, want)
	}
	if got, want := Bytes(1<<40), "1.0 TiB"; got != want {
		t.Errorf("Bytes(1TiB) = %q, want %q", got, want)
	}
	if got, want := RelTime(frozen(), frozen().Add(-45*time.Minute)), "45m0s"; got != want {
		t.Errorf("RelTime(45m) = %q, want %q", got, want)
	}
	if got, want := RelTime(frozen(), frozen().Add(time.Second)), "-"; got != want {
		t.Errorf("RelTime(future) = %q, want %q (future is not a sane age)", got, want)
	}
}

func TestAbbreviateIDs(t *testing.T) {
	full := "01J8ZQ4P8MEXAMPLEEXAMPLE45"
	if got, want := Abbrev(full), "01J8Z…E45"; got != want {
		t.Errorf("Abbrev(%q) = %q, want %q", full, got, want)
	}
	if got := Abbrev("short"); got != "short" {
		t.Errorf("Abbrev of a short id changed it: %q", got)
	}
	if got := Abbrev(""); got != "" {
		t.Errorf("Abbrev(\"\") = %q, want \"\"", got)
	}
}

func TestColourResolutionOrder(t *testing.T) {
	tests := []struct {
		name  string
		flag  string // --color value, "" = not given (auto default)
		env   map[string]string
		isTTY bool
		want  bool
	}{
		{name: "auto on tty", flag: "auto", isTTY: true, want: true},
		{name: "auto on pipe", flag: "auto", want: false},
		{name: "flag always wins over NO_COLOR", flag: "always", env: map[string]string{"NO_COLOR": "1"}, want: true},
		{name: "flag never wins everywhere", flag: "never", isTTY: true, want: false},
		{name: "NO_COLOR non-empty disables", flag: "auto", env: map[string]string{"NO_COLOR": "1"}, isTTY: true, want: false},
		{name: "NO_COLOR present but empty keeps colour (spec)", flag: "auto", env: map[string]string{"NO_COLOR": ""}, isTTY: true, want: true},
		{name: "CLICOLOR_FORCE non-empty non-zero forces", flag: "auto", env: map[string]string{"CLICOLOR_FORCE": "1"}, want: true},
		{name: "CLICOLOR_FORCE=0 does not force", flag: "auto", env: map[string]string{"CLICOLOR_FORCE": "0"}},
		{name: "TERM=dumb disables", flag: "auto", env: map[string]string{"TERM": "dumb"}, isTTY: true},
		{name: "env beats flag precedence order is flag first", flag: "never", env: map[string]string{"CLICOLOR_FORCE": "1"}, isTTY: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			getenv := func(k string) string { return tt.env[k] }
			colour := tt.flag
			if colour == "" {
				colour = ColourAuto
			}
			if got := Colour(colour, getenv, tt.isTTY); got != tt.want {
				t.Errorf("Colour(%q, %v, %v) = %v, want %v", tt.flag, tt.env, tt.isTTY, got, tt.want)
			}
		})
	}
}

func TestHintTTYAndQuietGates(t *testing.T) {
	var buf bytes.Buffer
	Hint(&buf, true, false, "next  messq peek orders")
	if buf.Len() == 0 {
		t.Error("Hint suppressed on a live TTY")
	}
	buf.Reset()
	Hint(&buf, false, false, "next  messq peek orders")
	if buf.Len() != 0 {
		t.Errorf("Hint wrote %q to a pipe; hints are TTY-only", buf.String())
	}
	buf.Reset()
	Hint(&buf, true, true, "next  messq peek orders")
	if buf.Len() != 0 {
		t.Errorf("--quiet did not suppress the hint: %q", buf.String())
	}
}

// FuzzSafe proves no input can smuggle a raw C0/C1 byte through the sanitiser.
func FuzzSafe(f *testing.F) {
	f.Add("")
	f.Add("\x1b]0;pwned\x07")
	f.Add("orders.created.v2")
	f.Add("café ☕ ünïcode")
	for _, seed := range [][]byte{[]byte("\x00\x01\x02"), {0xc3, 0xa9}} {
		f.Add(string(seed))
	}
	f.Fuzz(func(t *testing.T, in string) {
		out := Safe(in)
		for i := 0; i < len(out); i++ {
			if b := out[i]; b < 0x20 || b == 0x7f {
				t.Fatalf("Safe(%q) emitted raw C0/DEL byte %#x", in, b)
			}
		}
		for _, r := range out {
			if r < 0x20 || (r >= 0x7f && r <= 0x9f) {
				t.Fatalf("Safe(%q) emitted control rune %#x", in, r)
			}
		}
	})
}
