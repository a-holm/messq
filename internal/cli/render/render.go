// SPDX-License-Identifier: Apache-2.0

// Package render is the output contract's toolbox (issue #23 §5): exactly three
// formats — table, json, ndjson — never a fourth; the Renderable/Streamer seams every
// command implements so commands write data structures, not printers; the humanisers
// and relative times; ULID abbreviation; the colour profile resolution; and Safe, the
// sanitiser every server-provided string must pass before touching a terminal.
//
// The package never reads os state: TTY-ness, environment and clock arrive as
// parameters, so every rule here is a pure function a table test can pin.
package render

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
	"unicode/utf8"
)

// Format is one of the three output modes. FormatAuto is the --output default and is
// resolved once per invocation by [Resolve] before anything renders; [Emit] refuses
// it, so an unresolved mode cannot silently become a fourth behaviour.
type Format int

const (
	FormatAuto Format = iota
	FormatTable
	FormatJSON
	FormatNDJSON
)

func (f Format) String() string {
	switch f {
	case FormatAuto:
		return "auto"
	case FormatTable:
		return "table"
	case FormatJSON:
		return "json"
	case FormatNDJSON:
		return "ndjson"
	default:
		return fmt.Sprintf("format(%d)", int(f))
	}
}

const modesList = "auto|table|json|ndjson"

// Parse validates one --output value against the closed set of four. Anything else —
// yaml, wide, template=… — is refused with the list in the message, because the fix
// for "my mode does not exist" is picking one that does.
func Parse(s string) (Format, error) {
	switch s {
	case "auto":
		return FormatAuto, nil
	case "table":
		return FormatTable, nil
	case "json":
		return FormatJSON, nil
	case "ndjson":
		return FormatNDJSON, nil
	default:
		return FormatAuto, fmt.Errorf("invalid --output %q: want %s", s, modesList)
	}
}

// Resolve applies the §5 matrix once per invocation: auto means "read the terminal",
// an explicit setting always beats detection, and a streaming command on a pipe wants
// ndjson (a followed stream has no final document).
func Resolve(f Format, stdoutIsTTY, streaming bool) Format {
	if f != FormatAuto {
		return f
	}
	if !stdoutIsTTY {
		if streaming {
			return FormatNDJSON
		}
		return FormatJSON
	}
	return FormatTable
}

// Renderable is one command result carrying all three faces. Data returns the frozen
// JSON shape — field names are compatibility surface from #24 onward.
type Renderable interface {
	// Table writes the human face into the tabwriter-backed layout helper.
	Table(w *TableWriter) error
	// Data returns the machine document (an items array for lists, an object for
	// scalars).
	Data() any
}

// Streamer is an incremental result: ndjson frames each record, and a table face may
// draw rows as they arrive rather than buffering the world.
type Streamer interface {
	Each(func(Renderable) error) error
}

// Emit dispatches one resolved format. It never chooses a format itself.
func Emit(w io.Writer, f Format, v Renderable) error {
	return emit(w, f, v, false)
}

// EmitStyled is Emit with the colour profile applied where a face can use it.
// Machine modes ignore the flag entirely: no escape sequence ever reaches json or
// ndjson output, whatever --color says.
func EmitStyled(w io.Writer, f Format, v Renderable, colour bool) error {
	return emit(w, f, v, colour)
}

func emit(w io.Writer, f Format, v Renderable, colour bool) error {
	switch f {
	case FormatAuto:
		return errors.New("render: format was not resolved (call render.Resolve first)")
	case FormatJSON:
		enc := json.NewEncoder(w)
		return enc.Encode(v.Data())
	case FormatNDJSON:
		enc := json.NewEncoder(w)
		if st, ok := v.(Streamer); ok {
			return st.Each(func(item Renderable) error {
				return enc.Encode(item.Data())
			})
		}
		return enc.Encode(v.Data())
	case FormatTable:
		tw := NewTableWriter(w)
		if err := v.Table(tw); err != nil {
			return err
		}
		return tw.Flush()
	default:
		return fmt.Errorf("render: unknown format %d", int(f))
	}
}

// TableWriter wraps text/tabwriter with the house layout: 2-space padding, no
// borders. Commands write whole pre-formatted rows; column tuning lives here so it
// changes everywhere at once.
type TableWriter struct{ tw *tabwriter.Writer }

func NewTableWriter(w io.Writer) *TableWriter {
	return &TableWriter{tw: tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)}
}

// WriteLine emits one row; cells inside the row are separated by tabs.
func (t *TableWriter) WriteLine(line string) error {
	_, err := fmt.Fprintln(t.tw, line)
	return err
}

func (t *TableWriter) Flush() error { return t.tw.Flush() }

// Safe sanitises one server-provided string for terminal display: C0 controls, DEL
// and C1 controls are escaped as \xNN, valid UTF-8 above that passes untouched. A
// subject containing "\x1b]0;pwned\x07" must not retitle the operator's window.
func Safe(s string) string {
	if utf8.ValidString(s) && !needsEscape(s) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 8)
	for _, r := range s {
		if r < 0x20 || (r >= 0x7f && r <= 0x9f) {
			fmt.Fprintf(&b, "\\x%02x", r)
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

func needsEscape(s string) bool {
	for _, r := range s {
		if r < 0x20 || (r >= 0x7f && r <= 0x9f) {
			return true
		}
	}
	return false
}

// Bytes humanises a byte count in table mode only: raw integers stay raw in JSON.
func Bytes(n int64) string {
	units := []struct {
		name string
		size int64
	}{
		{"TiB", 1 << 40}, {"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10},
	}
	for _, u := range units {
		if n >= u.size {
			return fmt.Sprintf("%.1f %s", float64(n)/float64(u.size), u.name)
		}
	}
	return fmt.Sprintf("%d B", n)
}

// Count renders an integer with thousands separators for table mode (1,284).
func Count(n int64) string {
	s := strconv.FormatInt(n, 10)
	neg := strings.HasPrefix(s, "-")
	if neg {
		s = s[1:]
	}
	var b strings.Builder
	for i, digit := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteByte(digit)
	}
	if neg {
		return "-" + b.String()
	}
	return b.String()
}

// RelTime renders an age against the caller's frozen or real now: 4m12s under an
// hour, 2h07m beyond it, "-" for an absent time.
func RelTime(now, t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	d := now.Sub(t)
	if d < 0 {
		return "-"
	}
	if h := int(d.Hours()); h > 0 {
		return fmt.Sprintf("%dh%02dm", h, int(d.Minutes())%60)
	}
	return d.Truncate(time.Second).String()
}

// Abbrev shortens a ULID for table mode (first 5 + … + last 3). Hints and errors
// always print full ids — an abbreviated id inside a suggested command is a
// copy-paste trap — so call sites there pass the id straight through instead.
func Abbrev(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:5] + "…" + id[len(id)-3:]
}

// Colour settings for --color / MESSQ_COLOR.
const (
	ColourAuto   = "auto"
	ColourAlways = "always"
	ColourNever  = "never"
)

// Colour resolves whether ANSI styling may be emitted, first match wins:
//
//	--color flag → NO_COLOR non-empty ⇒ off → CLICOLOR_FORCE non-empty ≠ 0 ⇒ on →
//	TERM=dumb ⇒ off → stdout is a character device ⇒ on → off.
//
// Machine modes never consult this for their own output; no escape sequence reaches
// json or ndjson regardless of the answer.
func Colour(flagValue string, getenv func(string) string, stdoutIsTTY bool) bool {
	switch flagValue {
	case ColourAlways:
		return true
	case ColourNever:
		return false
	}
	if getenv("NO_COLOR") != "" {
		return false
	}
	if force := getenv("CLICOLOR_FORCE"); force != "" && force != "0" {
		return true
	}
	if getenv("TERM") == "dumb" {
		return false
	}
	return stdoutIsTTY
}

// Hint writes the next-useful-command footer to stderr — TTY-only and suppressed by
// --quiet, so pipes and CI logs stay clean while a human still sees what to type
// next (PLAN §8).
func Hint(w io.Writer, stderrIsTTY, quiet bool, lines ...string) {
	if !stderrIsTTY || quiet {
		return
	}
	for _, l := range lines {
		fmt.Fprintln(w, l)
	}
}
