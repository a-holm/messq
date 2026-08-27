// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"strings"
	"unicode/utf8"
)

// StderrMode picks which END of a too-long stderr the captured reason keeps
// (--exec-stderr-capture head|tail). Head is the default because PLAN.md §8 says
// "first 4 KiB"; tail exists because a stack trace's useful line is usually last.
type StderrMode uint8

const (
	// HeadStderr keeps the FIRST max bytes worth of whole runes.
	HeadStderr StderrMode = iota
	// TailStderr keeps the LAST max bytes worth of whole runes, original order.
	TailStderr
)

const (
	replacementRune = '\uFFFD'
	// maxReasonBytes is the --stderr-bytes hard ceiling (issue #25 §2): whatever
	// an operator asks for above this stays at this. It exists so a mistyped
	// flag cannot make the CLI buffer unbounded stderr.
	maxReasonBytes = 64 << 10
)

// ClampReasonCap returns the capture budget actually used, honouring the 64 KiB
// hard ceiling so every downstream consumer (nk reasons on the wire, DLQ header)
// sees one number.
func ClampReasonCap(max int) int {
	if max < 0 {
		return 0
	}
	if max > maxReasonBytes {
		return maxReasonBytes
	}
	return max
}

// reasonWindow accumulates cleaned, WHOLE runes under a byte budget. Head mode
// freezes the first fill; tail mode slides forward, evicting old runes.
type reasonWindow struct {
	mode     StderrMode
	limit    int
	size     int
	runes    []string // encoded rune fragments, oldest first
	start    int      // head of live region (tail mode drops via start++)
	headDone bool
}

func newReasonWindow(mode StderrMode, limit int) *reasonWindow {
	return &reasonWindow{mode: mode, limit: limit}
}

// push appends one cleaned rune fragment; returns false when head mode is full.
func (w *reasonWindow) push(r rune) bool {
	enc := string(r)
	n := len(enc)
	switch w.mode {
	case HeadStderr:
		if w.size+n > w.limit {
			w.headDone = true
			return false
		}
		w.size += n
		w.runes = append(w.runes, enc)
		return true
	case TailStderr:
		w.size += n
		w.runes = append(w.runes, enc)
		for w.size > w.limit {
			drop := w.runes[w.start]
			w.size -= len(drop)
			w.start++
		}
		return true
	default:
		return false
	}
}

func (w *reasonWindow) String() string {
	return strings.Join(w.runes[w.start:], "")
}

// sanitizeInto drives the source over the budgeted window. Stray single-byte
// DEL/C1 bytes (invalid UTF-8 by definition) are DROPPED rather than turned into
// U+FFFD: they are terminal-control noise, and replacing them would print mojibake.
func sanitizeInto(src string, win *reasonWindow) {
	i := 0
	for i < len(src) {
		r, sz := utf8.DecodeRuneInString(src[i:])
		i += sz
		if r == utf8.RuneError && sz == 1 {
			b0 := src[i-1]
			if b0 == 0x7f || (b0 >= 0x80 && b0 <= 0x9f) {
				continue // stray C1/DEL byte: control noise, not text
			}
		}
		if valid, out := clean(r, src[i:], &i); valid {
			if !win.push(out) {
				break
			}
		}
	}
}

// clean decides what becomes of one source rune. It also consumes ESC-sequence
// continuations by advancing past them; r the candidate output rune.
func clean(r rune, rest string, i *int) (bool, rune) {
	switch {
	case r == '\x1b':
		skipEscape(rest, i)
		return false, 0
	case r == '\r':
		if strings.HasPrefix(rest, "\n") {
			*i++ // \r\n lands as ONE newline
		}
		return true, '\n'
	case r == '	' || r == '\n':
		return true, r
	case r == utf8.RuneError:
		// DecodeRuneInString hands back RuneError for BOTH a genuine encoded
		// U+FFFD and an invalid byte; either maps onto U+FFFD, so one branch
		// covers both and the output stays valid UTF-8 by construction.
		return true, replacementRune
	case r < 0x20:
		return false, 0
	case r < 0x7f:
		return true, r
	case r <= 0x9f: // DEL + C1 controls incl. the 0x9b alt-CSI
		return false, 0
	default:
		return true, r
	}
}

// skipEscape consumes one whole ANSI-style sequence starting right after the
// ESC rune (i already advanced past ESC by the caller):
//
//	CSI:  ESC [ … final byte in 0x40–0x7E
//	OSC:  ESC ] … terminated by BEL (U+0007) or ST (ESC \)
//	else: a two-rune unit — ESC plus whatever followed — both dropped.
func skipEscape(rest string, i *int) {
	if len(rest) == 0 {
		return
	}
	switch c := rest[0]; c {
	case '[': // CSI: consume up to and including the final byte 0x40–0x7E
		j := 1
		for j < len(rest) {
			b := rest[j]
			j++
			if b >= 0x40 && b <= 0x7e {
				break
			}
		}
		*i += j
	case ']': // OSC: terminated by BEL or ST (ESC \)
		j := 1
		for j < len(rest) {
			if rest[j] == '\x07' {
				j++
				break
			}
			if rest[j] == '\x1b' && j+1 < len(rest) && rest[j+1] == '\\' {
				j += 2
				break
			}
			j++
		}
		*i += j
	default: // two-rune ESC unit
		_, sz := utf8.DecodeRuneInString(rest)
		*i += sz
	}
}

// SanitizeStderr renders a child's stderr capture as a safe reason string:
// valid UTF-8 always, no escape sequences a terminal would obey, no NUL, newlines
// normalised, capped at max bytes ON WHOLE-RUNE boundaries so a multi-byte rune is
// never cut mid-sequence. Rules, in order:
//
//   - each invalid UTF-8 byte becomes U+FFFD;
//   - ANSI escape sequences are REMOVED wholesale (CSI …final byte, OSC …BEL/ST,
//     and any other two-rune ESC form) — a reason must survive trace/pending/DLQ;
//   - NUL (0x00) is dropped;
//   - "\r\n" and a lone "\r" become a single "\n";
//   - every other C0/C1 control plus DEL is dropped; tab and newline stay, they
//     are log-file structure, not danger;
//   - selection by mode happens over runes of the CLEANED sequence, so the
//     output is ≤ max bytes and never ends mid-rune: head stops at the first
//     rune that would overflow, tail keeps the last whole runes.
//
// max is clamped through [ClampReasonCap]; max == 0 yields "".
func SanitizeStderr(b []byte, max int, mode StderrMode) string {
	max = ClampReasonCap(max)
	if len(b) == 0 || max == 0 {
		return ""
	}
	win := newReasonWindow(mode, max)
	sanitizeInto(string(b), win)
	return win.String()
}
