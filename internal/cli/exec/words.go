// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

var (
	// ErrUnterminatedQuote reports input whose quotes never close.
	ErrUnterminatedQuote = errors.New("unterminated quote")
	// ErrDanglingBackslash reports a trailing backslash with nothing to escape.
	ErrDanglingBackslash = errors.New("dangling backslash")
)

// SplitWords splits a command line into argv the way a shell would IF the shell
// had no expansion: whitespace separates words outside quotes, single and double
// quotes group them, backslash escapes the next rune outside single quotes.
// Deliberately ABSENT: variable expansion ($X), globbing (*), pipelines,
// redirection, command substitution. Those metacharacters are literal bytes of an
// argument — a `$PATH` reaches the child as the five-byte string "$PATH", which
// is exactly what makes this safe under hostile headers and paths (issue #25 §4).
//
// Grammar, pinned by TestSplitWordsQuoting and FuzzSplitWords:
//
//   - a word delimiter is any Unicode whitespace OUTSIDE quotes (Unicode.IsSpace,
//     so U+0020/U+0009/the RFC 5198 line breaks all split);
//   - inside single quotes nothing is special until the closing quote; backslash
//     is a literal byte there (POSIX behaviour);
//   - inside double quotes, and everywhere outside quotes, backslash escapes the
//     NEXT rune: "\” survives as a double quote inside a quoted word;
//   - adjacent quoted segments join into one token: `"ab"'cd'ef` is "abcdef";
//   - an empty quoted segment is one empty TOKEN, positionally real: `a "" b`
//     is three arguments, not two;
//   - errors refuse rather than guess: a quote that never closes is
//     ErrUnterminatedQuote, a backslash that escapes end-of-input is
//     ErrDanglingBackslash. Both name their input byte offset, and NOTHING is
//     returned on error — half-parsed argv has killed scripts before.
//
// The child runs argv[0] with argv[1:], so quoting rules here are startup-
// visible contract surface, tested fuzz-first.
func SplitWords(line string) ([]string, error) {
	var (
		out         []string
		cur         strings.Builder
		building    bool // does a token exist, possibly empty?
		inSingle    bool
		inDouble    bool
		escapedNext bool
		off         int // byte offset of the rune under consideration
	)

	flush := func() {
		if building {
			out = append(out, cur.String())
			cur.Reset()
			building = false
		}
	}

	for i := 0; i < len(line); {
		r, sz := utf8.DecodeRuneInString(line[i:])
		off = i
		i += sz

		switch {
		case escapedNext:
			escapedNext = false
			building = true
			cur.WriteRune(r)
		case !inSingle && r == '\\':
			if i >= len(line) {
				return nil, fmt.Errorf("%w: backslash at byte offset %d", ErrDanglingBackslash, off)
			}
			escapedNext = true
		case !inSingle && !inDouble && unicode.IsSpace(r):
			flush()
		case !inDouble && r == '\'':
			inSingle = !inSingle
			building = true
		case !inSingle && r == '"':
			inDouble = !inDouble
			building = true
		default:
			building = true
			cur.WriteRune(r)
		}
	}
	switch {
	case escapedNext:
		return nil, fmt.Errorf("%w: backslash at byte offset %d", ErrDanglingBackslash, len(line)-1)
	case inSingle || inDouble:
		return nil, fmt.Errorf("%w: input ended open", ErrUnterminatedQuote)
	}
	flush()
	return out, nil
}
