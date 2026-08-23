// SPDX-License-Identifier: Apache-2.0

package model

import (
	"strings"
)

// Pattern is a subject filter as a list of tokens. The zero value matches nothing. It is the
// naive, uncompiled twin of internal/subject's compiled Pattern: it transcribes the filter
// rules of docs/SEMANTICS.md (S5–S10) and is built by splitting on '.', never by compiling.
type Pattern string

// Match reports whether the subject s is accepted by this pattern. Both sides are split on
// '.' and walked token by token — no compilation, no index, no early exit beyond the walk
// itself — which is what keeps this a from-prose reference rather than a copy of the subject
// matcher.
//
// The transcribed rules are:
//
//   - "*" as a whole token matches exactly one subject token;
//   - ">" as a whole token, only in the final position, matches one or more trailing subject
//     tokens, so "a.>" matches "a.b" but not "a";
//   - any other token is a literal and matches only an equal token;
//   - a subject token that is empty or a bare "*" or ">" is structural noise and matches
//     nothing, mirroring the subject matcher's floor (rule S3);
//   - a pattern with no ">" matches subjects with the same token count (rule S9).
//
// Match never panics and treats structural nonsense as a non-match rather than re-validating,
// which is the subject matcher's contract as well.
func (p Pattern) Match(s string) bool {
	pt := strings.Split(string(p), ".")
	st := strings.Split(s, ".")
	si := 0
	for _, tok := range pt {
		if tok == ">" {
			// ">" must be the final pattern token and must consume at least one
			// non-empty subject token (rules S6 and S7).
			return si < len(st) && subjectTailOk(st[si:])
		}
		if si >= len(st) {
			return false
		}
		if !subjectTokenOk(st[si]) {
			return false
		}
		if tok == "*" {
			si++
			continue
		}
		if tok != st[si] {
			return false
		}
		si++
	}
	return si == len(st)
}

// subjectTokenOk is the one-token floor the subject matcher applies before comparing: an
// empty token, or a bare wildcard, is structural noise that never matches.
func subjectTokenOk(tok string) bool { return tok != "" && tok != "*" && tok != ">" }

// subjectTailOk reports whether every remaining subject token is on the floor — ">" consumes
// one or more valid tokens (rule S7).
func subjectTailOk(rest []string) bool {
	for _, tok := range rest {
		if !subjectTokenOk(tok) {
			return false
		}
	}
	return true
}
