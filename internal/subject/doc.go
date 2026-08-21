// SPDX-License-Identifier: Apache-2.0

// Package subject implements messq's NATS-style subject grammar and matcher (PLAN.md
// section 1.3). It is a sealed, pure leaf: no I/O, no clock, no globals, and [Pattern.Match]
// allocates nothing.
//
// The matcher gates publish, consumer filtering and cursor top-up, --trace-subjects, and the
// subject filters of peek and purge. A bug here misroutes messages silently and never raises
// an error, which is why the rules below are a table with one test per row, a differential
// fuzz target against a naive reference, and a generated document that docs/SEMANTICS.md
// includes.
//
// # Grammar
//
//   - S1: a subject is 1 to 32 tokens joined by ".", at most 512 bytes in total.
//   - S2: tokens are non-empty. "", ".", "a..b", ".a" and "a." are invalid.
//   - S3: a literal token may hold any valid UTF-8 except space, any control character, ".",
//     "*" and ">". There is no escape mechanism, by design.
//   - S4: subjects are case-sensitive and compared byte by byte. There is no Unicode
//     normalisation: two encodings of the same glyph are two subjects.
//   - S5: in a pattern, "*" is valid only as a whole token and matches exactly one token,
//     never a substring. "foo*" and "f*o" are invalid.
//   - S6: in a pattern, ">" is valid only as a whole token and only as the last one.
//   - S7: ">" matches one or more trailing tokens, so "a.>" matches "a.b" but not "a".
//   - S8: the bare pattern ">" matches every valid subject. It is the default filter.
//   - S9: a pattern with no ">" matches only subjects with the same token count.
//   - S10: a literal subject, meaning a publish target, holds no "*" and no ">".
//   - S11: stream and consumer names are 1 to 64 bytes of [A-Za-z0-9_-], plus "." for streams
//     only, so that <stream>.dlq is expressible (decision D3). A name never starts or ends
//     with ".", never holds ".." or "/", and is never "." or "..". The "/" ban is what keeps
//     the ack-token grammar of decision D7 unambiguous.
//
// # Matching arbitrary input
//
// [Pattern.Match] takes a plain string, never panics, and treats structural nonsense as a
// non-match rather than re-validating: the empty subject, an empty token from a leading,
// trailing or doubled separator, and a token that is exactly "*" or ">" all fail to match. The
// last of those is what makes handing a pattern in where a subject was expected a silent
// no-match instead of a silent mis-match. Match does not re-check UTF-8, length or control
// characters; [ParseSubject] does that once, at publish time.
package subject
