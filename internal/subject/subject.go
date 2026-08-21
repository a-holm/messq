// SPDX-License-Identifier: Apache-2.0

package subject

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/a-holm/messq/internal/errs"
)

// The explicit bounds of rule S1 and rule S11. Invariant I11 of PLAN.md section 5.2 wants
// every limit named rather than implied.
const (
	// MaxSubjectBytes is the longest subject or pattern, in bytes.
	MaxSubjectBytes = 512
	// MaxTokens is the deepest subject or pattern, in tokens.
	MaxTokens = 32
	// MaxStreamNameBytes is the longest stream name, derived names included.
	MaxStreamNameBytes = 64
	// MaxNewStreamNameBytes is the longest name an operator may give a stream. It leaves
	// room for the ".dlq" suffix of decision D3, so the derived dead-letter stream name is
	// always itself a valid stream name.
	MaxNewStreamNameBytes = MaxStreamNameBytes - len(".dlq")
	// MaxConsumerNameBytes is the longest consumer name.
	MaxConsumerNameBytes = 64
)

// The grammar's errors. Each carries its own message and classifies as errs.ErrBadSubject or
// errs.ErrBadRequest, so the HTTP layer and the CLI map them without a second table.
var (
	ErrEmptySubject       = errs.E(errs.ErrBadSubject, "", "the subject is empty (rule S1)")
	ErrEmptyToken         = errs.E(errs.ErrBadSubject, "", "a token is empty (rule S2)")
	ErrSubjectTooLong     = errs.E(errs.ErrBadSubject, "", "the subject is longer than %d bytes (rule S1)", MaxSubjectBytes)
	ErrTooManyTokens      = errs.E(errs.ErrBadSubject, "", "the subject has more than %d tokens (rule S1)", MaxTokens)
	ErrInvalidUTF8        = errs.E(errs.ErrBadSubject, "", "the subject is not valid UTF-8 (rule S3)")
	ErrControlChar        = errs.E(errs.ErrBadSubject, "", "a token holds a space or a control character (rule S3)")
	ErrWildcardInToken    = errs.E(errs.ErrBadSubject, "", `"*" and ">" are whole tokens, never part of one (rules S5 and S6)`)
	ErrGTNotLast          = errs.E(errs.ErrBadSubject, "", `">" is only valid as the last token (rule S6)`)
	ErrWildcardNotAllowed = errs.E(errs.ErrBadSubject, "", `a publish target holds no "*" and no ">" (rule S10)`)
	ErrEmptySet           = errs.E(errs.ErrBadSubject, "", "a subject set needs at least one pattern")

	ErrNameEmpty   = errs.E(errs.ErrBadRequest, "", "the name is empty (rule S11)")
	ErrNameTooLong = errs.E(errs.ErrBadRequest, "", "the name is too long (rule S11)")
	ErrNameCharset = errs.E(errs.ErrBadRequest, "", "a name holds only letters, digits, '_' and '-', and '.' for streams (rule S11)")
	ErrNameDot     = errs.E(errs.ErrBadRequest, "", "a name never starts or ends with '.' and never holds '..' (rule S11)")
)

// Subject is a validated literal subject: a publish target with no wildcards (rule S10).
type Subject string

// kind tells the three sorts of pattern token apart.
type kind uint8

const (
	kindLiteral kind = iota
	kindStar
	kindGT
)

// token is one compiled pattern token. lit is empty for the two wildcards.
type token struct {
	lit  string
	kind kind
}

// Pattern is a compiled subject filter. The zero value is invalid and matches nothing; build
// one with [ParsePattern].
type Pattern struct {
	raw    string
	tokens []token
	prefix string
	gt     bool
	stars  bool
}

// Set is a compiled disjunction: the subjects a stream accepts and the filters a consumer
// applies (PLAN.md section 4.2). The zero value matches nothing.
type Set struct{ pats []Pattern }

// ParseSubject validates a literal subject against rules S1 to S4 and S10.
func ParseSubject(s string) (Subject, error) {
	if _, err := parse(s, false); err != nil {
		return "", fmt.Errorf("subject %q: %w", s, err)
	}
	return Subject(s), nil
}

// ParsePattern compiles a subject filter against rules S1 to S9.
func ParsePattern(s string) (Pattern, error) {
	tokens, err := parse(s, true)
	if err != nil {
		return Pattern{}, fmt.Errorf("pattern %q: %w", s, err)
	}

	p := Pattern{raw: s, tokens: tokens}
	for _, t := range tokens {
		switch t.kind {
		case kindLiteral:
		case kindStar:
			p.stars = true
		case kindGT:
			p.gt = true
		}
	}
	p.prefix = literalPrefix(s, tokens)
	return p, nil
}

// ParseSet compiles a disjunction of patterns. Duplicates are dropped, so a PATCH that
// re-sends an existing subject list is idempotent.
func ParseSet(raw []string) (Set, error) {
	if len(raw) == 0 {
		return Set{}, fmt.Errorf("subject set: %w", ErrEmptySet)
	}

	seen := make(map[string]struct{}, len(raw))
	pats := make([]Pattern, 0, len(raw))
	for _, r := range raw {
		if _, dup := seen[r]; dup {
			continue
		}
		p, err := ParsePattern(r)
		if err != nil {
			return Set{}, err
		}
		seen[r] = struct{}{}
		pats = append(pats, p)
	}
	return Set{pats: pats}, nil
}

// String returns exactly the text the pattern was compiled from, so a pattern round-trips
// through JSON and through the CLI unchanged.
func (p Pattern) String() string { return p.raw }

// IsLiteral reports whether the pattern holds no wildcards, in which case Match is string
// equality.
func (p Pattern) IsLiteral() bool { return len(p.tokens) > 0 && !p.gt && !p.stars }

// MinTokens is the fewest tokens a matching subject can have. It is a cheap pre-filter for the
// cursor top-up scan.
func (p Pattern) MinTokens() int { return len(p.tokens) }

// Prefix is the longest literal prefix every matching subject starts with: "orders.eu." for
// "orders.eu.*.created", "a.b" for the literal "a.b", and "" when the first token is a
// wildcard. The cursor top-up query narrows the messages_subj index scan with it before
// matching in Go.
func (p Pattern) Prefix() string { return p.prefix }

// Match reports whether s is a subject this pattern accepts. It allocates nothing, never
// panics, and never matches structurally invalid input; see the package documentation for the
// exact contract.
func (p Pattern) Match(s string) bool {
	if len(p.tokens) == 0 {
		// The zero Pattern. Everything else, the empty subject included, is decided by
		// the loop below through validSegment.
		return false
	}

	rest := s
	exhausted := false
	for _, t := range p.tokens {
		if t.kind == kindGT {
			return validTail(rest)
		}
		if exhausted {
			// The subject ran out of tokens before the pattern did (rule S9).
			return false
		}

		var seg string
		if sep := strings.IndexByte(rest, '.'); sep < 0 {
			seg, rest, exhausted = rest, "", true
		} else {
			seg, rest = rest[:sep], rest[sep+1:]
		}
		if !validSegment(seg) {
			return false
		}
		if t.kind == kindLiteral && seg != t.lit {
			return false
		}
	}
	return exhausted
}

// Match reports whether any member matches.
func (s Set) Match(subj string) bool {
	for _, p := range s.pats {
		if p.Match(subj) {
			return true
		}
	}
	return false
}

// Strings returns the members in their compiled order, deduplicated. The result is a copy.
func (s Set) Strings() []string {
	if len(s.pats) == 0 {
		return nil
	}
	out := make([]string, 0, len(s.pats))
	for _, p := range s.pats {
		out = append(out, p.raw)
	}
	return out
}

// ValidateStreamName applies rule S11 to any stream name, derived ones included. Stream
// creation uses [ValidateNewStreamName] instead.
func ValidateStreamName(name string) error {
	return validateName(name, true, MaxStreamNameBytes)
}

// ValidateNewStreamName applies rule S11 with the tighter cap that leaves room for the ".dlq"
// suffix, so the dead-letter stream of decision D3 always has an expressible name.
func ValidateNewStreamName(name string) error {
	return validateName(name, true, MaxNewStreamNameBytes)
}

// ValidateConsumerName applies rule S11. Consumer names carry no ".": only streams need one,
// for the dead-letter derivation.
func ValidateConsumerName(name string) error {
	return validateName(name, false, MaxConsumerNameBytes)
}

// parse walks s token by token. wildcards decides whether "*" and ">" are tokens or errors.
func parse(s string, wildcards bool) ([]token, error) {
	switch {
	case s == "":
		return nil, ErrEmptySubject
	case len(s) > MaxSubjectBytes:
		return nil, ErrSubjectTooLong
	case !utf8.ValidString(s):
		return nil, ErrInvalidUTF8
	}

	tokens := make([]token, 0, 8)
	rest := s
	for {
		var seg string
		last := true
		if sep := strings.IndexByte(rest, '.'); sep >= 0 {
			seg, rest, last = rest[:sep], rest[sep+1:], false
		} else {
			seg = rest
		}

		if len(tokens) == MaxTokens {
			return nil, ErrTooManyTokens
		}
		t, err := classify(seg, wildcards, last)
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, t)

		if last {
			return tokens, nil
		}
	}
}

// classify turns one token's text into a compiled token.
func classify(seg string, wildcards, last bool) (token, error) {
	switch seg {
	case "":
		return token{}, ErrEmptyToken
	case "*":
		if !wildcards {
			return token{}, ErrWildcardNotAllowed
		}
		return token{kind: kindStar}, nil
	case ">":
		if !wildcards {
			return token{}, ErrWildcardNotAllowed
		}
		if !last {
			return token{}, ErrGTNotLast
		}
		return token{kind: kindGT}, nil
	}

	for _, r := range seg {
		switch {
		case r == '*' || r == '>':
			if wildcards {
				return token{}, ErrWildcardInToken
			}
			return token{}, ErrWildcardNotAllowed
		case r == ' ' || unicode.IsControl(r):
			return token{}, ErrControlChar
		}
	}
	return token{lit: seg, kind: kindLiteral}, nil
}

// literalPrefix computes the value [Pattern.Prefix] returns.
func literalPrefix(raw string, tokens []token) string {
	end := 0
	for i, t := range tokens {
		if t.kind != kindLiteral {
			if i == 0 {
				return ""
			}
			return raw[:end]
		}
		end += len(t.lit) + len(".")
	}
	return raw
}

// validSegment is the structural floor Match applies to the subject it is handed: a token is
// non-empty and is not a bare wildcard.
func validSegment(seg string) bool {
	return seg != "" && seg != "*" && seg != ">"
}

// validTail reports whether rest is one or more valid tokens, which is what ">" consumes
// (rule S7).
func validTail(rest string) bool {
	for {
		sep := strings.IndexByte(rest, '.')
		if sep < 0 {
			return validSegment(rest)
		}
		if !validSegment(rest[:sep]) {
			return false
		}
		rest = rest[sep+1:]
	}
}

// validateName implements rule S11. dots says whether "." is part of the charset, which is
// true for streams only.
func validateName(name string, dots bool, maxBytes int) error {
	switch {
	case name == "":
		return fmt.Errorf("name %q: %w", name, ErrNameEmpty)
	case len(name) > maxBytes:
		return fmt.Errorf("name %q: %w (at most %d bytes)", name, ErrNameTooLong, maxBytes)
	}

	for i := range len(name) {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_', c == '-':
		case c == '.' && dots:
		default:
			return fmt.Errorf("name %q: %w", name, ErrNameCharset)
		}
	}

	if dots && (name[0] == '.' || name[len(name)-1] == '.' || strings.Contains(name, "..")) {
		return fmt.Errorf("name %q: %w", name, ErrNameDot)
	}
	return nil
}
