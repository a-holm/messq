// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"bufio"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/a-holm/messq/internal/errs"
)

// maxTokenLineBytes is the longest accepted token-file line. A longer line is an operator
// error — most likely a pasted secret in the hash column — and is refused rather than scanned.
const maxTokenLineBytes = 8 * 1024

// Token is one entry of a parsed token file: an id, the SHA-256 of the full presented
// credential, and the roles that id holds on each stream pattern. The hash is stored decoded
// so [Registry.Verify] compares it directly; rendering re-encodes it lowercase.
type Token struct {
	ID       string
	Hash     [32]byte
	Roles    RoleSet
	Patterns []Pattern
}

// Principal builds the immutable principal a token carries: one grant per pattern, each holding
// the token's full role set. The id is the principal's id and appears in logs and events.
func (t Token) Principal() Principal {
	grants := make([]Grant, 0, len(t.Patterns))
	for _, p := range t.Patterns {
		grants = append(grants, Grant{Roles: t.Roles, Pattern: p})
	}
	return NewPrincipal(t.ID, KindToken, grants)
}

// String renders the token's canonical one-line form: id, lowercase hash, canonical roles, then
// the comma-separated patterns. It parses back to the same token.
func (t Token) String() string {
	pats := make([]string, len(t.Patterns))
	for i, p := range t.Patterns {
		pats[i] = p.String()
	}
	return fmt.Sprintf("%s %s %s %s", t.ID, hex.EncodeToString(t.Hash[:]), t.Roles.String(), strings.Join(pats, ","))
}

// File is a parsed token file: the tokens in file order.
type File struct {
	Tokens []Token
}

// Render returns the canonical token-file text: one [Token.String] line per token.
func (f File) Render() string {
	lines := make([]string, len(f.Tokens))
	for i, t := range f.Tokens {
		lines[i] = t.String()
	}
	return strings.Join(lines, "\n")
}

// Parse reads a token file. name is the label used in error messages ("tokens" in the
// operator-facing examples). The grammar is line-oriented: blank lines and lines whose first
// non-space character is "#" are ignored; every other line is exactly four whitespace-separated
// fields (id, hash, roles, streams). Duplicate ids and duplicate hashes are both fatal.
func Parse(name string, r io.Reader) (*File, error) {
	sc := bufio.NewScanner(r)
	// A generous buffer so the 8 KiB bound is enforced by the precise check below rather than
	// by the scanner's own (vaguer) ErrTooLong.
	sc.Buffer(make([]byte, 0, 64*1024), 64*1024)

	f := &File{}
	seenID := make(map[string]int)
	seenHash := make(map[[32]byte]int)

	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := sc.Text()
		if lineNo == 1 {
			line = strings.TrimPrefix(line, "\ufeff") // UTF-8 BOM
		}
		if len(line) > maxTokenLineBytes {
			return nil, lineErr(name, lineNo, "line is longer than %d bytes", maxTokenLineBytes)
		}
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) != 4 {
			if len(fields) > 4 {
				return nil, lineErr(name, lineNo, "expected four fields (id hash roles streams), got %d; a fifth field is an error, not a comment", len(fields))
			}
			return nil, lineErr(name, lineNo, "expected four fields (id hash roles streams), got %d", len(fields))
		}

		tok, err := parseToken(fields)
		if err != nil {
			return nil, lineErr(name, lineNo, "%s", err)
		}
		if first, dup := seenID[tok.ID]; dup {
			return nil, lineErr(name, lineNo, "duplicate token id %q (first seen on line %d)", tok.ID, first)
		}
		if first, dup := seenHash[tok.Hash]; dup {
			return nil, lineErr(name, lineNo, "duplicate hash (first seen on line %d); two ids sharing one credential would make the audit trail ambiguous", first)
		}
		seenID[tok.ID] = lineNo
		seenHash[tok.Hash] = lineNo
		f.Tokens = append(f.Tokens, tok)
	}
	if err := sc.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			return nil, lineErr(name, lineNo, "line is longer than %d bytes", maxTokenLineBytes)
		}
		return nil, fmt.Errorf("%s: read: %w", name, err)
	}
	return f, nil
}

// parseToken validates the four fields of one line into a Token. Field errors classify as
// errs.ErrBadRequest and carry the exact teaching text of issue #16 §3.
func parseToken(fields []string) (Token, error) {
	id := fields[0]
	if !validTokenID(id) {
		return Token{}, errs.E(errs.ErrBadRequest, "", "token id %q must match [a-z0-9][a-z0-9._-]{1,63}", id)
	}

	hashField := fields[1]
	if len(hashField) != 64 {
		return Token{}, errs.E(errs.ErrBadRequest, "", "expected 64 hex characters, got %d — is this the secret rather than its SHA-256?", len(hashField))
	}
	var hash [32]byte
	if _, err := hex.Decode(hash[:], []byte(hashField)); err != nil {
		return Token{}, errs.E(errs.ErrBadRequest, "", "hash %q is not 64 hexadecimal characters", hashField)
	}

	roles, err := ParseRoleSet(fields[2])
	if err != nil {
		return Token{}, err
	}

	patterns := make([]Pattern, 0, strings.Count(fields[3], ",")+1)
	for _, s := range strings.Split(fields[3], ",") {
		p, err := ParsePattern(s)
		if err != nil {
			return Token{}, err
		}
		patterns = append(patterns, p)
	}

	return Token{ID: id, Hash: hash, Roles: roles, Patterns: patterns}, nil
}

// validTokenID reports whether s matches [a-z0-9][a-z0-9._-]{1,63}: a lowercase alphanumeric
// lead, then 1–63 further chars from the id alphabet, for 2–64 bytes total.
func validTokenID(s string) bool {
	if len(s) < 2 || len(s) > 64 {
		return false
	}
	switch {
	case s[0] >= 'a' && s[0] <= 'z', s[0] >= '0' && s[0] <= '9':
	default:
		return false
	}
	for i := 1; i < len(s); i++ {
		switch c := s[i]; {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '.', c == '_', c == '-':
		default:
			return false
		}
	}
	return true
}

// lineErr builds a teaching error prefixed with "name:line: ".
func lineErr(name string, line int, format string, args ...any) error {
	return errs.E(errs.ErrBadRequest, "", "%s:%d: %s", name, line, fmt.Sprintf(format, args...))
}
