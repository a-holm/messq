// SPDX-License-Identifier: Apache-2.0

package client

// Name validation copied from internal/queue's grammar (rule S11 of
// internal/subject): pkg/client may not import internal packages (layers.sh), so
// this file mirrors those rules and nothing else. A name that fails here is refused
// locally — a bad name must not cost a round trip. The mirror is pinned by
// TestNameGrammarMatchesServerGrammar against the corpus in
// testdata/name_corpus.json, which carries the accepted/rejected verdicts of the
// server-side validator; regenerate it from an internal/subject scratch program when
// the grammar changes (the #18 wire gates fail loudly long before drift matters).

import "fmt"

// The explicit bounds of rule S11, mirroring internal/subject.
const (
	MaxStreamNameBytes    = 64
	MaxNewStreamNameBytes = MaxStreamNameBytes - len(".dlq")
	MaxConsumerNameBytes  = 64
)

// validName applies rule S11: non-empty, bounded length, charset [A-Za-z0-9_-] plus
// "." where dots are allowed, no leading/trailing dot and no ".." when they are.
func validName(what, name string, dots bool, maxBytes int) error {
	switch {
	case name == "":
		return &Error{Code: "bad_request", Message: what + " name is empty", err: ErrBadRequest}
	case len(name) > maxBytes:
		return &Error{
			Code:    "bad_request",
			Message: fmt.Sprintf("%s name %q is longer than %d bytes", what, name, maxBytes),
			err:     ErrBadRequest,
		}
	}
	for i := range len(name) {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_', c == '-':
		case c == '.' && dots:
		default:
			return &Error{Code: "bad_request", Message: fmt.Sprintf("%s name %q holds a character outside [A-Za-z0-9._-]", what, name), err: ErrBadRequest}
		}
	}
	if dots && (name[0] == '.' || name[len(name)-1] == '.' || contains(name, "..")) {
		return &Error{Code: "bad_request", Message: fmt.Sprintf("%s name %q: dots delimit tokens; no leading/trailing dot or empty token", what, name), err: ErrBadRequest}
	}
	return nil
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func validStreamName(name string) error {
	return validName("stream", name, true, MaxStreamNameBytes)
}

func validNewStreamName(name string) error {
	return validName("stream", name, true, MaxNewStreamNameBytes)
}

func validConsumerName(name string) error {
	return validName("consumer", name, false, MaxConsumerNameBytes)
}
