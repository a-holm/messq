// SPDX-License-Identifier: Apache-2.0

package client

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
)

// This file implements #16's frozen client credential contract:
//
//   - a client credential is ONE whole-file value: CredentialFromFile reads a file
//     whose entire trimmed content is the token;
//   - the predictable mistake — handing over the SERVER's --auth-file (four
//     whitespace-separated fields per line) — is refused with an error that says
//     exactly which file was wanted;
//   - every rendering of a Credential is redacted to msq1_<id>_*** : %v, %s, %#v,
//     slog and json.Marshal can be pointed at one safely;
//   - Reveal is the single, greppable exit for the raw token, banned outside
//     internal/auth and this package (.golangci.yml forbidigo);
//   - nothing here reads environment variables on its own — TokenFromEnv/AddrFromEnv
//     are explicit helpers the caller (#23) chooses to use.

// Credential is an opaque bearer credential. The zero value means "none".
type Credential struct{ token string }

// TokenCredential wraps a raw credential string, e.g. one typed on a command line.
func TokenCredential(s string) Credential { return Credential{token: s} }

// redacted renders the safe form: the id stays visible for support conversations,
// the secret never does. Unrecognised shapes collapse to plain ***.
func (c Credential) redacted() string {
	t := c.token
	if rest, ok := strings.CutPrefix(t, "msq1_"); ok {
		if id, _, found := strings.Cut(rest, "_"); found && id != "" {
			return "msq1_" + id + "_***"
		}
	}
	if t == "" {
		return ""
	}
	return "***"
}

// String implements fmt.Stringer, redacted.
func (c Credential) String() string { return c.redacted() }

// Format implements fmt.Formatter so EVERY verb (%q, %x, %d, …) renders redacted.
func (c Credential) Format(f fmt.State, verb rune) {
	s := c.redacted()
	write := func(str string) {
		//nolint:errcheck // fmt.State writers from Printf never fail mid-verb and Format reports nothing
		_, _ = fmt.Fprint(f, str)
	}
	switch verb {
	case 'q':
		write(strconv.Quote(s))
	case 'x':
		write(fmt.Sprintf("%x", []byte(s)))
	case 'X':
		write(strings.ToUpper(fmt.Sprintf("%x", []byte(s))))
	default:
		write(s)
	}
}

// MarshalText implements encoding.TextMarshaler, redacted.
func (c Credential) MarshalText() ([]byte, error) { return []byte(c.redacted()), nil }

// UnmarshalText accepts the literal token form; the round trip is deliberately
// asymmetric (marshal redacts, unmarshal trusts the caller who already has it).
func (c *Credential) UnmarshalText(b []byte) error {
	c.token = string(b)
	return nil
}

// MarshalJSON implements json.Marshaler, redacted.
func (c Credential) MarshalJSON() ([]byte, error) { return json.Marshal(c.redacted()) }

// LogValue implements slog.LogValuer, redacted.
func (c Credential) LogValue() slog.Value { return slog.StringValue(c.redacted()) }

// Reveal returns the raw token — THE single sanctioned exit for the secret, kept
// greppable by name so audits stay trivial. Callers must never log or serialize it.
func (c Credential) Reveal() string { return c.token }

// CredentialFromFile loads #16's client credential: a 0600 file whose entire trimmed
// content is one token. A file whose first non-comment line holds four whitespace-
// separated fields is the SERVER's --auth-file; that confusion gets its own error
// naming both facts. Mode checks are left to the caller: CI checkouts legitimately
// carry wider modes, and refusing there would break `messq` for everyone (#16 keeps
// the server-side fatal; the client-side warning belongs to the CLI, which Stats the
// mode itself).
func CredentialFromFile(path string) (Credential, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Credential{}, &Error{
			Code:    "config_error",
			Message: "read credential file: " + err.Error(),
			err:     fmt.Errorf("%w: %w", ErrConfig, err),
		}
	}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if len(strings.Fields(line)) >= 4 {
			return Credential{}, &Error{
				Code: "config_error",
				Message: fmt.Sprintf(
					"%s looks like the SERVER's --auth-file (four whitespace-separated fields per line); "+
						"a CLIENT credential file contains just the one token, e.g. what "+
						"`messq auth issue …` writes", path),
				err: fmt.Errorf("%w: %s is an auth table, not a credential", ErrConfig, path),
			}
		}
		break // first real line decides
	}
	token := strings.TrimSpace(string(raw))
	if token == "" {
		return Credential{}, &Error{
			Code:    "config_error",
			Message: fmt.Sprintf("%s is empty; write the credential to it as one line", path),
			err:     fmt.Errorf("%w: %s holds no credential", ErrConfig, path),
		}
	}
	return Credential{token: token}, nil
}

// TokenFromEnv reads MESSQ_TOKEN. A helper the CALLER invokes explicitly — the
// package itself reads no environment variables, ever.
func TokenFromEnv() (Credential, bool) {
	v, ok := os.LookupEnv("MESSQ_TOKEN")
	if !ok || v == "" {
		return Credential{}, false
	}
	return Credential{token: v}, true
}

// AddrFromEnv reads MESSQ_ADDR, same explicit-helper contract as TokenFromEnv.
func AddrFromEnv() (string, bool) {
	v, ok := os.LookupEnv("MESSQ_ADDR")
	if !ok || v == "" {
		return "", false
	}
	return v, true
}
