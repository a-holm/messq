// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"log/slog"
)

// Secret is a bearer credential that never prints itself. Every formatting path — %v, %s,
// %+v, %#v, slog.LogValue, the text marshaler and the JSON marshaler — yields the REDACTED
// marker, so a secret cannot reach an output stream through whichever interface a caller
// happens to use (issue #16, §11). The marker is deliberately "REDACTED" and not the "msq1_"
// prefix, so a redacted value does not itself trip the gitleaks/secret-scanning rule that
// greps for that prefix.
//
// [Secret.Reveal] is the single, greppable exit; a forbidigo rule bans it outside this package
// and pkg/client.
type Secret string

// String keeps %v and %s from printing the secret.
func (Secret) String() string { return "REDACTED" }

// GoString keeps %#v from printing the secret.
func (Secret) GoString() string { return `auth.Secret("REDACTED")` }

// LogValue keeps slog from printing the secret.
func (Secret) LogValue() slog.Value { return slog.StringValue("REDACTED") }

// MarshalText keeps text-marshalling encoders from printing the secret.
func (Secret) MarshalText() ([]byte, error) { return []byte("REDACTED"), nil }

// MarshalJSON keeps encoding/json from printing the secret.
func (Secret) MarshalJSON() ([]byte, error) { return []byte(`"REDACTED"`), nil }

// Reveal returns the secret itself. It is the one place a credential becomes readable, and its
// callers are the token-file writer (messq auth add) and the client credential loader.
func (s Secret) Reveal() string { return string(s) }
