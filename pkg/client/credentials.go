// SPDX-License-Identifier: Apache-2.0

package client

// Credential is an opaque bearer credential (#16). Its every rendering — String,
// Format, MarshalText, MarshalJSON, LogValue — is redacted (credentials.go).
type Credential struct{ token string }

// TokenCredential wraps a raw credential string, e.g. one typed on a command line.
// The empty credential means "none": no Authorization header is ever sent.
func TokenCredential(s string) Credential { return Credential{token: s} }
