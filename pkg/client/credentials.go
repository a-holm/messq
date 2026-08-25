// SPDX-License-Identifier: Apache-2.0

package client

// Credential is an opaque bearer credential (#16). Its every rendering — String,
// Format, MarshalText, MarshalJSON, LogValue — is redacted; see credentials.go.
type Credential struct{ token string }
