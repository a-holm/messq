// SPDX-License-Identifier: Apache-2.0

// Package auth is messq's authentication and authorization decision engine.
//
// Authorization here is a pure function over (Principal, Role, stream name). A principal is a
// set of grants, each grant a role set scoped to a stream pattern; a request is allowed when
// some grant holds a required role on the stream in question. There is no role hierarchy, no
// implication table and no deny rules — a principal holds a role on a stream, or it does not
// (issue #16, decision 1). That flat shape is what keeps the permission matrix exhaustive and
// instant, and the package is pure Go over internal/errs plus the standard library so the
// matrix stays that way.
//
// Layers: this package reaches internal/errs only, never internal/api, internal/store,
// internal/cli or net/http (enforced by scripts/layers.sh). The HTTP middleware that resolves a
// [Principal] from an Authorization header and enforces it against #15's route registry lives
// in internal/api and lands with #14/#15; the pieces here are the decision engine those
// slices call into.
package auth
