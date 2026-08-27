// SPDX-License-Identifier: Apache-2.0

package api

import (
	"github.com/a-holm/messq/internal/auth"
)

// Role/Confirm/DryRun declarations for every registered route (issue #15 design §1,
// acceptance "every route declares Role, Mutates, DryRun, Confirm"). #16 makes these
// load-bearing: its middleware enforces the declared set per route, and roles are a SET
// (ADR from #16, decision 1) so worker-facing verbs declare consume|admin while an
// operator-only route declares admin alone. An empty set means unauthenticated — exactly
// the two probes, which cannot carry a token.

// Route-role sets used in routes(). Declared here once so each registry row reads as
// intent rather than bitmask arithmetic.
var (
	rolesNone            auth.RoleSet // probes only: the empty set grants nothing
	rolesAdmin           = auth.RoleSet(1 << auth.RoleAdmin)
	rolesConsumeAndAdmin = auth.RoleSet(1<<auth.RoleConsume) | auth.RoleSet(1<<auth.RoleAdmin)
)
