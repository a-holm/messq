// SPDX-License-Identifier: Apache-2.0

package help

import "github.com/a-holm/messq/internal/cli/exit"

// exitDocumented adapts the exit package's table. The indirection keeps the
// generated topic compile-time bound to the const block while letting tests
// replace the adapter when they sabotage a row.
var exitDocumented = exit.Documented
