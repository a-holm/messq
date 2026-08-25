// SPDX-License-Identifier: Apache-2.0

// Package flagx holds the shared flag value types every CLI command parses its flags
// with (issue #24 slice 1): Duration, Bytes, Headers, Patterns, Backoff and Position.
//
// One parser per concept (PLAN.md D14): a duration accepted by --max-age is accepted by
// --older-than, because both go through Duration.Set. Every rejection wraps
// errs.ErrBadRequest so the exit-code classifier maps client-side validation to exit 2
// without a round trip.
//
// The types implement the pflag.Value shape (Set(string) error / String() string /
// Type() string) by structure: cobra's pflag accepts any value with those three methods,
// so no pflag import lives here and the package stays leaf-pure until #23 lands the
// chassis.
package flagx
