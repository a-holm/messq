// SPDX-License-Identifier: Apache-2.0

// Package gates holds the sabotage matrix: one fixture per quality gate, plus a driver that
// applies each fixture to a scratch copy of the repository and asserts that the matching make
// target fails with the matching message.
//
// The driver is behind the gatecheck build tag so `go test ./...` does not pay for it; this
// file carries the package clause so the directory is still a package without the tag. Run the
// matrix with `make gates-selftest`.
package gates
