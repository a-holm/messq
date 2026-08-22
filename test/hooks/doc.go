// SPDX-License-Identifier: Apache-2.0

// Package hooks holds the hook tests: one fixture per .githooks script contract, plus a
// driver that feeds each fixture to the real script through git's documented stdin format
// and asserts what it executed and what it printed. The tests are gated behind the
// hookcheck build tag so plain builds and vet never pay for them; run them with:
//
//	go test -tags hookcheck ./test/hooks/
package hooks
