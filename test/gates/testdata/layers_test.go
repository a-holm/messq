// SPDX-License-Identifier: Apache-2.0

package queue_test

import (
	"testing"

	_ "github.com/a-holm/messq/internal/store"
)

// TestSabotageLayers exists only for its import: a test for package queue reaching into the
// store is invisible to a guard that walks the production build alone. It lives in an external
// test package because internal/queue's production code must not import internal/store (which
// imports queue back); an internal `package queue` test with this import would be an import
// cycle, not a layer violation.
func TestSabotageLayers(t *testing.T) {
	t.Log("the import is the sabotage")
}
