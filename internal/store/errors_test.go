// SPDX-License-Identifier: Apache-2.0

package store

import (
	"errors"
	"fmt"
	"testing"

	"github.com/a-holm/messq/internal/errs"
)

// TestSentinelTexts pins the exact error texts of the store taxonomy. They are quoted verbatim
// by the CLI's teaching-error format (SEMANTICS §8) and matched on by the acceptance tests, so
// a wording change here is a user-visible contract change, not a refactor.
func TestSentinelTexts(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"ErrDataDirLocked", ErrDataDirLocked.Error(), errs.ErrLocked.Error()},
		{"ErrSchemaTooNew", ErrSchemaTooNew.Error(), errs.ErrSchemaNewer.Error()},
		{"ErrDataDirPerms", ErrDataDirPerms.Error(), "data directory or database file permissions are too broad"},
		{"ErrMigrationDrift", ErrMigrationDrift.Error(), "an already-applied migration file has changed"},
		{"ErrPragmaMismatch", ErrPragmaMismatch.Error(), "pragma read back with an unexpected value"},
		{"ErrCorrupt", ErrCorrupt.Error(), "integrity check failed"},
		{"ErrWriterTaken", ErrWriterTaken.Error(), "read-write handle already taken"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("sentinel text changed:\n got: %s\nwant: %s", tt.got, tt.want)
			}
		})
	}
}

// TestStoreSentinelsWrapCoreSentinels checks that the two sentinels with a counterpart in the
// core set actually wrap them: callers above the store match with errors.Is against
// internal/errs and get the CLI exit-code mapping for free.
func TestStoreSentinelsWrapCoreSentinels(t *testing.T) {
	tests := []struct {
		store error
		core  error
	}{
		{ErrDataDirLocked, errs.ErrLocked},
		{ErrSchemaTooNew, errs.ErrSchemaNewer},
	}
	for _, tt := range tests {
		if !errors.Is(tt.store, tt.core) {
			t.Errorf("%s does not wrap %s", tt.store, tt.core)
		}
	}
}

// TestLocalSentinelsAreDistinctFromCoreSet guards against accidental aliasing: a local
// sentinel that happens to equal a core one would silently change the meaning of every
// errors.Is check in the packages above.
func TestLocalSentinelsAreDistinctFromCoreSet(t *testing.T) {
	local := []error{ErrDataDirPerms, ErrMigrationDrift, ErrPragmaMismatch, ErrCorrupt, ErrWriterTaken}
	for _, l := range local {
		for _, c := range errs.All() {
			if errors.Is(l, c) {
				t.Errorf("%s aliases core sentinel %s", l, c)
			}
		}
	}
}

// TestWrappedStoreErrorMatchesBothLayers verifies the full wrap chain a startup refusal
// produces: fmt.Errorf("open %s: %w", dir, ErrDataDirLocked) must satisfy errors.Is for the
// store sentinel, the core sentinel, and nothing else by accident.
func TestWrappedStoreErrorMatchesBothLayers(t *testing.T) {
	err := fmt.Errorf("open /var/lib/messq: %w", ErrDataDirLocked)
	if !errors.Is(err, ErrDataDirLocked) {
		t.Error("wrapped error does not match ErrDataDirLocked")
	}
	if !errors.Is(err, errs.ErrLocked) {
		t.Error("wrapped error does not match errs.ErrLocked through the chain")
	}
	if errors.Is(err, ErrWriterTaken) {
		t.Error("unrelated sentinel matched")
	}
}
