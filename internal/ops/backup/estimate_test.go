// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestEstimateBytes(t *testing.T) {
	tests := []struct {
		name                      string
		pageCount, freelist, size int64
		want                      int64
	}{
		{"empty database", 0, 0, 4096, 0},
		{"single page no freelist", 1, 0, 4096, 4096},
		{"issue example", 100_000, 3_100, 4096, (100_000 - 3_100) * 4096},
		{"freelist larger than pages clamps to zero", 5, 9, 4096, 0},
		{"64 KiB pages", 1_000, 100, 65_536, 900 * 65_536},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EstimateBytes(tt.pageCount, tt.freelist, tt.size)
			if got != tt.want {
				t.Fatalf("EstimateBytes(%d, %d, %d) = %d, want %d",
					tt.pageCount, tt.freelist, tt.size, got, tt.want)
			}
		})
	}
}

func TestRequiredBytesHeadroom(t *testing.T) {
	tests := []struct {
		name     string
		estimate int64
		want     int64
	}{
		{"zero stays zero", 0, 0},
		{"ten percent rounded up", 1_000, 1_100},
		{"odd sizes round up to the whole byte", 999, 1_099}, // 999×1.1 = 1098.9 → 1099
		{"one byte still gets its headroom", 1, 2},           // 1.1 → 2
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RequiredBytes(tt.estimate)
			if got != tt.want {
				t.Fatalf("RequiredBytes(%d) = %d, want %d", tt.estimate, got, tt.want)
			}
			if got < tt.estimate {
				t.Fatalf("RequiredBytes(%d) = %d is below the estimate itself", tt.estimate, got)
			}
		})
	}
}

func TestCheckSpace(t *testing.T) {
	t.Run("free above required passes", func(t *testing.T) {
		if err := CheckSpace(1_101, 1_100); err != nil {
			t.Fatalf("CheckSpace(1101, 1100) = %v, want nil", err)
		}
	})
	t.Run("exactly the required budget passes (boundary)", func(t *testing.T) {
		if err := CheckSpace(1_100, 1_100); err != nil {
			t.Fatalf("CheckSpace(1100, 1100) = %v, want nil", err)
		}
	})
	t.Run("one byte short refuses with both numbers", func(t *testing.T) {
		err := CheckSpace(1_099, 1_100)
		if err == nil {
			t.Fatal("CheckSpace(1099, 1100) = nil, want refusal")
		}
		var short *InsufficientSpaceError
		if !errors.As(err, &short) {
			t.Fatalf("refusal is %T, want *InsufficientSpaceError", err)
		}
		if short.Free != 1_099 || short.Required != 1_100 {
			t.Fatalf("refusal carries Free=%d Required=%d, want 1099/1100", short.Free, short.Required)
		}
	})
}

// TestFreeSpaceBudgetEndToEnd is G4's arithmetic: refusals happen before a page is
// copied, so the budget math must refuse exactly one byte under the ×1.1 requirement.
func TestFreeSpaceBudgetEndToEnd(t *testing.T) {
	const (
		pageCount = int64(10_000)
		freelist  = int64(0)
		pageSize  = int64(4_096)
	)
	required := RequiredBytes(EstimateBytes(pageCount, freelist, pageSize))
	if err := CheckSpace(required, required); err != nil {
		t.Fatalf("disk exactly at the budget refused: %v", err)
	}
	if err := CheckSpace(required-1, required); err == nil {
		t.Fatal("disk one byte under the budget passed; refusals must happen before any copy")
	}
}

func TestFreeBytes(t *testing.T) {
	dir := t.TempDir()
	free, freeErr := FreeBytes(dir)
	if freeErr != nil {
		t.Fatalf("FreeBytes(%q) = %v, want nil", dir, freeErr)
	}
	if free <= 0 {
		t.Fatalf("FreeBytes(%q) = %d, want a positive number on a writable filesystem", dir, free)
	}
	if _, missErr := FreeBytes(filepath.Join(dir, "missing")); missErr == nil {
		t.Fatal("FreeBytes on a missing directory = nil error, want failure")
	}
}
