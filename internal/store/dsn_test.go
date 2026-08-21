package store

import (
	"strings"
	"testing"
	"time"
)

const goldenPath = "/var/lib/messq/messq.db"

// TestBuildDSNGolden pins the exact DSN strings per role and durability mode. The DSN is
// the carrier of the durability promise (ADR-0002), so its shape is a contract: the pragma
// list order, the parenthesised values and the position of query_only are all load-bearing.
func TestBuildDSNGolden(t *testing.T) {
	full := Options{}
	relaxed := Options{Durability: DurabilityRelaxed}

	for _, tc := range []struct {
		name string
		dsn  string
	}{
		{"writer-full", buildDSN(goldenPath, poolWriter, full)},
		{"writer-relaxed", buildDSN(goldenPath, poolWriter, relaxed)},
		{"reader-full", buildDSN(goldenPath, poolReader, full)},
		{"readonly-full", buildDSN(goldenPath, poolReadOnly, full)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var want string
			switch tc.name {
			case "writer-full":
				want = "file:" + goldenPath +
					"?_txlock=immediate" +
					"&_pragma=busy_timeout(5000)" +
					"&_pragma=journal_mode(WAL)" +
					"&_pragma=synchronous(FULL)" +
					"&_pragma=foreign_keys(1)" +
					"&_pragma=temp_store(MEMORY)" +
					"&_pragma=cache_size(-65536)" +
					"&_pragma=wal_autocheckpoint(4000)"
			case "writer-relaxed":
				want = "file:" + goldenPath +
					"?_txlock=immediate" +
					"&_pragma=busy_timeout(5000)" +
					"&_pragma=journal_mode(WAL)" +
					"&_pragma=synchronous(NORMAL)" +
					"&_pragma=foreign_keys(1)" +
					"&_pragma=temp_store(MEMORY)" +
					"&_pragma=cache_size(-65536)" +
					"&_pragma=wal_autocheckpoint(4000)"
			case "reader-full":
				want = "file:" + goldenPath +
					"?_pragma=busy_timeout(5000)" +
					"&_pragma=journal_mode(WAL)" +
					"&_pragma=synchronous(FULL)" +
					"&_pragma=foreign_keys(1)" +
					"&_pragma=temp_store(MEMORY)" +
					"&_pragma=cache_size(-65536)" +
					"&_pragma=wal_autocheckpoint(4000)" +
					"&_pragma=query_only(1)"
			case "readonly-full":
				want = "file:" + goldenPath +
					"?mode=ro" +
					"&_pragma=busy_timeout(5000)" +
					"&_pragma=journal_mode(WAL)" +
					"&_pragma=synchronous(FULL)" +
					"&_pragma=foreign_keys(1)" +
					"&_pragma=temp_store(MEMORY)" +
					"&_pragma=cache_size(-65536)" +
					"&_pragma=wal_autocheckpoint(4000)" +
					"&_pragma=query_only(1)"
			}
			if tc.dsn != want {
				t.Errorf("built DSN\n got %s\nwant %s", tc.dsn, want)
			}
		})
	}
}

// TestBuildDSNOrdering asserts the structural rules the goldens imply, independently of the
// exact strings: _txlock only on the writer, mode=ro only on ReadOnly (and first), and
// query_only always last on read pools — the driver applies _pragma params in a fixed order
// of its own, but a reader of the DSN must be able to trust these positions.
func TestBuildDSNOrdering(t *testing.T) {
	full := Options{}

	t.Run("query_only is the last parameter on read pools", func(t *testing.T) {
		for _, role := range []poolRole{poolReader, poolReadOnly} {
			dsn := buildDSN(goldenPath, role, full)
			if !strings.HasSuffix(dsn, "&_pragma=query_only(1)") && !strings.HasSuffix(dsn, "?_pragma=query_only(1)") {
				t.Errorf("role %d: query_only is not last: %s", role, dsn)
			}
		}
	})

	t.Run("_txlock appears only on the writer", func(t *testing.T) {
		if dsn := buildDSN(goldenPath, poolWriter, full); !strings.Contains(dsn, "_txlock=immediate") {
			t.Errorf("writer DSN lacks _txlock=immediate: %s", dsn)
		}
		for _, role := range []poolRole{poolReader, poolReadOnly} {
			if dsn := buildDSN(goldenPath, role, full); strings.Contains(dsn, "_txlock") {
				t.Errorf("role %d: writer-only _txlock leaked into read DSN: %s", role, dsn)
			}
		}
	})

	t.Run("mode=ro appears only on ReadOnly, as the first parameter", func(t *testing.T) {
		dsn := buildDSN(goldenPath, poolReadOnly, full)
		if !strings.HasPrefix(dsn, "file:"+goldenPath+"?mode=ro&") {
			t.Errorf("mode=ro is not the first parameter: %s", dsn)
		}
		for _, role := range []poolRole{poolWriter, poolReader} {
			if dsn := buildDSN(goldenPath, role, full); strings.Contains(dsn, "mode=ro") {
				t.Errorf("role %d: mode=ro leaked into non-offline DSN: %s", role, dsn)
			}
		}
	})
}

// TestBuildDSNCustomOptions proves the configured busy timeout and cache budget flow into
// the DSN as milliseconds and negative KiB respectively.
func TestBuildDSNCustomOptions(t *testing.T) {
	opt := Options{
		BusyTimeout: 250 * time.Millisecond,
		CacheBytes:  2 << 20,
	}
	dsn := buildDSN(goldenPath, poolWriter, opt)
	if !strings.Contains(dsn, "_pragma=busy_timeout(250)") {
		t.Errorf("busy_timeout(250) missing: %s", dsn)
	}
	if !strings.Contains(dsn, "_pragma=cache_size(-2048)") {
		t.Errorf("cache_size(-2048) missing: %s", dsn)
	}
}
