// SPDX-License-Identifier: Apache-2.0

package store

import (
	"strconv"
	"strings"
)

// The DSN builder owns everything about the connection strings that is not driver-specific:
// which pragmas each pool carries, in which order, and with which values. The wire format is
// modernc's `_pragma=name(value)` spelling, but the shape below is deliberately plain — the
// cgo twin (slice 8) reuses these parts and renames only the few keys where mattn diverges.
//
// Two documented deviations from a literal reading of PLAN §4.1 (recorded in ADR-0002):
//
//  1. No `_txlock=immediate` on the read pool. `query_only=1` forbids acquiring a write
//     lock, so a BEGIN IMMEDIATE on a read connection fails outright; `immediate` exists to
//     kill the deferred-upgrade SQLITE_BUSY class on the writer, which never upgrades its
//     reads. Only the writer DSN carries it.
//  2. `mode=ro` is opt-in (Options.ReadOnly), `query_only(1)` is the enforcement. A
//     read-only OS-level handle on a WAL database cannot create the -shm file, so opening a
//     quiescent copied data directory with mode=ro fails outright. The read pool therefore
//     opens the file read-write at the OS level and is fenced read-only by query_only,
//     verified on every connection by the hook; mode=ro is reserved for offline inspection
//     tools that accept the shm constraint.

// poolRole selects which database/sql handle a DSN is built for: the sole writer handle or
// one of the two read flavours (the daemon's read pool and offline inspection tools).
type poolRole uint8

const (
	// poolWriter is the single read-write handle owned by the writer goroutine (#6).
	poolWriter poolRole = iota
	// poolReader is the daemon's read pool (peek, trace, list, lag, metrics): fenced
	// write-off by query_only(1), no transaction-lock upgrade.
	poolReader
	// poolReadOnly additionally opens the file itself read-only (mode=ro): offline
	// inspection over a directory whose WAL siblings are known quiescent.
	poolReadOnly
)

const (
	// walAutocheckpointPages is PLAN §4.1's tuned auto-checkpoint interval in WAL pages
	// (4000 × 4 KiB ≈ 16 MiB); steady-state checkpoint scheduling belongs to #27.
	walAutoCheckpointPages = "4000"
	// txlockWriter is the writer's BEGIN mode: immediate removes the deferred-upgrade
	// SQLITE_BUSY class on the only connection that writes.
	txlockWriter = "immediate"
)

// pragmaSetting is one enforced pragma in both of its forms: the value written into the DSN
// and the normalized read-back a healthy connection must report. Keeping the pair together
// makes the DSN-to-readback mapping (FULL→"2", MEMORY→"2", …) visible in one place instead
// of drifted across two lists. Slice order is significant twice over: it is the DSN parameter
// order and the hook's read-back order — journal_mode first (persistent file property),
// query_only last (it would fence everything after it).
type pragmaSetting struct {
	name string // PRAGMA name as spelled in the DSN and on read-back
	set  string // value inside _pragma=name(…) on the wire
	want string // normalized value every pooled connection must read back
}

// pragmaSettings returns the enforced pragma set for role under opt, with defaults applied.
// Read-back spellings were probed against modernc.org/sqlite v1.57.0: numeric pragmas come
// back as decimal integers (temp_store=MEMORY reads back "2"), journal_mode lower-case.
func pragmaSettings(role poolRole, opt Options) []pragmaSetting {
	opt.applyDefaults()
	s := []pragmaSetting{
		{
			"busy_timeout", strconv.FormatInt(opt.BusyTimeout.Milliseconds(), 10),
			strconv.FormatInt(opt.BusyTimeout.Milliseconds(), 10),
		},
		{"journal_mode", "WAL", "wal"},
		{
			"synchronous", synchronousWord(opt.Durability),
			strconv.Itoa(opt.Durability.Synchronous()),
		},
		{"foreign_keys", "1", "1"},
		{"temp_store", "MEMORY", "2"},
		{
			"cache_size", strconv.FormatInt(-cacheKiB(opt.CacheBytes), 10),
			strconv.FormatInt(-cacheKiB(opt.CacheBytes), 10),
		},
		{"wal_autocheckpoint", walAutoCheckpointPages, walAutoCheckpointPages},
	}
	if role != poolWriter {
		// Last, always: once applied it fences journal_mode changes and checkpoints.
		s = append(s, pragmaSetting{"query_only", "1", "1"})
	}
	return s
}

// buildDSN renders the connection string for path under role. Writer DSNs lead with
// _txlock=immediate; ReadOnly DSNs lead with mode=ro; read pools close with query_only(1).
func buildDSN(path string, role poolRole, opt Options) string {
	settings := pragmaSettings(role, opt)

	var b strings.Builder
	b.WriteString("file:")
	b.WriteString(path)
	b.WriteByte('?')
	sep := ""
	if role == poolReadOnly {
		b.WriteString("mode=ro")
		sep = "&"
	}
	if role == poolWriter {
		b.WriteString(sep)
		b.WriteString("_txlock=" + txlockWriter)
		sep = "&"
	}
	for _, s := range settings {
		b.WriteString(sep)
		b.WriteString("_pragma=")
		b.WriteString(s.name)
		b.WriteByte('(')
		b.WriteString(s.set)
		b.WriteByte(')')
		sep = "&"
	}
	return b.String()
}

// synchronousWord renders the durability mode as the PRAGMA spelling used on the wire:
// FULL by default and for any corrupted value, mirroring [Durability.Synchronous]'s
// fail-safe direction — an unknown mode must never reduce fsyncing.
func synchronousWord(d Durability) string {
	if d == DurabilityRelaxed {
		return "NORMAL"
	}
	return "FULL"
}

// cacheKiB converts the byte budget into the negative KiB figure SQLite's cache_size takes;
// the sign is the contract for "measured in KiB".
func cacheKiB(cacheBytes int64) int64 {
	return cacheBytes / 1024
}
