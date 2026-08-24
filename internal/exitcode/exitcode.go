// SPDX-License-Identifier: Apache-2.0

// Package exitcode is the single shared home for messq's daemon exit-code contract
// (issue #17, body :405–419). PLAN §8's 0–7 table is the CLIENT contract and lives
// in internal/cli; serve reuses 0/1/2 and adds three sysexits values that do not
// collide with the client's 3–7 range.
//
// The operational payoff is systemd's RestartPreventExitStatus=2 78: a mistyped
// flag or a rolled-back binary must fail loudly and stay failed, not restart-loop.
//
// Both internal/cli (composition root) and other packages that map errors to exits
// (#16's schema-newer refusal) import these constants; nothing else may invent a
// numeric exit code.
package exitcode

// Serve-process exit codes. Values are the contract — do not renumber.
const (
	// OK is a clean drain: final commit + wal_checkpoint(TRUNCATE) +
	// clean_shutdown='1' all landed (§4.4).
	OK = 0
	// Error is an unclassified failure; also the third-signal escalation exit.
	Error = 1
	// Usage is a bad flag or environment value (client contract, PLAN §8).
	Usage = 2
	// IOERR (EX_IOERR) is D4's storage.fatal outcome: read-only drain elapsed,
	// NO clean_shutdown marker written, next start runs quick_check.
	IOERR = 74
	// TEMPFAIL (EX_TEMPFAIL) is the second instance on a locked datadir; the
	// message carries the holder's pid.
	TEMPFAIL = 75
	// CONFIG (EX_CONFIG) is schema-newer-than-binary; restarting cannot help.
	CONFIG = 78
)
