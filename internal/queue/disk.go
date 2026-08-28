// SPDX-License-Identifier: Apache-2.0

package queue

// The hysteretic disk state machine (#27 slice 1, PLAN §4.5, G5). Pure arithmetic over
// (state, free bytes, policy): the statfs/Fallocate syscalls live in internal/janitor's
// DiskProbe and Reserve, and layers.sh holds internal/queue away from them. The janitor's
// disk job feeds the latest reading in on its --disk-check-interval cadence; #14's publish
// handlers and the store's publishTx read the resulting gate.
//
// Hysteresis is the whole point of the two-threshold shape: below MinFree the broker
// enters DiskLow (publishes refused 507 disk_full, the fallocated reserve released so
// deletion has room); it leaves DiskLow only above MinFree×Recover. A reading that
// oscillates just around MinFree therefore produces exactly ONE transition — each one
// would otherwise be a disk.degraded event, a reserve unlink/fallocate pair and a flap
// visible on every dashboard.

// DiskState is the closed disk-pressure state (exhaustive-linted).
type DiskState uint8

const (
	// DiskOK is a disk with free space at or above --min-free-bytes: publishes allowed.
	DiskOK DiskState = iota
	// DiskLow is free space strictly below --min-free-bytes: publish-class commands
	// are refused while ack/nak/term/sweep/dead-letter/retention/reads continue
	// (§4.5's table). /healthz reports it in degraded[]; /readyz never sees it.
	DiskLow
)

// String renders the state for logs and metrics labels.
func (s DiskState) String() string {
	switch s {
	case DiskOK:
		return "ok"
	case DiskLow:
		return "low"
	default:
		return "unknown"
	}
}

// DiskPolicy carries the three --disk-* facts the state machine decides with.
type DiskPolicy struct {
	// MinFree is --min-free-bytes. <= 0 disables the guard entirely: NextDiskState
	// then always returns the current state with no actions (documented as a bad idea).
	MinFree int64

	// Recover is the hysteresis multiplier: DiskLow ends only at or above
	// MinFree*Recover (default 1.25). Values < 1 are clamped to 1, which makes the
	// recovery threshold exactly MinFree rather than unreachable.
	Recover float64

	// Reserve is --disk-reserve-bytes, the size of the fallocated messq.reserve file.
	// 0 disables the reserve: enter/exit action lists then omit ReleaseReserve /
	// RestoreReserve instead of asking for an unlink of a file that does not exist.
	Reserve int64
}

// DiskAction is one thing the caller must do because the state changed (exhaustive-
// linted). Actions arrive in a canonical order so callers can execute the list literally:
// reserve first (deletion needs room before anything else happens), then the publish
// gate, then Emit for the disk.degraded event/metric pair.
type DiskAction uint8

const (
	// ReleaseReserve unlinks the fallocated reserve file, handing its space to the WAL
	// writes that retention deletes need on a nearly full disk.
	ReleaseReserve DiskAction = iota
	// RestoreReserve re-fallocates the reserve file after recovery.
	RestoreReserve
	// RejectPublishes flips the shared gate so publish-class commands return
	// errs.ErrDiskFull (507) until AllowPublishes.
	RejectPublishes
	// AllowPublishes restores the publish path after recovery.
	AllowPublishes
	// Emit fires disk.degraded — on entering Low with rejecting=true, on recovering
	// with rejecting=false (the same event name both ways, §9.2's closed vocabulary).
	Emit
)

// String renders the action for logs.
func (a DiskAction) String() string {
	switch a {
	case ReleaseReserve:
		return "release_reserve"
	case RestoreReserve:
		return "restore_reserve"
	case RejectPublishes:
		return "reject_publishes"
	case AllowPublishes:
		return "allow_publishes"
	case Emit:
		return "emit"
	default:
		return "unknown"
	}
}

// recoverThreshold returns the free-space level at which DiskLow ends: MinFree scaled by
// Recover, clamped to at least MinFree so a Recover < 1 policy degrades to "recover
// exactly at MinFree" instead of never recovering.
func recoverThreshold(p DiskPolicy) int64 {
	r := p.Recover
	if r < 1 {
		r = 1
	}
	t := int64(float64(p.MinFree) * r)
	if t < p.MinFree { // float rounding must not push the band past MinFree
		t = p.MinFree
	}
	return t
}

// NextDiskState advances the machine by one reading. It is total: every (cur, free,
// policy) triple yields exactly one state plus the (possibly empty) action list to apply;
// it never panics and reads no clock. An unknown cur behaves as a no-op pass-through so a
// corrupted value cannot manufacture transitions.
func NextDiskState(cur DiskState, free int64, p DiskPolicy) (DiskState, []DiskAction) {
	if p.MinFree <= 0 { // guard disabled: safety is off, housekeeping is unaffected
		return cur, nil
	}
	switch cur {
	case DiskOK:
		if free < p.MinFree {
			return DiskLow, enterActions(p)
		}
		return DiskOK, nil
	case DiskLow:
		if free >= recoverThreshold(p) {
			return DiskOK, exitActions(p)
		}
		return DiskLow, nil
	default:
		return cur, nil
	}
}

// enterActions builds DiskOK → DiskLow's list in canonical order.
func enterActions(p DiskPolicy) []DiskAction {
	acts := make([]DiskAction, 0, 3)
	if p.Reserve > 0 {
		acts = append(acts, ReleaseReserve)
	}
	return append(acts, RejectPublishes, Emit)
}

// exitActions builds DiskLow → DiskOK's list in canonical order.
func exitActions(p DiskPolicy) []DiskAction {
	acts := make([]DiskAction, 0, 3)
	if p.Reserve > 0 {
		acts = append(acts, RestoreReserve)
	}
	return append(acts, AllowPublishes, Emit)
}
