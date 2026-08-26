package queue

import (
	"slices"
	"testing"
)

// NextDiskState's nasty-table tests (#27 slice 1, G5). The disk state machine is pure
// arithmetic over (state, free bytes, policy); the syscalls live in internal/janitor's
// DiskProbe. Hysteresis (MinFree × Recover) is what bounds flapping: a mutant that
// recovers at MinFree itself must fail TestNextDiskStateFlappingSeries.

func TestNextDiskStateDisabledGuard(t *testing.T) {
	// --min-free-bytes 0 disables the guard entirely: never low, never any action,
	// whatever the reading says.
	for _, free := range []int64{0, -1, 1 << 20} {
		state, actions := NextDiskState(DiskOK, free, DiskPolicy{})
		if state != DiskOK || len(actions) != 0 {
			t.Fatalf("MinFree=0 free=%d: (%v, %v), want (DiskOK, nil)", free, state, actions)
		}
	}
}

func TestNextDiskStateStaysOKAboveMark(t *testing.T) {
	p := DiskPolicy{MinFree: 256 << 20, Recover: 1.25, Reserve: 64 << 20}
	state, actions := NextDiskState(DiskOK, p.MinFree, p)
	if state != DiskOK || len(actions) != 0 {
		t.Fatalf("at exactly MinFree: (%v,%v), want (DiskOK, nil): the mark is strictly-less", state, actions)
	}
}

func TestNextDiskStateEntersLow(t *testing.T) {
	p := DiskPolicy{MinFree: 256 << 20, Recover: 1.25, Reserve: 64 << 20}
	state, actions := NextDiskState(DiskOK, p.MinFree-1, p)
	if state != DiskLow {
		t.Fatalf("state = %v, want DiskLow", state)
	}
	want := []DiskAction{ReleaseReserve, RejectPublishes, Emit}
	if !slices.Equal(actions, want) {
		t.Fatalf("actions = %v, want %v", actions, want)
	}
}

func TestNextDiskStateHysteresisHoldsLow(t *testing.T) {
	// Between MinFree and the recovery threshold a Low stream stays Low with no actions:
	// that band is exactly what hysteresis exists to keep quiet.
	p := DiskPolicy{MinFree: 256 << 20, Recover: 1.25, Reserve: 64 << 20}
	for _, free := range []int64{p.MinFree, p.MinFree + 1000, (p.MinFree + recoverThreshold(p)) / 2} {
		state, actions := NextDiskState(DiskLow, free, p)
		if state != DiskLow || len(actions) != 0 {
			t.Fatalf("free=%d inside hysteresis band: (%v,%v), want (DiskLow, nil)", free, state, actions)
		}
	}
}

func TestNextDiskStateRecovers(t *testing.T) {
	p := DiskPolicy{MinFree: 256 << 20, Recover: 1.25, Reserve: 64 << 20}
	at := recoverThreshold(p)
	state, actions := NextDiskState(DiskLow, at, p)
	if state != DiskOK {
		t.Fatalf("state = %v, want DiskOK at the threshold %d", state, at)
	}
	want := []DiskAction{RestoreReserve, AllowPublishes, Emit}
	if !slices.Equal(actions, want) {
		t.Fatalf("actions = %v, want %v", actions, want)
	}
}

func TestNextDiskStateFlappingSeries(t *testing.T) {
	// The brief's named red: a series oscillating around MinFree but always below the
	// recovery threshold must produce EXACTLY ONE transition. A mutant without
	// hysteresis flaps — an event per oscillation, a reserve unlink/fallocate pair per
	// oscillation.
	p := DiskPolicy{MinFree: 1000, Recover: 1.25, Reserve: 64}
	series := []int64{999, 1001, 998, 1200, 1001, 999, 1249, 1000, 1100}

	transitions := 0
	cur := DiskOK
	for _, free := range series {
		next, actions := NextDiskState(cur, free, p)
		if next != cur && slices.Contains(actions, Emit) {
			transitions++
		}
		cur = next
	}
	if transitions != 1 {
		t.Fatalf("transitions = %d, want 1: hysteresis must bound flapping", transitions)
	}
	if cur != DiskLow {
		t.Fatalf("final state = %v, want DiskLow", cur)
	}
}

func TestNextDiskStateReserveDisabledOmitsReserveActions(t *testing.T) {
	// --disk-reserve-bytes 0: enter/exit still gate publishes and emit, but no reserve
	// action appears.
	p := DiskPolicy{MinFree: 1000, Recover: 1.25}
	_, enter := NextDiskState(DiskOK, 999, p)
	if !slices.Equal(enter, []DiskAction{RejectPublishes, Emit}) {
		t.Fatalf("enter actions = %v, want [RejectPublishes Emit]", enter)
	}
	_, exit := NextDiskState(DiskLow, 1250, p)
	if !slices.Equal(exit, []DiskAction{AllowPublishes, Emit}) {
		t.Fatalf("exit actions = %v, want [AllowPublishes Emit]", exit)
	}
}

func TestNextDiskStateRecoverBelowOneClampsToNoBand(t *testing.T) {
	// Recover < 1 is operator error; clamp to 1 so leaving Low happens exactly at
	// MinFree rather than never.
	p := DiskPolicy{MinFree: 1000, Recover: 0.5}
	state, _ := NextDiskState(DiskLow, 1000, p)
	if state != DiskOK {
		t.Fatalf("state = %v, want DiskOK: clamped threshold == MinFree", state)
	}
}

func TestNextDiskStateStaysLowWhenStillScarce(t *testing.T) {
	p := DiskPolicy{MinFree: 1000, Recover: 1.25, Reserve: 64}
	state, actions := NextDiskState(DiskLow, 10, p)
	if state != DiskLow || len(actions) != 0 {
		t.Fatalf("(%v,%v), want (DiskLow, nil): repeated ticks re-emit nothing", state, actions)
	}
}

func TestNextDiskStateTotalOverInputs(t *testing.T) {
	// Total function: every (state, free) pair yields one of the two states and never
	// panics — the fuzzers (#33) will drive this directly.
	p := DiskPolicy{MinFree: 8, Recover: 2, Reserve: 4}
	for _, s := range []DiskState{DiskOK, DiskLow} {
		for _, free := range []int64{-1, 0, 7, 8, 15, 16, 1 << 40} {
			got, _ := NextDiskState(s, free, p)
			if got != DiskOK && got != DiskLow {
				t.Fatalf("state=%v free=%d produced out-of-range state %v", s, free, got)
			}
		}
	}
}
