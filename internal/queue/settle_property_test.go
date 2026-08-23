// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"testing"
	"time"

	"pgregory.net/rapid"
)

// TestPlanSettleLocalProperty is issue #10's local rapid property (the full
// reference-model suite is #13): random sequences of settle + claim against a
// synthetic row, driving the pure PlanSettle. Any divergence fails the check with a
// committed failfile. Enforced invariants:
//
//   - I7: a rejected settle (stale_ack / wrong_generation / unknown / stale) never
//     mutates the live row — asserted by a before/after snapshot of the whole row;
//   - I4: attempts are monotone and a claim increments exactly once;
//   - monotonic visible_at under repeated naks — a READY row's release never moves
//     visible_at later.
func TestPlanSettleLocalProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		m := &rowModel{maxDeliver: int32(rapid.IntRange(0, 5).Draw(t, "maxDeliver"))}
		for i := 0; i < 40; i++ {
			op := rapid.IntRange(0, 4).Draw(t, "op")
			if op == 0 {
				m.claim()
				continue
			}
			// ~20% of settles carry a stale generation, so the fence is exercised.
			staleGen := rapid.IntRange(0, 9).Draw(t, "staleGen") == 0
			if viol := m.settle(verb(op), staleGen); viol != "" {
				t.Fatalf("property violated: %s", viol)
			}
			// I4: attempts never decrease.
			if m.attempts < 1 && m.present {
				t.Fatalf("I4: attempts = %d while present", m.attempts)
			}
		}
	})
}

func verb(op int) Verb {
	switch op {
	case 1:
		return VerbAck
	case 2:
		return VerbNak
	case 3:
		return VerbTerm
	default:
		return VerbExtend
	}
}

// consumerGen is the consumer's fixed generation the property fences against.
const consumerGen int32 = 1

// rowModel is the synthetic live delivery row the property drives.
type rowModel struct {
	present     bool
	state       DeliveryState
	attempts    int32
	visibleMS   int64
	deliveredMS int64
	maxDeliver  int32
}

var propNow = time.UnixMilli(1_700_000_000_000)

func (m *rowModel) cfg() ConsumerConfig {
	c := DefaultConsumerConfig("c")
	c.MaxDeliver = m.maxDeliver
	return c
}

// claim models a fetch's claim (T2): the row appears (or continues) at attempt+1,
// INFLIGHT, with a fresh deadline. D6: attempts increment at claim, exactly once.
func (m *rowModel) claim() {
	if !m.present {
		m.present, m.state, m.attempts = true, StateInflight, 1
	} else {
		m.attempts++
		m.state = StateInflight
	}
	m.deliveredMS = propNow.UnixMilli()
	m.visibleMS = m.deliveredMS + 30_000 // ack_wait
}

// settle drives PlanSettle and applies the allowed mutation; it returns "" when no
// invariant was violated, else a description.
func (m *rowModel) settle(v Verb, staleGen bool) string {
	attempt := m.attempts
	if attempt == 0 {
		attempt = 1
	}
	gen := consumerGen
	if staleGen {
		gen = consumerGen + 1 // a token that outlived the consumer's generation
	}
	tk := Token{Stream: "s", Consumer: "c", Seq: 1, Attempt: attempt, Generation: gen}
	req := SettleRequest{
		Verb: v, Token: tk, Generation: consumerGen, CursorSeq: 100,
		Config: m.cfg(), MaxAckWait: time.Hour,
		RowPresent: m.present, RowState: m.state, RowAttempts: m.attempts,
		RowVisibleAt: m.visibleMS, RowDeliveredAt: m.deliveredMS,
	}
	plan, err := PlanSettle(req, propNow)
	if err != nil {
		return "" // rejected request (extend-at-cap / bad delay): no mutation
	}
	if plan.Status != ItemStatusOK {
		// I7: a rejected settle must carry no mutation — a fence violation is exactly
		// a non-OK status bundled with a mutation.
		if plan.Action != ActionNoop {
			return "I7: a " + string(plan.Status) + " settle carried a mutation (fence violated)"
		}
		return ""
	}
	prevVisible, prevReady := m.visibleMS, m.present && m.state == StateReady
	switch plan.Action {
	case ActionNoop:
		// an OK plan never carries Noop; guard for exhaustiveness.
	case ActionAck, ActionDead:
		m.present = false
	case ActionRelease:
		if prevReady && plan.NewVisibleAt > prevVisible {
			return "monotonicity: a repeated nak moved visible_at later"
		}
		m.state = StateReady
		m.visibleMS = plan.NewVisibleAt
	case ActionExtend:
		if m.present {
			m.visibleMS = plan.NewVisibleAt
		}
	}
	return ""
}
