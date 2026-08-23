// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"testing"
	"time"
)

// TestPlanSettleMatrix is the exhaustive cross-product of issue #10 §Test plan: verb ×
// {absent<cursor, absent>=cursor, READY, INFLIGHT} × attempt{<,=,>} ×
// generation{<,=,>} × {attempts<max_deliver, attempts==max_deliver}. Every one of the
// 4*4*3*3*2 = 288 combinations must be classified by PlanSettle into one of the five
// statuses and a mutation (or Noop); an unclassified or wrong combination fails. This
// is the "no case unthought" gate a reviewer reads first.

func TestPlanSettleMatrix(t *testing.T) {
	const gen = int32(10)
	now := time.UnixMilli(1_700_000_000_000)
	mk := func() SettleRequest {
		r := SettleRequest{
			Generation: gen, CursorSeq: 100,
			Config:         DefaultConsumerConfig("worker"),
			MaxAckWait:     time.Hour,
			RowState:       StateInflight,
			RowAttempts:    3,
			RowVisibleAt:   now.UnixMilli(),
			RowDeliveredAt: now.Add(-30 * time.Second).UnixMilli(),
		}
		r.Config.AckWait = 30 * time.Second
		return r
	}

	rowStates := []struct {
		name    string
		present bool
		state   DeliveryState
	}{
		{"absent<cursor", false, StateReady},
		{"absent>=cursor", false, StateReady},
		{"READY", true, StateReady},
		{"INFLIGHT", true, StateInflight},
	}
	attemptRels := []struct {
		name string
		tok  int32
	}{{"<", 2}, {"=", 3}, {">", 4}}
	genRels := []struct {
		name string
		tok  int32
	}{{"<", gen - 1}, {"=", gen}, {">", gen + 1}}
	maxOf := []struct {
		name string
		md   int32
	}{{"below", 5}, {"at", 3}}
	verbs := []Verb{VerbAck, VerbNak, VerbTerm, VerbExtend}

	total := 0
	for _, v := range verbs {
		for _, rs := range rowStates {
			for _, a := range attemptRels {
				for _, g := range genRels {
					for _, m := range maxOf {
						total++
						req := mk()
						req.Verb = v
						req.Token = Token{Stream: "s", Consumer: "c", Seq: 50, Attempt: a.tok, Generation: g.tok}
						req.RowPresent = rs.present
						req.RowState = rs.state
						req.Config.MaxDeliver = m.md
						if rs.present {
							req.Token.Seq = 50
						} else if g.name == "=" && a.name == "=" {
							// absent rows decide stale vs unknown by seq against cursor.
							req.Token.Seq = 50 // < cursor
							if rs.name == "absent>=cursor" {
								req.Token.Seq = 150
							}
						}
						name := string(v) + ":" + rs.name + ":" + a.name + ":" + g.name + ":" + m.name
						plan, err := PlanSettle(req, now)
						if err != nil {
							t.Fatalf("[%s] PlanSettle error %v; every matrix combo must classify", name, err)
						}
						want := reference(req)
						if plan.Status != want.status || plan.Action != want.action ||
							plan.Late != want.late || plan.Dead != want.dead || plan.Cause != want.cause {
							t.Fatalf("[%s] PlanSettle = {%s %d late=%t dead=%t cause=%s}, want {%s %d late=%t dead=%t cause=%s}",
								name, plan.Status, plan.Action, plan.Late, plan.Dead, plan.Cause,
								want.status, want.action, want.late, want.dead, want.cause)
						}
					}
				}
			}
		}
	}
	if total != 288 {
		t.Fatalf("cross-product enumerated %d combos, want 288", total)
	}
}

// decision is the reference expectation tuple, kept independent of SettlePlan's layout.
type decision struct {
	status ItemStatus
	action SettleAction
	late   bool
	dead   bool
	cause  DeadCause
}

// reference is the decision the exhaustive cross-product asserts PlanSettle matches. It
// mirrors issue #10 §3's normative table by independent reasoning so a mutation cannot
// make both red.
func reference(req SettleRequest) decision {
	// Fence step 3: generation mismatch wins over everything.
	if req.Token.Generation != req.Generation {
		return decision{ItemStatusWrongGen, ActionNoop, false, false, ""}
	}
	if !req.RowPresent {
		if req.Token.Seq < req.CursorSeq {
			return decision{ItemStatusStale, ActionNoop, false, false, ""}
		}
		return decision{ItemStatusUnknown, ActionNoop, false, false, ""}
	}
	if req.Token.Attempt != req.RowAttempts {
		return decision{ItemStatusStaleAck, ActionNoop, false, false, ""}
	}
	switch req.Verb {
	case VerbAck:
		return decision{ItemStatusOK, ActionAck, req.RowState == StateReady, false, ""}
	case VerbNak:
		if req.RowState == StateInflight && req.Config.MaxDeliver > 0 && req.RowAttempts >= req.Config.MaxDeliver {
			return decision{ItemStatusOK, ActionDead, false, true, DeadCauseMaxDeliver}
		}
		return decision{ItemStatusOK, ActionRelease, false, false, ""}
	case VerbTerm:
		return decision{ItemStatusOK, ActionDead, false, true, DeadCauseTerminated}
	case VerbExtend:
		if req.RowState != StateInflight {
			return decision{ItemStatusStaleAck, ActionNoop, false, false, ""}
		}
		return decision{ItemStatusOK, ActionExtend, false, false, ""}
	default:
		return decision{ItemStatusUnknown, ActionNoop, false, false, ""}
	}
}

// TestPlanSettleWrongGenAndStaleDetails pins the reason strings and counts of the
// fence rejections the cross-product asserts as actionless — the I7 no-mutation cells.
func TestPlanSettleWrongGenAndStaleDetails(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	base := SettleRequest{
		Generation: 5, CursorSeq: 100, Config: DefaultConsumerConfig("c"),
		RowPresent: true, RowState: StateInflight, RowAttempts: 3,
		RowDeliveredAt: now.Add(-time.Second).UnixMilli(),
		MaxAckWait:     time.Hour,
	}
	base.Config.AckWait = 30 * time.Second

	gen := base
	gen.Verb = VerbAck
	gen.Token = Token{"s", "c", 50, 3, 6} // generation newer than consumer's 5
	p, err := PlanSettle(gen, now)
	if err != nil || p.Status != ItemStatusWrongGen || p.Reason != "generation" || p.Action != ActionNoop {
		t.Fatalf("gen mismatch = %+v err=%v, want wrong_generation/noop/reason generation", p, err)
	}

	stale := base
	stale.Verb = VerbAck
	stale.Token = Token{"s", "c", 50, 4, 5} // attempt 4 vs row attempt 3
	p2, err := PlanSettle(stale, now)
	if err != nil || p2.Status != ItemStatusStaleAck || p2.CurrentAttempt != 3 || p2.Action != ActionNoop {
		t.Fatalf("attempt-mismatch = %+v err=%v, want stale_ack currentAttempt=3 noop", p2, err)
	}
}
