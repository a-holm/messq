// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"errors"
	"time"
)

// The dead-letter planner of issue #12 (PLAN D3/D1, SEMANTICS S4.4 lineage, S9.2
// closed vocabulary, ADR-0004). The pure half of the DLQ: it resolves, from a fully
// populated DeadCtx and the process DLQ configuration, whether and where a death
// produces a copy. It performs no I/O and reads no wall clock — `now`, `published_at`
// and the origin headers come in as parameters so property tests and the reference
// model are deterministic. The store turns the plan into the one SQLite transaction
// (copy + delivery-row delete + the single msg.dead, D1).

// DeadTrigger names what reached the dead path (issue #11 distinguishes these and
// #12's provenance table exposes them): a plain lease timeout, an explicit nak at the
// max_deliver bound, an explicit terminal error, or a max_deliver that was lowered
// under a stranded row.
type DeadTrigger string

// The dead triggers (closed set, exhaustive-linted).
const (
	DeadTriggerAckWait       DeadTrigger = "ack_wait"
	DeadTriggerNak           DeadTrigger = "nak"
	DeadTriggerTerm          DeadTrigger = "term"
	DeadTriggerPolicyLowered DeadTrigger = "policy_lowered"
)

// DLQConfig is the process-wide template a broker-created DLQ stream is created from,
// plus the per-transaction copy budget. The template fields bind AT CREATION TIME
// ONLY (changing a flag never rewrites an existing DLQ stream); the budget fields
// bound every transaction.
type DLQConfig struct {
	// MaxAge is the max_age_ms of an auto-created DLQ stream (--dlq-max-age, 720h).
	MaxAge time.Duration
	// MaxMsgs / MaxBytes are the count/byte limits of an auto-created DLQ stream
	// (--dlq-max-msgs / --dlq-max-bytes, 0 = unlimited).
	MaxMsgs  int64
	MaxBytes int64
	// MaxMsgSize is the per-message ceiling of an auto-created DLQ stream
	// (the process --max-msg-size-ceiling): a message must always be able to die.
	MaxMsgSize int64
	// ReasonHeaderBytes caps the sanitised Messq-Last-Reason header
	// (--dlq-reason-header-bytes, 1 KiB, on a rune boundary).
	ReasonHeaderBytes int
	// MaxCopiesPerCommit bounds the copies one transaction may write
	// (--dlq-max-copies-per-commit, 128).
	MaxCopiesPerCommit int
	// MaxBytesPerCommit bounds the payload bytes one transaction may copy
	// (--dlq-max-bytes-per-commit, 32 MiB).
	MaxBytesPerCommit int64
}

// DefaultDLQConfig returns the §12 defaults behind the serve flags, given the process
// limits for the size ceiling.
func DefaultDLQConfig(limits Limits) DLQConfig {
	return DLQConfig{
		MaxAge:             720 * time.Hour,
		MaxMsgs:            0,
		MaxBytes:           0,
		MaxMsgSize:         limits.MaxMsgSizeCeiling,
		ReasonHeaderBytes:  1 << 10,
		MaxCopiesPerCommit: 128,
		MaxBytesPerCommit:  32 << 20,
	}
}

// DeadBudget bounds one transaction's DLQ copies. It is decremented BEFORE a copy is
// written; when either limit is already crossed the copy is refused without writing
// (ErrDeadBudget) and the remaining dead transitions defer to a later transaction.
// A nil *DeadBudget means unlimited.
type DeadBudget struct {
	Copies int   // remaining copies allowed in this transaction
	Bytes  int64 // remaining payload bytes allowed in this transaction
}

// NewDeadBudget returns a budget seeded from a DLQConfig's per-commit defaults.
func NewDeadBudget(cfg DLQConfig) *DeadBudget {
	return &DeadBudget{Copies: cfg.MaxCopiesPerCommit, Bytes: cfg.MaxBytesPerCommit}
}

// CanCopy reports whether a copy of size bytes is still within budget (no mutation).
func (b *DeadBudget) CanCopy(size int64) bool {
	if b == nil {
		return true
	}
	if b.Copies <= 0 {
		return false
	}
	if b.Bytes > 0 && size > b.Bytes {
		return false
	}
	return true
}

// Take reserves one copy of size bytes. The caller MUST have verified CanCopy first
// (decrement-before-write), so a take never goes negative.
func (b *DeadBudget) Take(size int64) {
	if b == nil {
		return
	}
	b.Copies--
	b.Bytes -= size
}

// ErrDeadBudget reports that the transaction's DLQ copy budget is exhausted. It is a
// plain error, deliberately NOT an errs sentinel (the S13 closed set does not grow):
// the caller treats it as "stop routing dead transitions in this transaction; the
// remaining rows retry later" — a bound on attempts, not a deadline on dying (I4).
var ErrDeadBudget = errors.New("messq: dead-letter budget exhausted for this transaction")

// DeadPlan is the pure decision of PlanDead: whether a copy is made and where it
// lands. The copy's message id is minted by the store (C1: every dead-letter copy gets
// a NEW ULID; lineage rides Messq-Origin-Id + both ids in the msg.dead detail).
type DeadPlan struct {
	// Copy is false when dead_policy=drop (delete + msg.dead only, G8).
	Copy bool
	// DLQStream is the derived "<origin>.dlq" name ("" when !Copy).
	DLQStream string
	// Subject is the ORIGINAL subject, unchanged (PLAN §5.1: "under the original
	// subject").
	Subject string
	// TraceID is the origin's trace_id, preserved byte-for-byte (S4.4).
	TraceID string
}

// PlanDead resolves the copy decision from a fully populated DeadCtx and the DLQ
// configuration. dead_policy=dlq on a non-.dlq stream copies; everything else drops.
// A consumer on a .dlq stream cannot carry dead_policy=dlq (#9 refuses it), so the
// derived name never chains: orders.dlq.dlq is impossible.
func PlanDead(d DeadCtx, _ DLQConfig) DeadPlan {
	plan := DeadPlan{Subject: d.Subject, TraceID: d.TraceID}
	if d.Policy != DeadPolicyDLQ {
		return plan // Copy=false, DLQStream=""
	}
	plan.Copy = true
	plan.DLQStream = DLQName(d.Stream)
	return plan
}
