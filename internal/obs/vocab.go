// SPDX-License-Identifier: Apache-2.0

package obs

import (
	"fmt"
	"log/slog"
)

// Kind identifies one event of messq's closed event vocabulary (PLAN §9.2, SEMANTICS
// S2.4). The set is frozen at 1.0: renaming a member, adding one or removing one is a
// breaking change under S16 and must land as a reviewable diff to this table plus the
// TestVocabularyNames golden plus docs/log-schema.md plus a #13 fold rule.
//
// The DB column, the log field and the metric label ALL derive from [Kind.String], so
// the three surfaces cannot disagree about a name — drift is inexpressible (D11).
type Kind uint8

// The members, in PLAN §9.2 order. KindInvalid is the zero value so that a forgotten
// initialisation can never pass as a real event; it is not part of the closed set.
const (
	KindInvalid Kind = iota

	ServerStart
	ServerStop
	ServerReload
	RecoveryUnclean
	RecoveryReclaimed
	StorageFatal

	StreamCreate
	StreamUpdate
	StreamDelete
	StreamPurge
	RetentionExpire
	RetentionBlocked

	ConsumerCreate
	ConsumerUpdate
	ConsumerDelete
	ConsumerSeek
	ConsumerPause
	ConsumerLag

	MsgPublish
	MsgDup
	MsgDeliver
	MsgAck
	MsgAckDup
	MsgAckStale

	MsgNak
	MsgTerm
	MsgExtend
	MsgTimeout
	MsgDead

	DLQRedrive
	FlowBlocked
	DiskDegraded
	AuthDenied
	APIError
	AdminAction
)

// numKinds counts the Kind slots INCLUDING KindInvalid. It deliberately lives OUTSIDE
// the enum's const block and as an untyped constant: a count declared inside the iota
// block becomes a member of the type, and then every switch over Kind needs a case for
// the count sentinel. Adding a member means extending both the block above and the
// metadata table below; TestVocabularyNames fails until they agree.
const numKinds = int(AdminAction) + 1

// meta is one row of the vocabulary table: everything the pipeline knows about a kind.
//
//	level  baseline severity; an Event may RAISE it at runtime (msg.extend capped,
//	       api.error 5xx) via Event.Level, never lower it — Validate enforces.
//	sample may --log-sample drop it from LOGS. Only ever true together with level ==
//	       DEBUG (ADR-0012's allow-list becomes this derived property); the events
//	       TABLE is never sampled regardless (D11).
//	repeat subject to --event-repeat-interval row limiting (§8). State-change kinds
//	       are never limited: 10 000 timeouts ⇒ 10 000 rows.
//	fold   §5.2 I10: #13's fold model must have a rule reproducing this kind when it
//	       replays the folded journal (TestEventFoldCoverage cross-references).
type meta struct {
	name   string     // the wire/DB/log identifier, frozen at 1.0
	level  slog.Level // baseline; runtime may only raise
	sample bool       // sampleable from logs; implies level == DEBUG
	repeat bool       // repeat-limited; state-change kinds excluded
	fold   bool       // needs a fold rule in #13's model
}

// kinds is THE vocabulary table (issue #19 §2, verbatim). Index i describes Kind(i);
// row 0 is KindInvalid's deliberately empty entry. Read it through the accessor methods,
// which bounds-check so a stray cast panics nothing and Validate owns the loud refusal.
var kinds = [numKinds]meta{
	KindInvalid: {},

	ServerStart:       {name: "server.start", level: slog.LevelInfo},
	ServerStop:        {name: "server.stop", level: slog.LevelInfo},
	ServerReload:      {name: "server.reload", level: slog.LevelInfo},
	RecoveryUnclean:   {name: "recovery.unclean", level: slog.LevelWarn},
	RecoveryReclaimed: {name: "recovery.reclaimed", level: slog.LevelInfo, fold: true},
	StorageFatal:      {name: "storage.fatal", level: slog.LevelError},

	StreamCreate:     {name: "stream.create", level: slog.LevelInfo, fold: true},
	StreamUpdate:     {name: "stream.update", level: slog.LevelInfo, fold: true},
	StreamDelete:     {name: "stream.delete", level: slog.LevelWarn, fold: true},
	StreamPurge:      {name: "stream.purge", level: slog.LevelWarn, fold: true},
	RetentionExpire:  {name: "retention.expire", level: slog.LevelInfo, fold: true},
	RetentionBlocked: {name: "retention.blocked", level: slog.LevelWarn},

	ConsumerCreate: {name: "consumer.create", level: slog.LevelInfo, fold: true},
	ConsumerUpdate: {name: "consumer.update", level: slog.LevelInfo, fold: true},
	ConsumerDelete: {name: "consumer.delete", level: slog.LevelWarn, fold: true},
	ConsumerSeek:   {name: "consumer.seek", level: slog.LevelWarn, fold: true},
	ConsumerPause:  {name: "consumer.pause", level: slog.LevelInfo, fold: true},
	ConsumerLag:    {name: "consumer.lag", level: slog.LevelInfo, repeat: true},

	MsgPublish:  {name: "msg.publish", level: slog.LevelDebug, sample: true, fold: true},
	MsgDup:      {name: "msg.dup", level: slog.LevelDebug, sample: true},
	MsgDeliver:  {name: "msg.deliver", level: slog.LevelDebug, sample: true, fold: true},
	MsgAck:      {name: "msg.ack", level: slog.LevelDebug, sample: true, fold: true},
	MsgAckDup:   {name: "msg.ack_dup", level: slog.LevelDebug, sample: true, repeat: true},
	MsgAckStale: {name: "msg.ack_stale", level: slog.LevelWarn, repeat: true},

	MsgNak:     {name: "msg.nak", level: slog.LevelWarn, fold: true},
	MsgTerm:    {name: "msg.term", level: slog.LevelWarn, fold: true},
	MsgExtend:  {name: "msg.extend", level: slog.LevelDebug, sample: true},
	MsgTimeout: {name: "msg.timeout", level: slog.LevelWarn, fold: true},
	MsgDead:    {name: "msg.dead", level: slog.LevelWarn, fold: true},

	DLQRedrive:   {name: "dlq.redrive", level: slog.LevelInfo, fold: true},
	FlowBlocked:  {name: "flow.blocked", level: slog.LevelWarn, repeat: true},
	DiskDegraded: {name: "disk.degraded", level: slog.LevelWarn, repeat: true},
	AuthDenied:   {name: "auth.denied", level: slog.LevelWarn, repeat: true},
	APIError:     {name: "api.error", level: slog.LevelWarn, repeat: true},
	AdminAction:  {name: "admin.action", level: slog.LevelInfo},
}

// String returns the kind's frozen identifier ("msg.publish"), the single source for the
// events.event column, the log line's event field and the metric's event label. The empty
// string marks values outside the closed set, including the zero value KindInvalid.
func (k Kind) String() string {
	if int(k) >= numKinds {
		return ""
	}
	return kinds[k].name
}

// Level returns the kind's baseline severity. Out-of-range values yield the zero level;
// raising past it at runtime is Validate's business, not the caller's.
func (k Kind) Level() slog.Level {
	if int(k) >= numKinds {
		return 0
	}
	return kinds[k].level
}

// Sampleable reports whether --log-sample may ever drop this kind from logs. Only DEBUG
// baselines qualify (TestVocabConsistency enforces the equivalence); the events table is
// never sampled either way.
func (k Kind) Sampleable() bool {
	return int(k) < numKinds && kinds[k].sample
}

// Repeatable reports whether --event-repeat-interval may collapse repeated rows of this
// kind into one row carrying detail.suppressed=N. State-change kinds answer false.
func (k Kind) Repeatable() bool {
	return int(k) < numKinds && kinds[k].repeat
}

// Fold reports whether replaying the folded journal must reproduce this kind's effect,
// which makes it #13's fold model's responsibility (§5.2 I10).
func (k Kind) Fold() bool {
	return int(k) < numKinds && kinds[k].fold
}

// ParseKind looks a vocabulary name up exactly — case-sensitive, whitespace-significant —
// and answers the kind it indexes. It serves #20's ?event= filter; anything that is not a
// verbatim member of the closed set, including the empty string, is refused.
func ParseKind(name string) (Kind, error) {
	for i := 1; i < numKinds; i++ {
		if kinds[i].name == name {
			return Kind(i), nil
		}
	}
	return KindInvalid, fmt.Errorf("obs: %q is not a member of the closed event vocabulary", name)
}
