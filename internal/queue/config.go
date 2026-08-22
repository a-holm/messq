// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"time"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/subject"
)

// Retention is the closed set of stream retention policies (§4.2).
type Retention string

// The retention policies.
const (
	// RetentionLimits keeps messages until a limit (msgs/bytes/age) forces them out.
	RetentionLimits Retention = "limits"
	// RetentionWorkQueue removes every message once its last consumer acks it.
	RetentionWorkQueue Retention = "workqueue"
)

// Discard is the closed set of behaviours when a stream is at a limit and a publish
// arrives (enforced from #27; this issue validates and stores it).
type Discard string

// The discard policies.
const (
	// DiscardOld evicts the oldest message to make room.
	DiscardOld Discard = "old"
	// DiscardNew refuses the arriving publish with errs.ErrStreamFull.
	DiscardNew Discard = "new"
)

// StreamConfig is the validated shape of a stream's configuration, as stored in the
// streams row and as the publish path consumes it. Durations carry milliseconds on the
// wire; here they are time.Duration so arithmetic stays honest.
type StreamConfig struct {
	Name        string
	Subjects    []string      // publish-accepted patterns, NATS syntax, matcher from #3
	Retention   Retention     // "limits" | "workqueue"
	MaxMsgs     int64         // 0 = unlimited
	MaxBytes    int64         // 0 = unlimited
	MaxAge      time.Duration // 0 = unlimited
	MaxMsgSize  int64         // per-message body cap in bytes, >= 1
	Discard     Discard       // "old" | "new"
	DedupWindow time.Duration // 0 = dedup disabled
}

// Limits are the process-wide ceilings from serve flags (§8 of issue #7): they bound
// what any single stream may ask for.
type Limits struct {
	MaxMsgSizeCeiling int64         // 8 MiB hard ceiling (PLAN §4.2)
	MaxHeaderBytes    int           // total user-header JSON per message, 4 KiB
	MaxHeaders        int           // headers per message, 32
	MaxSubjects       int           // patterns per stream, 32
	MaxDedupWindow    time.Duration // 24 h
}

// Defaults for a stream created without an explicit field (issue §1) and the default
// process limits.
const (
	defaultMaxAge      = 7 * 24 * time.Hour
	defaultMaxMsgSize  = int64(1) << 20
	defaultDedupWindow = 120 * time.Second
)

// DefaultConfig returns the configuration a POST /v1/streams body that names nothing
// but the stream itself receives: subjects [">"], retention limits, discard old,
// 7-day age, 1 MiB bodies, a two-minute dedup window.
func DefaultConfig(name string) StreamConfig {
	return StreamConfig{
		Name:        name,
		Subjects:    []string{">"},
		Retention:   RetentionLimits,
		MaxAge:      defaultMaxAge,
		MaxMsgSize:  defaultMaxMsgSize,
		Discard:     DiscardOld,
		DedupWindow: defaultDedupWindow,
	}
}

// DefaultLimits returns the §4.2 ceilings behind the serve-flag defaults.
func DefaultLimits() Limits {
	return Limits{
		MaxMsgSizeCeiling: 8 << 20,
		MaxHeaderBytes:    4 << 10,
		MaxHeaders:        32,
		MaxSubjects:       32,
		MaxDedupWindow:    24 * time.Hour,
	}
}

// ValidateStreamConfig checks one whole configuration against the process limits:
// name rules (including the ".dlq" reservation), subject-pattern grammar and count via
// the #3 compiler, the two closed sets, non-negative unlimited-able fields, the
// per-message size window [1, MaxMsgSizeCeiling], and the dedup window window.
func ValidateStreamConfig(c StreamConfig, l Limits) error {
	if err := ValidateStreamName(c.Name); err != nil {
		return err
	}
	if len(c.Subjects) < 1 || len(c.Subjects) > l.MaxSubjects {
		return errs.E(errs.ErrBadRequest, "",
			"a stream carries %d subject patterns, want 1..%d", len(c.Subjects), l.MaxSubjects)
	}
	if _, err := subject.ParseSet(c.Subjects); err != nil {
		return err
	}
	switch c.Retention {
	case RetentionLimits, RetentionWorkQueue:
	default:
		return errs.E(errs.ErrBadRequest, "", "retention %q is not one of \"limits\", \"workqueue\"", c.Retention)
	}
	switch c.Discard {
	case DiscardOld, DiscardNew:
	default:
		return errs.E(errs.ErrBadRequest, "", "discard %q is not one of \"old\", \"new\"", c.Discard)
	}
	for name, v := range map[string]int64{"max_msgs": c.MaxMsgs, "max_bytes": c.MaxBytes} {
		if v < 0 {
			return errs.E(errs.ErrBadRequest, "", "%s is %d, want >= 0 (0 = unlimited)", name, v)
		}
	}
	if c.MaxAge < 0 {
		return errs.E(errs.ErrBadRequest, "", "max_age is %v, want >= 0 (0 = unlimited)", c.MaxAge)
	}
	if c.MaxMsgSize < 1 || c.MaxMsgSize > l.MaxMsgSizeCeiling {
		// A misconfigured ceiling is bad_request; only an actual oversized body is
		// errs.ErrTooLarge ("too_large" in the API's error enum).
		return errs.E(errs.ErrBadRequest, "",
			"max_msg_size is %d, want 1..%d", c.MaxMsgSize, l.MaxMsgSizeCeiling)
	}
	if c.DedupWindow < 0 || c.DedupWindow > l.MaxDedupWindow {
		return errs.E(errs.ErrBadRequest, "",
			"dedup_window is %v, want 0..%v", c.DedupWindow, l.MaxDedupWindow)
	}
	return nil
}
