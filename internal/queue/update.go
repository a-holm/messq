// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"fmt"
	"io"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/id"
)

// WouldLoseDataError reports a stream update the janitor would turn into deletions on
// its next pass. The counts are measured by the storage layer before validation and
// travel in the error, so the API names the messages and bytes at risk instead of a
// vague refusal.
type WouldLoseDataError struct {
	Field       string
	AtRiskMsgs  int64
	AtRiskBytes int64
}

func (e *WouldLoseDataError) Error() string {
	return fmt.Sprintf("changing %s would delete %d messages (%d bytes);"+
		" pass allow_data_loss to proceed", e.Field, e.AtRiskMsgs, e.AtRiskBytes)
}
func (e *WouldLoseDataError) Unwrap() error { return errs.ErrConflict }

// Usage is the live state an update decision needs: what the stream holds now and
// which of it falls outside the proposed limits. AtRisk* are storage-measured; this
// package only decides whether they constitute a refusal.
type Usage struct {
	NowMS       int64 // wall-clock ms the age comparison runs against
	Msgs        int64 // rows in messages for this stream
	Bytes       int64 // their summed size
	AtRiskMsgs  int64 // rows the new limits would delete on the janitor's next pass
	AtRiskBytes int64 // their summed size
}

// ValidateUpdate decides the data-loss rules between one stored config and the next.
// Everything else (field validity, name rules) is ValidateStreamConfig's job and runs
// first; here only the deltas that could delete stored data are judged:
//
//   - Name is immutable;
//   - lowering max_msgs or max_bytes below current usage refuses with
//     WouldLoseDataError unless allowLoss;
//   - shortening max_age so the oldest stored message falls outside refuses likewise
//     (the store measures that case into Usage.AtRisk*);
//   - switching retention limits → workqueue refuses likewise: workqueue deletes every
//     acked message, and existing unacked rows predate the policy change.
//
// Narrowing subjects never refuses — it only affects future publishes — but the store
// reports how many stored rows no longer match alongside the new info.
func ValidateUpdate(old, next StreamConfig, u Usage, allowLoss bool) error {
	if next.Name != old.Name {
		return errs.E(errs.ErrBadRequest, "",
			"stream names are immutable: %q cannot become %q", old.Name, next.Name)
	}
	refuse := func(field string) error {
		if allowLoss {
			return nil
		}
		return &WouldLoseDataError{Field: field, AtRiskMsgs: u.AtRiskMsgs, AtRiskBytes: u.AtRiskBytes}
	}
	if next.MaxMsgs != 0 && u.Msgs > next.MaxMsgs && (old.MaxMsgs == 0 || old.MaxMsgs > next.MaxMsgs) {
		if err := refuse("max_msgs"); err != nil {
			return err
		}
	}
	if next.MaxBytes != 0 && u.Bytes > next.MaxBytes && (old.MaxBytes == 0 || old.MaxBytes > next.MaxBytes) {
		if err := refuse("max_bytes"); err != nil {
			return err
		}
	}
	if next.MaxAge != 0 && u.AtRiskMsgs > 0 &&
		(old.MaxAge == 0 || old.MaxAge > next.MaxAge) {
		if err := refuse("max_age"); err != nil {
			return err
		}
	}
	if old.Retention == RetentionLimits && next.Retention == RetentionWorkQueue {
		if err := refuse("retention"); err != nil {
			return err
		}
	}
	return nil
}

// ResolveTraceID applies the Messq-Trace-Id precedence: an explicit id wins; else the
// 32-hex trace-id field of a W3C traceparent; else a fresh mint from rnd. A malformed
// traceparent mints rather than fails — observability must never reject data. The
// explicit value must already have passed ValidateTraceIDToken by its caller.
func ResolveTraceID(explicit, traceparent string, rnd io.Reader) string {
	if explicit != "" {
		return explicit
	}
	if tid, _, _, ok := id.ParseTraceparent(traceparent); ok {
		return tid.String()
	}
	return id.NewTraceID(rnd).String()
}
