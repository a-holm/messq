// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/errs"
)

func TestValidateUpdate(t *testing.T) {
	old := DefaultConfig("orders")
	usage := Usage{
		NowMS:       1_700_000_100_000,
		Msgs:        100,
		Bytes:       50 << 20,
		AtRiskMsgs:  60,
		AtRiskBytes: 40 << 20,
	}

	t.Run("identity patch passes", func(t *testing.T) {
		if err := ValidateUpdate(old, old, usage, false); err != nil {
			t.Fatalf("ValidateUpdate(old, old) = %v", err)
		}
	})

	t.Run("name is immutable", func(t *testing.T) {
		next := old
		next.Name = "other"
		err := ValidateUpdate(old, next, usage, true)
		if !errors.Is(err, errs.ErrBadRequest) {
			t.Fatalf("rename = %v, want bad_request", err)
		}
	})

	t.Run("narrowing subjects is allowed", func(t *testing.T) {
		next := old
		next.Subjects = []string{"orders.eu.>"}
		if err := ValidateUpdate(old, next, usage, false); err != nil {
			t.Fatalf("subject narrowing = %v", err)
		}
	})

	lowerMsgs := func(c *StreamConfig) { c.MaxMsgs = 99 }
	lowerBytes := func(c *StreamConfig) { c.MaxBytes = (50 << 20) - 1 }

	t.Run("max_msgs below usage refuses naming the risk", func(t *testing.T) {
		next := old
		lowerMsgs(&next)
		err := ValidateUpdate(old, next, usage, false)
		var wle *WouldLoseDataError
		if !errors.As(err, &wle) {
			t.Fatalf("lowered max_msgs = %v, want WouldLoseDataError", err)
		}
		if wle.AtRiskMsgs != 60 || wle.AtRiskBytes != 40<<20 {
			t.Errorf("risk numbers = %+v", wle)
		}
	})

	t.Run("max_msgs below usage accepted under allow_data_loss", func(t *testing.T) {
		next := old
		lowerMsgs(&next)
		if err := ValidateUpdate(old, next, usage, true); err != nil {
			t.Fatalf("allow_data_loss = %v", err)
		}
	})

	t.Run("max_msgs raised to unlimited passes", func(t *testing.T) {
		next := old
		next.MaxMsgs = 200
		if err := ValidateUpdate(old, next, usage, false); err != nil {
			t.Fatalf("raised max_msgs = %v", err)
		}
	})

	t.Run("max_bytes below usage refuses", func(t *testing.T) {
		next := old
		lowerBytes(&next)
		if err := ValidateUpdate(old, next, usage, false); !errors.Is(err, errs.ErrConflict) {
			t.Fatalf("lowered max_bytes = %v, want conflict-class refusal", err)
		}
	})

	t.Run("limits to workqueue refuses without allow_data_loss", func(t *testing.T) {
		next := old
		next.Retention = RetentionWorkQueue
		err := ValidateUpdate(old, next, usage, false)
		var wle *WouldLoseDataError
		if !errors.As(err, &wle) {
			t.Fatalf("retention switch = %v, want WouldLoseDataError", err)
		}
		if err2 := ValidateUpdate(old, next, usage, true); err2 != nil {
			t.Fatalf("retention switch under allow_data_loss = %v", err2)
		}
	})

	t.Run("shortened max_age past oldest message refuses", func(t *testing.T) {
		next := old
		next.MaxAge = 30 * time.Second // now - 30s > oldest(1_700_000_000_000)
		if err := ValidateUpdate(old, next, usage, false); err == nil {
			t.Fatal("shortened max_age accepted")
		}
	})
}

func TestResolveTraceID(t *testing.T) {
	deterministic := strings.Repeat("ab", 8)
	rnd := strings.NewReader(deterministic + deterministic)

	t.Run("explicit wins over traceparent", func(t *testing.T) {
		got := ResolveTraceID("4bf92f3577b34da6a3ce929d0e0e4736",
			"00-11111111111111111111111111111111-2222222222222222-01", rnd)
		if got != "4bf92f3577b34da6a3ce929d0e0e4736" {
			t.Errorf("ResolveTraceID = %q, want the explicit id", got)
		}
	})

	t.Run("valid traceparent supplies the id", func(t *testing.T) {
		got := ResolveTraceID("",
			"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", rnd)
		if got != "4bf92f3577b34da6a3ce929d0e0e4736" {
			t.Errorf("ResolveTraceID = %q, want the traceparent id", got)
		}
	})

	t.Run("malformed traceparent mints instead of failing", func(t *testing.T) {
		got := ResolveTraceID("", "not-a-traceparent", rnd)
		if len(got) != 32 {
			t.Errorf("minted trace id %q is not 32 chars", got)
		}
		for _, c := range got {
			if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
				t.Errorf("minted trace id %q is not lowercase hex", got)
				break
			}
		}
	})

	t.Run("absent inputs mint", func(t *testing.T) {
		a := ResolveTraceID("", "", strings.NewReader(strings.Repeat("\x01", 64)))
		b := ResolveTraceID("", "", strings.NewReader(strings.Repeat("\x02", 64)))
		if a == b || len(a) != 32 || len(b) != 32 {
			t.Errorf("minted ids collide or malformed: %q vs %q", a, b)
		}
	})
}
