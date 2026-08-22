// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/errs"
)

func validConfig() StreamConfig {
	return StreamConfig{
		Name:        "orders",
		Subjects:    []string{"orders.>"},
		Retention:   RetentionLimits,
		MaxAge:      7 * 24 * time.Hour,
		MaxMsgSize:  1 << 20,
		Discard:     DiscardOld,
		DedupWindow: 120 * time.Second,
	}
}

func TestValidateStreamConfig(t *testing.T) {
	const mib = int64(1) << 20
	tests := []struct {
		name    string
		mutate  func(*StreamConfig)
		wantErr error
	}{
		{"valid", func(*StreamConfig) {}, nil},
		{"bad name", func(c *StreamConfig) { c.Name = "orders.dlq" }, ErrReservedName},
		{"no subjects", func(c *StreamConfig) { c.Subjects = nil }, errs.ErrBadRequest},
		{"empty subject", func(c *StreamConfig) { c.Subjects = []string{""} }, errs.ErrBadSubject},
		{"invalid pattern", func(c *StreamConfig) { c.Subjects = []string{"orders..x"} }, errs.ErrBadSubject},
		{"too many subjects", func(c *StreamConfig) {
			c.Subjects = strings.Split(strings.Repeat("a.b.", 8)+"a", "")[:0]
			c.Subjects = make([]string, 33)
			for i := range c.Subjects {
				c.Subjects[i] = "s" + string(rune('a'+i))
			}
		}, errs.ErrBadRequest},
		{"retention closed set", func(c *StreamConfig) { c.Retention = "fancy" }, errs.ErrBadRequest},
		{"discard closed set", func(c *StreamConfig) { c.Discard = "maybe" }, errs.ErrBadRequest},
		{"negative max msgs", func(c *StreamConfig) { c.MaxMsgs = -1 }, errs.ErrBadRequest},
		{"negative max bytes", func(c *StreamConfig) { c.MaxBytes = -1 }, errs.ErrBadRequest},
		{"negative max age", func(c *StreamConfig) { c.MaxAge = -time.Second }, errs.ErrBadRequest},
		{"max msg size zero", func(c *StreamConfig) { c.MaxMsgSize = 0 }, errs.ErrBadRequest},
		{"max msg size at ceiling", func(c *StreamConfig) { c.MaxMsgSize = 8 * mib }, nil},
		{"max msg size over ceiling", func(c *StreamConfig) { c.MaxMsgSize = 8*mib + 1 }, errs.ErrBadRequest},
		{"dedup window zero is legal", func(c *StreamConfig) { c.DedupWindow = 0 }, nil},
		{"dedup window at max", func(c *StreamConfig) { c.DedupWindow = 24 * time.Hour }, nil},
		{"dedup window over max", func(c *StreamConfig) { c.DedupWindow = 24*time.Hour + time.Millisecond }, errs.ErrBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			tt.mutate(&cfg)
			err := ValidateStreamConfig(cfg, DefaultLimits())
			switch {
			case tt.wantErr == nil && err != nil:
				t.Fatalf("ValidateStreamConfig() = %v, want nil", err)
			case tt.wantErr != nil && !errors.Is(err, tt.wantErr):
				t.Fatalf("ValidateStreamConfig() = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultConfigAndLimits(t *testing.T) {
	cfg := DefaultConfig("orders")
	if cfg.Subjects == nil || len(cfg.Subjects) != 1 || cfg.Subjects[0] != ">" {
		t.Errorf("DefaultConfig subjects = %v, want [\">\"]", cfg.Subjects)
	}
	if cfg.Retention != RetentionLimits || cfg.Discard != DiscardOld {
		t.Errorf("DefaultConfig retention/discard = %q/%q, want limits/old", cfg.Retention, cfg.Discard)
	}
	if cfg.MaxMsgSize != 1<<20 {
		t.Errorf("DefaultConfig MaxMsgSize = %d, want 1 MiB", cfg.MaxMsgSize)
	}
	if cfg.MaxAge != 7*24*time.Hour {
		t.Errorf("DefaultConfig MaxAge = %v, want 7d", cfg.MaxAge)
	}
	if cfg.DedupWindow != 120*time.Second {
		t.Errorf("DefaultConfig DedupWindow = %v, want 2m", cfg.DedupWindow)
	}
	l := DefaultLimits()
	if l.MaxMsgSizeCeiling != 8<<20 || l.MaxHeaderBytes != 4<<10 || l.MaxHeaders != 32 ||
		l.MaxSubjects != 32 || l.MaxDedupWindow != 24*time.Hour {
		t.Errorf("DefaultLimits = %+v, want the §4.2 ceilings", l)
	}
	if err := ValidateStreamConfig(cfg, l); err != nil {
		t.Fatalf("DefaultConfig does not pass its own validator: %v", err)
	}
}

func TestValidateStreamConfigBoundaryCounts(t *testing.T) {
	l := DefaultLimits()
	one := []string{"orders"}
	exact := make([]string, 0, l.MaxSubjects)
	for i := range l.MaxSubjects {
		exact = append(exact, "s"+string(rune('a'+i%26))+string(rune('a'+i/26)))
	}
	over := append(append([]string{}, exact...), "onemore")

	cfg := validConfig()
	cfg.Subjects = exact
	if err := ValidateStreamConfig(cfg, l); err != nil {
		t.Fatalf("%d subjects refused: %v", l.MaxSubjects, err)
	}
	cfg.Subjects = one
	if err := ValidateStreamConfig(cfg, l); err != nil {
		t.Fatalf("one subject refused: %v", err)
	}
	cfg.Subjects = over
	if err := ValidateStreamConfig(cfg, l); !errors.Is(err, errs.ErrBadRequest) {
		t.Fatalf("%d subjects accepted: %v", len(over), err)
	}
}
