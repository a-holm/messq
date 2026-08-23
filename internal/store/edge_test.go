// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/queue"
)

// TestCreateStreamConflictNamesEachDifferingField walks every configDiff arm: a
// re-created stream with exactly one knob moved must name that knob and nothing else.
func TestCreateStreamConflictNamesEachDifferingField(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	if _, _, err := st.CreateStream(ctx, queue.DefaultConfig("orders"), "test"); err != nil {
		t.Fatalf("create: %v", err)
	}

	retention := queue.RetentionWorkQueue
	discard := queue.DiscardNew
	cases := map[string]func(*queue.StreamConfig){
		"subjects":     func(c *queue.StreamConfig) { c.Subjects = []string{"x.y"} },
		"retention":    func(c *queue.StreamConfig) { c.Retention = retention },
		"max_msgs":     func(c *queue.StreamConfig) { c.MaxMsgs = 9 },
		"max_bytes":    func(c *queue.StreamConfig) { c.MaxBytes = 999 },
		"max_age":      func(c *queue.StreamConfig) { c.MaxAge = 1_000_000 },
		"max_msg_size": func(c *queue.StreamConfig) { c.MaxMsgSize = 4096 },
		"discard":      func(c *queue.StreamConfig) { c.Discard = discard },
		"dedup_window": func(c *queue.StreamConfig) { c.DedupWindow = 5_000 },
	}
	for wantField, mutate := range cases {
		t.Run(wantField, func(t *testing.T) {
			cfg := queue.DefaultConfig("orders")
			mutate(&cfg)
			_, _, err := st.CreateStream(ctx, cfg, "test")
			var see *StreamExistsError
			if !errors.As(err, &see) {
				t.Fatalf("mutated %s = %v, want StreamExistsError", wantField, err)
			}
			if !slices.Equal(see.Diff, []string{wantField}) {
				t.Errorf("diff = %v, want [%s]", see.Diff, wantField)
			}
		})
	}
}

func TestGetStreamReportsCorruptStoredSubjects(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	mustCreate(t, st, queue.DefaultConfig("orders"))
	if _, xErr := st.rw.ExecContext(ctx,
		`UPDATE streams SET subjects='not json' WHERE name='orders'`); xErr != nil {
		t.Fatalf("corrupt subjects: %v", xErr)
	}
	info, err := st.GetStream(ctx, "orders")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(info.Subjects) != 1 || !strings.HasPrefix(info.Subjects[0], "<corrupt subjects:") {
		t.Errorf("subjects = %v, want corruption marker", info.Subjects)
	}
}

func TestRecreateRefusesCorruptHighWaterMark(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	mustCreate(t, st, queue.DefaultConfig("orders"))
	if _, err := st.DeleteStream(ctx, "orders", "orders", "test"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// A corrupted marker must refuse creation rather than resume from garbage.
	if _, xErr := st.rw.ExecContext(ctx,
		`UPDATE meta SET v='not-a-number' WHERE k='seq_hwm.orders'`); xErr != nil {
		t.Fatalf("corrupt marker: %v", xErr)
	}
	_, _, err := st.CreateStream(ctx, queue.DefaultConfig("orders"), "test")
	if err == nil || !strings.Contains(err.Error(), "not an integer") {
		t.Fatalf("recreate over corrupt marker = %v, want refusal", err)
	}
}

func TestReadOnlyOpenStillListsStreams(t *testing.T) {
	ctx := context.Background()
	dir := testDataDir(t)
	st, _, oErr := Open(ctx, testOptions(dir, fakeClock(), &logCapture{}))
	if oErr != nil {
		t.Fatalf("open: %v", oErr)
	}
	mustCreate(t, st, queue.DefaultConfig("kept"))
	if cerr := st.Close(ctx); cerr != nil {
		t.Fatalf("close: %v", cerr)
	}

	roOpts := testOptions(dir, fakeClock(), &logCapture{})
	roOpts.ReadOnly = true
	ro, _, rErr := Open(ctx, roOpts)
	if rErr != nil {
		t.Fatalf("reopen ro: %v", rErr)
	}
	defer func() {
		if cerr := ro.Close(ctx); cerr != nil {
			t.Logf("close ro: %v", cerr)
		}
	}()
	list, lErr := ro.ListStreams(ctx)
	if lErr != nil || !slices.ContainsFunc(list, func(si StreamInfo) bool { return si.Name == "kept" }) {
		t.Errorf("ro list = %+v err=%v, want kept visible", list, lErr)
	}
}
