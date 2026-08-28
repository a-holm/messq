// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"testing"

	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
)

// Helpers for the purge/seek tests. Kept in one file so the red-first test bodies
// stay readable.

func newPurgeServer(t *testing.T) (*Server, *store.Store) {
	t.Helper()
	return newUpsertServer(t)
}

// mustStart parses a start position, failing fast.
func mustStart(t *testing.T, s string) queue.StartPosition {
	t.Helper()
	start, err := queue.ParseStartPosition(s)
	if err != nil {
		t.Fatalf("parse start %q: %v", s, err)
	}
	return start
}

// eventTotal counts every journal row the read path returns — the whole-truth
// counter for "the dry run wrote zero rows". Test journals are tiny, one page.
func eventTotal(t *testing.T, st *store.Store) int64 {
	t.Helper()
	page, err := st.Events(context.Background(), store.EventFilter{})
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	return int64(len(page.Events))
}

func consumerGeneration(t *testing.T, st *store.Store, stream, name string) int64 {
	t.Helper()
	info, err := st.GetConsumer(context.Background(), stream, name)
	if err != nil {
		t.Fatalf("get consumer %s/%s: %v", stream, name, err)
	}
	return info.Generation
}
