// SPDX-License-Identifier: Apache-2.0

//go:build messq_fault

package cli

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/store"
)

// TestCommitVsReplyFault proves the D4 ordering (issue #7 §acceptance) with the
// commit-vs-reply fault point armed. MESSQ_FAULT=commit_before_reply:2 crashes the daemon
// after the 2nd publish's commit fsync but before its reply, so:
//
//   - the message the client saw a 2xx for (publish 1) is provably on disk, and
//   - the message whose reply was stolen (publish 2) is also present — the fault fires
//     strictly AFTER commit, never before it. A message that never got a reply is legally
//     either present (commit won, reply lost) or absent (the process died first): this test
//     asserts the "present" leg, the SIGKILL smoke test the "absent" leg.
func TestCommitVsReplyFault(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	sock := filepath.Join(dir, "messq.sock")

	proc := startServe(t, dataDir, sock, "MESSQ_FAULT=commit_before_reply:2")
	client := unixHTTPClient(sock)

	if status, body := doPost(t, client, "/v1/streams", `{"name":"orders","subjects":["orders.>"]}`, nil); status != http.StatusCreated {
		t.Fatalf("create stream: %d %s", status, body)
	}

	// Publish 1: the client gets a 2xx — a durability promise.
	status, body, err := publishFault(client, "m1", "first")
	if err != nil {
		t.Fatalf("publish 1: %v", err)
	}
	if status/100 != 2 {
		t.Fatalf("publish 1 status = %d, want 2xx (%s)", status, body)
	}

	// Publish 2: the fault crashes the daemon after its commit, before the reply, so the
	// client gets no 2xx — it gets a connection error instead.
	if _, _, pubErr := publishFault(client, "m2", "second"); pubErr == nil {
		t.Fatalf("publish 2 returned cleanly; the fault should have killed the daemon before the reply")
	}

	if waitErr := proc.Wait(); waitErr == nil {
		t.Fatalf("serve exited cleanly; the commit_before_reply fault should have crashed it")
	}

	// Reopen the store (recovery) and read the on-disk truth.
	ctx := context.Background()
	st, _, err := store.Open(ctx, store.Options{
		DataDir:    dataDir,
		Durability: store.DurabilityFull,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer func() {
		if closeErr := st.Close(ctx); closeErr != nil {
			t.Fatalf("close store: %v", closeErr)
		}
	}()

	// The acked message is committed: 2xx ⇒ on disk (the D4 promise).
	assertBody(t, st, ctx, 1, "first")
	// The never-replied message is present too: the fault fired after its commit, not before.
	assertBody(t, st, ctx, 2, "second")

	violations, err := st.CheckPublishInvariants(ctx)
	if err != nil {
		t.Fatalf("CheckPublishInvariants: %v", err)
	}
	if len(violations) != 0 {
		t.Fatalf("CheckPublishInvariants found %d violations: %+v", len(violations), violations)
	}
}

// publishFault publishes one message and returns the status and body, or an error — which is
// the expected outcome for the publish whose reply the fault steals (the connection dies).
func publishFault(client *http.Client, msgID, body string) (int, string, error) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		"http://messq/v1/streams/orders/messages?subject=orders.eu.created", strings.NewReader(body))
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Messq-Msg-Id", msgID)
	resp, err := client.Do(req)
	if err != nil {
		return 0, "", err
	}
	b, readErr := io.ReadAll(resp.Body)
	closeErr := resp.Body.Close()
	if readErr != nil {
		return 0, "", readErr
	}
	if closeErr != nil {
		return 0, "", closeErr
	}
	return resp.StatusCode, string(b), nil
}

func assertBody(t *testing.T, st *store.Store, ctx context.Context, seq int64, want string) {
	t.Helper()
	msg, err := st.PeekSeq(ctx, "orders", seq)
	if err != nil {
		t.Fatalf("seq %d is gone: %v", seq, err)
	}
	if string(msg.Body) != want {
		t.Errorf("seq %d body = %q, want %q", seq, msg.Body, want)
	}
}
