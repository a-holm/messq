// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/a-holm/messq/internal/store"
)

// TestPublishSurvivesSIGKILL is the smoke-level crash test (issue #7 acceptance, generalised
// by #8's three-valued ledger): publish n messages over the Unix socket, SIGKILL the daemon
// mid-load, restart, and assert that every 2xx-acked sequence is present, PRAGMA
// integrity_check is clean, and CheckPublishInvariants reports nothing. This is the evidence
// the issue merges with: a 2xx publish under durability=full survives a kill.
func TestPublishSurvivesSIGKILL(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	sock := filepath.Join(dir, "messq.sock")

	proc := startServe(t, dataDir, sock)
	client := unixHTTPClient(sock)

	// Create the stream over the socket so the publish path below is the real one.
	if status, body := doPost(t, client, "/v1/streams",
		`{"name":"orders","subjects":["orders.>"]}`, nil); status != http.StatusCreated {
		t.Fatalf("create stream: status %d (%s)", status, body)
	}

	const (
		publishers = 8
		per        = 50
		ackTarget  = 16 // kill once this many publishes are durably acked, mid-load
	)
	type receipt struct {
		seq  int64
		body string
	}
	var (
		mu         sync.Mutex
		acked      []receipt
		ackReached = make(chan struct{})
		ackOnce    sync.Once
		wg         sync.WaitGroup
	)
	publish := func(msgID string) (int64, error) {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
			"http://messq/v1/streams/orders/messages?subject=orders.eu.created",
			strings.NewReader(msgID))
		if err != nil {
			return 0, err
		}
		req.Header.Set("Messq-Msg-Id", msgID)
		resp, err := client.Do(req)
		if err != nil {
			return 0, err
		}
		b, readErr := io.ReadAll(resp.Body)
		closeErr := resp.Body.Close()
		if readErr != nil {
			return 0, readErr
		}
		if closeErr != nil {
			return 0, closeErr
		}
		if resp.StatusCode/100 != 2 {
			return 0, fmt.Errorf("status %d: %s", resp.StatusCode, b)
		}
		var ack struct {
			Seq int64 `json:"seq"`
		}
		if err := json.Unmarshal(b, &ack); err != nil {
			return 0, err
		}
		return ack.Seq, nil
	}

	for g := 0; g < publishers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				msgID := fmt.Sprintf("m-%d-%d", g, i)
				seq, err := publish(msgID)
				if err != nil {
					return // the daemon was killed; stop publishing
				}
				mu.Lock()
				acked = append(acked, receipt{seq: seq, body: msgID})
				reached := len(acked) >= ackTarget
				mu.Unlock()
				if reached {
					ackOnce.Do(func() { close(ackReached) })
				}
			}
		}(g)
	}

	// SIGKILL mid-load: once ackTarget publishes are durably acked — the rest still in
	// flight — kill the daemon outright.
	<-ackReached
	if err := proc.Process.Kill(); err != nil {
		t.Fatalf("kill serve: %v", err)
	}
	if err := proc.Wait(); err == nil {
		t.Fatalf("serve exited cleanly after SIGKILL, want a signal death")
	}
	wg.Wait()

	mu.Lock()
	nAcked := len(acked)
	mu.Unlock()
	if nAcked == 0 {
		t.Fatalf("no publish was acked before the kill; the crash assertion is vacuous")
	}
	t.Logf("acked %d/%d publishes before SIGKILL", nAcked, publishers*per)

	// Restart = reopen the store: recovery reclaims the dead writer's lease, then the
	// assertions below read the on-disk truth.
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

	var integrity string
	if scanErr := st.RO().QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&integrity); scanErr != nil {
		t.Fatalf("integrity_check: %v", scanErr)
	}
	if integrity != "ok" {
		t.Fatalf("PRAGMA integrity_check = %q, want ok", integrity)
	}

	violations, err := st.CheckPublishInvariants(ctx)
	if err != nil {
		t.Fatalf("CheckPublishInvariants: %v", err)
	}
	if len(violations) != 0 {
		t.Fatalf("CheckPublishInvariants found %d violations: %+v", len(violations), violations)
	}

	// Every 2xx-acked seq is present with its body byte-identical — the durability promise.
	mu.Lock()
	defer mu.Unlock()
	for _, r := range acked {
		msg, peekErr := st.PeekSeq(ctx, "orders", r.seq)
		if peekErr != nil {
			t.Fatalf("acked seq %d (%s) is gone after SIGKILL: %v", r.seq, r.body, peekErr)
		}
		if string(msg.Body) != r.body {
			t.Errorf("acked seq %d body = %q, want %q", r.seq, msg.Body, r.body)
		}
	}
}
