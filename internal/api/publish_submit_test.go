// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
)

// G4/G11: publish is disconnect-immune (the command submits on a background-derived
// context bounded by --writer-submit-timeout), size is refused before a byte moves,
// and Expect: 100-continue is never answered for an oversized body.

// countingBody counts every byte pulled out of the request body.
type countingBody struct {
	r io.Reader
	n *int
}

func (c countingBody) Read(p []byte) (int, error) { return c.r.Read(p) }

func TestOversizedPublishReadsZeroBodyBytes(t *testing.T) {
	t.Parallel()

	const maxMsgSize = 1024
	body := strings.NewReader(strings.Repeat("x", 64))
	read := 0
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost,
		"/v1/streams/orders/messages", body)
	req.ContentLength = 4096 // declared far above the cap
	req.Body = io.NopCloser(countingBody{r: req.Body, n: &read})

	w := httptest.NewRecorder()
	_, err := readBody(w, req, maxMsgSize)
	if err == nil {
		t.Fatal("oversized publish accepted")
	}
	if !errors.Is(err, errs.ErrTooLarge) {
		t.Fatalf("err = %v, want ErrTooLarge", err)
	}
	if read != 0 {
		t.Fatalf("%d body bytes read before the size refusal, want 0", read)
	}
}

// startHTTP serves the full chain on a real loopback listener.
func startHTTP(t *testing.T, srv *Server) net.Addr {
	t.Helper()

	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if serveErr := srv.Serve(ctx, ln); serveErr != nil {
			t.Logf("serve: %v", serveErr)
		}
	}()
	sysClk := clock.System{}
	t.Cleanup(func() {
		cancel()
		timer := sysClk.NewTimer(5 * time.Second)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C():
			t.Log("serve did not stop within 5s")
		}
	})
	return ln.Addr()
}

func TestPublishCommitsAfterClientDisconnect(t *testing.T) {
	t.Parallel()

	clk := clock.NewFake(time.UnixMilli(1_700_000_000_000))
	st := openTestStore(t, clk, store.DurabilityFull)
	srv := New(Config{Store: st, Clock: clk, Logger: discardLogger()})
	addr := startHTTP(t, srv)

	if _, _, err := st.CreateStream(context.Background(),
		queue.DefaultConfig("orders"), actorAPI); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	payload := `{"order":"4711"}`
	raw := "POST /v1/streams/orders/messages?subject=orders.eu.created HTTP/1.1\r\n" +
		"Host: messq\r\nContent-Type: application/octet-stream\r\n" +
		"Messq-Msg-Id: order-4711-confirm\r\n" +
		"Content-Length: " + itoaLen(payload) + "\r\n\r\n" + payload

	conn, err := (&net.Dialer{Timeout: 2 * time.Second}).DialContext(context.Background(), "tcp", addr.String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if _, wErr := conn.Write([]byte(raw)); wErr != nil {
		t.Fatalf("write request: %v", wErr)
	}
	// Half-close: the client is gone as far as sending goes, before any response.
	if tc, ok := conn.(*net.TCPConn); ok {
		if cErr := tc.CloseWrite(); cErr != nil {
			t.Fatalf("close write: %v", cErr)
		}
	}
	// Drain until the server gives up on us; a copy error IS the expected EOF.
	if _, cErr := io.Copy(io.Discard, conn); cErr != nil {
		t.Logf("drain (expected after half-close): %v", cErr)
	}
	if cErr := conn.Close(); cErr != nil {
		t.Fatalf("close: %v", cErr)
	}

	// The commit must land anyway (G4): poll the store through peek reads.
	found := false
	for i := 0; i < 200000 && !found; i++ {
		if _, pErr := st.PeekSeq(context.Background(), "orders", 1); pErr == nil {
			found = true
		}
	}
	if !found {
		t.Fatal("message absent after publisher disconnected post-submit — lost publish")
	}

	// The retry with the same Messq-Msg-Id reports the duplicate honestly.
	res, err := st.Publish(context.Background(), store.PublishCmd{
		Stream: "orders",
		Req: queue.PublishReq{
			Subject: "orders.eu.created", Body: []byte(payload),
			MsgID: "order-4711-confirm",
		},
	})
	if err != nil {
		t.Fatalf("dedup retry: %v", err)
	}
	if !res.Duplicate {
		t.Error("retry with same Messq-Msg-Id was not reported duplicate:true")
	}
}

func TestOversizedPublishNeverSends100Continue(t *testing.T) {
	t.Parallel()

	clk := clock.NewFake(time.UnixMilli(1_700_000_000_000))
	st := openTestStore(t, clk, store.DurabilityFull)
	srv := New(Config{Store: st, Clock: clk, Logger: discardLogger()})
	addr := startHTTP(t, srv)

	cfg := queue.DefaultConfig("tiny")
	cfg.MaxMsgSize = 8
	if _, _, err := st.CreateStream(context.Background(), cfg, actorAPI); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	conn, err := (&net.Dialer{Timeout: 2 * time.Second}).DialContext(context.Background(), "tcp", addr.String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() {
		if cErr := conn.Close(); cErr != nil {
			t.Logf("close: %v", cErr)
		}
	}()

	req := "POST /v1/streams/tiny/messages?subject=tiny.x HTTP/1.1\r\n" +
		"Host: messq\r\nContent-Length: 1000000\r\nExpect: 100-continue\r\n\r\n"
	if _, wErr := conn.Write([]byte(req)); wErr != nil {
		t.Fatalf("write: %v", wErr)
	}
	if dErr := conn.SetReadDeadline(clock.System{}.Now().Add(5 * time.Second)); dErr != nil {
		t.Fatalf("read deadline: %v", dErr)
	}
	buf := make([]byte, 4096)
	n, rErr := conn.Read(buf)
	if rErr != nil && !errors.Is(rErr, io.EOF) {
		t.Fatalf("read: %v", rErr)
	}
	resp := string(buf[:n])
	if strings.Contains(resp, "100 Continue") {
		t.Fatal("server sent 100-continue for an oversized publish — the client uploads 1 MB for nothing")
	}
	if !strings.HasPrefix(resp, "HTTP/1.1 413") {
		t.Fatalf("response = %q, want 413", resp)
	}
}

// classifySubmit unit pins: submit-window timeout → busy, commit-phase timeout/cancel →
// commit_unknown (via the typed store error), anything else untouched.
func TestClassifySubmitMapping(t *testing.T) {
	t.Parallel()

	srv := mappingServer()

	busyErr := srv.classifySubmit("t", context.DeadlineExceeded)
	if got := mapCode(busyErr); got != CodeBusy {
		t.Errorf("deadline-exceeded mapped to %s, want busy", got)
	}

	unknown := srv.classifySubmit("t", store.ErrCommitUnknown)
	if got := mapCode(unknown); got != CodeCommitUnknown {
		t.Errorf("commit-unknown mapped to %s, want commit_unknown", got)
	}

	domain := srv.classifySubmit("t", errs.E(errs.ErrBadSubject, "t", "nope"))
	if got := mapCode(domain); got != CodeBadSubject {
		t.Errorf("domain error disturbed by classification: %s", got)
	}
}

func itoaLen(s string) string {
	return strconv.Itoa(len(s))
}
