// SPDX-License-Identifier: Apache-2.0

package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeBroker answers worker requests entirely in memory: a RoundTripper instead of a
// socket server, so a Worker under test runs inside a testing/synctest bubble where
// every timer is virtual and no real I/O ever blocks the bubble.
type fakeBroker struct {
	mu      sync.Mutex
	clk     Clock
	consum  ConsumerView
	msgs    [][]Delivered // fetch responses popped in order
	holdQ   []FetchResponse
	fetches []fetchRequestWireShape
	extends [][]string
	acks    [][]string
	naks    []settleItemWire
	terms   []settleItemWire

	// extendOverride replaces the extend answer (e.g. stale results, 409).
	extendOverride func(tokens []string) (any, int)

	// failPath/failBudget answer the next N requests to path with a transport
	// error — the "daemon died mid-flight" class.
	failPath   string
	failBudget int
}

func newFakeBroker() *fakeBroker {
	return &fakeBroker{
		clk:    realClock{},
		consum: ConsumerView{Name: "w", Stream: "orders", AckWaitMS: 30000, MaxAckPending: 100},
	}
}

func (b *fakeBroker) ackedTokens() [][]string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([][]string(nil), b.acks...)
}

func (b *fakeBroker) nakkedItems() []settleItemWire {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]settleItemWire(nil), b.naks...)
}

func (b *fakeBroker) termedItems() []settleItemWire {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]settleItemWire(nil), b.terms...)
}

func (b *fakeBroker) extendCalls() [][]string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([][]string(nil), b.extends...)
}

func (b *fakeBroker) fetchCalls() []fetchRequestWireShape {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]fetchRequestWireShape(nil), b.fetches...)
}

func (b *fakeBroker) respond(req *http.Request) *http.Response {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch {
	case strings.HasSuffix(req.URL.Path, "/fetch"):
		var fr fetchRequestWireShape
		_ = json.NewDecoder(req.Body).Decode(&fr)
		req.Body.Close()
		b.fetches = append(b.fetches, fr)
		if len(b.msgs) > 0 {
			next := b.msgs[0]
			take := next
			if fr.Batch > 0 && len(take) > fr.Batch {
				take = take[:fr.Batch]       // the server clamps to the requested batch…
				b.msgs[0] = next[len(take):] // …and keeps the rest queued
			} else {
				b.msgs = b.msgs[1:]
			}
			for i := range take {
				take[i].Consumer = "w"
			}
			return brokerJSON(req, fetchResponseWire{
				Messages: take, Batch: fr.Batch, MaxBytes: fr.MaxBytes, WaitMS: fr.WaitMS,
				Pending: int64(len(take)), Backlog: int64(len(take)),
			})
		}
		if len(b.holdQ) > 0 {
			next := b.holdQ[0]
			b.holdQ = b.holdQ[1:]
			return brokerJSON(req, fetchResponseWire{
				HoldReason: string(next.Hold), RetryAfterMS: next.RetryAfter.Milliseconds(),
				Pending: next.Pending, Backlog: next.Backlog,
				Batch: fr.Batch, MaxBytes: fr.MaxBytes, WaitMS: fr.WaitMS,
			})
		}
		resp := brokerJSON(req, fetchResponseWire{
			Messages: []Delivered{}, Batch: fr.Batch, MaxBytes: fr.MaxBytes, WaitMS: fr.WaitMS,
		})
		resp.Header.Set(parkHeader, fmt.Sprint(fr.WaitMS)) // simulate the server-side park
		return resp

	case req.URL.Path == "/v1/ack":
		var body struct {
			Tokens []string `json:"tokens"`
		}
		_ = json.NewDecoder(req.Body).Decode(&body)
		req.Body.Close()
		b.acks = append(b.acks, body.Tokens)
		results := make([]SettleItem, len(body.Tokens))
		for i, tok := range body.Tokens {
			results[i] = SettleItem{Token: tok, Status: SettleOK}
		}
		return brokerJSON(req, SettleResult{Results: results, OK: len(results)})

	case req.URL.Path == "/v1/nak":
		var body settleItemWire
		_ = json.NewDecoder(req.Body).Decode(&body)
		req.Body.Close()
		b.naks = append(b.naks, body)
		return brokerJSON(req, SettleResult{Results: []SettleItem{{Token: body.Token, Status: SettleOK}}, OK: 1})

	case req.URL.Path == "/v1/term":
		var body settleItemWire
		_ = json.NewDecoder(req.Body).Decode(&body)
		req.Body.Close()
		b.terms = append(b.terms, body)
		return brokerJSON(req, SettleResult{Results: []SettleItem{{Token: body.Token, Status: SettleOK}}, OK: 1})

	case req.URL.Path == "/v1/extend":
		var body struct {
			Items []struct {
				Token string `json:"token"`
			} `json:"items"`
		}
		_ = json.NewDecoder(req.Body).Decode(&body)
		req.Body.Close()
		tokens := make([]string, len(body.Items))
		results := make([]SettleItem, len(body.Items))
		for i, it := range body.Items {
			tokens[i] = it.Token
			results[i] = SettleItem{Token: it.Token, Status: SettleOK}
		}
		b.extends = append(b.extends, tokens)
		if b.extendOverride != nil {
			anyBody, code := b.extendOverride(tokens)
			return brokerJSONCode(req, anyBody, code)
		}
		return brokerJSON(req, SettleResult{Results: results, OK: len(results)})

	case strings.Contains(req.URL.Path, "/consumers/"):
		return brokerJSON(req, b.consum)

	default:
		return brokerJSON(req, map[string]any{})
	}
}

func brokerJSON(req *http.Request, body any) *http.Response {
	return brokerJSONCode(req, body, 200)
}

// parkHeader carries the simulated long-poll duration from respond (which holds the
// lock) to the adapter, which sleeps on the virtual clock WITHOUT holding it.
const parkHeader = "X-Fake-Park-Ms"

func brokerJSONCode(req *http.Request, body any, code int) *http.Response {
	b, _ := json.Marshal(body)
	rec := &recResponse{code: code, header: http.Header{"Content-Type": []string{"application/json"}}, body: b}
	return rec.response(req)
}

type recResponse struct {
	code   int
	header http.Header
	body   []byte
}

func (r *recResponse) response(req *http.Request) *http.Response {
	return &http.Response{
		Status:     fmt.Sprintf("%d %s", r.code, http.StatusText(r.code)),
		StatusCode: r.code,
		Header:     r.header.Clone(),
		Body:       io.NopCloser(bytes.NewReader(r.body)),
		Request:    req,
	}
}

// roundTripperAdapter turns a fakeBroker into an http.RoundTripper.
type roundTripperAdapter struct{ b *fakeBroker }

func (a roundTripperAdapter) RoundTrip(req *http.Request) (*http.Response, error) {
	if a.b.takeFailure(req.URL.Path) {
		return nil, errors.New("connection refused by fake broker")
	}
	resp := a.b.respond(req)
	if ms := resp.Header.Get(parkHeader); ms != "" {
		resp.Header.Del(parkHeader)
		var n int
		fmt.Sscanf(ms, "%d", &n)
		if n > 0 {
			wait(req.Context(), a.b.clk, time.Duration(n)*time.Millisecond)
		}
	}
	return resp, nil
}

// newFakeClient builds a Client wired straight to the fake broker (no sockets).
func newFakeClient(t *testing.T, b *fakeBroker) *Client {
	t.Helper()
	c, err := New("tcp://messq.test")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	c.hc.Transport = roundTripperAdapter{b}
	return c
}

// advance sleeps d on the VIRTUAL clock (a synctest bubble advances while every
// goroutine is durably blocked on timers like this one) without a direct
// time-package call, which internal/clock's confinement gate would catch.
func advance(d time.Duration) {
	c := realClock{}.NewTimer(d)
	<-c.C()
}

// asyncAfter is advance's channel form for selects.
func asyncAfter(d time.Duration) <-chan time.Time {
	return realClock{}.NewTimer(d).C()
}

// eventCollector gathers OnEvent callbacks safely from any goroutine.
type eventCollector struct {
	mu  sync.Mutex
	got []WorkerEvent
}

func (ec *eventCollector) add(e WorkerEvent) {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	ec.got = append(ec.got, e)
}

func (ec *eventCollector) all() []WorkerEvent {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	return append([]WorkerEvent(nil), ec.got...)
}

func (ec *eventCollector) count(kind EventKind) int {
	n := 0
	for _, e := range ec.all() {
		if e.Kind == kind {
			n++
		}
	}
	return n
}

func delivered(seq int64, token string) Delivered {
	return Delivered{
		Stream: "orders", Consumer: "w", Seq: seq, Subject: "orders.west",
		Body: []byte{}, AckToken: token, AckWaitMS: 30000,
		DeadlineMS: realClock{}.Now().UnixMilli() + 30000, Attempt: 1,
	}
}
