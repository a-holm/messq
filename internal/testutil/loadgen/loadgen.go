// SPDX-License-Identifier: Apache-2.0

// Package loadgen is the crash harness's concurrent load generator: it drives the real
// daemon over its Unix socket, recording every intent durably in the external ledger
// BEFORE the request is sent, and classifying every outcome into the three-valued ledger.
// It is deterministic from a seed (keys, payload sizes and scheduling are all derived from
// the driver's RNG), so a cycle can be replayed by plan even though wall-clock interleaving
// is not reproducible.
package loadgen

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync/atomic"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/testutil/ledger"
)

// Payload returns the deterministic body for a publish: a keyed fill of exactly size bytes.
// Bodies are never stored in the ledger, so the reconciler byte-compares a recovered body
// against Payload(key, size) instead of carrying the workload around. The fill is a
// repeated key-hash XOR position byte, so a flipped body byte differs at exactly its offset.
func Payload(key string, size int) []byte {
	h := sha256.Sum256([]byte(key))
	b := make([]byte, size)
	for i := range b {
		b[i] = h[i%len(h)] ^ byte(i)
	}
	return b
}

// Observations are the running verdict counts the killer strategy reads and the vacuity
// guards later assert on. They are the load generator's output, shared across publishers.
type Observations struct {
	OK      atomic.Int64
	Unknown atomic.Int64
	Failed  atomic.Int64
}

// Total returns the number of attempts observed so far.
func (o *Observations) Total() int64 { return o.OK.Load() + o.Unknown.Load() + o.Failed.Load() }

// UnixClient returns an http.Client that dials the given Unix socket for every request.
func UnixClient(sock string) *http.Client {
	return &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		},
	}}
}

// Publisher is one concurrent publisher. Each instance is used by exactly one goroutine;
// the shared state is the ledger (concurrent-safe) and the observations (atomic).
type Publisher struct {
	Stream   string
	Subject  string
	Sizes    []int
	Cycle    int
	Ledger   *ledger.Ledger
	Client   *http.Client
	NewKey   func() string
	Clk      clock.Clock
	Obs      *Observations
	nextSize int
}

// ack is the daemon's publish receipt (store.Ack's wire shape, frozen by #7). Only the
// fields the harness needs are decoded; unknown fields are ignored so the harness tolerates
// a superset without re-pinning.
type ack struct {
	Seq       int64  `json:"seq"`
	ID        string `json:"id"`
	Duplicate bool   `json:"duplicate"`
}

// Publish performs exactly one attempt: mint a key, record the intent durably, POST the
// deterministic body, and record the classified outcome. It returns a non-nil error only
// for a fatal condition — an unclassified response code (which must fail the cycle, not
// silently become a verdict) or a ledger failure. A transport error after the kill is the
// expected outcome and resolves to UNKNOWN.
func (p *Publisher) Publish(ctx context.Context) error {
	key := p.NewKey()
	size := p.Sizes[p.nextSize%len(p.Sizes)]
	p.nextSize++

	intent := ledger.Record{
		Key:     key,
		Stream:  p.Stream,
		Subject: p.Subject,
		Size:    size,
		Cycle:   p.Cycle,
		SentAt:  p.Clk.Now().UnixMilli(),
		Verdict: ledger.Unknown,
	}
	if err := p.Ledger.Attempt(intent); err != nil {
		return fmt.Errorf("ledger.Attempt: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://messq/v1/streams/"+p.Stream+"/messages?subject="+p.Subject,
		bytes.NewReader(Payload(key, size)))
	if err != nil {
		return fmt.Errorf("build publish request: %w", err)
	}
	req.Header.Set("Messq-Msg-Id", key)

	resp, err := p.Client.Do(req)
	if err != nil {
		// Connection reset, EOF, write error, client timeout, or the daemon is gone: the
		// kill window. UNKNOWN.
		p.Obs.Unknown.Add(1)
		return p.Ledger.Resolve(key, ledger.Unknown, ledger.Outcome{})
	}
	body, readErr := io.ReadAll(resp.Body)
	if cerr := resp.Body.Close(); cerr != nil && readErr == nil {
		// A close failure after an otherwise-intact body is still an incomplete response.
		readErr = cerr
	}
	if readErr != nil {
		p.Obs.Unknown.Add(1)
		return p.Ledger.Resolve(key, ledger.Unknown, ledger.Outcome{Status: resp.StatusCode})
	}

	if resp.StatusCode/100 == 2 {
		var a ack
		if decodeErr := json.Unmarshal(body, &a); decodeErr != nil {
			return fmt.Errorf("publish 2xx body is not an ack (status %d): %w", resp.StatusCode, decodeErr)
		}
		p.Obs.OK.Add(1)
		return p.Ledger.Resolve(key, ledger.OK, ledger.Outcome{
			Seq: a.Seq, ID: a.ID, Duplicate: a.Duplicate, Status: resp.StatusCode,
		})
	}

	verdict, code, err := ledger.ClassifyResponse(resp.StatusCode, body)
	if err != nil {
		return fmt.Errorf("cycle failed: %w", err)
	}
	switch verdict {
	case ledger.OK:
		// Unreachable: a 2xx is handled above. Listed so the exhaustive linter keeps the
		// fold honest when a verdict is added.
		return fmt.Errorf("classifier returned OK for non-2xx (status %d)", resp.StatusCode)
	case ledger.Failed:
		p.Obs.Failed.Add(1)
	case ledger.Unknown:
		p.Obs.Unknown.Add(1)
	default:
		return fmt.Errorf("classifier returned unexpected verdict %v", verdict)
	}
	return p.Ledger.Resolve(key, verdict, ledger.Outcome{Status: resp.StatusCode, Code: code})
}

// Run loops Publish until stop is closed or ctx is done, returning the first fatal error
// (unclassified code or ledger failure) it hit, or nil on a clean stop. A transport error
// is not fatal: it is the kill the harness is looking for.
func (p *Publisher) Run(ctx context.Context, stop <-chan struct{}) error {
	for {
		select {
		case <-stop:
			return nil
		case <-ctx.Done():
			return nil
		default:
		}
		if err := p.Publish(ctx); err != nil {
			return err
		}
	}
}
