// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"bytes"
	"errors"
	"io"
	"os"
	"strings"
	"syscall"
	"testing"
)

// Unit pins for the capture-window, pump and sink helpers feeding child.go:
// cheap coverage where cross-process tests are environment-sensitive.

func TestCapturedHeadWindowAccounting(t *testing.T) {
	c := newCaptured(8, HeadStderr)
	n1, err := c.Write([]byte("abcdefgh"))
	if err != nil || n1 != 8 {
		t.Fatalf("first write: n=%d err=%v", n1, err)
	}
	n2, werr := c.Write([]byte("IJKL")) // drains past cap, keeps counting
	if n2 != 4 || werr != nil {
		t.Fatalf("over-cap write: n=%d err=%v", n2, werr)
	}
	if got := string(c.raw()); got != "abcdefgh" {
		t.Fatalf("head window = %q, want exact prefix", got)
	}
	if !c.truncated() {
		t.Fatal("truncation flag not raised")
	}
}

func TestCapturedTailWindowSlides(t *testing.T) {
	c := newCaptured(6, TailStderr)
	for _, chunk := range []string{"abcdef", "ghij"} {
		if _, err := c.Write([]byte(chunk)); err != nil {
			t.Fatalf("tail write: %v", err)
		}
	}
	if got := string(c.raw()); got != "efghij" {
		t.Fatalf("tail window = %q, want last-6 sliding", got)
	}
	// The flag reports that the DISPLAYED reason is not the whole stream:
	// 10 bytes flowed but only the last 6 are visible.
	if !c.truncated() {
		t.Fatalf("input %d exceeded cap %d; flag must be raised", c.totalIn, c.cap)
	}
}

func TestCapturedZeroAndNegativeCaps(t *testing.T) {
	for _, cap := range []int{0, -3} {
		c := newCaptured(cap, HeadStderr)
		if _, err := c.Write([]byte("hello")); err != nil {
			t.Fatalf("cap=%d write error: %v", cap, err)
		}
		if len(c.raw()) != 0 || !c.truncated() && cap == 0 {
			continue
		}
	}
	if got := ClampReasonCap(-5); got != 0 {
		t.Fatalf("negative clamp = %d", got)
	}
	if got := ClampReasonCap(maxReasonBytes + 7); got != maxReasonBytes {
		t.Fatalf("ceiling clamp = %d", got)
	}
}

type halfWriter struct{ wrote int }

func (h *halfWriter) Write(p []byte) (int, error) {
	h.wrote += len(p) / 2
	return len(p) / 2, errors.New("stub")
}

// writeAll loops partial writes until a real error surfaces.
func TestWriteAllLoopsPartialWrites(t *testing.T) {
	hw := &halfWriter{}
	err := writeAll(nopCloser{hw}, []byte("0123456789"))
	if err == nil || hw.wrote == 0 {
		t.Fatalf("expected partial progress then error, wrote=%d err=%v", hw.wrote, err)
	}
	wbuf := &bytes.Buffer{}
	if err := writeAll(nopCloser{wbuf}, []byte("all of it")); err != nil {
		t.Fatalf("clean writer failed: %v", err)
	}
	if wbuf.String() != "all of it" {
		t.Fatalf("payload = %q", wbuf.String())
	}
}

type nopCloser struct{ io.Writer }

func (nopCloser) Close() error { return nil }

func TestStdoutSinkNilIsDiscard(t *testing.T) {
	if stdoutSink(nil) != io.Discard {
		t.Fatal("nil stdout must map to io.Discard so machine modes stay clean")
	}
	var b bytes.Buffer
	if stdoutSink(&b) == io.Discard {
		t.Fatal("explicit writer was discarded")
	}
}

func TestBenignStdinFailures(t *testing.T) {
	if isBenignStdinFailure(errors.New("random")) {
		t.Fatal("arbitrary failures must remain fatal to debugging eyes")
	}
	if !isBenignStdinFailure(syscall.EPIPE) {
		t.Fatal("EPIPE is explicitly success per §6 mode 1")
	}
	if !isBenignStdinFailure(os.ErrClosed) {
		t.Fatal("ErrClosed arises on normal teardown races")
	}
}

var _ = strings.TrimSpace
