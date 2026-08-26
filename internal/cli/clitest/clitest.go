// SPDX-License-Identifier: Apache-2.0

// Package clitest is the in-process golden harness every CLI test builds on
// (issue §7): a fake daemon speaking real HTTP over a unix socket in a temp dir,
// a runner with forced-TTY/frozen-clock/env-map seams so no test needs a
// subprocess or t.Setenv, and an -update golden flow.
//
// This package is imported by tests only; production internal/cli never links it,
// and its net/http use is the FAKE daemon's, never a second client (D14).
package clitest

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/cli"
)

// Update mirrors the repo-wide -update convention: goldens rewrite instead of fail.
var Update = flag.Bool("update", false, "rewrite golden files instead of failing on drift")

// Response is one scripted daemon reply.
type Response struct {
	Status int
	Body   string
}

// Request is one observed request, kept for assertions on what the CLI sent
// (headers, paths, methods).
type Request struct {
	Method string
	Path   string
	Header http.Header
	Body   string
}

// FakeDaemon serves scripted responses over a unix socket inside t.TempDir(). It
// speaks real HTTP, so pkg/client's transport talks to it exactly as it would talk
// to the real daemon.
type FakeDaemon struct {
	t        *testing.T
	sock     string
	listener net.Listener
	server   *http.Server

	mu       sync.Mutex
	routes   map[string]Response
	requests []Request
}

// NewFakeDaemon binds its socket and starts serving; cleanup is registered with t.
func NewFakeDaemon(t *testing.T) *FakeDaemon {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "messq.sock")
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "unix", sock)
	if err != nil {
		t.Fatalf("clitest: bind fake daemon socket: %v", err)
	}
	d := &FakeDaemon{
		t:        t,
		sock:     sock,
		listener: ln,
		routes:   make(map[string]Response),
	}
	d.server = &http.Server{
		Handler:           http.HandlerFunc(d.serveHTTP),
		ReadHeaderTimeout: 5 * time.Second,
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- d.server.Serve(ln) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if shErr := d.server.Shutdown(ctx); shErr != nil {
			t.Logf("clitest: fake daemon shutdown: %v", shErr)
		}
		if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Logf("clitest: fake daemon serve: %v", err)
		}
	})
	return d
}

// Route scripts one method+path. Unrouted requests answer 404 with an empty body,
// which is exactly how the classifier wants an unknown path to look.
func (d *FakeDaemon) Route(method, path string, resp Response) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.routes[method+" "+path] = resp
}

// Addr returns the address in --addr form.
func (d *FakeDaemon) Addr() string { return "unix://" + d.sock }

// SocketPath returns the bare socket path for dialling directly.
func (d *FakeDaemon) SocketPath() string { return d.sock }

// Requests returns everything the daemon saw, oldest first.
func (d *FakeDaemon) Requests() []Request {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]Request, len(d.requests))
	copy(out, d.requests)
	return out
}

func (d *FakeDaemon) serveHTTP(w http.ResponseWriter, r *http.Request) {
	var body bytes.Buffer
	if r.Body != nil {
		if _, cErr := io.Copy(&body, r.Body); cErr != nil {
			http.Error(w, "read failed", http.StatusInternalServerError)
			return
		}
	}
	d.mu.Lock()
	resp, ok := d.routes[r.Method+" "+r.URL.Path]
	d.requests = append(d.requests, Request{
		Method: r.Method,
		Path:   r.URL.Path,
		Header: r.Header.Clone(),
		Body:   body.String(),
	})
	d.mu.Unlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(resp.Status)
	if _, wErr := io.WriteString(w, resp.Body); wErr != nil {
		// The client vanished mid-write; the request log above is still honest.
		d.t.Logf("fake daemon: write body: %v", wErr)
	}
}

// Runner describes one invocation's seams: environment map, forced TTY-ness, frozen
// clock. Zero fields mean neutral defaults.
type Runner struct {
	Env map[string]string // MESSQ_* layer; empty means nothing set
	TTY bool              // forced IsTerminal answer for stdout/stderr
	Now time.Time         // frozen clock; zero means a fixed arbitrary instant
}

// Result captures one invocation's observable behaviour.
type Result struct {
	Exit   int
	Stdout string
	Stderr string
}

// Run executes messq once, in process, against the Runner's seams.
func Run(t *testing.T, r Runner, args ...string) Result {
	t.Helper()
	envMap := r.Env
	now := r.Now
	if now.IsZero() {
		now = time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	}
	var stdout, stderr bytes.Buffer
	env := &cli.Env{
		Stdin:  strings.NewReader(""),
		Stdout: &stdout,
		Stderr: &stderr,
		Getenv: func(k string) string { return envMap[k] },
		Now:    func() time.Time { return now },
		IsTerminal: func(io.Writer) bool {
			return r.TTY
		},
		Width: func() int { return 0 },
	}
	res := Result{Exit: cli.RunEnv(context.Background(), env, args)}
	res.Stdout = stdout.String()
	res.Stderr = stderr.String()
	return res
}

// Golden compares got against the named file under testdata; a missing file or
// -update rewrites it, drift fails.
func Golden(t *testing.T, name, got string) string {
	t.Helper()
	path := filepath.Join("testdata", name)
	want, readErr := os.ReadFile(path)
	if os.IsNotExist(readErr) || *Update {
		if mkErr := os.MkdirAll(filepath.Dir(path), 0o750); mkErr != nil {
			t.Fatalf("clitest: golden dir: %v", mkErr)
		}
		if wErr := os.WriteFile(path, []byte(got), 0o600); wErr != nil {
			t.Fatalf("clitest: write golden %s: %v", name, wErr)
		}
		return got
	}
	if readErr != nil {
		t.Fatalf("clitest: read golden %s: %v", name, readErr)
	}
	if string(want) != got {
		t.Fatalf("golden %s drifted\n--- want (%s)\n%q\n+++ got\n%q",
			name, path, string(want), got)
	}
	return got
}
