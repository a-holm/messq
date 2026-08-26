// SPDX-License-Identifier: Apache-2.0

package clitest_test

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/cli/clitest"
)

func TestFakeDaemonSpeaksHTTPOverUnixSocket(t *testing.T) {
	d := clitest.NewFakeDaemon(t)
	d.Route("GET", "/v1/info", clitest.Response{
		Status: 200,
		Body:   `{"version":"v0.3.0","uptime_s":42}`,
	})

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", d.SocketPath())
			},
		},
	}
	req, reqErr := http.NewRequestWithContext(context.Background(), "GET", "http://messq.invalid/v1/info", nil)
	if reqErr != nil {
		t.Fatal(reqErr)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET through the socket: %v", err)
	}
	defer func() {
		if cErr := resp.Body.Close(); cErr != nil {
			t.Logf("close body: %v", cErr)
		}
	}()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var doc map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		t.Fatalf("body: %v", err)
	}
	if doc["version"] != "v0.3.0" {
		t.Errorf("scripted body drifted: %v", doc)
	}
}

func TestFakeDaemonScriptsEnvelopesAndRecordsRequests(t *testing.T) {
	d := clitest.NewFakeDaemon(t)
	d.Route("POST", "/streams", clitest.Response{
		Status: 409,
		Body:   `{"error":{"code":"stream_exists","message":"canary stream exists"}}`,
	})
	d.Route("GET", "/v1/info", clitest.Response{Status: 500, Body: `{"error":{"code":"internal"}}`})

	probe := func(method, path string) int {
		client := unixClient(t, d.SocketPath())
		req, err := http.NewRequestWithContext(context.Background(), method, "http://messq.invalid"+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		defer func() {
			if cErr := resp.Body.Close(); cErr != nil {
				t.Logf("close body: %v", cErr)
			}
		}()
		return resp.StatusCode
	}
	if got := probe("POST", "/streams"); got != 409 {
		t.Errorf("first route status = %d, want 409", got)
	}
	if got := probe("GET", "/v1/info"); got != 500 {
		t.Errorf("second route status = %d, want 500", got)
	}
	if got := probe("GET", "/no/such/route"); got != 404 {
		t.Errorf("unrouted request status = %d, want 404", got)
	}

	reqs := d.Requests()
	if len(reqs) != 3 || reqs[0].Method != "POST" || reqs[1].Path != "/v1/info" ||
		reqs[2].Path != "/no/such/route" {
		t.Errorf("recorded requests = %+v", reqs)
	}
}

func unixClient(t *testing.T, sock string) *http.Client {
	t.Helper()
	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", sock)
			},
		},
	}
}

func TestRunnerForcesTTYAndCapturesFaces(t *testing.T) {
	t.Run("forced tty resolves auto to the human face", func(t *testing.T) {
		res := clitest.Run(t, clitest.Runner{Env: map[string]string{}, TTY: true}, "version")
		if res.Exit != 0 {
			t.Fatalf("exit = %d, stderr %q", res.Exit, res.Stderr)
		}
		if !strings.HasPrefix(strings.TrimSpace(res.Stdout), "messq ") {
			t.Errorf("auto on a forced TTY did not pick the table face: %q", res.Stdout)
		}
	})
	t.Run("pipe picks the machine face", func(t *testing.T) {
		res := clitest.Run(t, clitest.Runner{Env: map[string]string{}}, "version")
		if res.Exit != 0 {
			t.Fatalf("exit = %d, stderr %q", res.Exit, res.Stderr)
		}
		var doc map[string]any
		if err := json.Unmarshal([]byte(res.Stdout), &doc); err != nil {
			t.Fatalf("auto on a pipe did not pick the json face: %q", res.Stdout)
		}
	})
	t.Run("environment reaches the three-layer resolution", func(t *testing.T) {
		res := clitest.Run(t, clitest.Runner{
			Env: map[string]string{"MESSQ_OUTPUT": "table"},
			TTY: false,
		}, "version")
		if !strings.HasPrefix(strings.TrimSpace(res.Stdout), "messq ") {
			t.Errorf("MESSQ_OUTPUT=table ignored: %q", res.Stdout)
		}
	})
}

func TestRunnerFreezesTheClock(t *testing.T) {
	frozen := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	got := clitest.Run(t, clitest.Runner{
		Env: map[string]string{},
		Now: frozen,
	}, "--output", "table", "version")
	if got.Exit != 0 {
		t.Fatalf("exit = %d", got.Exit)
	}
	// The clock seam is consumed from #24's renderers onward; here we pin that a
	// frozen Now is accepted and does not perturb the offline path.
	if !strings.Contains(got.Stdout, "messq ") {
		t.Errorf("frozen-clock run lost output: %q", got.Stdout)
	}
}

func TestGoldenFlow(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	first := clitest.Golden(t, "probe.golden", "stable-bytes\n")
	if first != "stable-bytes\n" {
		t.Errorf("golden returned %q", first)
	}
	path := filepath.Join(dir, "testdata", "probe.golden")
	written, err := os.ReadFile(path)
	if err != nil || string(written) != "stable-bytes\n" {
		t.Fatalf("golden file missing or wrong: %v %q", err, written)
	}
	// A drifted value fails via t.Fatalf — visible as a failed test, not
	// something to recover here. What we CAN pin: an unchanged value round-trips
	// and the file bytes are exactly what was written.
	if again := clitest.Golden(t, "probe.golden", "stable-bytes\n"); again != "stable-bytes\n" {
		t.Errorf("second golden call returned %q", again)
	}
}
