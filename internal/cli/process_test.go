// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
)

// helperServeEnv marks a re-executed test binary as the serve child: the child runs the real
// `messq serve` command and exits with its code, so every process-level test (curl transcript,
// SIGKILL crash, fault injection) drives a genuine daemon rather than a goroutine in the test
// process. A real daemon is never required — the child is this same test binary.
const helperServeEnv = "MESSQ_TEST_HELPER_SERVE"

// TestHelperServeProcess is the re-exec entry point. It is a no-op for the parent test run
// (MESSQ_TEST_HELPER_SERVE is unset) and a real `messq serve` for the child.
func TestHelperServeProcess(t *testing.T) {
	if os.Getenv(helperServeEnv) != "1" {
		t.Skip("helper process only")
	}
	// The child IS the command entry point: runServe's exit code becomes the process exit.
	os.Exit(runServe(nil, os.Getenv, os.Stdout, os.Stderr)) //nolint:forbidigo // re-exec child's process boundary, not a Run-style mapping
}

// startServe re-executes this test binary as `messq serve` on dataDir and sock, waits until
// /healthz answers, and returns the running process. extraEnv entries (KEY=VALUE) are appended
// after the mandatory MESSQ_DATA_DIR/MESSQ_LISTEN. A leftover child is reaped at cleanup.
func startServe(t *testing.T, dataDir, sock string, extraEnv ...string) *exec.Cmd {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	cmd := exec.CommandContext(context.Background(), exe, "-test.run=^TestHelperServeProcess$")
	cmd.Env = append(os.Environ(),
		helperServeEnv+"=1",
		"MESSQ_DATA_DIR="+dataDir,
		"MESSQ_LISTEN=unix://"+sock,
		"MESSQ_DURABILITY=full",
	)
	cmd.Env = append(cmd.Env, extraEnv...)
	var stderr bytes.Buffer
	cmd.Stdout = io.Discard
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start serve: %v", err)
	}
	t.Cleanup(func() {
		if cmd.Process == nil || cmd.ProcessState != nil {
			return
		}
		if err := cmd.Process.Kill(); err != nil {
			return
		}
		if err := cmd.Wait(); err != nil {
			// A killed child exits non-zero by design; nothing to report.
			return
		}
	})
	waitForServe(t, sock, &stderr)
	return cmd
}

// waitForServe polls /healthz over the socket until the child answers or the deadline passes.
// The poll uses the Clock seam's cancellable sleep, never time.Sleep, and dumps the child's
// stderr when the daemon fails to come up so a startup error is not a silent timeout.
func waitForServe(t *testing.T, sock string, stderr *bytes.Buffer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clk := clock.System{}
	client := unixHTTPClient(sock)
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://messq/healthz", nil)
		if err != nil {
			t.Fatalf("build healthz request: %v", err)
		}
		resp, err := client.Do(req)
		if err == nil {
			closeErr := resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				if closeErr != nil {
					t.Fatalf("close healthz response: %v", closeErr)
				}
				return
			}
		}
		if sleepErr := clk.Sleep(ctx, 10*time.Millisecond); sleepErr != nil {
			t.Fatalf("serve did not become ready: %v\n--- serve stderr ---\n%s", sleepErr, stderr.String())
		}
	}
}

// unixHTTPClient returns an http.Client that dials the given Unix socket for every request.
func unixHTTPClient(sock string) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", sock)
			},
		},
	}
}

// stopServe asks a startServe child to shut down gracefully (SIGTERM) and reaps it. A clean
// serve exits 0; any other exit code is reported.
func stopServe(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatalf("signal serve: %v", err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("serve did not shut down cleanly: %v", err)
	}
}

// doPost issues one POST over the unix socket and returns the status and body.
func doPost(t *testing.T, client *http.Client, path, body string, headers map[string]string) (int, string) {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://messq"+path, rd)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			t.Logf("close body: %v", cerr)
		}
	}()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, string(b)
}
