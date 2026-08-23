// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"encoding/json"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestCurlTranscript runs the §7 curl transcript (issue #7 §7, PLAN §11.5) verbatim against a
// live in-test server — the executable-documentation proof that `curl` is the broker's client.
// The commands are the issue's own: create a stream, publish a raw body with Messq-* headers
// (headers dumped), retry inside the dedup window, and hit the too_large refusal. Inline bodies
// stand in for the issue's @order.json / @big.bin files; the routes, headers and statuses are
// unchanged. curl is the one external binary the transcript needs, so it skips where curl is
// absent rather than failing a hermetic build.
func TestCurlTranscript(t *testing.T) {
	if _, err := exec.LookPath("curl"); err != nil {
		t.Skip("curl not installed; the §7 transcript is executable documentation for environments that have it (CI)")
	}

	dir := t.TempDir()
	sock := filepath.Join(dir, "messq.sock")
	cmd := startServe(t, filepath.Join(dir, "data"), sock)
	defer stopServe(t, cmd)

	// 1. Create the stream — the issue's first command.
	out := runCurl(t, sock, "", "-s", "-XPOST", "http://x/v1/streams",
		"-d", `{"name":"orders","subjects":["orders.>"],"max_msg_size":262144,"dedup_window_ms":300000}`)
	if !json.Valid([]byte(out)) {
		t.Fatalf("create stream: not JSON: %s", out)
	}
	if !strings.Contains(out, `"name":"orders"`) {
		t.Fatalf("create stream response does not name the stream: %s", out)
	}

	// 2. Publish a raw body with the Messq-* headers, dumping response headers.
	out = runCurl(t, sock, "", "-s", "-D-",
		"-H", "Messq-Msg-Id: order-4711-confirm",
		"-H", "Messq-Header-Tenant: acme",
		"--data-binary", `{"id":40}`,
		"http://x/v1/streams/orders/messages?subject=orders.eu.created")
	if !strings.Contains(out, "201 Created") {
		t.Fatalf("publish status missing 201: %s", out)
	}
	if !strings.Contains(out, "Messq-Seq: 1") {
		t.Fatalf("publish missing Messq-Seq header: %s", out)
	}

	// 3. The same request again, inside dedup_window_ms — the publisher's retry is safe.
	out = runCurl(t, sock, "", "-s",
		"-H", "Messq-Msg-Id: order-4711-confirm",
		"--data-binary", `{"id":40}`,
		"http://x/v1/streams/orders/messages?subject=orders.eu.created")
	if !strings.Contains(out, `"duplicate":true`) {
		t.Fatalf("retry did not dedup: %s", out)
	}

	// 4. A body over the stream's max_msg_size is refused without buffering the whole body.
	big := strings.Repeat("x", 300000) // > 262144 (256 KiB) max_msg_size
	out = runCurl(t, sock, big, "-s", "--data-binary", "@-",
		"http://x/v1/streams/orders/messages?subject=orders.eu.created")
	if !strings.Contains(out, `"code":"too_large"`) {
		t.Fatalf("oversized publish did not return too_large: %s", out)
	}
}

// runCurl invokes curl against the Unix socket, feeding stdin as the request body when non-empty
// (the @- file argument reads it). It returns the combined stdout+stderr, which for -D- carries
// the dumped response headers alongside the body.
func runCurl(t *testing.T, sock, stdin string, args ...string) string {
	t.Helper()
	argv := append([]string{"--unix-socket", sock}, args...)
	c := exec.CommandContext(context.Background(), "curl", argv...)
	if stdin != "" {
		c.Stdin = strings.NewReader(stdin)
	}
	var out strings.Builder
	c.Stdout = &out
	c.Stderr = &out
	if err := c.Run(); err != nil {
		t.Fatalf("curl %v: %v\n%s", argv, err, out.String())
	}
	return out.String()
}
