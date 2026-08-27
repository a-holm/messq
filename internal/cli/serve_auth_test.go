// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/a-holm/messq/internal/auth"
	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/exitcode"
)

// ---------------------------------------------------------------------------
// Flag surface: --auth-file and --socket-mode resolve flag → MESSQ_* → default,
// exactly like every other serve setting (ADR-0009).
// ---------------------------------------------------------------------------

func TestParseServeFlagsAuthFileDefaults(t *testing.T) {
	t.Parallel()
	cfg, err := parseServeFlags([]string{"--data-dir", "/tmp/x"}, noEnv)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.authFile != "" {
		t.Errorf("authFile = %q, want empty default (loopback trust)", cfg.authFile)
	}
	if cfg.socketMode != 0o660 {
		t.Errorf("socketMode = %04o, want the documented 0660 default (ADR-0013)", cfg.socketMode)
	}
}

func TestParseServeFlagsAuthFileEnvFallback(t *testing.T) {
	t.Parallel()
	env := func(k string) string {
		switch k {
		case "MESSQ_AUTH_FILE":
			return "/etc/messq/tokens"
		case "MESSQ_SOCKET_MODE":
			return "0640"
		default:
			return ""
		}
	}
	cfg, err := parseServeFlags([]string{"--data-dir", "/d"}, env)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.authFile != "/etc/messq/tokens" {
		t.Errorf("authFile = %q, want MESSQ_AUTH_FILE fallback", cfg.authFile)
	}
	if cfg.socketMode != 0o640 {
		t.Errorf("socketMode = %04o, want MESSQ_SOCKET_MODE fallback", cfg.socketMode)
	}
}

func TestParseServeFlagsAuthFileFlagWins(t *testing.T) {
	t.Parallel()
	cfg, err := parseServeFlags([]string{
		"--data-dir", "/d",
		"--auth-file", "/f/tokens",
		"--socket-mode", "0600",
	}, func(k string) string {
		if k == "MESSQ_AUTH_FILE" {
			return "/env/wins-not"
		}
		return ""
	})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.authFile != "/f/tokens" {
		t.Errorf("authFile = %q, want the flag to win over MESSQ_AUTH_FILE", cfg.authFile)
	}
	if cfg.socketMode != 0o600 {
		t.Errorf("socketMode = %04o, want 0600", cfg.socketMode)
	}
}

func TestParseServeFlagsSocketModeErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, val, wantErr string
	}{
		{"not octal", "999", "must be octal"},
		{"zero", "0000", "socket-mode"},
		{"beyond permission bits", "1700", "socket-mode"},
		{"empty", "", "socket-mode"},
	}
	for _, tc := range tests {
		_, err := parseServeFlags([]string{"--data-dir", "/d", "--socket-mode", tc.val}, noEnv)
		if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
			t.Errorf("%s: parseServeFlags(--socket-mode %q) err = %v, want it to name %q", tc.name, tc.val, err, tc.wantErr)
		}
	}
}

// ---------------------------------------------------------------------------
// The admission policy table: (auth.Classify(addr), token count) -> decision.
// Pure over a stubbed resolver so hostname rows are deterministic.
//
// Killers (brief G5): mixed-hostname classified loopback -> table row refuses;
// refusal exits 1 -> runServe maps refusals to exitcode.CONFIG.
// ---------------------------------------------------------------------------

// staticLookup resolves a fixed hostname map; anything else is an error.
func staticLookup(table map[string][]net.IPAddr) func(context.Context, string) ([]net.IPAddr, error) {
	return func(_ context.Context, host string) ([]net.IPAddr, error) {
		addrs, ok := table[host]
		if !ok {
			return nil, &net.DNSError{Name: host, IsNotFound: true}
		}
		return addrs, nil
	}
}

func TestEvaluateListenerAdmissionTable(t *testing.T) {
	t.Parallel()

	mixedHost := "mixed.example.net" // resolves to loopback AND public -> PUBLIC
	loopbackHost := "localhost"
	ctx := context.Background()

	classify := func(addr string) (auth.Class, listenerAdmission, string) {
		class, adm, err := evaluateListenerAdmission(ctx, addr, 0, staticLookup(map[string][]net.IPAddr{
			mixedHost:    {{IP: net.ParseIP("127.0.0.1")}, {IP: net.ParseIP("203.0.113.7")}},
			loopbackHost: {{IP: net.ParseIP("127.0.0.1")}},
		}))
		if err != nil {
			return 0, listenerAdmission{}, err.Error()
		}
		return class, adm, ""
	}

	assertRefused := func(t *testing.T, wantMsgPart string, gotClass auth.Class, gotAdm listenerAdmission, tag string) {
		t.Helper()
		if gotClass != auth.ClassPublic {
			t.Fatalf("%s: class = %v, want public", tag, gotClass)
		}
		if gotAdm.refuse == "" {
			t.Fatalf("%s: admission.refuse empty, want a refusal naming %q", tag, wantMsgPart)
		}
		for _, part := range []string{wantMsgPart} {
			if !strings.Contains(gotAdm.refuse, part) {
				t.Errorf("%s: refusal %q does not contain %q", tag, gotAdm.refuse, part)
			}
		}
	}

	assertStarted := func(t *testing.T, wantClass auth.Class, gotClass auth.Class, gotAdm listenerAdmission, tag string) {
		t.Helper()
		if gotClass != wantClass {
			t.Fatalf("%s: class = %v, want %v", tag, gotClass, wantClass)
		}
		if gotAdm.refuse != "" {
			t.Fatalf("%s: unexpected refusal %q", tag, gotAdm.refuse)
		}
	}

	tests := []struct {
		name string
		addr string
	}{
		{"all interfaces literal", "tcp://0.0.0.0:44360"},
		{"public IPv4 literal", "tcp://192.0.2.1:44360"},
		{"IPv6 unspecified", "tcp://[::]:44360"},
		{"mixed-resolving hostname", "tcp://" + mixedHost + ":44360"},
	}
	for _, tc := range tests {
		class, adm, errText := classify(tc.addr)
		if errText != "" {
			t.Fatalf("%s: classify: %s", tc.name, errText)
		}
		// Zero tokens on a public address: fatal refusal whose stable sentence
		// names authentication. This is the row #14's acceptance test greps.
		assertRefused(t, "non-loopback bind needs authentication (#16); use 127.0.0.1 or ::1", class, adm, tc.name+": no tokens")
		if !strings.Contains(adm.refuse, "--auth-file") || !strings.Contains(adm.refuse, "tcp://127.0.0.1:") {
			t.Errorf("%s: refusal must name both fixing commands (--auth-file and a loopback --listen): %q", tc.name, adm.refuse)
		}
	}

	// The refusal carries two fix commands, rendered as the exact lines an
	// operator types next: a loopback --listen line and an --auth-file line.
	_, publicAdm, _ := classify("tcp://0.0.0.0:44360")
	hasListenFix, hasAuthFix := false, false
	for _, line := range strings.Split(publicAdm.refuse, "\n") {
		switch {
		case strings.HasPrefix(strings.TrimSpace(line), "messq serve --listen tcp://127.0.0.1:"):
			hasListenFix = true
		case strings.HasPrefix(strings.TrimSpace(line), "messq auth add <id> --auth-file"):
			hasAuthFix = true
		}
	}
	if !hasListenFix || !hasAuthFix {
		t.Errorf("refusal fixes incomplete (loopback listen=%v authfile=%v):\n%s", hasListenFix, hasAuthFix, publicAdm.refuse)
	}

	// Loopback opt-in without tokens starts, with the repeating warning banner.
	class, adm, errText := classify("tcp://127.0.0.1:44360")
	if errText != "" {
		t.Fatalf("loopback literal: %s", errText)
	}
	assertStarted(t, auth.ClassLoopback, class, adm, "loopback")
	if adm.warnBanner == "" {
		t.Error("loopback without tokens must carry an immediate warning banner")
	}
	if !adm.repeatWarn {
		t.Error("loopback without tokens must repeat its banner every window")
	}

	// A hostname resolving ONLY to loopback addresses is loopback, not public.
	class, adm, errText = classify("tcp://" + loopbackHost + ":44360")
	if errText != "" {
		t.Fatalf("loopback hostname: %s", errText)
	}
	assertStarted(t, auth.ClassLoopback, class, adm, "loopback hostname")

	// A Unix socket never warns or refuses: filesystem permissions are the ACL.
	class, adm, errText = classify("unix:///run/messq/messq.sock")
	if errText != "" {
		t.Fatalf("unix: %s", errText)
	}
	assertStarted(t, auth.ClassUnix, class, adm, "unix")
	if adm.warnBanner != "" || adm.repeatWarn {
		t.Error("unix sockets are silent in the policy table")
	}
}

// With credentials loaded, the SAME public addresses start and emit exactly one
// cleartext warning (the authfile is the boundary; TLS remains #40).
func TestEvaluateListenerAdmissionPublicWithTokens(t *testing.T) {
	t.Parallel()

	class, adm, err := evaluateListenerAdmission(context.Background(), "tcp://0.0.0.0:44360", 2, staticLookup(nil))
	if err != nil {
		t.Fatalf("classify: %v", err)
	}
	if class != auth.ClassPublic {
		t.Fatalf("class = %v, want public", class)
	}
	if adm.refuse != "" {
		t.Fatalf("tokens loaded: unexpected refusal %q", adm.refuse)
	}
	if adm.warnBanner == "" || !strings.Contains(strings.ToLower(adm.warnBanner), "cleartext") {
		t.Errorf("public-with-tokens banner = %q, want a cleartext-exposure warning", adm.warnBanner)
	}
	if adm.repeatWarn {
		t.Error("the cleartext warning fires once, not every window")
	}
}

// Tokens loaded while the address stays loopback silence the repeating banner
// entirely: the posture is strictly better than unauthenticated loopback.
func TestEvaluateListenerAdmissionLoopbackWithTokens(t *testing.T) {
	t.Parallel()

	_, adm, err := evaluateListenerAdmission(context.Background(), "tcp://127.0.0.1:44360", 1, staticLookup(nil))
	if err != nil {
		t.Fatalf("classify: %v", err)
	}
	if adm.refuse != "" || adm.warnBanner != "" || adm.repeatWarn {
		t.Errorf("authenticated loopback = %+v, want a quiet start", adm)
	}
}

// ---------------------------------------------------------------------------
// runServe integration: the public refusal exits EX_CONFIG before anything binds.
// Drives the real child process through the re-exec harness so the process exit
// CODE itself is under test (mutant: returning exitError=1 fails this suite).
// ---------------------------------------------------------------------------

// execServeExpectFailure runs the helper child with extra MESSQ_* env entries and
// returns its exit code plus captured stdout/stderr.
func execServeExpectFailure(t *testing.T, extraEnv ...string) (int, string, string) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	cmd := exec.CommandContext(context.Background(), exe, "-test.run=^TestHelperServeProcess$")
	base := []string{
		helperServeEnv + "=1",
		"MESSQ_DATA_DIR=" + t.TempDir(),
		"MESSQ_DURABILITY=full",
	}
	cmd.Env = append(os.Environ(), append(base, extraEnv...)...)
	var out, errOut bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	runErr := cmd.Run()
	if cmd.ProcessState == nil {
		t.Fatalf("child did not exit: %v", runErr)
	}
	return cmd.ProcessState.ExitCode(), out.String(), errOut.String()
}

const (
	// refuseStableSentence lives in serve.go; assertions here pin its bytes.
	// TEST-NET-1 documentation address: reserved, unroutable, non-loopback. The
	// policy refuses BEFORE any bind attempt, so no network traffic occurs.
	testNetAddr = "192.0.2.10"
	testNetPort = "44360"
)

func TestServeRefusesPublicBindWithoutAuthExitsConfig(t *testing.T) {
	code, _, stderr := execServeExpectFailure(t, "MESSQ_LISTEN=tcp://"+testNetAddr+":"+testNetPort)

	if code != exitcode.CONFIG {
		t.Errorf("exit code = %d, want exitcode.CONFIG (%d); a misconfiguration must fail loudly once and stay failed (#17 systemd RestartPreventExitStatus)", code, exitcode.CONFIG)
	}
	if !strings.Contains(stderr, refuseStableSentence) {
		t.Errorf("stderr does not carry the merged refusal sentence verbatim:\n%s", stderr)
	}
	if !strings.Contains(stderr, "--auth-file") {
		t.Errorf("stderr does not name --auth-file:\n%s", stderr)
	}
	if !strings.Contains(stderr, "messq serve --listen tcp://127.0.0.1:") || !strings.Contains(stderr, "messq auth add <id> --auth-file") {
		t.Errorf("stderr does not carry both fixing commands (loopback --listen and --auth-file):\n%s", stderr)
	}
	if !strings.Contains(stderr, "outcome=refused") {
		t.Errorf("no server.start outcome=refused emission before exit:\n%s", stderr)
	}
	if strings.Contains(stderr, `"outcome":"started"`) || strings.Contains(stderr, "outcome=started") {
		t.Errorf("a refused daemon must not also claim a successful start:\n%s", stderr)
	}
}

func TestServeRefusalNamesBothFixCommands(t *testing.T) {
	code, _, stderr := execServeExpectFailure(t, "MESSQ_LISTEN=tcp://"+testNetAddr+":"+testNetPort)
	if code != exitcode.CONFIG {
		t.Fatalf("exit code = %d, want %d", code, exitcode.CONFIG)
	}
	wantFixes := []string{
		"messq serve --listen tcp://127.0.0.1:" + testNetPort,
		"messq auth add <id> --auth-file ",
	}
	for _, fix := range wantFixes {
		if !strings.Contains(stderr, fix) {
			t.Errorf("stderr missing fix command %q:\n%s", fix, stderr)
		}
	}
}

// The refusal happens BEFORE the data directory is even opened: no store files
// appear behind a misconfigured bind, keeping a broken deployment inert.
func TestServePublicRefusalLeavesDataDirUnopened(t *testing.T) {
	dataDir := t.TempDir()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	cmd := exec.CommandContext(context.Background(), exe, "-test.run=^TestHelperServeProcess$")
	cmd.Env = append(os.Environ(),
		helperServeEnv+"=1",
		"MESSQ_DATA_DIR="+dataDir,
		"MESSQ_LISTEN=tcp://"+testNetAddr+":"+testNetPort,
		"MESSQ_DURABILITY=full",
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err == nil {
		t.Fatalf("serve unexpectedly succeeded")
	}
	entries, readErr := os.ReadDir(dataDir)
	if readErr != nil {
		t.Fatalf("read data dir: %v", readErr)
	}
	if len(entries) != 0 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("data dir touched during refusal: %v", names)
	}
}

// ---------------------------------------------------------------------------
// The loopback no-auth banner repeats every 10 minutes on the Clock seam.
// Runs inside a synctest bubble: virtual time advances while every goroutine
// blocks, so ten fictional minutes cost zero wall-clock nanoseconds and the
// test cannot be flaky.
// ---------------------------------------------------------------------------

func TestAuthBannerRepeatsEveryTenMinutesSynctest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var sink lockedBuffer
		logger := slog.New(slog.NewTextHandler(&sink, nil))
		ctx, cancel := context.WithCancel(context.Background())

		emitted := make(chan struct{}, 64)
		done := make(chan struct{})
		go func() {
			defer close(done)
			warnLoop(ctx, logger, clock.System{}, auth.ClassLoopback, "lo-banner", 10*time.Minute, emitted)
		}()

		// Immediate first emission, then exactly one per window.
		synctest.Wait()
		if got := drain(emitted); got != 1 {
			t.Fatalf("first phase: %d emissions, want exactly 1", got)
		}
		time.Sleep(10 * time.Minute) //nolint:forbidigo // virtual time inside the testing/synctest bubble; the ban's own sanctioned exemption
		synctest.Wait()
		if got := drain(emitted); got != 1 {
			t.Fatalf("after one window: %d new emissions, want exactly 1", got)
		}
		time.Sleep(20 * time.Minute) //nolint:forbidigo // virtual time inside the bubble
		synctest.Wait()
		if got := drain(emitted); got != 2 {
			t.Fatalf("after two windows: %d new emissions, want exactly 2", got)
		}
		cancel()
		<-done
	})
}

func drain(ch <-chan struct{}) int {
	n := 0
	for drained := false; !drained; {
		select {
		case <-ch:
			n++
		default:
			drained = true
		}
	}
	return n
}

// The public-with-tokens warning does NOT repeat: one shot at startup.
func TestAuthCleartextWarningFiresOnceSynctest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger := slog.New(slog.NewTextHandler(&lockedDiscard{}, nil))
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		emitted := make(chan struct{}, 64)
		done := make(chan struct{})
		go func() {
			defer close(done)
			warnLoop(ctx, logger, clock.System{}, auth.ClassPublic, "pub-banner", 10*time.Minute, emitted)
		}()

		synctest.Wait()
		if got := drain(emitted); got != 1 {
			t.Fatalf("startup: %d emissions, want 1", got)
		}
		time.Sleep(31 * time.Minute) //nolint:forbidigo // virtual time inside the bubble; three windows pass instantly
		synctest.Wait()
		select {
		case <-emitted:
			t.Fatal("the cleartext warning repeated; it must fire once per process")
		default:
		}
	})
}

// ---------------------------------------------------------------------------
// --socket-mode reaches the listener: a freshly bound Unix socket carries the
// requested mode (default 0660), applied immediately after Listen returns
// because the node does not exist during any earlier hook (issue #16 §4).
// ---------------------------------------------------------------------------

func TestListenUnixAppliesRequestedSocketMode(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "messq.sock")

	ln, err := listenUnix(context.Background(), path, 0o640)
	if err != nil {
		t.Fatalf("listenUnix: %v", err)
	}
	defer func() {
		if cerr := ln.Close(); cerr != nil {
			t.Errorf("close: %v", cerr)
		}
	}()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat socket: %v", err)
	}
	if perm := st.Mode().Perm(); perm != 0o640 {
		t.Errorf("socket mode = %04o, want 0640", perm)
	}
}

func TestListenUnixDefaultMatchesDocumentedMode(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "messq.sock")
	mode := uint32(0o660)
	ln, err := listenUnix(context.Background(), path, mode)
	if err != nil {
		t.Fatalf("listenUnix: %v", err)
	}
	defer func() {
		if cerr := ln.Close(); cerr != nil {
			t.Errorf("close: %v", cerr)
		}
	}()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat socket: %v", err)
	}
	if st.Mode().Perm() != 0o660 {
		t.Errorf("socket mode = %04o, want the documented 0660", st.Mode().Perm())
	}
}

// lockedBuffer is a mutex-guarded bytes.Buffer so parallel-emitting goroutines in
// synctest bubbles can log without racing.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (w *lockedBuffer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

// lockedDiscard sinks logs without keeping them; same locking story.
type lockedDiscard struct{}

func (lockedDiscard) Write(p []byte) (int, error) { return len(p), nil }
