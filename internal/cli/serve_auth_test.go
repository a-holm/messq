// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
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
func staticLookup(table map[string][]net.IPAddr) resolveHostFunc {
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

	for _, tc := range []struct {
		name string
		addr string
	}{
		{"all interfaces literal", "tcp://0.0.0.0:44360"},
		{"public IPv4 literal", "tcp://192.0.2.1:44360"},
		{"IPv6 unspecified", "tcp://[::]:44360"},
		{"mixed-resolving hostname", "tcp://" + mixedHost + ":44360"},
	} {
		class, adm, errText := classify(tc.addr)
		if errText != "" {
			t.Fatalf("%s: classify: %s", tc.name, errText)
		}
		// Zero tokens on a public address: fatal refusal whose stable sentence
		// names authentication. This is the row #14's acceptance test greps.
		assertRefused(t, refuseStableSentence, class, adm, tc.name+": no tokens")
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

// refuseCredential is a fixed, OBVIOUSLY-fake credential used by fixtures: it
// satisfies the wire shape ([A-Za-z0-9._~-]{16..512}) so files parse, while
// carrying no real secret material.
const refuseCredential = "ci-only-not-a-real-secret-0000000000000000__fixture"

const (
	// TEST-NET-1 documentation address: reserved, unroutable, non-loopback. The
	// policy refuses BEFORE any bind attempt, so no network traffic occurs.
	testNetAddr = "192.0.2.10"
	testNetPort = "44360"
)

func prepareDataDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatalf("chmod datadir: %v", err)
	}
	return dir
}

func writeAuthFixture(t *testing.T, path string) {
	t.Helper()
	sum := sha256.Sum256([]byte(refuseCredential))
	content := fmt.Sprintf("ci-test %s publish,consume orders*\n", hex.EncodeToString(sum[:]))
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write auth fixture: %v", err)
	}
}

// execServe runs the helper child with the given MESSQ_* env entries. The caller
// decides whether blocking-until-exit or stop-and-reap is part of the scenario;
// cleanup kills any leftover child so a failed case cannot wedge the suite. The
// child's stderr is drained line-by-line into a mutex-guarded sink, because the
// log reader and the running daemon write concurrently.
func execServe(t *testing.T, extraEnv ...string) (*exec.Cmd, *lockedBuffer) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	cmd := exec.CommandContext(context.Background(), exe, "-test.run=^TestHelperServeProcess$")
	cmd.Env = append(os.Environ(), append([]string{helperServeEnv + "=1"}, extraEnv...)...)
	cmd.Stdout = io.Discard
	errOut := &lockedBuffer{}
	stderr, pipeErr := cmd.StderrPipe()
	if pipeErr != nil {
		t.Fatalf("stderr pipe: %v", pipeErr)
	}
	if startErr := cmd.Start(); startErr != nil {
		t.Fatalf("start serve child: %v", startErr)
	}
	go func() {
		sc := bufio.NewScanner(stderr)
		for sc.Scan() {
			errOut.WriteLine(sc.Text())
		}
	}()
	t.Cleanup(func() {
		if cmd.ProcessState == nil && cmd.Process != nil {
			if killErr := cmd.Process.Kill(); killErr == nil {
				_ = cmd.Wait() //nolint:errcheck // reaping a killed child
			}
		}
	})
	return cmd, errOut
}

func execServeExpectExit(t *testing.T, extraEnv ...string) (int, string) {
	t.Helper()
	cmd, errOut := execServe(t, extraEnv...)
	runErr := cmd.Wait()
	if cmd.ProcessState == nil {
		t.Fatalf("child did not exit: %v", runErr)
	}
	return cmd.ProcessState.ExitCode(), errOut.String()
}

func TestServeRefusesPublicBindWithoutAuthExitsConfig(t *testing.T) {
	code, stderr := execServeExpectExit(t,
		"MESSQ_DATA_DIR="+prepareDataDir(t),
		"MESSQ_LISTEN=tcp://"+testNetAddr+":"+testNetPort,
	)

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
	if strings.Contains(stderr, "outcome=started") {
		t.Errorf("a refused daemon must not also claim a successful start:\n%s", stderr)
	}
}

// The refusal happens BEFORE the data directory is even opened: no store files
// appear behind a misconfigured bind, keeping a broken deployment inert.
func TestServePublicRefusalLeavesDataDirUnopened(t *testing.T) {
	dataDir := prepareDataDir(t)

	code, _ := execServeExpectExit(t,
		"MESSQ_DATA_DIR="+dataDir,
		"MESSQ_LISTEN=tcp://"+testNetAddr+":"+testNetPort,
	)
	if code != exitcode.CONFIG {
		t.Fatalf("exit code = %d, want %d", code, exitcode.CONFIG)
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
		time.Sleep(10 * time.Minute) //nolint:forbidigo // virtual time inside the synctest bubble
		synctest.Wait()
		if got := drain(emitted); got != 1 {
			t.Fatalf("after one window: %d new emissions, want exactly 1", got)
		}
		time.Sleep(20 * time.Minute) //nolint:forbidigo // virtual time inside the synctest bubble
		synctest.Wait()
		if got := drain(emitted); got != 2 {
			t.Fatalf("after two windows: %d new emissions, want exactly 2", got)
		}
		cancel()
		<-done
	})
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
		time.Sleep(31 * time.Minute) //nolint:forbidigo // three windows pass instantly in the bubble
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
	ln, err := listenUnix(context.Background(), path, defaultSocketMode)
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

// ---------------------------------------------------------------------------
// Preflight wiring: ADR-0013's "verified at startup, refuse to run otherwise".
// Every fatal row exits exitcode.CONFIG with its exact fix command printed, and
// nothing binds or opens while the posture is broken.
// ---------------------------------------------------------------------------

// NOTE: a loose (0755) DATA DIR never reaches this wiring's preflight: store.Open
// itself refuses such directories at open time with its own teaching error (see
// internal/store/datadir_test.go). Serve keeps the DIR row in its audit output for
// doctor symmetry (#30) — the load-bearing NEW rows here are the auth-file ones,
// the socket-mode row, and zero-tokens-while-required.

func TestServePreflightRefusesLooseAuthFileEvenOnLoopback(t *testing.T) {
	dataDir := prepareDataDir(t)
	authFile := filepath.Join(dataDir, "tokens")
	writeAuthFixture(t, authFile)
	if err := os.Chmod(authFile, 0o644); err != nil {
		t.Fatalf("loosen auth file: %v", err)
	}

	code, stderr := execServeExpectExit(t,
		"MESSQ_DATA_DIR="+dataDir,
		"MESSQ_LISTEN=tcp://127.0.0.1:0",
		"MESSQ_AUTH_FILE="+authFile,
	)
	if code != exitcode.CONFIG {
		t.Fatalf("exit code = %d, want exitcode.CONFIG (loopback does NOT excuse a readable token file)", code)
	}
	if !strings.Contains(stderr, "want 0600") || !strings.Contains(stderr, `chmod 600 "`+authFile+`"`) {
		t.Errorf("stderr missing the auth-file finding/exact fix:\n%s", stderr)
	}
}

func TestServePreflightRefusesOtherReadableSocketModeBeforeBind(t *testing.T) {
	sock := filepath.Join(prepareDataDir(t), "p.sock")

	code, stderr := execServeExpectExit(t,
		"MESSQ_DATA_DIR="+filepath.Dir(sock),
		"MESSQ_LISTEN=unix://"+sock,
		"MESSQ_SOCKET_MODE=0644",
	)
	if code != exitcode.CONFIG {
		t.Fatalf("exit code = %d, want exitcode.CONFIG (--socket-mode 0644 lets other users read)", code)
	}
	if !strings.Contains(stderr, "--socket-mode must not grant other read or write") {
		t.Errorf("stderr missing the socket-mode fix text:\n%s", stderr)
	}
	// Refusal happened BEFORE any bind: no socket node was ever created.
	if _, err := os.Stat(sock); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("socket node exists after a pre-bind refusal: %v", err)
	}
}

func TestServePreflightAllowsTightPosture(t *testing.T) {
	dataDir := prepareDataDir(t)
	authFile := filepath.Join(dataDir, "tokens")
	writeAuthFixture(t, authFile)

	// The established unix-socket harness drives the same startup sequence:
	// flags parse -> admission -> preflight -> store open -> listen -> SERVING.
	// A clean SIGTERM exit is the assertion that the daemon was genuinely up,
	// not merely past parse — startServe itself waits for an honest /healthz 200
	// before returning, so reaching here proves preflight allowed the posture.
	sock := filepath.Join(dataDir, "messq.sock")
	cmd := startServe(t, dataDir, sock, "MESSQ_AUTH_FILE="+authFile, "MESSQ_DURABILITY=relaxed")
	stopServe(t, cmd)
}

// lockedBuffer is a mutex-guarded line sink so the parent test can read log lines
// while the child daemon keeps writing them.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

// WriteLine appends one drained stderr line (newline added on read, not stored).
func (w *lockedBuffer) WriteLine(line string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.b.WriteString(line)
	w.b.WriteByte('\n')
}

// String snapshots the sink; safe against concurrent WriteLine calls.
func (w *lockedBuffer) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.String()
}

func (w *lockedBuffer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

// lockedDiscard sinks logs without keeping them; same locking story.
type lockedDiscard struct{}

func (lockedDiscard) Write(p []byte) (int, error) { return len(p), nil }
