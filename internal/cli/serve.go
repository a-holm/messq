// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/a-holm/messq/internal/api"
	"github.com/a-holm/messq/internal/auth"
	"github.com/a-holm/messq/internal/buildinfo"
	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/exitcode"
	"github.com/a-holm/messq/internal/janitor"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
)

// Housekeeping constants the §8 flag layer feeds into the janitor (#27): A1's fair-
// share window sizes for the two row-bound jobs and the hysteresis multiplier queue
// itself defaults to when one is not spelled out.
const (
	defaultRetentionBatch = 512
	defaultDedupLimit     = 32
	defaultDiskRecover    = 1.25
)

// serveFlagNames is the closed §8 flag set for `messq serve`. Every flag takes a value;
// there is no config file and no viper (D8). The transport bounds (--max-waiters et al)
// are issue #14's §9 table; --max-header-bytes stays #7's message-user-header cap — the
// http.Server header cap is spelled --max-request-header-bytes so the two do not collide.
var serveFlagNames = map[string]struct{}{
	"--data-dir":                 {},
	"--listen":                   {},
	"--durability":               {},
	"--max-msg-size-ceiling":     {},
	"--max-header-bytes":         {},
	"--max-batch-messages":       {},
	"--max-batch-bytes":          {},
	"--peek-scan-limit":          {},
	"--peek-max-limit":           {},
	"--dedup-sweep-interval":     {},
	"--drain-timeout":            {},
	"--max-waiters":              {},
	"--max-waiters-per-consumer": {},
	"--max-fetch-wait":           {},
	"--fetch-empty-damper":       {},
	"--max-request-bytes":        {},
	"--read-header-timeout":      {},
	"--idle-timeout":             {},
	"--max-request-header-bytes": {},
	"--max-conns":                {},
	"--writer-submit-timeout":    {},
	"--auth-file":                {},
	"--socket-mode":              {},
	// Issue #27 §8 housekeeping + disk-safety flags.
	"--janitor-interval":      {},
	"--event-retention":       {},
	"--event-max-rows":        {},
	"--wal-max-bytes":         {},
	"--min-free-bytes":        {},
	"--disk-reserve-bytes":    {},
	"--vacuum-freelist-pages": {},
	"--vacuum-pages-per-tick": {},
	"--lag-report-threshold":  {},
	"--lag-report-interval":   {},
	// Issue #26 §2: the composite dev flag. Boolean: it takes no value.
	"--dev": {},
}

// serveBoolFlags take no value: their presence in the argv map is the truth.
var serveBoolFlags = map[string]struct{}{
	"--dev": {},
}

// devDrainTimeout is --dev's implication on --drain-timeout: Ctrl-C is instant
// in a throwaway daemon (issue #26 §2; #17 owns the flag, this composition).
const devDrainTimeout = 2 * time.Second

// devBanner renders the unmissable development-mode banner (issue #26 §2). It
// names the data dir and the deletion (or the keep), the instant Ctrl-C, and the
// line that starts the daemon an operator actually keeps. The box width is
// fixed; COLUMNS rendering is #19's log surface, not this banner's.
func devBanner(dataDir string, drain time.Duration, kept bool) string {
	var b strings.Builder
	fmt.Fprint(&b, "╭─ DEVELOPMENT MODE "+strings.Repeat("─", 49)+"╮\n")
	if kept {
		fmt.Fprintf(&b, "│ data dir %s\n", dataDir)
		fmt.Fprint(&b, "│ is KEPT when this process exits (explicit --data-dir)\n")
	} else {
		fmt.Fprintf(&b, "│ data dir %s\n", dataDir)
		fmt.Fprint(&b, "│ is DELETED when this process exits\n")
	}
	fmt.Fprint(&b, "│ streams and consumers are auto-created · drain "+drain.String()+"\n")
	fmt.Fprint(&b, "│ for anything you care about:  messq serve --data-dir /var/lib/messq\n")
	fmt.Fprint(&b, "╰"+strings.Repeat("─", 69)+"╯\n")
	return b.String()
}

// serveConfig is the fully resolved serve configuration, one field per §8 flag. It is
// produced by [parseServeFlags] and consumed by [runServe]; [serveConfig.storeOptions]
// maps the store-relevant subset onto store.Options.
type serveConfig struct {
	dataDir            string
	listen             string
	durability         store.Durability
	maxMsgSizeCeiling  int64
	maxHeaderBytes     int
	maxBatchMessages   int
	maxBatchBytes      int64
	peekScanLimit      int
	peekMaxLimit       int
	dedupSweepInterval time.Duration
	// drainTimeout is the SIGTERM drain budget (PLAN §4.4). The default is SEMANTICS
	// A1's register value; the flag and MESSQ_DRAIN_TIMEOUT override it at runtime.
	// Consumed by the lifecycle manager when #17's composition-root slice lands.
	drainTimeout time.Duration

	maxWaiters            int
	maxWaitersPerConsumer int
	maxFetchWait          time.Duration
	fetchEmptyDamper      time.Duration
	maxRequestBytes       int64
	readHeaderTimeout     time.Duration
	idleTimeout           time.Duration
	maxRequestHeaderBytes int64
	maxConns              int
	writerSubmitTimeout   time.Duration

	// authFile is the --auth-file path (issue #16): a 0600 file of SHA-256-hashed
	// bearer tokens. Empty means loopback trust only. The file is DATA, not
	// configuration (D8): it reloads on SIGHUP while flags do not.
	authFile string
	// socketMode is --socket-mode: the mode applied to a freshly bound Unix socket
	// immediately after Listen returns (the node does not exist earlier). Default
	// and documented value is 0660 (ADR-0013).
	socketMode uint32

	// Issue #27 §8 housekeeping and disk-safety rows of the closed flag set.
	janitorInterval     time.Duration // --janitor-interval; 0 disables all housekeeping jobs (dev-only)
	eventRetention      time.Duration // --event-retention
	eventMaxRows        int64         // --event-max-rows
	walMaxBytes         int64         // --wal-max-bytes
	minFreeBytes        int64         // --min-free-bytes
	diskReserveBytes    int64         // --disk-reserve-bytes
	vacuumFreelistPages int64         // --vacuum-freelist-pages
	vacuumPagesPerTick  int64         // --vacuum-pages-per-tick
	lagReportThreshold  int64         // --lag-report-threshold
	lagReportInterval   time.Duration // --lag-report-interval

	// dev is --dev (issue #26 §2): the composite flag. data-dir and drain-timeout
	// implications were applied at parse time; this flag turns on auto-create and
	// the "dev":true self-report in the daemon.
	dev bool
	// devEphemeral records that --dev implied the data dir: runServe creates a
	// temp dir, prints it in the banner, and removes it at exit. An explicit
	// --data-dir (or MESSQ_DATA_DIR... no: dev beats env) leaves this false and
	// the directory is never removed.
	devEphemeral bool
}

// storeOptions maps the serve configuration onto the store's Options. The process-wide
// validation limits start from the §4.2 defaults and only the two fields the serve flags
// expose are overridden; the other Limits members keep their DefaultLimits values.
func (c serveConfig) storeOptions() store.Options {
	limits := queue.DefaultLimits()
	limits.MaxMsgSizeCeiling = c.maxMsgSizeCeiling
	limits.MaxHeaderBytes = c.maxHeaderBytes
	return store.Options{
		DataDir:            c.dataDir,
		Durability:         c.durability,
		Limits:             limits,
		PeekScanLimit:      c.peekScanLimit,
		PeekMaxLimit:       c.peekMaxLimit,
		MaxBatchMessages:   c.maxBatchMessages,
		DedupSweepInterval: c.dedupSweepInterval,
	}
}

// parseServeFlags parses the serve command line by hand, exactly like runVersion. Each
// setting resolves flag → MESSQ_* environment variable → default (ADR-0009: flags win);
// --data-dir has no default and must come from a flag or MESSQ_DATA_DIR.
func parseServeFlags(args []string, getenv func(string) string) (serveConfig, error) {
	flags := make(map[string]string)
	for len(args) > 0 {
		arg := args[0]
		args = args[1:]

		name, val, hasEq := arg, "", false
		if eq := strings.Index(arg, "="); eq >= 0 {
			name, val, hasEq = arg[:eq], arg[eq+1:], true
		}
		if !strings.HasPrefix(name, "--") {
			return serveConfig{}, fmt.Errorf("unexpected argument %q", arg)
		}
		if _, ok := serveFlagNames[name]; !ok {
			return serveConfig{}, fmt.Errorf("unknown flag %q", name)
		}
		if _, boolFlag := serveBoolFlags[name]; boolFlag {
			// A boolean flag's presence is its truth; `--dev=false` stays legal
			// for scripting symmetry.
			switch val {
			case "", "true":
				flags[name] = "true"
			case "false":
				flags[name] = "false"
			default:
				return serveConfig{}, fmt.Errorf("%s takes no value (want nothing, true or false)", name)
			}
			continue
		}
		if !hasEq {
			if len(args) == 0 {
				return serveConfig{}, fmt.Errorf("%s needs a value", name)
			}
			val = args[0]
			args = args[1:]
		}
		flags[name] = val
	}

	// resolve resolves flag → env → default for one setting. For the settings
	// --dev implies, parseServeFlags handles the layering itself (flag > dev >
	// env); everything else keeps this classic ladder.
	resolve := func(name, envName, def string) string {
		if v, ok := flags[name]; ok {
			return v
		}
		if v := getenv(envName); v != "" {
			return v
		}
		return def
	}

	cfg := serveConfig{}

	// Issue #26 §2: --dev is a composite whose implications apply ONLY where the
	// operator did not set the flag explicitly — an explicit flag always wins.
	// Env fallbacks sit BELOW the dev implication: dev is by definition a
	// throwaway mode, so a MESSQ_DATA_DIR pointing at production must not be
	// silently adopted by `messq serve --dev`.
	dev := flags["--dev"] == "true"
	if dev {
		cfg.dev = true
	}

	// --data-dir: explicit flag > dev ephemeral > env > error. The ephemeral dir
	// is created by runServe (so the banner can print the real path); the parser
	// only records the decision.
	if v, ok := flags["--data-dir"]; ok {
		cfg.dataDir = v
	} else if dev {
		cfg.devEphemeral = true
	} else if cfg.dataDir = getenv("MESSQ_DATA_DIR"); cfg.dataDir == "" {
		return serveConfig{}, errors.New("--data-dir is required (or set MESSQ_DATA_DIR)")
	}

	dur, err := store.ParseDurability(resolve("--durability", "MESSQ_DURABILITY", "full"))
	if err != nil {
		return serveConfig{}, err
	}
	cfg.durability = dur
	cfg.listen = resolve("--listen", "MESSQ_LISTEN", "unix:///run/messq/messq.sock")

	if cfg.maxMsgSizeCeiling, err = parseByteSize(resolve("--max-msg-size-ceiling", "MESSQ_MAX_MSG_SIZE_CEILING", "8MiB")); err != nil {
		return serveConfig{}, fmt.Errorf("--max-msg-size-ceiling: %w", err)
	}
	headerBytes, err := parseByteSize(resolve("--max-header-bytes", "MESSQ_MAX_HEADER_BYTES", "4KiB"))
	if err != nil {
		return serveConfig{}, fmt.Errorf("--max-header-bytes: %w", err)
	}
	cfg.maxHeaderBytes = int(headerBytes)
	if cfg.maxBatchMessages, err = parsePositiveInt(resolve("--max-batch-messages", "MESSQ_MAX_BATCH_MESSAGES", "1000"), "--max-batch-messages"); err != nil {
		return serveConfig{}, err
	}
	if cfg.maxBatchBytes, err = parseByteSize(resolve("--max-batch-bytes", "MESSQ_MAX_BATCH_BYTES", "8MiB")); err != nil {
		return serveConfig{}, fmt.Errorf("--max-batch-bytes: %w", err)
	}
	if cfg.peekScanLimit, err = parsePositiveInt(resolve("--peek-scan-limit", "MESSQ_PEEK_SCAN_LIMIT", "10000"), "--peek-scan-limit"); err != nil {
		return serveConfig{}, err
	}
	if cfg.peekMaxLimit, err = parsePositiveInt(resolve("--peek-max-limit", "MESSQ_PEEK_MAX_LIMIT", "1000"), "--peek-max-limit"); err != nil {
		return serveConfig{}, err
	}
	if cfg.dedupSweepInterval, err = time.ParseDuration(resolve("--dedup-sweep-interval", "MESSQ_DEDUP_SWEEP_INTERVAL", "60s")); err != nil {
		return serveConfig{}, fmt.Errorf("--dedup-sweep-interval: %w", err)
	}
	// 10s is A1's register value for the graceful-drain bound; the flag only overrides
	// it at runtime, the register stays the source of truth (brief-17 §8 Q1 ruling).
	if cfg.drainTimeout, err = time.ParseDuration(resolve("--drain-timeout", "MESSQ_DRAIN_TIMEOUT", "10s")); err != nil {
		return serveConfig{}, fmt.Errorf("--drain-timeout: %w", err)
	}
	// --dev implication (issue #26 §2): an explicit --drain-timeout flag beats the
	// dev default, but the env fallback does not — dev replaces env.
	if dev {
		if _, explicit := flags["--drain-timeout"]; !explicit {
			cfg.drainTimeout = devDrainTimeout
		}
	}

	// Issue #14 §9 transport bounds: flag → MESSQ_* env → default.
	if cfg.maxWaiters, err = parsePositiveInt(resolve("--max-waiters", "MESSQ_MAX_WAITERS", "4096"), "--max-waiters"); err != nil {
		return serveConfig{}, err
	}
	if cfg.maxWaitersPerConsumer, err = parsePositiveInt(resolve("--max-waiters-per-consumer", "MESSQ_MAX_WAITERS_PER_CONSUMER", "256"), "--max-waiters-per-consumer"); err != nil {
		return serveConfig{}, err
	}
	if cfg.maxFetchWait, err = time.ParseDuration(resolve("--max-fetch-wait", "MESSQ_MAX_FETCH_WAIT", "5m")); err != nil {
		return serveConfig{}, fmt.Errorf("--max-fetch-wait: %w", err)
	}
	if cfg.fetchEmptyDamper, err = time.ParseDuration(resolve("--fetch-empty-damper", "MESSQ_FETCH_EMPTY_DAMPER", "5ms")); err != nil {
		return serveConfig{}, fmt.Errorf("--fetch-empty-damper: %w", err)
	}
	if cfg.maxRequestBytes, err = parseByteSize(resolve("--max-request-bytes", "MESSQ_MAX_REQUEST_BYTES", "1MiB")); err != nil {
		return serveConfig{}, fmt.Errorf("--max-request-bytes: %w", err)
	}
	if cfg.readHeaderTimeout, err = time.ParseDuration(resolve("--read-header-timeout", "MESSQ_READ_HEADER_TIMEOUT", "10s")); err != nil {
		return serveConfig{}, fmt.Errorf("--read-header-timeout: %w", err)
	}
	if cfg.idleTimeout, err = time.ParseDuration(resolve("--idle-timeout", "MESSQ_IDLE_TIMEOUT", "120s")); err != nil {
		return serveConfig{}, fmt.Errorf("--idle-timeout: %w", err)
	}
	if cfg.maxRequestHeaderBytes, err = parseByteSize(resolve("--max-request-header-bytes", "MESSQ_MAX_REQUEST_HEADER_BYTES", "16KiB")); err != nil {
		return serveConfig{}, fmt.Errorf("--max-request-header-bytes: %w", err)
	}
	if cfg.maxConns, err = parsePositiveInt(resolve("--max-conns", "MESSQ_MAX_CONNS", "1024"), "--max-conns"); err != nil {
		return serveConfig{}, err
	}
	if cfg.writerSubmitTimeout, err = time.ParseDuration(resolve("--writer-submit-timeout", "MESSQ_WRITER_SUBMIT_TIMEOUT", "5s")); err != nil {
		return serveConfig{}, fmt.Errorf("--writer-submit-timeout: %w", err)
	}
	cfg.authFile = resolve("--auth-file", "MESSQ_AUTH_FILE", "")
	mode, merr := parseSocketMode(resolve("--socket-mode", "MESSQ_SOCKET_MODE", "0660"))
	if merr != nil {
		return serveConfig{}, merr
	}
	cfg.socketMode = mode

	// Issue #27 §8: housekeeping cadence and the disk-safety bounds. The interval is
	// the one flag where 0 is a legal value — it disarms every bounded job (documented
	// dev-only); the disk monitor keeps sampling regardless. Every byte/page bound
	// must be strictly positive because zero would silently unbound a collection
	// PLAN §5.2 I11 says cannot exist unbounded.
	if cfg.janitorInterval, err = time.ParseDuration(resolve("--janitor-interval", "MESSQ_JANITOR_INTERVAL", "60s")); err != nil {
		return serveConfig{}, fmt.Errorf("--janitor-interval: %w", err)
	}
	if cfg.janitorInterval < 0 {
		return serveConfig{}, errors.New("--janitor-interval must be 0 (disable) or positive")
	}
	if cfg.eventRetention, err = time.ParseDuration(resolve("--event-retention", "MESSQ_EVENT_RETENTION", "72h")); err != nil {
		return serveConfig{}, fmt.Errorf("--event-retention: %w", err)
	}
	if cfg.eventRetention <= 0 {
		return serveConfig{}, errors.New("--event-retention must be positive")
	}
	if cfg.eventMaxRows, err = parseI64(resolve("--event-max-rows", "MESSQ_EVENT_MAX_ROWS", "1000000"), "--event-max-rows"); err != nil {
		return serveConfig{}, err
	}
	if cfg.walMaxBytes, err = parseByteSize(resolve("--wal-max-bytes", "MESSQ_WAL_MAX_BYTES", "256MiB")); err != nil {
		return serveConfig{}, fmt.Errorf("--wal-max-bytes: %w", err)
	}
	if cfg.minFreeBytes, err = parseByteSize(resolve("--min-free-bytes", "MESSQ_MIN_FREE_BYTES", "256MiB")); err != nil {
		return serveConfig{}, fmt.Errorf("--min-free-bytes: %w", err)
	}
	if cfg.diskReserveBytes, err = parseByteSize(resolve("--disk-reserve-bytes", "MESSQ_DISK_RESERVE_BYTES", "64MiB")); err != nil {
		return serveConfig{}, fmt.Errorf("--disk-reserve-bytes: %w", err)
	}
	if cfg.vacuumFreelistPages, err = parseI64(resolve("--vacuum-freelist-pages", "MESSQ_VACUUM_FREELIST_PAGES", "10000"), "--vacuum-freelist-pages"); err != nil {
		return serveConfig{}, err
	}
	if cfg.vacuumPagesPerTick, err = parseI64(resolve("--vacuum-pages-per-tick", "MESSQ_VACUUM_PAGES_PER_TICK", "2000"), "--vacuum-pages-per-tick"); err != nil {
		return serveConfig{}, err
	}
	if cfg.lagReportThreshold, err = parseI64(resolve("--lag-report-threshold", "MESSQ_LAG_REPORT_THRESHOLD", "10000"), "--lag-report-threshold"); err != nil {
		return serveConfig{}, err
	}
	if cfg.lagReportInterval, err = time.ParseDuration(resolve("--lag-report-interval", "MESSQ_LAG_REPORT_INTERVAL", "60s")); err != nil {
		return serveConfig{}, fmt.Errorf("--lag-report-interval: %w", err)
	}
	if cfg.lagReportInterval <= 0 {
		return serveConfig{}, errors.New("--lag-report-interval must be positive")
	}

	for _, bound := range []struct {
		name string
		v    int64
	}{
		{"--wal-max-bytes", cfg.walMaxBytes},
		{"--min-free-bytes", cfg.minFreeBytes},
		{"--disk-reserve-bytes", cfg.diskReserveBytes},
		{"--vacuum-freelist-pages", cfg.vacuumFreelistPages},
		{"--vacuum-pages-per-tick", cfg.vacuumPagesPerTick},
		{"--lag-report-threshold", cfg.lagReportThreshold},
	} {
		if bound.v <= 0 {
			return serveConfig{}, fmt.Errorf("%s must be positive", bound.name)
		}
	}

	if cfg.maxMsgSizeCeiling <= 0 {
		return serveConfig{}, errors.New("--max-msg-size-ceiling must be positive")
	}
	if cfg.maxHeaderBytes <= 0 {
		return serveConfig{}, errors.New("--max-header-bytes must be positive")
	}
	if cfg.maxBatchBytes <= 0 {
		return serveConfig{}, errors.New("--max-batch-bytes must be positive")
	}
	if cfg.dedupSweepInterval <= 0 {
		return serveConfig{}, errors.New("--dedup-sweep-interval must be positive")
	}
	if cfg.drainTimeout <= 0 {
		return serveConfig{}, errors.New("--drain-timeout must be positive")
	}

	return cfg, nil
}

// parseI64 parses a decimal int64 flag value and refuses non-positive values: these
// bounds size hard collections, and a zero would silently unbound one.
func parseI64(s, name string) (int64, error) {
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: invalid integer %q", name, s)
	}
	if n <= 0 {
		return 0, fmt.Errorf("%s must be positive", name)
	}
	return n, nil
}

// parsePositiveInt parses a decimal flag value and refuses zero and negatives: a limit of
// zero is never a meaningful value for these flags, and the store's "<= 0 means default"
// fallback would silently mask it.
func parsePositiveInt(s, name string) (int, error) {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	if n <= 0 {
		return 0, fmt.Errorf("%s must be positive, got %d", name, n)
	}
	return n, nil
}

// parseByteSize parses a byte count with an optional binary suffix (B, KiB, MiB, GiB,
// TiB) or a bare decimal byte count. Suffixes are 1024-based, matching the IEC spellings
// in the §8 defaults.
func parseByteSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("empty byte size")
	}
	// Longest suffix first so "8MiB" is not mistaken for "8M" plus a stray "iB".
	units := []struct {
		suffix string
		mult   int64
	}{
		{"TiB", 1 << 40},
		{"GiB", 1 << 30},
		{"MiB", 1 << 20},
		{"KiB", 1 << 10},
		{"B", 1},
	}
	for _, u := range units {
		if strings.HasSuffix(s, u.suffix) {
			num := strings.TrimSpace(strings.TrimSuffix(s, u.suffix))
			n, err := strconv.ParseInt(num, 10, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid byte size %q", s)
			}
			if n < 0 {
				return 0, fmt.Errorf("byte size %q is negative", s)
			}
			return n * u.mult, nil
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid byte size %q: want a count with a B/KiB/MiB/GiB/TiB suffix", s)
	}
	if n < 0 {
		return 0, fmt.Errorf("byte size %q is negative", s)
	}
	return n, nil
}

// defaultSocketMode is the documented --socket-mode default (ADR-0013): group-
// writable so the messq group reaches the daemon.
const defaultSocketMode = uint32(0o660)

// parseSocketMode validates a --socket-mode value: plain octal permission bits,
// non-zero. Setuid/setgid/sticky are refused — a socket node has no use for them,
// and their presence is an operator slip worth teaching about.
func parseSocketMode(s string) (uint32, error) {
	v, err := strconv.ParseUint(s, 8, 32)
	if err != nil {
		return 0, fmt.Errorf("--socket-mode %q must be octal permission bits (e.g. 0660)", s)
	}
	if v == 0 {
		return 0, errors.New("--socket-mode must not be 0000; leave --listen on tcp://127.0.0.1 instead of closing the socket")
	}
	if v > 0o777 {
		return 0, fmt.Errorf("--socket-mode %04o carries setuid/setgid/sticky bits; want plain permission bits like 0660", v)
	}
	return uint32(v), nil
}

// The #16 listener-policy banners. Both name the boundary that protects the
// deployment today and the flag that tightens it.
const (
	bannerLoopbackUnauth  = "unauthenticated loopback listener: any local process can read and write every stream (#16); provide --auth-file to require bearer tokens"
	bannerPublicCleartext = "serving cleartext HTTP on a public address: bearer tokens from --auth-file are the only boundary until native TLS lands (#40)"
)

// refuseStableSentence is the sentence merged #7 emits for an unauthenticated
// public bind. Issue #16's wiring keeps it byte-for-byte so cross-lane acceptance
// tests can pin it verbatim.
const refuseStableSentence = "non-loopback bind needs authentication (#16); use 127.0.0.1 or ::1"

// listenerAdmission is the decision of the issue #16 §7 policy table for one
// --listen address with n tokens loaded: refuse to start, warn once, and/or keep
// warning every window for as long as the process runs.
type listenerAdmission struct {
	refuse     string // non-empty: fatal startup text; everything else is moot
	warnBanner string // non-empty: WARN once at startup
	repeatWarn bool   // re-emit warnBanner every 10 minutes while running
}

// resolveHostFunc is the resolver shape [evaluateListenerAdmission] takes so tests
// can stub hostname resolution without DNS. An alias keeps it assignable to
// internal/auth's lookupIP parameter type.
type resolveHostFunc = func(ctx context.Context, host string) ([]net.IPAddr, error)

// serveLookupIP is the resolver [evaluateListenerAdmission] uses in production;
// a package variable purely because Classify takes the lookup function as data.
var serveLookupIP resolveHostFunc = net.DefaultResolver.LookupIPAddr

// evaluateListenerAdmission classifies addr via internal/auth.Classify and applies
// the closed table:
//
//	unix                  -> start silently (filesystem permissions are the ACL)
//	loopback,  0 tokens   -> start, warn now AND every 10 minutes
//	loopback, >0 tokens   -> start silently
//	public,    0 tokens   -> REFUSE: stable sentence + both fixing commands
//	public,   >0 tokens   -> start, warn once about cleartext HTTP
//
// Hostnames resolving to ANY non-loopback address classify as public (the safe
// direction). A refused row is exitcode.CONFIG at the caller.
func evaluateListenerAdmission(ctx context.Context, addr string, tokens int, lookup resolveHostFunc) (auth.Class, listenerAdmission, error) {
	class, err := auth.Classify(ctx, addr, lookup)
	if err != nil {
		return 0, listenerAdmission{}, fmt.Errorf("bad --listen %q: %w", addr, err)
	}
	switch class {
	case auth.ClassUnix:
		return class, listenerAdmission{}, nil
	case auth.ClassLoopback:
		if tokens == 0 {
			return class, listenerAdmission{warnBanner: bannerLoopbackUnauth, repeatWarn: true}, nil
		}
		return class, listenerAdmission{}, nil
	case auth.ClassPublic:
		if tokens == 0 {
			return class, listenerAdmission{refuse: publicRefusal(addr)}, nil
		}
		return class, listenerAdmission{warnBanner: bannerPublicCleartext}, nil
	default:
		return class, listenerAdmission{}, fmt.Errorf("unclassified --listen %q", addr)
	}
}

// publicRefusal renders the fatal startup text for a public bind without tokens:
// the stable #7 sentence first, then exactly the two commands that fix the
// deployment — listen loopback instead, or provide credentials.
func publicRefusal(addr string) string {
	hostport := strings.TrimPrefix(addr, "tcp://")
	port := ""
	if _, p, splitErr := net.SplitHostPort(hostport); splitErr == nil {
		port = p
	}
	var b strings.Builder
	fmt.Fprintf(&b, "refusing to bind %q: %s\n", hostport, refuseStableSentence)
	fmt.Fprintln(&b, "fix one of these and start again:")
	fmt.Fprintf(&b, "  messq serve --listen tcp://127.0.0.1:%s          # keep the daemon loopback-local\n", port)
	fmt.Fprintf(&b, "  messq auth add <id> --auth-file <path>           # require bearer credentials (#16)")
	return b.String()
}

// logRefusedStartup emits the refused row of server.start before a fatal exit so a
// refusal is exactly as greppable as a start (issue #16 §4): same event name,
// outcome=refused, plus the reason.
func logRefusedStartup(logger *slog.Logger, addr, reason string) {
	logger.Error("server.start",
		"outcome", "refused",
		"reason", reason,
		"listen", addr,
		"version", buildinfo.Get().Version,
	)
}

// loadAuthTokenCount reads and parses an --auth-file far enough to know how many
// tokens it holds. A non-zero error is always a misconfiguration: callers refuse
// startup rather than treat the file as empty.
func loadAuthTokenCount(path string) (int, *auth.File, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, nil, fmt.Errorf("cannot read --auth-file %q: %w", path, err)
	}
	file, err := auth.Parse(path, bytes.NewReader(data))
	if err != nil {
		return 0, nil, fmt.Errorf("--auth-file %q is not usable: %w", path, err)
	}
	return len(file.Tokens), file, nil
}

// authBannerWindow is how often the unauthenticated-loopback warning repeats.
// Measured in wall-clock terms but driven through the Clock seam so tests run it
// inside a testing/synctest bubble at zero cost.
const authBannerWindow = 10 * time.Minute

// warnLoop emits banner once immediately, then — exactly when class is loopback —
// once per window until ctx ends. Every emission also lands on emitted so tests
// can count them deterministically; production passes a never-drained channel
// whose drops are harmless duplicates. Public-class processes do not repeat: the
// one-shot cleartext warning is all they get.
func warnLoop(ctx context.Context, logger *slog.Logger, clk clock.Clock, class auth.Class, banner string, every time.Duration, emitted chan<- struct{}) {
	emit := func() {
		logger.Warn(banner, "listener_class", class.String())
		select {
		case emitted <- struct{}{}:
		default:
		}
	}
	emit()
	if class != auth.ClassLoopback {
		return
	}
	ticker := clk.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
			emit()
		}
	}
}

// listen creates the daemon listener from a --listen address. Whether the address
// is ALLOWED to bind was already decided by [evaluateListenerAdmission]; this
// function only binds. unix://PATH applies socketMode to the node immediately after
// Listen returns — the filesystem node does not exist earlier (issue #16 §4) — and
// the preflight audit asserts the final mode.
func listen(ctx context.Context, addr string, socketMode uint32) (net.Listener, error) {
	switch {
	case strings.HasPrefix(addr, "unix://"):
		return listenUnix(ctx, strings.TrimPrefix(addr, "unix://"), socketMode)
	case strings.HasPrefix(addr, "tcp://"):
		hostport := strings.TrimPrefix(addr, "tcp://")
		ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", hostport)
		if err != nil {
			return nil, fmt.Errorf("listen on %s: %w", hostport, err)
		}
		return ln, nil
	default:
		return nil, fmt.Errorf("unsupported --listen %q: want unix://PATH or tcp://HOST:PORT", addr)
	}
}

// listenUnix binds a Unix socket at path and fixes its mode to socketMode. A crashed previous
// run leaves a stale file where the socket path used to be; that path is removed only
// when it is genuinely stale (nothing answers a connect), never a live daemon's socket.
func listenUnix(ctx context.Context, path string, socketMode uint32) (net.Listener, error) {
	ln, err := (&net.ListenConfig{}).Listen(ctx, "unix", path)
	if err == nil {
		if chErr := chmodSocket(path, socketMode); chErr != nil {
			return nil, errors.Join(fmt.Errorf("chmod socket %s: %w", path, chErr), ln.Close())
		}
		return ln, nil
	}
	stale, staleErr := socketIsStale(ctx, path)
	if staleErr != nil {
		return nil, staleErr
	}
	if !stale {
		return nil, fmt.Errorf("listen on %s: %w", path, err)
	}
	if rmErr := os.Remove(path); rmErr != nil {
		return nil, fmt.Errorf("remove stale socket %s: %w", path, rmErr)
	}
	ln, err = (&net.ListenConfig{}).Listen(ctx, "unix", path)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", path, err)
	}
	if chErr := chmodSocket(path, socketMode); chErr != nil {
		return nil, errors.Join(fmt.Errorf("chmod socket %s: %w", path, chErr), ln.Close())
	}
	return ln, nil
}

// chmodSocket applies the requested mode to a freshly bound Unix socket. 0660 is
// the documented default — group-writable so the messq group can reach the daemon
// (ADR-0013/0003) — and any operator-requested mode is honoured verbatim.
func chmodSocket(path string, socketMode uint32) error {
	return os.Chmod(path, os.FileMode(socketMode).Perm())
}

// socketIsStale reports whether nothing is listening at a Unix socket path: a failed
// connect means the path is a leftover file, not a live socket.
func socketIsStale(ctx context.Context, path string) (bool, error) {
	conn, err := (&net.Dialer{Timeout: 250 * time.Millisecond}).DialContext(ctx, "unix", path)
	if err != nil {
		// A refused connect is the stale answer, not a probe error to propagate.
		return true, nil //nolint:nilerr // the dial failure is the stale signal, not an error
	}
	if closeErr := conn.Close(); closeErr != nil {
		return false, fmt.Errorf("close probe connection: %w", closeErr)
	}
	return false, nil
}

// refuseNonLoopback rejects a TCP host:port that is not loopback-only. It is the
// enforcement of ADR-0013's "a non-loopback bind without authentication is a fatal
// startup error". An empty host (all interfaces) is refused outright, as is any
// resolved address that is not a loopback address. The classification lives in ONE
// function across the codebase (internal/auth.Classify); this wrapper carries only
// the refusal sentence.
func refuseNonLoopback(ctx context.Context, hostport string) error {
	if _, _, err := net.SplitHostPort(hostport); err != nil {
		return fmt.Errorf("invalid --listen tcp address %q: %w", hostport, err)
	}
	class, err := auth.Classify(ctx, "tcp://"+hostport, net.DefaultResolver.LookupIPAddr)
	if err != nil {
		return err
	}
	if class == auth.ClassPublic {
		return fmt.Errorf("refusing to bind %q: %s", hostport, refuseStableSentence)
	}
	return nil
}

// runServe is the `messq serve` command: parse flags, open the store, bind the listener,
// then serve until interrupted. Recovery has finished inside store.Open, so the /healthz
// 200 is honest from the first request.
func runServe(args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	cfg, err := parseServeFlags(args, getenv)
	if err != nil {
		return usageError(stderr, err.Error())
	}

	// Issue #26 §2: --dev with no --data-dir runs on a throwaway directory that is
	// removed at exit. The dir is created HERE (not by the parser) so the banner
	// can print the path that will really be deleted.
	if cfg.devEphemeral {
		dir, mkErr := os.MkdirTemp("", "messq-dev-")
		if mkErr != nil {
			fmt.Fprintf(stderr, "messq: cannot create ephemeral data dir: %v\n", mkErr)
			return exitError
		}
		cfg.dataDir = dir
		defer func() {
			if rmErr := os.RemoveAll(dir); rmErr != nil {
				fmt.Fprintf(stderr, "messq: remove ephemeral data dir %s: %v\n", dir, rmErr)
			}
		}()
	}

	logger := slog.New(slog.NewTextHandler(stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Issue #16 §7: the listener policy runs FIRST — a misconfigured or unauthenticated
	// public bind refuses before the store is even opened, emits server.start
	// outcome=refused as greppably as starts are, and exits exitcode.CONFIG.
	tokens := 0
	if cfg.authFile != "" {
		n, _, loadErr := loadAuthTokenCount(cfg.authFile)
		if loadErr != nil {
			fmt.Fprintf(stderr, "messq: %v\n", loadErr)
			logRefusedStartup(logger, cfg.listen, loadErr.Error())
			return exitcode.CONFIG
		}
		tokens = n
	}
	class, adm, admitErr := evaluateListenerAdmission(ctx, cfg.listen, tokens, serveLookupIP)
	if admitErr != nil {
		return usageError(stderr, admitErr.Error())
	}
	if adm.refuse != "" {
		fmt.Fprintf(stderr, "%s\n", adm.refuse)
		logRefusedStartup(logger, cfg.listen, refuseStableSentence)
		return exitcode.CONFIG
	}

	// The dev banner (issue §2): unmissable, names what it costs, and printed only
	// after admission so a refused --dev never advertises itself. Narration goes
	// to stderr; a dev daemon's stdout stays data-free for pipelines.
	if cfg.dev {
		fmt.Fprint(stderr, devBanner(cfg.dataDir, cfg.drainTimeout, !cfg.devEphemeral))
	}

	clk := clock.System{}
	opt := cfg.storeOptions()
	opt.Clock = clk
	opt.Logger = logger

	st, _, err := store.Open(ctx, opt)
	if err != nil {
		fmt.Fprintf(stderr, "messq: cannot open store: %v\n", err)
		return exitError
	}
	defer func() {
		if closeErr := st.Close(context.Background()); closeErr != nil {
			fmt.Fprintf(stderr, "messq: close store: %v\n", closeErr)
		}
	}()

	// ADR-0013: the filesystem posture is verified at startup and the daemon
	// refuses to run otherwise. The audit runs after store.Open so the rows can
	// observe the REAL db/-wal/-shm modes the store created (umask-proof 0600),
	// and before listen() so a refused posture never binds. Fatal findings exit
	// exitcode.CONFIG with their exact fix commands; warn findings narrate.
	preflight := auth.Preflight(auth.Options{
		DataDir:     cfg.dataDir,
		AuthFile:    cfg.authFile,
		RequireAuth: class == auth.ClassPublic,
		SocketMode:  os.FileMode(cfg.socketMode),
		UID:         os.Getuid(),
	})
	fatalCount := 0
	for _, f := range preflight {
		if f.Level == auth.LevelFatal {
			fatalCount++
		}
	}
	if fatalCount > 0 {
		fmt.Fprintf(stderr, "messq serve: refusing to start with %d misconfiguration(s):\n", fatalCount)
		for _, f := range preflight {
			if f.Level != auth.LevelFatal {
				continue
			}
			fmt.Fprintf(stderr, "  %s: %s\n", f.What, f.Detail)
			if f.Fix != "" {
				fmt.Fprintf(stderr, "  %s\n", f.Fix)
			}
		}
		logRefusedStartup(logger, cfg.listen, "preflight refused startup")
		return exitcode.CONFIG
	}
	for _, f := range preflight {
		logger.Warn("server.preflight", "what", f.What, "detail", f.Detail, "fix", f.Fix)
	}

	ln, err := listen(ctx, cfg.listen, cfg.socketMode)
	if err != nil {
		fmt.Fprintf(stderr, "messq: %v\n", err)
		return exitError
	}
	defer func() {
		if closeErr := ln.Close(); closeErr != nil {
			fmt.Fprintf(stderr, "messq: close listener: %v\n", closeErr)
		}
	}()

	// The policy table's warning banner(s). warnLoop emits once immediately and —
	// for unauthenticated loopback only — once per authBannerWindow on the Clock
	// seam until shutdown. Unix sockets never warn: their permissions ARE the ACL.
	if adm.warnBanner != "" {
		go warnLoop(ctx, logger, clk, class, adm.warnBanner, authBannerWindow, make(chan struct{}, 1))
	}

	// Startup narration (§8). db_bytes and streams are best-effort here.
	dbBytes, _, sizeErr := st.Sizes()
	if sizeErr != nil {
		dbBytes = 0
	}
	streamCount := 0
	if streams, listErr := st.ListStreams(ctx); listErr == nil {
		streamCount = len(streams)
	}
	logger.Info("server.start",
		"version", buildinfo.Get().Version,
		"durability", st.Durability().String(),
		"synchronous", st.Durability().Synchronous(),
		"data_dir", cfg.dataDir,
		"db_bytes", dbBytes,
		"streams", streamCount,
		"listen", cfg.listen,
		"max_msg_size_ceiling", cfg.maxMsgSizeCeiling,
		"max_header_bytes", cfg.maxHeaderBytes,
		"max_batch_messages", cfg.maxBatchMessages,
		"max_batch_bytes", cfg.maxBatchBytes,
		"peek_scan_limit", cfg.peekScanLimit,
		"peek_max_limit", cfg.peekMaxLimit,
		"dedup_sweep_interval", cfg.dedupSweepInterval,
		"drain_timeout", cfg.drainTimeout,
		"max_waiters", cfg.maxWaiters,
		"max_waiters_per_consumer", cfg.maxWaitersPerConsumer,
		"max_fetch_wait", cfg.maxFetchWait.String(),
		"fetch_empty_damper", cfg.fetchEmptyDamper.String(),
		"max_request_bytes", cfg.maxRequestBytes,
		"max_conns", cfg.maxConns,
		"writer_submit_timeout", cfg.writerSubmitTimeout.String(),
		// #27 housekeeping + disk safety
		"janitor_interval", cfg.janitorInterval.String(),
		"event_retention", cfg.eventRetention.String(),
		"event_max_rows", cfg.eventMaxRows,
		"wal_max_bytes", cfg.walMaxBytes,
		"min_free_bytes", cfg.minFreeBytes,
		"disk_reserve_bytes", cfg.diskReserveBytes,
		"vacuum_freelist_pages", cfg.vacuumFreelistPages,
		"vacuum_pages_per_tick", cfg.vacuumPagesPerTick,
		"lag_report_threshold", cfg.lagReportThreshold,
		"lag_report_interval", cfg.lagReportInterval.String(),
	)

	srv := api.New(api.Config{
		Store:                 st,
		Clock:                 clk,
		Logger:                logger,
		Dev:                   cfg.dev,
		SweepEvery:            cfg.dedupSweepInterval,
		Limits:                opt.Limits,
		MaxBatchBytes:         cfg.maxBatchBytes,
		MaxWaiters:            cfg.maxWaiters,
		MaxWaitersPerConsumer: cfg.maxWaitersPerConsumer,
		MaxFetchWait:          cfg.maxFetchWait,
		FetchEmptyDamper:      cfg.fetchEmptyDamper,
		MaxRequestBytes:       cfg.maxRequestBytes,
		ReadHeaderTimeout:     cfg.readHeaderTimeout,
		IdleTimeout:           cfg.idleTimeout,
		MaxRequestHeaderBytes: int(cfg.maxRequestHeaderBytes),
		WriterSubmitTimeout:   cfg.writerSubmitTimeout,
		MaxConns:              cfg.maxConns,
	})

	// Attach the group-commit engine with the registry as its committed-event sink, so
	// a publish wakes the parked long polls whose filter snapshot matches (issue #14
	// §7 wake source A). Without the engine the store still serves via runSolo, but no
	// fan-out exists — the engine IS the production path.
	wr, err := st.NewWriter(store.Config{
		Durability: cfg.durability,
		// QueueDepth <= 0 means the engine's 2048 default (A1's --cmd-queue row);
		// the flag itself stays unwired until the cobra tree (#23) owns it.
	}, store.WithEventSink(srv.WaiterRegistry()))
	if err != nil {
		fmt.Fprintf(stderr, "messq: attach writer engine: %v\n", err)
		return exitError
	}
	defer func() {
		if closeErr := wr.Close(context.Background()); closeErr != nil {
			fmt.Fprintf(stderr, "messq: close writer: %v\n", closeErr)
		}
	}()

	// The expiry sweeper wakes parked fetches when a redelivery becomes due (#11 §7
	// wake source B): the registry is its store.Waker.
	sweeper := store.NewSweeper(st, store.SweepConfig{}, srv.WaiterRegistry(), logger)
	sweepDone := make(chan struct{})
	go func() {
		defer close(sweepDone)
		if sweepErr := sweeper.Run(ctx); sweepErr != nil {
			logger.Warn("sweeper exited", "err", sweepErr)
		}
	}()
	defer func() {
		select {
		case <-sweepDone:
		case <-clk.NewTimer(2 * time.Second).C():
			logger.Warn("sweeper did not stop within 2s")
		}
	}()

	// The janitor (#27): ONE housekeeping goroutine running bounded jobs in canonical
	// order — reaper finishes authorised deletions first, then retention starts new
	// ones, then dedup/events trims, then checkpoint/vacuum as solo commands between
	// commit windows, then stats. Stop cancels and never awaits; a cancelled ctx cuts
	// the sweep between transactions, which is always valid because every batch is
	// its own transaction. --janitor-interval 0 disarms the job list entirely while
	// the disk monitor below keeps sampling (its component is deliberately separate).
	walBytes := func() (int64, error) {
		_, walSz, szErr := st.Sizes()
		return walSz, szErr
	}
	freelist := func() (int64, error) {
		var pages int64
		if qErr := st.RO().QueryRowContext(ctx,
			`PRAGMA freelist_count`).Scan(&pages); qErr != nil {
			return 0, qErr
		}
		return pages, nil
	}
	lagSampler := func() ([]janitor.LagSample, error) { return janitor.SampleLag(ctx, st.RO()) }

	janitorJobs := []janitor.Job{
		janitor.NewReaperJob(st),
		janitor.NewRetentionJob(st, defaultRetentionBatch),
		janitor.NewDedupJobForStore(st, janitor.DedupCursor{Start: 0, Limit: defaultDedupLimit}),
		janitor.NewEventsJob(st, janitor.TrimPolicy{
			MaxAgeMs: cfg.eventRetention.Milliseconds(),
			MaxRows:  cfg.eventMaxRows,
		}),
		&janitor.CheckpointJob{
			W:           wr,
			WalMaxBytes: cfg.walMaxBytes,
			WalBytes:    walBytes,
		},
		&janitor.VacuumJob{
			W:             wr,
			Pages:         int(cfg.vacuumPagesPerTick),
			FreelistPages: cfg.vacuumFreelistPages,
			Freelist:      freelist,
			FreelistAfter: freelist,
			Log:           logger,
		},
		janitor.NewStatsJob(lagSampler, janitor.StatsConfig{
			Threshold: cfg.lagReportThreshold,
			Interval:  cfg.lagReportInterval,
			Log:       logger,
		}, clk),
	}

	diskMonitor := janitor.NewDiskMonitor(janitor.DiskMonitorConfig{
		Path:     cfg.dataDir,
		Policy:   queue.DiskPolicy{MinFree: cfg.minFreeBytes, Recover: defaultDiskRecover, Reserve: cfg.diskReserveBytes},
		Probe:    janitor.StatfsProbe{},
		Interval: time.Minute,
		OnState:  nil, // /healthz degraded[] home is #15/Q4's unresolved ruling; logged below regardless
		Log:      logger,
	}, clk)
	if err := diskMonitor.Start(context.Background()); err != nil {
		fmt.Fprintf(stderr, "messq: start disk monitor: %v\n", err)
		return exitError
	}
	defer func() {
		if stopErr := diskMonitor.Stop(context.Background()); stopErr != nil {
			logger.Warn("disk monitor stop", "err", stopErr)
		}
	}()

	if cfg.janitorInterval > 0 {
		jan, janErr := janitor.New(janitor.Config{
			Interval: cfg.janitorInterval,
			Budget:   time.Second / 4, // A1's 250ms tick budget, split fairly per due job
			Clock:    clk,
			Logger:   logger,
			Jitter:   nil, // ±10% jitter lands with slice 10's bench round; deterministic for now
		}, janitorJobs)
		if janErr != nil {
			fmt.Fprintf(stderr, "messq: build janitor: %v\n", janErr)
			return exitError
		}
		if err := jan.Start(ctx); err != nil {
			fmt.Fprintf(stderr, "messq: start janitor: %v\n", err)
			return exitError
		}
		defer func() {
			if stopErr := jan.Stop(context.Background()); stopErr != nil {
				logger.Warn("janitor stop", "err", stopErr)
			}
		}()
	} else {
		logger.Warn("janitor.disabled",
			"hint", "--janitor-interval 0 turns off ALL housekeeping jobs; dev/test use only")
	}

	if err := srv.Serve(ctx, ln); err != nil {
		fmt.Fprintf(stderr, "messq: serve: %v\n", err)
		return exitError
	}
	return exitOK
}
