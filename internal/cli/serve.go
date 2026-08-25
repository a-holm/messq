// SPDX-License-Identifier: Apache-2.0

package cli

import (
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
	"github.com/a-holm/messq/internal/buildinfo"
	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
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
		if !hasEq {
			if len(args) == 0 {
				return serveConfig{}, fmt.Errorf("%s needs a value", name)
			}
			val = args[0]
			args = args[1:]
		}
		flags[name] = val
	}

	// resolve resolves flag → env → default for one setting.
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

	if cfg.dataDir = resolve("--data-dir", "MESSQ_DATA_DIR", ""); cfg.dataDir == "" {
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

// listen creates the daemon listener from a --listen address. Two schemes exist:
// unix://PATH (a Unix socket, chmod 0660) and tcp://HOST:PORT (loopback only — a
// non-loopback bind is refused as a fatal startup error until #16 lands authentication).
func listen(ctx context.Context, addr string) (net.Listener, error) {
	switch {
	case strings.HasPrefix(addr, "unix://"):
		return listenUnix(ctx, strings.TrimPrefix(addr, "unix://"))
	case strings.HasPrefix(addr, "tcp://"):
		hostport := strings.TrimPrefix(addr, "tcp://")
		if err := refuseNonLoopback(ctx, hostport); err != nil {
			return nil, err
		}
		ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", hostport)
		if err != nil {
			return nil, fmt.Errorf("listen on %s: %w", hostport, err)
		}
		return ln, nil
	default:
		return nil, fmt.Errorf("unsupported --listen %q: want unix://PATH or tcp://HOST:PORT", addr)
	}
}

// listenUnix binds a Unix socket at path and fixes its mode to 0660. A crashed previous
// run leaves a stale file where the socket path used to be; that path is removed only
// when it is genuinely stale (nothing answers a connect), never a live daemon's socket.
func listenUnix(ctx context.Context, path string) (net.Listener, error) {
	ln, err := (&net.ListenConfig{}).Listen(ctx, "unix", path)
	if err == nil {
		if chErr := chmodSocket(path); chErr != nil {
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
	if chErr := chmodSocket(path); chErr != nil {
		return nil, errors.Join(fmt.Errorf("chmod socket %s: %w", path, chErr), ln.Close())
	}
	return ln, nil
}

// chmodSocket fixes a freshly bound Unix socket to 0660: group-writable so the messq
// group can reach the daemon (ADR-0013/0003). 0660 is the documented socket mode, not
// an accidental widening.
func chmodSocket(path string) error {
	return os.Chmod(path, 0o660) //nolint:gosec // 0660 is the documented socket mode (ADR-0013/0003)
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
// resolved address that is not a loopback address.
func refuseNonLoopback(ctx context.Context, hostport string) error {
	host, _, err := net.SplitHostPort(hostport)
	if err != nil {
		return fmt.Errorf("invalid --listen tcp address %q: %w", hostport, err)
	}
	if host == "" {
		return fmt.Errorf("refusing to bind %q: non-loopback bind needs authentication (#16); use 127.0.0.1 or ::1", hostport)
	}
	if ip := net.ParseIP(host); ip != nil {
		if !ip.IsLoopback() {
			return fmt.Errorf("refusing to bind %q: non-loopback bind needs authentication (#16); use 127.0.0.1 or ::1", hostport)
		}
		return nil
	}
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return fmt.Errorf("resolve %q: %w", host, err)
	}
	if len(addrs) == 0 {
		return fmt.Errorf("refusing to bind %q: resolves to no addresses", hostport)
	}
	for _, addr := range addrs {
		if !addr.IP.IsLoopback() {
			return fmt.Errorf("refusing to bind %q: non-loopback bind needs authentication (#16); use 127.0.0.1 or ::1", hostport)
		}
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

	logger := slog.New(slog.NewTextHandler(stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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

	ln, err := listen(ctx, cfg.listen)
	if err != nil {
		fmt.Fprintf(stderr, "messq: %v\n", err)
		return exitError
	}
	defer func() {
		if closeErr := ln.Close(); closeErr != nil {
			fmt.Fprintf(stderr, "messq: close listener: %v\n", closeErr)
		}
	}()

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
	)

	srv := api.New(api.Config{
		Store:                 st,
		Clock:                 clk,
		Logger:                logger,
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

	if err := srv.Serve(ctx, ln); err != nil {
		fmt.Fprintf(stderr, "messq: serve: %v\n", err)
		return exitError
	}
	return exitOK
}
