// SPDX-License-Identifier: Apache-2.0

package scriptenv

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/a-holm/messq/internal/api"
	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
	"github.com/a-holm/messq/pkg/client"
)

// Daemon is the inproc lane's daemon: the same construction pipeline `messq
// serve` runs (store.Open → api.New → writer engine with the waiter registry as
// its event sink → expiry sweeper), built inside the test process on a clock.Fake
// and listening on a unix socket under $WORK.
//
// Scripts drive it through the REAL CLI: `messq <args…>` re-execs the test binary
// (testscript.RunMain) and talks HTTP over the socket, so every script exercises
// the same client/transport/API path production uses. The fake clock belongs to
// the test process; `clock advance` moves it between script lines.
type Daemon struct {
	sock    string
	dataDir string
	clk     *clock.Fake
	st      *store.Store
	wr      *store.Writer
	srv     *api.Server
	cancel  context.CancelFunc
	serveCh chan error
}

// daemonReadyBudget bounds the /readyz poll after start. The daemon is local and
// needs no recovery of note; if it cannot become ready in this budget something is
// genuinely broken and the script must fail rather than hang.
const daemonReadyBudget = 10 * time.Second

// StartDaemon builds and starts the inproc daemon under workDir. It returns only
// once /readyz answers — the same readiness rule `messq serve`'s systemd unit
// relies on — so a script line that follows `daemon start` never races startup.
func StartDaemon(workDir string, clk *clock.Fake) (*Daemon, error) {
	sock := filepath.Join(workDir, "messq.sock")
	if err := removeStaleSocket(sock); err != nil {
		return nil, err
	}
	dataDir := filepath.Join(workDir, "data")
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, fmt.Errorf("scriptenv: create data dir: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	d := &Daemon{
		sock:    sock,
		dataDir: dataDir,
		clk:     clk,
		cancel:  cancel,
		serveCh: make(chan error, 1),
	}
	zero := func(err error) (*Daemon, error) {
		cancel()
		return nil, err
	}

	st, _, err := store.Open(ctx, store.Options{
		DataDir:    dataDir,
		Durability: store.DurabilityFull, // the tour's honest default (issue §1)
		Limits:     queue.DefaultLimits(),
		Clock:      clk,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		return zero(fmt.Errorf("scriptenv: open store: %w", err))
	}
	d.st = st

	srv := api.New(api.Config{Store: st, Clock: clk})
	d.srv = srv

	ln, err := (&net.ListenConfig{}).Listen(ctx, "unix", sock)
	if err != nil {
		return zero(fmt.Errorf("scriptenv: listen on %s: %w", sock, err))
	}

	// The group-commit engine with the waiter registry as its committed-event
	// sink, exactly as runServe attaches it: without it publishes still serve via
	// runSolo but no long-poll fan-out exists.
	wr, err := st.NewWriter(store.Config{Durability: store.DurabilityFull},
		store.WithEventSink(srv.WaiterRegistry()))
	if err != nil {
		return zero(fmt.Errorf("scriptenv: attach writer engine: %w", err))
	}
	d.wr = wr

	// The expiry sweeper wakes parked fetches; the fake clock drives its ticks.
	sweeper := store.NewSweeper(st, store.SweepConfig{}, srv.WaiterRegistry(),
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	go func() {
		if sweepErr := sweeper.Run(ctx); sweepErr != nil && ctx.Err() == nil {
			// A dead sweeper is a script-visible fault; surfacing it through the
			// serve channel fails the next command deterministically.
			d.serveCh <- sweepErr
		}
	}()

	go func() { d.serveCh <- srv.Serve(ctx, ln) }()

	if err := d.waitReady(ctx); err != nil {
		return zero(err)
	}
	return d, nil
}

// waitReady polls /healthz through the real client until the daemon answers.
// store.Open finished recovery before Open returned, so a 200 here is honest from
// the first poll — the same readiness rule `messq serve`'s systemd unit relies on.
// The poll sleeps through the clock seam, never time.Sleep.
func (d *Daemon) waitReady(ctx context.Context) error {
	deadline := (clock.System{}).Now().Add(daemonReadyBudget)
	cl, err := client.New(d.Addr())
	if err != nil {
		return fmt.Errorf("scriptenv: client for readiness poll: %w", err)
	}
	for {
		hErr := cl.Healthz(context.Background())
		if hErr == nil {
			return nil
		}
		if !(clock.System{}).Now().Before(deadline) {
			return fmt.Errorf("scriptenv: daemon not ready within %s: %w", daemonReadyBudget, hErr)
		}
		if sleepErr := (clock.System{}).Sleep(ctx, 5*time.Millisecond); sleepErr != nil {
			return fmt.Errorf("scriptenv: readiness poll cancelled: %w", sleepErr)
		}
	}
}

// Addr returns the daemon address in --addr form.
func (d *Daemon) Addr() string { return "unix://" + d.sock }

// SocketPath returns the bare socket path.
func (d *Daemon) SocketPath() string { return d.sock }

// DataDir returns the daemon's data directory (under $WORK).
func (d *Daemon) DataDir() string { return d.dataDir }

// Clock exposes the lane's fake clock for `clock advance`/`clock set`.
func (d *Daemon) Clock() *clock.Fake { return d.clk }

// Quiesce returns once everything issued before it has finished: a healthz
// round-trip through the real client gives a happens-after edge for the sweeper
// ticks `clock advance` fired. When delivery commands (#13/#14) land, this gains
// the writer-queue drain those scripts need; the scripts that exist today need no
// more than this barrier.
func (d *Daemon) Quiesce() error {
	cl, err := client.New(d.Addr())
	if err != nil {
		return fmt.Errorf("scriptenv: client for quiesce probe: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), daemonReadyBudget)
	defer cancel()
	if err := cl.Healthz(ctx); err != nil {
		return fmt.Errorf("scriptenv: quiesce probe failed: %w", err)
	}
	return nil
}

// Stop tears the daemon down. Graceful=false is `daemon kill`: no shutdown
// grace, the socket file left behind exactly as a SIGKILLed serve would.
func (d *Daemon) Stop(graceful bool) {
	if d.cancel != nil {
		d.cancel()
	}
	if d.serveCh != nil {
		select {
		case <-d.serveCh:
		case <-(clock.System{}).NewTimer(2 * time.Second).C():
		}
	}
	ctx := context.Background()
	if graceful && d.srv != nil {
		sctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		sink(d.srv.Shutdown(sctx))
	}
	if d.wr != nil {
		sink(d.wr.Close(ctx))
	}
	if d.st != nil {
		sink(d.st.Close(ctx))
	}
	d.cancel = nil
}

// removeStaleSocket clears a leftover socket file from a previous kill. A live
// socket is never removed: a connect that succeeds proves it.
func removeStaleSocket(path string) error {
	if _, err := os.Stat(path); err != nil {
		return nil //nolint:nilerr // nothing at the path is the no-stale-socket answer
	}
	conn, err := (&net.Dialer{Timeout: 250 * time.Millisecond}).DialContext(
		context.Background(), "unix", path)
	if err == nil {
		sink(conn.Close())
		return fmt.Errorf("scriptenv: %s is a live socket, refusing to reuse", path)
	}
	return os.Remove(path)
}
