// SPDX-License-Identifier: Apache-2.0

package quickstart

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

// Daemon is the tour's ephemeral in-process daemon: the same construction
// pipeline `messq serve` runs — store.Open (durability=full, the tour's honest
// default), api.New, the group-commit writer with the waiter registry as its
// committed-event sink, and the expiry sweeper — listening on a unix socket
// inside the tour's directory. Nothing outside the directory is touched.
type Daemon struct {
	sock    string
	dataDir string
	cancel  context.CancelFunc
	serveCh chan error
	wr      *store.Writer
	st      *store.Store
}

// readyBudget bounds the readiness poll; the daemon is local and freshly
// recovered, so anything past this is a real fault the tour must name.
const readyBudget = 10 * time.Second

// StartDaemon builds and starts the tour's daemon, returning only once /healthz
// answers 200 — recovery finished inside store.Open, so the 200 is honest.
func StartDaemon(dataDir string, clk clock.Clock) (*Daemon, error) {
	sock := filepath.Join(dataDir, "messq.sock")
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, fmt.Errorf("quickstart: create data dir: %w", err)
	}
	// The tour marker is the reaper's second guard (issue §8): a directory that
	// does not carry it is NEVER considered a leftover tour.
	if err := os.WriteFile(filepath.Join(dataDir, markerName), []byte{}, 0o600); err != nil {
		return nil, fmt.Errorf("quickstart: write tour marker: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	d := &Daemon{sock: sock, dataDir: dataDir, cancel: cancel, serveCh: make(chan error, 1)}
	fail := func(err error) (*Daemon, error) {
		cancel()
		return nil, err
	}

	st, _, err := store.Open(ctx, store.Options{
		DataDir:    dataDir,
		Durability: store.DurabilityFull,
		Limits:     queue.DefaultLimits(),
		Clock:      clk,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		return fail(fmt.Errorf("quickstart: open store: %w", err))
	}
	d.st = st

	srv := api.New(api.Config{Store: st, Clock: clk, Dev: true})

	ln, err := (&net.ListenConfig{}).Listen(ctx, "unix", sock)
	if err != nil {
		return fail(fmt.Errorf("quickstart: listen on %s: %w", sock, err))
	}

	wr, err := st.NewWriter(store.Config{Durability: store.DurabilityFull},
		store.WithEventSink(srv.WaiterRegistry()))
	if err != nil {
		return fail(fmt.Errorf("quickstart: attach writer engine: %w", err))
	}
	d.wr = wr

	sweeper := store.NewSweeper(st, store.SweepConfig{}, srv.WaiterRegistry(),
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	go func() {
		if sweepErr := sweeper.Run(ctx); sweepErr != nil && ctx.Err() == nil {
			d.serveCh <- sweepErr
		}
	}()
	go func() { d.serveCh <- srv.Serve(ctx, ln) }()

	// Readiness poll through the real client, sleeping through the clock seam.
	cl, err := client.New(d.Addr())
	if err != nil {
		return fail(fmt.Errorf("quickstart: readiness client: %w", err))
	}
	deadline := (clock.System{}).Now().Add(readyBudget)
	for {
		if hErr := cl.Healthz(context.Background()); hErr == nil {
			return d, nil
		}
		if !(clock.System{}).Now().Before(deadline) {
			return fail(fmt.Errorf("quickstart: daemon not ready within %s", readyBudget))
		}
		if sleepErr := (clock.System{}).Sleep(ctx, 5*time.Millisecond); sleepErr != nil {
			return fail(fmt.Errorf("quickstart: readiness poll cancelled: %w", sleepErr))
		}
	}
}

// Addr returns the daemon address in --addr form.
func (d *Daemon) Addr() string { return "unix://" + d.sock }

// Stop tears the daemon down before the tour removes the directory.
func (d *Daemon) Stop() {
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
	sinkDaemonErr(d.wr.Close(ctx))
	sinkDaemonErr(d.st.Close(ctx))
	d.cancel = nil
}

// sinkDaemonErr consumes teardown errors: the directory is about to be removed
// either way, and a tour mid-ctrl-c has no stderr contract left to honour.
func sinkDaemonErr(err error) { _ = err }
