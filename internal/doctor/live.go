// SPDX-License-Identifier: Apache-2.0

package doctor

import (
	"context"
	"time"

	"github.com/a-holm/messq/pkg/client"
)

// liveCollectBudget bounds every request the collector makes: doctor runs
// during incidents and must never hang behind a wedged daemon.
const liveCollectBudget = 2 * time.Second

// LiveCollector collects from a running daemon over pkg/client. A daemon that
// does not answer is NOT a collection failure — it is snapshot state
// (Unreachable) that server.unreachable turns into its fail finding, because
// doctor's whole job is to run when things are broken (§10).
type LiveCollector struct {
	Addr string
}

// Collect fills the snapshot in three steps, each degrading independently:
// Info first (identity + durability facts), then streams, then consumers.
func (l LiveCollector) Collect(ctx context.Context) (*Snapshot, error) {
	snap := &Snapshot{Source: SourceLive}

	cl, err := client.New(l.Addr)
	if err != nil {
		snap.Unreachable = err.Error()
		return snap, nil
	}

	cctx, cancel := context.WithTimeout(ctx, liveCollectBudget)
	defer cancel()

	info, infoErr := cl.Info(cctx)
	if infoErr != nil {
		snap.Unreachable = infoErr.Error()
		return snap, nil
	}
	snap.Server = &ServerFacts{
		Version:        info.Version,
		NodeID:         info.NodeID,
		DurabilityMode: info.Durability,
		Synchronous:    info.Synchronous,
		UptimeMS:       info.UptimeMS,
	}

	streams, sErr := cl.ListStreams(ctx)
	if sErr != nil {
		snap.CollectErrors = append(snap.CollectErrors, "streams: "+sErr.Error())
	}
	for _, sv := range streams {
		snap.Streams = append(snap.Streams, StreamState{
			Name:        sv.Name,
			Msgs:        sv.Msgs,
			Bytes:       sv.Bytes,
			MaxAgeMS:    sv.MaxAgeMS,
			MaxBytes:    sv.MaxBytes,
			CreatedAtMS: sv.CreatedAt,
		})
	}

	// pkg/client exposes consumers per stream (no daemon-wide listing), so
	// the collector walks the streams it saw. Everything rides inside the
	// same budget context: on expiry later streams simply stay missing and
	// their checks degrade honestly.
	for _, st := range snap.Streams {
		consumers, cErr := cl.ListConsumers(ctx, st.Name)
		if cErr != nil {
			snap.CollectErrors = append(snap.CollectErrors,
				"consumers["+st.Name+"]: "+cErr.Error())
			continue
		}
		for _, cv := range consumers {
			snap.Consumers = append(snap.Consumers, ConsumerState{
				Stream:      cv.Stream,
				Name:        cv.Name,
				AckWaitMS:   cv.AckWaitMS,
				MaxDeliver:  cv.MaxDeliver,
				DeadPolicy:  cv.DeadPolicy,
				Paused:      cv.Paused,
				CreatedAtMS: cv.CreatedAt,
			})
		}
	}
	return snap, nil
}
