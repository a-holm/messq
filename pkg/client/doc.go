// SPDX-License-Identifier: Apache-2.0

// Package client is the public Go client for a messq daemon (github.com/a-holm/messq/pkg/client):
// a thin request/response layer over the HTTP surface plus a Worker helper that owns
// lease renewal, so callers never hand-roll heartbeats — the #1 at-least-once footgun,
// solved in the library (PLAN.md D14). The messq CLI itself consumes this package, which
// keeps it real: every CLI exercise is an exercise of this code.
//
// # Delivery is at-least-once
//
// A message claimed by Fetch stays invisible for its ack_wait while you work on it.
// Settle it with Ack when the work is durable; do nothing (or Nak) and it redelivers
// after ack_wait. If your process dies mid-handler, the message comes back: handlers
// must be idempotent. The key PLAN.md §6 recommends for deduplication is stream+sequence,
// stable across every redelivery AND across a dead-letter redrive — [Delivered.DedupKey]
// hands it to you so the right answer is the easy answer:
//
//	seen := map[string]bool{} // your own transactional store in real code
//	err := worker.Run(ctx, func(ctx context.Context, m *client.Delivered) error {
//	    if seen[m.DedupKey()] {
//	        return nil // already done; the ack still lands
//	    }
//	    if err := apply(ctx, m); err != nil {
//	        return client.Permanent(fmt.Errorf("unsupported payload: %w", err))
//	    }
//	    seen[m.DedupKey()] = true
//	    return nil
//	})
//
// The Worker extends leases at half the ack wait ([WorkerConfig.ExtendAt]) so a long
// handler is never raced by the broker's sweeper, naks what it holds when draining, and
// never settles a token whose lease it has reported lost — a duplicate handler run shows
// up as a named redelivery, not as a stale ack.
//
// # Lenient decode, strict encode
//
// This package ignores unknown fields in responses and preserves unknown error codes
// verbatim: a minor daemon release may add fields or codes without breaking deployed
// workers. Requests are the opposite — the server rejects what it does not know, loudly.
//
// # Compatibility promise
//
// Everything exported here under github.com/a-holm/messq/pkg/client follows the Go 1
// compatibility promise from its first tagged release: no exported name changes meaning,
// none disappears. Additions are minor versions; breaking the promise requires a new
// module path.
package client
