-- SPDX-License-Identifier: Apache-2.0
-- Migration 0003: deliveries_expiry partial index (issue #11 §2). The existing
-- deliveries_ready(stream, consumer, state, visible_at, seq) index is keyed
-- stream-first, so "which lease expires next, anywhere" — the question the sweeper
-- asks four times a second — cannot use it. This partial index over INFLIGHT rows
-- (state = 1) turns "what expires next, globally" into an O(log n) b-tree descent and
-- gives the expiry scan a total order (visible_at, stream, consumer, seq) for free, so
-- the sweeper's LIMIT cannot flip a coin at ties. It is tiny: it indexes only INFLIGHT
-- rows, whose count is bounded by the sum of max_ack_pending across consumers (I5, #9).
CREATE INDEX deliveries_expiry
    ON deliveries(visible_at, stream, consumer, seq)
 WHERE state = 1;