// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"fmt"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/queue"
)

// Batch publish (issue §7): one command, one transaction, all-or-nothing. A
// validation failure at line k stores nothing and names k; a successful batch of n
// entries receives n results in input order with contiguous sequences for every
// newly stored message. Duplicates inside the batch reuse the first occurrence's
// receipt — the pre-check sees rows written earlier in the same transaction.

// BatchCmd carries the decoded NDJSON entries of messages:batch.
type BatchCmd struct {
	Stream string
	Reqs   []queue.PublishReq
}

// Bytes estimates this command's WAL footprint for #6's commit-max-bytes budget:
// the sum of its entries' estimates.
func (c BatchCmd) Bytes() int {
	total := 0
	for _, r := range c.Reqs {
		total += PublishCmd{Req: r}.Bytes()
	}
	return total
}

// BatchAck reports one receipt per entry, in input order.
type BatchAck struct {
	Stream  string `json:"stream"`
	Results []Ack  `json:"results"`
}

// PublishBatch stores every entry or none of them. The authoritative stream config
// is read once and memoised for the whole batch (§2); the batch's own earlier
// inserts are visible to later dedup pre-checks on the same connection.
func (s *Store) PublishBatch(ctx context.Context, c BatchCmd) (BatchAck, error) {
	if err := queue.ValidateExistingStreamName(c.Stream); err != nil {
		return BatchAck{}, err
	}
	if len(c.Reqs) == 0 {
		return BatchAck{}, errs.E(errs.ErrBadRequest, "store.PublishBatch",
			"batch holds no entries")
	}
	if len(c.Reqs) > s.maxBatchMsgs {
		return BatchAck{}, errs.E(errs.ErrBadRequest, "store.PublishBatch",
			"batch holds %d entries, at most %d are allowed", len(c.Reqs), s.maxBatchMsgs)
	}

	res, err := s.enqueue(ctx, "store.PublishBatch", batchPublishCmd{
		cmd: c, limits: s.limits, newID: s.newID,
	})
	if err != nil {
		return BatchAck{}, err
	}
	ack, ok := res.(BatchAck)
	if !ok {
		return BatchAck{},
			fmt.Errorf("store.PublishBatch: engine returned %T, want BatchAck", res)
	}
	ack.Stream = c.Stream
	return ack, nil
}
