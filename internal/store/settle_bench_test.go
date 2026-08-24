// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"strconv"
	"testing"

	"github.com/a-holm/messq/internal/queue"
)

// BenchmarkSettle measures the batch settle cost through the writer. A fresh batch of
// `batch` ack tokens is closed per iteration; with an engine-less store the dominant
// cost is the single group commit, not per-token work (issue #10 §11).
func BenchmarkSettle(b *testing.B) {
	for _, batch := range []int{1, 16, 256} {
		b.Run("ack/batch="+strconv.Itoa(batch), func(b *testing.B) {
			st, _, _ := openSettleStore(b)
			toks := seedSettle(b, st, batch, batch)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				items := make([]SettleItem, len(toks))
				for j := range toks {
					items[j] = SettleItem{Token: toks[j], Verb: queue.VerbAck}
				}
				if _, err := st.Settle(context.Background(), settleCmd(items...)); err != nil {
					b.Fatalf("settle: %v", err)
				}
			}
		})
	}
}
