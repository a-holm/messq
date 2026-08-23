// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"testing"

	"github.com/a-holm/messq/internal/queue"
)

// checkHas reports whether the violations contain an ID (optionally for a consumer).
func checkHas(vs []Violation, id string) bool {
	for _, v := range vs {
		if v.ID == id {
			return true
		}
	}
	return false
}

func TestCheckConsumerInvariantsClean(t *testing.T) {
	st, _ := newFetchStore(t)
	publishSubjs(t, st, "orders.1", "orders.2", "orders.3")
	cfg := queue.DefaultConsumerConfig("worker")
	cfg.Filters = []string{">"}
	newFetchConsumer(t, st, "worker", cfg)
	if _, err := st.Fetch(context.Background(), FetchReq{Stream: "orders", Consumer: "worker", Batch: 2}); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	vs, err := st.CheckConsumerInvariants(context.Background())
	if err != nil {
		t.Fatalf("CheckConsumerInvariants: %v", err)
	}
	if len(vs) != 0 {
		t.Fatalf("clean state reported %d violations: %+v", len(vs), vs)
	}
}

func TestCheckConsumerInvariantsPlantedViolations(t *testing.T) {
	tests := []struct {
		name  string
		plant func(t *testing.T, st *Store)
		want  string
	}{
		{
			"C1 cursor below delivery",
			func(t *testing.T, st *Store) {
				plantDelivery(t, st, 5, 0, 0) // cursor stays 1, so seq 5 >= cursor
			},
			"C1",
		},
		{
			"C2 orphan delivery",
			func(t *testing.T, st *Store) {
				if _, err := st.rw.ExecContext(context.Background(),
					`INSERT INTO deliveries (stream, consumer, seq, subject, state, attempts, visible_at, generation)
					 VALUES ('orders','worker',99,'orders.ghost',0,0,0,1)`); err != nil {
					t.Fatalf("plant C2 orphan: %v", err)
				}
			},
			"C2",
		},
		{
			"C2 subject mismatch",
			func(t *testing.T, st *Store) {
				// seed a real message at seq 1 then lie about its subject in the delivery
				publishSubjs(t, st, "orders.real")
				if _, err := st.rw.ExecContext(context.Background(),
					`INSERT INTO deliveries (stream, consumer, seq, subject, state, attempts, visible_at, generation)
					 VALUES ('orders','worker',1,'orders.wrong',0,0,0,1)`); err != nil {
					t.Fatalf("plant C2 mismatch: %v", err)
				}
			},
			"C2",
		},
		{
			"C3 generation mismatch",
			func(t *testing.T, st *Store) {
				if _, err := st.rw.ExecContext(context.Background(),
					`INSERT INTO deliveries (stream, consumer, seq, subject, state, attempts, visible_at, generation)
					 VALUES ('orders','worker',7,'orders.1',0,0,0,2)`); err != nil {
					t.Fatalf("plant C3: %v", err)
				}
			},
			"C3",
		},
		{
			"C4 cursor out of range",
			func(t *testing.T, st *Store) {
				if _, err := st.rw.ExecContext(context.Background(),
					`UPDATE consumers SET cursor_seq = 0 WHERE stream='orders' AND name='worker'`); err != nil {
					t.Fatalf("plant C4: %v", err)
				}
			},
			"C4",
		},
		{
			"C6 bad state",
			func(t *testing.T, st *Store) {
				if _, err := st.rw.ExecContext(context.Background(),
					`INSERT INTO deliveries (stream, consumer, seq, subject, state, attempts, visible_at, generation, delivered_at)
					 VALUES ('orders','worker',8,'orders.1',2,0,0,1,NULL)`); err != nil {
					t.Fatalf("plant C6: %v", err)
				}
			},
			"C6",
		},
		{
			"C6 inflight without delivered_at",
			func(t *testing.T, st *Store) {
				if _, err := st.rw.ExecContext(context.Background(),
					`INSERT INTO deliveries (stream, consumer, seq, subject, state, attempts, visible_at, generation, delivered_at)
					 VALUES ('orders','worker',9,'orders.1',1,1,0,1,NULL)`); err != nil {
					t.Fatalf("plant C6 inflight: %v", err)
				}
			},
			"C6",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := newConsumerStream(t)
			if _, err := st.CreateConsumer(context.Background(), "orders", queue.DefaultConsumerConfig("worker"), queue.StartPosition{Kind: queue.StartNew}, "test"); err != nil {
				t.Fatalf("create consumer: %v", err)
			}
			tt.plant(t, st)
			vs, err := st.CheckConsumerInvariants(context.Background())
			if err != nil {
				t.Fatalf("CheckConsumerInvariants: %v", err)
			}
			if !checkHas(vs, tt.want) {
				t.Fatalf("violations = %+v, want a %s finding", vs, tt.want)
			}
		})
	}
}

func TestCheckConsumerInvariantsShrinkIsAdvisory(t *testing.T) {
	st := newConsumerStream(t)
	ctx := context.Background()
	if _, err := st.CreateConsumer(ctx, "orders", queue.DefaultConsumerConfig("worker"), queue.StartPosition{Kind: queue.StartNew}, "test"); err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := int64(1); i <= 3; i++ {
		plantDelivery(t, st, i, 1, 1) // all INFLIGHT
	}
	// Lower the bound: shrink can drop nothing (no READY rows), leaving advisory residue.
	bound := int64(1)
	if _, err := st.UpdateConsumer(ctx, "orders", "worker", ConsumerPatch{MaxAckPending: &bound}, "test"); err != nil {
		t.Fatalf("update: %v", err)
	}
	vs, err := st.CheckConsumerInvariants(ctx)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	found := false
	for _, v := range vs {
		if v.ID == "C5" && !v.Advisory {
			t.Fatalf("C5 finding %+v should be advisory (shrink residue), not a violation", v)
		}
		if v.ID == "C5" {
			found = true
		}
	}
	if !found {
		t.Fatalf("violations = %+v, want an advisory C5", vs)
	}
}

func TestCheckConsumerInvariantsAdmissionViolation(t *testing.T) {
	st := newConsumerStream(t)
	ctx := context.Background()
	cfg := queue.DefaultConsumerConfig("worker")
	cfg.MaxAckPending = 1
	if _, err := st.CreateConsumer(ctx, "orders", cfg, queue.StartPosition{Kind: queue.StartNew}, "test"); err != nil {
		t.Fatalf("create: %v", err)
	}
	// Two READY rows above the bound of 1 = an admission bug, not shrink residue.
	plantDelivery(t, st, 1, 0, 0)
	plantDelivery(t, st, 2, 0, 0)
	vs, err := st.CheckConsumerInvariants(ctx)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	found := false
	for _, v := range vs {
		if v.ID == "C5" && v.Advisory {
			t.Fatalf("C5 finding %+v should be a violation (READY rows above bound), not advisory", v)
		}
		if v.ID == "C5" {
			found = true
		}
	}
	if !found {
		t.Fatalf("violations = %+v, want a C5 violation", vs)
	}
}
