package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/a-holm/messq/internal/errs"
)

// TestWriterMixedLoadUnderRace drives a mixed accepted/rejected workload through the engine
// from many goroutines while a randomised stall (the before_apply fault point toggling) makes
// batches form under contention, then verifies the ledger against disk and checks that Close
// retires every goroutine cleanly. Run under -race by make test; this test is its subject.
func TestWriterMixedLoadUnderRace(t *testing.T) {
	if testing.Short() {
		t.Skip("mixed-load stress skipped in -short")
	}
	ctx := context.Background()
	handler := &logCapture{}
	st, _, err := Open(ctx, testOptions(testDataDir(t), fakeClock(), handler))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if closeErr := st.Close(ctx); closeErr != nil {
			t.Errorf("close store: %v", closeErr)
		}
	}()

	// A stall that opens and closes: some batches assemble slowly, others instantly.
	var stallMu sync.Mutex
	stalled := false
	hks := hooks{beforeApply: func() {
		stallMu.Lock()
		s := stalled
		stallMu.Unlock()
		if s {
			for i := 0; i < 50; i++ {
				// Bounded spin: yields the goroutine without time.Sleep (banned).
				for j := 0; j < 1000; j++ {
				}
			}
		}
	}}
	w, err := st.NewWriter(Config{}, withLogger(handler.asLogger()), withHooks(hks))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	const workers = 64
	const opsPerWorker = 25

	mustExist := sync.Map{} // key -> val
	mustAbsent := sync.Map{}

	var wg sync.WaitGroup
	for g := 0; g < workers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				key := int64(g*opsPerWorker + i)
				reject := key%7 == 3 // deterministic spread of rejections
				cmd := &probeCmd{key: key, val: fmt.Sprintf("w%d", g)}
				if reject {
					cmd.bizErr = errs.ErrTooLarge
					mustAbsent.Store(key, true)
				} else {
					mustExist.Store(key, cmd.val)
				}
				res, doErr := w.Do(ctx, cmd)
				switch {
				case !reject && doErr != nil:
					t.Errorf("accepted command key=%d failed: %v", key, doErr)
					return
				case reject && !errors.Is(doErr, errs.ErrTooLarge):
					t.Errorf("rejected command key=%d got %v", key, doErr)
					return
				}
				resStr, resOK := res.(string)
				if !reject && (!resOK || resStr != cmd.val) {
					t.Errorf("key=%d result %v, want %q", key, res, cmd.val)
				}
				if i%5 == 0 { // toggle the stall periodically
					stallMu.Lock()
					stalled = !stalled
					stallMu.Unlock()
				}
			}
		}(g)
	}
	wg.Wait()

	if err := w.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}

	rows := readProbe(t, st.RO())
	seen := map[int64]string{}
	for _, r := range rows {
		seen[r.K] = r.V
	}
	missedOK, leakedRejected := 0, 0
	mustExist.Range(func(k, v any) bool {
		key, keyOK := k.(int64)
		val, valOK := v.(string)
		if !keyOK || !valOK || seen[key] != val {
			missedOK++
		}
		return true
	})
	mustAbsent.Range(func(k, _ any) bool {
		key, keyOK := k.(int64)
		if !keyOK {
			leakedRejected++
			return true
		}
		if _, ok := seen[key]; ok {
			leakedRejected++
		}
		return true
	})
	if missedOK != 0 || leakedRejected != 0 {
		t.Fatalf("ledger mismatch: %d accepted rows wrong/missing, %d rejected rows leaked",
			missedOK, leakedRejected)
	}
}
