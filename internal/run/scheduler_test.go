package run

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wyw14/cry-161/internal/model"
)

func testLayer(number int) model.Layer {
	return model.Layer{Number: number, Material: "aluminum", TargetNanometers: 10, RateNanometers: 1}
}

func TestLayersOfSameRunTrackedIndependently(t *testing.T) {
	scheduler := NewScheduler(2)
	if err := scheduler.Enqueue("run-1", testLayer(1), 2); err != nil {
		t.Fatalf("enqueue layer 1: %v", err)
	}
	if err := scheduler.Enqueue("run-1", testLayer(2), 1); err != nil {
		t.Fatalf("enqueue layer 2: %v", err)
	}

	first, ok := scheduler.Next()
	if !ok || first.Layer.Number != 1 {
		t.Fatalf("first dispatch = %+v, ok=%v, want layer 1", first, ok)
	}
	second, ok := scheduler.Next()
	if !ok || second.Layer.Number != 2 {
		t.Fatalf("second dispatch = %+v, ok=%v, want layer 2", second, ok)
	}

	pending, running, finished := scheduler.Counts()
	if pending != 0 || running != 2 || finished != 0 {
		t.Fatalf("counts = (%d,%d,%d), want (0,2,0)", pending, running, finished)
	}

	// Finishing one layer must release only that layer's slot.
	if _, err := scheduler.Finish(first, nil); err != nil {
		t.Fatalf("finish layer 1: %v", err)
	}
	pending, running, finished = scheduler.Counts()
	if pending != 0 || running != 1 || finished != 1 {
		t.Fatalf("counts after first finish = (%d,%d,%d), want (0,1,1)", pending, running, finished)
	}
	if _, err := scheduler.Finish(second, nil); err != nil {
		t.Fatalf("finish layer 2: %v", err)
	}
	pending, running, finished = scheduler.Counts()
	if pending != 0 || running != 0 || finished != 2 {
		t.Fatalf("counts after second finish = (%d,%d,%d), want (0,0,2)", pending, running, finished)
	}

	results := scheduler.Results()
	seen := map[int]bool{}
	for _, result := range results {
		if result.RunID != "run-1" || result.Status != "completed" {
			t.Fatalf("unexpected result: %+v", result)
		}
		seen[result.Layer.Number] = true
	}
	if !seen[1] || !seen[2] {
		t.Fatalf("results lost a layer: %+v", results)
	}
}

func TestLimitCannotBeBypassedWithinOneRun(t *testing.T) {
	scheduler := NewScheduler(2)
	for _, number := range []int{1, 2, 3} {
		if err := scheduler.Enqueue("run-1", testLayer(number), 3-number); err != nil {
			t.Fatalf("enqueue layer %d: %v", number, err)
		}
	}

	if _, ok := scheduler.Next(); !ok {
		t.Fatal("first dispatch blocked")
	}
	if _, ok := scheduler.Next(); !ok {
		t.Fatal("second dispatch blocked")
	}
	if _, ok := scheduler.Next(); ok {
		t.Fatal("third dispatch bypassed the concurrency limit within one run")
	}
}

func TestFinishUnknownAndDuplicateRejected(t *testing.T) {
	scheduler := NewScheduler(1)
	if err := scheduler.Enqueue("run-1", testLayer(1), 1); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	item, ok := scheduler.Next()
	if !ok {
		t.Fatal("dispatch blocked")
	}

	// A layer that was never dispatched cannot be finished.
	foreign := ScheduledLayer{RunID: "run-1", Layer: testLayer(2), Status: "running"}
	if _, err := scheduler.Finish(foreign, nil); !errors.Is(err, ErrLayerNotRunning) {
		t.Fatalf("foreign finish err = %v, want ErrLayerNotRunning", err)
	}

	// A layer of a different run with the same number must not collide.
	other := ScheduledLayer{RunID: "run-2", Layer: testLayer(1), Status: "running"}
	if _, err := scheduler.Finish(other, nil); !errors.Is(err, ErrLayerNotRunning) {
		t.Fatalf("cross-run finish err = %v, want ErrLayerNotRunning", err)
	}

	if _, err := scheduler.Finish(item, nil); err != nil {
		t.Fatalf("first finish: %v", err)
	}
	// The same dispatch can only be finished once.
	if _, err := scheduler.Finish(item, nil); !errors.Is(err, ErrLayerNotRunning) {
		t.Fatalf("duplicate finish err = %v, want ErrLayerNotRunning", err)
	}
	if _, _, finished := scheduler.Counts(); finished != 1 {
		t.Fatalf("finished count = %d, want 1", finished)
	}
}

func TestStaleTokenAfterRequeueRejected(t *testing.T) {
	scheduler := NewScheduler(1)
	if err := scheduler.Enqueue("run-1", testLayer(1), 1); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	stale, ok := scheduler.Next()
	if !ok {
		t.Fatal("dispatch blocked")
	}
	if _, err := scheduler.Finish(stale, nil); err != nil {
		t.Fatalf("finish: %v", err)
	}
	if err := scheduler.Enqueue("run-1", testLayer(1), 1); err != nil {
		t.Fatalf("re-enqueue: %v", err)
	}
	current, ok := scheduler.Next()
	if !ok {
		t.Fatal("redispatch blocked")
	}
	if _, err := scheduler.Finish(stale, nil); err == nil {
		t.Fatal("stale dispatch token finished the new dispatch")
	}
	if _, running, _ := scheduler.Counts(); running != 1 {
		t.Fatalf("running = %d, stale finish corrupted the live slot", running)
	}
	if _, err := scheduler.Finish(current, nil); err != nil {
		t.Fatalf("finish current dispatch: %v", err)
	}
}

func TestDuplicateEnqueueRejected(t *testing.T) {
	scheduler := NewScheduler(2)
	if err := scheduler.Enqueue("run-1", testLayer(1), 1); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := scheduler.Enqueue("run-1", testLayer(1), 1); !errors.Is(err, ErrLayerAlreadyScheduled) {
		t.Fatalf("duplicate pending enqueue err = %v, want ErrLayerAlreadyScheduled", err)
	}
	item, _ := scheduler.Next()
	if err := scheduler.Enqueue("run-1", testLayer(1), 1); !errors.Is(err, ErrLayerAlreadyScheduled) {
		t.Fatalf("duplicate running enqueue err = %v, want ErrLayerAlreadyScheduled", err)
	}
	// Same layer number in a different run is a distinct task.
	if err := scheduler.Enqueue("run-2", testLayer(1), 1); err != nil {
		t.Fatalf("enqueue same layer number in another run: %v", err)
	}
	if _, err := scheduler.Finish(item, nil); err != nil {
		t.Fatalf("finish: %v", err)
	}
}

func TestConcurrentDispatchRespectsLimitAndTracksEveryLayer(t *testing.T) {
	const (
		runs      = 5
		perRun    = 10
		limit     = 3
		workers   = 8
		totalTask = runs * perRun
	)
	scheduler := NewScheduler(limit)
	for r := 0; r < runs; r++ {
		runID := model.ID(fmt.Sprintf("run-%d", r))
		for n := 1; n <= perRun; n++ {
			if err := scheduler.Enqueue(runID, testLayer(n), perRun-n); err != nil {
				t.Fatalf("enqueue: %v", err)
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var maxRunning atomic.Int64
	var failures atomic.Int64
	errCh := make(chan error, totalTask)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				item, ok := scheduler.Next()
				if !ok {
					_, running, _ := scheduler.Counts()
					if observed := maxRunning.Load(); int64(running) > observed {
						maxRunning.Store(int64(running))
					}
					time.Sleep(time.Millisecond)
					continue
				}
				_, running, _ := scheduler.Counts()
				for {
					observed := maxRunning.Load()
					if int64(running) <= observed || maxRunning.CompareAndSwap(observed, int64(running)) {
						break
					}
				}
				if _, err := scheduler.Finish(item, nil); err != nil {
					failures.Add(1)
					errCh <- err
				}
			}
		}()
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		pending, running, finished := scheduler.Counts()
		if pending == 0 && running == 0 && finished == totalTask {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("scheduler did not drain: pending=%d running=%d finished=%d", pending, running, finished)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	wg.Wait()

	if failures.Load() != 0 {
		t.Fatalf("%d finish failures, first: %v", failures.Load(), <-errCh)
	}
	if maxRunning.Load() > limit {
		t.Fatalf("observed %d running layers, limit is %d", maxRunning.Load(), limit)
	}

	results := scheduler.Results()
	if len(results) != totalTask {
		t.Fatalf("results = %d, want %d", len(results), totalTask)
	}
	seen := map[layerKey]int{}
	for _, result := range results {
		key := layerIdentity(result.RunID, result.Layer)
		seen[key]++
	}
	if len(seen) != totalTask {
		t.Fatalf("unique finished layers = %d, want %d", len(seen), totalTask)
	}
	for key, count := range seen {
		if count != 1 {
			t.Fatalf("layer %+v finished %d times, want exactly once", key, count)
		}
	}
}

func TestExecuteRunsEveryLayerOfSameRun(t *testing.T) {
	scheduler := NewScheduler(1)
	for _, number := range []int{1, 2, 3} {
		if err := scheduler.Enqueue("run-1", testLayer(number), 3-number); err != nil {
			t.Fatalf("enqueue layer %d: %v", number, err)
		}
	}
	var done []int
	var mu sync.Mutex
	err := scheduler.Execute(context.Background(), func(_ context.Context, runID model.ID, layer model.Layer) error {
		if runID != "run-1" {
			return fmt.Errorf("unexpected run id %q", runID)
		}
		mu.Lock()
		done = append(done, layer.Number)
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(done) != 3 {
		t.Fatalf("executed layers = %v, want all 3", done)
	}
	if _, running, finished := scheduler.Counts(); running != 0 || finished != 3 {
		t.Fatalf("counts = running %d finished %d, want 0 and 3", running, finished)
	}
}
