package run

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/wyw14/cry-161/internal/model"
)

type ScheduledLayer struct {
	RunID      model.ID    `json:"run_id"`
	Layer      model.Layer `json:"layer"`
	Priority   int         `json:"priority"`
	EnqueuedAt time.Time   `json:"enqueued_at"`
	StartedAt  time.Time   `json:"started_at,omitempty"`
	FinishedAt time.Time   `json:"finished_at,omitempty"`
	Status     string      `json:"status"`
	Error      string      `json:"error,omitempty"`

	// token identifies a specific dispatch of this layer. It is assigned by
	// Next and verified by Finish so a stale or foreign ScheduledLayer copy
	// can never release another dispatch's running slot. Unexported, so it
	// never leaves the process via JSON.
	token uint64
}

// layerKey identifies a single layer task within a run independently of other
// layers of the same run.
type layerKey struct {
	runID model.ID
	layer int
}

var (
	ErrLayerAlreadyScheduled = errors.New("layer is already scheduled for run")
	ErrLayerNotRunning       = errors.New("layer is not running for run")
)

type Scheduler struct {
	mu          sync.Mutex
	pending     []ScheduledLayer
	pendingKeys map[layerKey]struct{}
	running     map[layerKey]ScheduledLayer
	finished    []ScheduledLayer
	limit       int
	seq         uint64
}

func NewScheduler(limit int) *Scheduler {
	if limit < 1 {
		limit = 1
	}
	return &Scheduler{
		pending:     make([]ScheduledLayer, 0),
		pendingKeys: map[layerKey]struct{}{},
		running:     map[layerKey]ScheduledLayer{},
		finished:    make([]ScheduledLayer, 0),
		limit:       limit,
	}
}

func layerIdentity(runID model.ID, layer model.Layer) layerKey {
	return layerKey{runID: runID, layer: layer.Number}
}

func (s *Scheduler) Enqueue(runID model.ID, layer model.Layer, priority int) error {
	if runID == "" || layer.Number < 1 || layer.Material == "" {
		return fmt.Errorf("scheduled layer is invalid")
	}
	key := layerIdentity(runID, layer)
	item := ScheduledLayer{RunID: runID, Layer: layer, Priority: priority, EnqueuedAt: time.Now().UTC(), Status: "pending"}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.pendingKeys[key]; exists {
		return fmt.Errorf("%w: run %s layer %d", ErrLayerAlreadyScheduled, runID, layer.Number)
	}
	if _, exists := s.running[key]; exists {
		return fmt.Errorf("%w: run %s layer %d", ErrLayerAlreadyScheduled, runID, layer.Number)
	}
	s.pending = append(s.pending, item)
	s.pendingKeys[key] = struct{}{}
	sort.SliceStable(s.pending, func(i, j int) bool {
		if s.pending[i].Priority == s.pending[j].Priority {
			return s.pending[i].EnqueuedAt.Before(s.pending[j].EnqueuedAt)
		}
		return s.pending[i].Priority > s.pending[j].Priority
	})
	return nil
}

func (s *Scheduler) Next() (ScheduledLayer, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// len(running) counts in-flight layer tasks (not runs), so each layer of a
	// run occupies its own slot and the concurrency limit cannot be bypassed.
	if len(s.pending) == 0 || len(s.running) >= s.limit {
		return ScheduledLayer{}, false
	}
	item := s.pending[0]
	s.pending = s.pending[1:]
	key := layerIdentity(item.RunID, item.Layer)
	delete(s.pendingKeys, key)
	item.Status = "running"
	item.StartedAt = time.Now().UTC()
	s.seq++
	item.token = s.seq
	s.running[key] = item
	return item, true
}

// Finish records the completion of the exact dispatch handed out by Next. It
// refuses layers that were never dispatched or whose token does not match the
// currently running dispatch, so a stale Finish cannot delete another layer's
// slot or fabricate a result.
func (s *Scheduler) Finish(item ScheduledLayer, err error) (ScheduledLayer, error) {
	key := layerIdentity(item.RunID, item.Layer)
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.running[key]
	if !ok {
		return ScheduledLayer{}, fmt.Errorf("%w: run %s layer %d", ErrLayerNotRunning, item.RunID, item.Layer.Number)
	}
	if item.token == 0 || item.token != current.token {
		return ScheduledLayer{}, fmt.Errorf("stale finish token for run %s layer %d", item.RunID, item.Layer.Number)
	}
	delete(s.running, key)
	current.FinishedAt = time.Now().UTC()
	current.Status = "completed"
	if err != nil {
		current.Status = "failed"
		current.Error = err.Error()
	}
	s.finished = append(s.finished, current)
	return current, nil
}

func (s *Scheduler) Execute(ctx context.Context, worker func(context.Context, model.ID, model.Layer) error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		item, ok := s.Next()
		if !ok {
			pending, running, _ := s.Counts()
			if pending == 0 && running == 0 {
				return nil
			}
			time.Sleep(time.Millisecond)
			continue
		}
		err := worker(ctx, item.RunID, item.Layer)
		if _, finishErr := s.Finish(item, err); finishErr != nil && err == nil {
			err = finishErr
		}
		if err != nil {
			return err
		}
	}
}

func (s *Scheduler) Counts() (int, int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending), len(s.running), len(s.finished)
}

func (s *Scheduler) Results() []ScheduledLayer {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]ScheduledLayer, len(s.finished))
	copy(result, s.finished)
	return result
}
