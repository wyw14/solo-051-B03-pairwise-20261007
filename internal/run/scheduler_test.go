package run

import (
	"testing"

	"github.com/wyw14/cry-161/internal/model"
)

func layer(number int) model.Layer {
	return model.Layer{Number: number, Material: "SiO2", TargetNanometers: 100, RateNanometers: 1}
}

func TestSchedulerTracksLayersOfSameRunIndependently(t *testing.T) {
	s := NewScheduler(2)
	if err := s.Enqueue("run-1", layer(1), 1); err != nil {
		t.Fatalf("enqueue layer 1: %v", err)
	}
	if err := s.Enqueue("run-1", layer(2), 1); err != nil {
		t.Fatalf("enqueue layer 2: %v", err)
	}

	first, ok := s.Next()
	if !ok {
		t.Fatal("expected first layer to start")
	}
	second, ok := s.Next()
	if !ok {
		t.Fatal("expected second layer to start within limit")
	}
	if _, ok := s.Next(); ok {
		t.Fatal("limit must reject a third concurrent layer")
	}
	if _, running, _ := s.Counts(); running != 2 {
		t.Fatalf("expected 2 running, got %d", running)
	}

	s.Finish(first, nil)
	if _, running, _ := s.Counts(); running != 1 {
		t.Fatalf("finishing one layer must leave the other running, got %d", running)
	}
	s.Finish(second, nil)

	pending, running, finished := s.Counts()
	if pending != 0 || running != 0 || finished != 2 {
		t.Fatalf("unexpected counts: pending=%d running=%d finished=%d", pending, running, finished)
	}
	results := s.Results()
	seen := map[int]bool{}
	for _, r := range results {
		if r.Status != "completed" {
			t.Fatalf("layer %d status = %q, want completed", r.Layer.Number, r.Status)
		}
		seen[r.Layer.Number] = true
	}
	if !seen[1] || !seen[2] {
		t.Fatalf("results must contain both layers, got %+v", results)
	}
}

func TestSchedulerRejectsDuplicateLayerIdentity(t *testing.T) {
	s := NewScheduler(2)
	if err := s.Enqueue("run-1", layer(1), 1); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := s.Enqueue("run-1", layer(1), 1); err == nil {
		t.Fatal("duplicate pending layer must be rejected")
	}
	if _, ok := s.Next(); !ok {
		t.Fatal("expected layer to start")
	}
	if err := s.Enqueue("run-1", layer(1), 1); err == nil {
		t.Fatal("duplicate running layer must be rejected")
	}
}

func TestSchedulerRejectsStaleFinish(t *testing.T) {
	s := NewScheduler(1)
	stale := ScheduledLayer{RunID: "run-1", Layer: layer(9)}
	s.Finish(stale, nil)
	if _, _, finished := s.Counts(); finished != 0 {
		t.Fatalf("stale finish must not be recorded, got %d finished", finished)
	}
}
