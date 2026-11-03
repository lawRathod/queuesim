package model

import (
	"testing"

	"github.com/lawRathod/queuesim/godes/core"
)

// TestWorkerPullsAndCompletes: a Worker fleet in a builder topology parks
// on a queue, sync-matches / dispatches added tasks, executes them, and
// completes. The topology's pump drives the worker parking. All tasks get
// processed.
func TestWorkerPullsAndCompletes(t *testing.T) {
	sim, err := core.NewSimulation(1, core.FIFOPolicy{})
	if err != nil {
		t.Fatalf("NewSimulation: %v", err)
	}
	b := NewTopologyBuilder(sim)
	b.AddQueue("in", 100)
	b.AddQueue("done", 0)
	q := b.Queue("in")
	b.AddWorker(WorkerConfig{Name: "worker", Queue: q, ServiceMs: 5, Count: 2, LongPollMs: 60000})
	// Feed the completed queue via a finisher processor (so done accumulates).
	b.AddProcessor(ProcessorConfig{Name: "finisher", ServiceMs: 1})
	fin := b.Processor("finisher")
	b.Feed(b.Queue("done"), fin)
	b.SetEnd(1000)
	top, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	w := top.workers["worker"]
	// Add tasks into 'in' at t=0 (via a scheduled event so the pump sees them).
	sim.ScheduleAt(sim.Now(), func(*core.Simulation) {
		for i := 0; i < 5; i++ {
			if !q.Add(Task{ID: uint64(i), Attempt: 1}) {
				t.Errorf("add %d rejected", i)
			}
		}
	})
	top.Run()
	if w.Completed() != 5 {
		t.Fatalf("completed = %d, want 5", w.Completed())
	}
	if q.Len() != 0 {
		t.Fatalf("backlog len = %d, want 0 (worker drained it)", q.Len())
	}
}
