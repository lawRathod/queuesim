package core

import (
	"testing"

	"github.com/lawRathod/queuesim/godes/simtest"
)

func TestSchedulerEqualTimeUsesStableSequenceID(t *testing.T) {
	h := newEventHeap()
	now := NewSimTime(10)
	h.push(scheduledEvent{at: now, seq: 2})
	h.push(scheduledEvent{at: now, seq: 0})
	h.push(scheduledEvent{at: now, seq: 1})

	if got, ok := h.pop(); !ok || got.seq != 0 {
		t.Fatalf("first pop: expected seq 0, got %+v (ok=%v)", got, ok)
	}
	if got, ok := h.pop(); !ok || got.seq != 1 {
		t.Fatalf("second pop: expected seq 1, got %+v (ok=%v)", got, ok)
	}
	if got, ok := h.pop(); !ok || got.seq != 2 {
		t.Fatalf("third pop: expected seq 2, got %+v (ok=%v)", got, ok)
	}
}

func TestSimulationSameTimeDeterministicOrderingAcrossRuns(t *testing.T) {
	run := func(seed int64) string {
		sim, err := NewSimulation(seed, FIFOPolicy{})
		if err != nil {
			t.Fatalf("NewSimulation error: %v", err)
		}

		trace := make([]string, 0, 5)
		for i := range 5 {
			i := i
			sim.ScheduleAt(NewSimTime(7), func(*Simulation) {
				trace = append(trace, string(rune('A'+i)))
			})
		}

		sim.Run()
		return simtest.HashTrace(trace)
	}

	simtest.AssertDeterministic(t, 10, 1234, run)
}
