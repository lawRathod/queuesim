package process

import (
	"testing"

	"github.com/lawRathod/queuesim/godes/core"
)

func newSim(t *testing.T, seed int64) *core.Simulation {
	t.Helper()
	s, err := core.NewSimulation(seed, core.FIFOPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestProcessWaitDelayCompletes(t *testing.T) {
	sim := newSim(t, 42)
	var ran bool
	p := New(sim, func(e *Env) {
		e.Wait(10)
		ran = true
		e.Wait(5)
	})
	p.Start()
	sim.Run()
	if !ran {
		t.Fatal("body never completed")
	}
	if sim.Now().Int64() != 15 {
		t.Fatalf("expected completion at t=15, got %d", sim.Now().Int64())
	}
}

func TestProcessDeterministic(t *testing.T) {
	trace := func(seed int64) []int64 {
		s := newSim(t, seed)
		var out []int64
		p := New(s, func(e *Env) {
			e.Wait(5)
			out = append(out, s.Now().Int64())
			e.Wait(3)
			out = append(out, s.Now().Int64())
		})
		p.Start()
		s.Run()
		return out
	}
	// Same seed => identical trace.
	a1 := trace(7)
	a2 := trace(7)
	if len(a1) != len(a2) {
		t.Fatalf("traces differ in length")
	}
	for i := range a1 {
		if a1[i] != a2[i] {
			t.Fatalf("determinism violation at %d: %d vs %d", i, a1[i], a2[i])
		}
	}
	if a1[0] != 5 || a1[1] != 8 {
		t.Fatalf("unexpected trace %v, want [5 8]", a1)
	}
}

func TestProcessWaitUntil(t *testing.T) {
	sim := newSim(t, 1)
	var at int64
	p := New(sim, func(e *Env) {
		e.Wait(10)
		e.WaitUntil(core.NewSimTime(25))
		at = e.Now().Int64()
	})
	p.Start()
	sim.Run()
	if at != 25 {
		t.Fatalf("WaitUntil resumed at t=%d, want 25", at)
	}
}

func TestProcessWaitUntilPastResumesNow(t *testing.T) {
	sim := newSim(t, 1)
	var at int64
	p := New(sim, func(e *Env) {
		e.Wait(10)
		e.WaitUntil(core.NewSimTime(3)) // in the past -> resume now (10)
		at = e.Now().Int64()
	})
	p.Start()
	sim.Run()
	if at != 10 {
		t.Fatalf("past WaitUntil resumed at t=%d, want 10 (now)", at)
	}
}

func TestProcessYieldInterleaves(t *testing.T) {
	// Two processes both starting at t=0, each doing two cooperative
	// zero-delay yields. They should interleave in insertion order.
	sim := newSim(t, 3)
	var order []string
	New(sim, func(e *Env) {
		e.Yield() // t=0
		order = append(order, "A1")
		e.Yield() // t=0
		order = append(order, "A2")
	}).Start()
	New(sim, func(e *Env) {
		e.Yield() // t=0
		order = append(order, "B1")
		e.Yield() // t=0
		order = append(order, "B2")
	}).Start()
	sim.Run()
	want := []string{"A1", "B1", "A2", "B2"}
	if len(order) != len(want) {
		t.Fatalf("order %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order %v, want %v", order, want)
		}
	}
}

func TestProcessMultipleProcessesRun(t *testing.T) {
	sim := newSim(t, 9)
	var count int
	for i := 0; i < 5; i++ {
		New(sim, func(e *Env) {
			e.Wait(2)
			count++
		}).Start()
	}
	sim.Run()
	if count != 5 {
		t.Fatalf("count = %d, want 5", count)
	}
}

func TestProcessFinishedProcessIgnoresStart(t *testing.T) {
	sim := newSim(t, 2)
	ran := 0
	p := New(sim, func(e *Env) {
		e.Wait(1)
		ran++
	})
	p.Start()
	sim.Run()
	before := ran
	// After completion, Start must be a no-op and not re-run the body.
	p.Start()
	sim.Run()
	if ran != before {
		t.Fatalf("Start() re-ran a finished process: %d -> %d", before, ran)
	}
}

func TestProcessNoBodySchedulesNothing(t *testing.T) {
	sim := newSim(t, 5)
	p := New(sim, func(*Env) {})
	p.Start()
	sim.Run()
	if sim.Now().Int64() != 0 {
		t.Fatalf("empty body should not advance time, got %d", sim.Now().Int64())
	}
}
