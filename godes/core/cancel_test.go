package core

import "testing"

func TestCancelPendingTaskReturnsTrue(t *testing.T) {
	sim, _ := NewSimulation(1, FIFOPolicy{})
	fired := false
	h := sim.ScheduleAt(NewSimTime(10), func(*Simulation) { fired = true })
	if !sim.Cancel(h) {
		t.Fatal("expected Cancel of pending task to return true")
	}
	sim.Run()
	if fired {
		t.Fatal("cancelled task must not fire")
	}
}

func TestCancelAlreadyFiredReturnsFalse(t *testing.T) {
	sim, _ := NewSimulation(1, FIFOPolicy{})
	fired := false
	h := sim.ScheduleAt(NewSimTime(1), func(*Simulation) { fired = true })
	sim.Run()
	if sim.Cancel(h) {
		t.Fatal("expected Cancel of already-fired task to return false")
	}
	if !fired {
		t.Fatal("task should have fired before cancel")
	}
}

func TestDoubleCancelReturnsFalseSecondTime(t *testing.T) {
	sim, _ := NewSimulation(1, FIFOPolicy{})
	h := sim.ScheduleAt(NewSimTime(10), func(*Simulation) {})
	first := sim.Cancel(h)
	second := sim.Cancel(h)
	if !first {
		t.Fatal("first Cancel should return true")
	}
	if second {
		t.Fatal("second Cancel should return false (tombstone already set by first Cancel)")
	}
}

// LOAD-BEARING: a cancelled event MUST NOT advance sim time.
// This is the DesCartes scheduler.rs:1064-1091 invariant and the
// reason the at-pop-skip choice is the correct one for Temporal
// early-completion timeouts (which must not move the sim clock).
func TestCancelDoesNotAdvanceSimTime(t *testing.T) {
	sim, _ := NewSimulation(1, FIFOPolicy{})
	h := sim.ScheduleAt(NewSimTime(10), func(*Simulation) {})
	sim.Cancel(h)
	sim.Run()
	if sim.Now() != 0 {
		t.Fatalf("expected sim time to remain 0 after cancel, got %d", sim.Now())
	}
}

func TestCancelSkipsEventInStep(t *testing.T) {
	sim, _ := NewSimulation(1, FIFOPolicy{})
	order := []string{}
	sim.ScheduleAt(NewSimTime(1), func(*Simulation) { order = append(order, "first") })
	cancelled := sim.ScheduleAt(NewSimTime(2), func(*Simulation) { order = append(order, "cancelled") })
	sim.ScheduleAt(NewSimTime(3), func(*Simulation) { order = append(order, "third") })
	sim.Cancel(cancelled)
	sim.Run()
	want := []string{"first", "third"}
	if len(order) != 2 || order[0] != want[0] || order[1] != want[1] {
		t.Fatalf("expected %v, got %v", want, order)
	}
}

func TestCancelZeroTaskHandleReturnsFalse(t *testing.T) {
	sim, _ := NewSimulation(1, FIFOPolicy{})
	if sim.Cancel(TaskHandle{}) {
		t.Fatal("expected Cancel of zero TaskHandle to return false")
	}
}

func TestCancelDoesNotPerturbOrderingOfOtherEvents(t *testing.T) {
	sim, _ := NewSimulation(1, FIFOPolicy{})
	var trace []int
	h1 := sim.ScheduleAt(NewSimTime(5), func(*Simulation) { trace = append(trace, 1) })
	sim.ScheduleAt(NewSimTime(5), func(*Simulation) { trace = append(trace, 2) })
	sim.ScheduleAt(NewSimTime(5), func(*Simulation) { trace = append(trace, 3) })
	sim.Cancel(h1)
	sim.Run()
	// h1 was scheduled first (seq=1, TaskID=1). After cancel, the next two
	// events should fire in seq order: 2 (seq=2) then 3 (seq=3).
	if len(trace) != 2 || trace[0] != 2 || trace[1] != 3 {
		t.Fatalf("expected [2 3] after cancel of first, got %v", trace)
	}
}

// TestCancelDoesNotFireInRunUntil verifies that cancelled events are skipped
// in RunUntil, not just in Step.
func TestCancelDoesNotFireInRunUntil(t *testing.T) {
	sim, _ := NewSimulation(1, FIFOPolicy{})
	fired := false
	h := sim.ScheduleAt(NewSimTime(5), func(*Simulation) { fired = true })
	sim.Cancel(h)
	sim.RunUntil(NewSimTime(10))
	if fired {
		t.Fatal("cancelled event must not fire in RunUntil")
	}
	if sim.Now() != 0 {
		t.Fatalf("expected sim time 0, got %d", sim.Now())
	}
}

// TestRunUntilDoesNotAdvanceBeyondTarget verifies that when a cancelled event
// sits before the target and a non-cancelled event sits beyond it, RunUntil
// does NOT pop the beyond-target event. This is the popNextCancelable maxAt
// boundary invariant (Issue #1 from code review).
func TestRunUntilDoesNotAdvanceBeyondTarget(t *testing.T) {
	sim, _ := NewSimulation(1, FIFOPolicy{})
	h := sim.ScheduleAt(NewSimTime(5), func(*Simulation) {})
	sim.ScheduleAt(NewSimTime(15), func(*Simulation) {})
	sim.Cancel(h)
	sim.RunUntil(NewSimTime(10))
	if sim.Now() != 0 {
		t.Fatalf("expected sim time 0 (cancelled at boundary, no non-cancelled <= target), got %d", sim.Now())
	}
}

// TestRunUntilSkipsCancelledButFiresLaterNonCancelled verifies that RunUntil
// skips a cancelled event and fires a non-cancelled event that IS within the
// target window.
func TestRunUntilSkipsCancelledButFiresLaterNonCancelled(t *testing.T) {
	sim, _ := NewSimulation(1, FIFOPolicy{})
	fired := false
	h := sim.ScheduleAt(NewSimTime(5), func(*Simulation) {})
	sim.ScheduleAt(NewSimTime(8), func(*Simulation) { fired = true })
	sim.Cancel(h)
	sim.RunUntil(NewSimTime(10))
	if !fired {
		t.Fatal("non-cancelled event at t=8 should have fired")
	}
	if sim.Now() != 8 {
		t.Fatalf("expected sim time 8, got %d", sim.Now())
	}
}

// TestCancelChainAtPopSkip verifies that a chain re-scheduling itself under
// the same TaskID is skipped at pop time after Cancel, WITHOUT advancing sim time.
func TestCancelChainAtPopSkip(t *testing.T) {
	sim, _ := NewSimulation(1, FIFOPolicy{})
	var fireCount int
	var h TaskHandle
	h = sim.ScheduleAtEx(NewSimTime(1), func(s *Simulation) {
		fireCount++
		s.ScheduleAtEx(NewSimTime(2), func(s *Simulation) {
			fireCount++
		}, ScheduleOptions{TaskID: h.ID(), Kind: KindTask})
	}, ScheduleOptions{Kind: KindTask})

	if !sim.Cancel(h) {
		t.Fatal("Cancel of pending chain should return true")
	}
	sim.Run()
	if fireCount != 0 {
		t.Fatalf("expected no fires after cancel, got %d", fireCount)
	}
	if sim.Now() != 0 {
		t.Fatalf("expected sim time 0 (no advance from cancelled event), got %d", sim.Now())
	}
}

// TestRunUntilUsesFrontierPolicyLikeStep verifies that RunUntil applies the
// same frontier policy as Step for same-time events. ADR-011 replay fidelity
// requires this.
func TestRunUntilUsesFrontierPolicyLikeStep(t *testing.T) {
	// Run via Step (which uses the policy)
	stepRun := func(seed int64) []uint64 {
		sim, _ := NewSimulation(seed, &SeededRandomPolicy{Seed: 999})
		var trace []uint64
		for i := range 3 {
			i := uint64(i)
			sim.ScheduleAt(0, func(*Simulation) { trace = append(trace, i) })
		}
		sim.Run() // uses Step, which uses the policy
		return trace
	}
	// Run via RunUntil (should also use the policy after Task 5)
	untilRun := func(seed int64) []uint64 {
		sim, _ := NewSimulation(seed, &SeededRandomPolicy{Seed: 999})
		var trace []uint64
		for i := range 3 {
			i := uint64(i)
			sim.ScheduleAt(0, func(*Simulation) { trace = append(trace, i) })
		}
		sim.RunUntil(NewSimTime(1))
		return trace
	}
	s1 := stepRun(1)
	s2 := untilRun(1)
	if len(s1) != 3 {
		t.Fatalf("expected 3 fires from Step, got %d", len(s1))
	}
	for i := range s1 {
		if s1[i] != s2[i] {
			t.Fatalf("ordering mismatch at %d: Step=%v, RunUntil=%v", i, s1, s2)
		}
	}
}
