package core

import (
	"math"
	"testing"

	"github.com/lawRathod/queuesim/godes/simtest"
)

func TestFIFOPolicy_TieBreakByKey(t *testing.T) {
	frontier := []FrontierEvent{
		{ComponentID: "b", TaskID: 5, Seq: 99},
		{ComponentID: "a", TaskID: 9, Seq: 1},
		{ComponentID: "a", TaskID: 3, Seq: 2},
	}
	var p FIFOPolicy
	if got := p.Choose(0, frontier); got != 2 {
		t.Fatalf("expected key order to pick index 2 (a/3), got %d", got)
	}
}

func TestFIFOPolicy_TieBreakSeqTertiary(t *testing.T) {
	frontier := []FrontierEvent{
		{ComponentID: "a", TaskID: 3, Seq: 99},
		{ComponentID: "a", TaskID: 3, Seq: 1},
	}
	var p FIFOPolicy
	if got := p.Choose(0, frontier); got != 1 {
		t.Fatalf("expected lower seq to win same-key tie, got %d", got)
	}
}

func TestRunUntilStopsBeforeFutureEvent(t *testing.T) {
	sim, err := NewSimulation(1, FIFOPolicy{})
	if err != nil {
		t.Fatalf("NewSimulation error: %v", err)
	}
	trace := make([]string, 0, 2)

	sim.ScheduleAt(NewSimTime(2), func(*Simulation) { trace = append(trace, "at2") })
	sim.ScheduleAt(NewSimTime(5), func(*Simulation) { trace = append(trace, "at5") })

	sim.RunUntil(NewSimTime(2))

	if len(trace) != 1 || trace[0] != "at2" {
		t.Fatalf("expected only at2 executed, got %v", trace)
	}
	if sim.Now() != NewSimTime(2) {
		t.Fatalf("expected now=2, got %d", sim.Now())
	}

	sim.Run()
	if len(trace) != 2 || trace[1] != "at5" {
		t.Fatalf("expected at5 to execute later, got %v", trace)
	}
}

func TestScheduleAtAcceptsEventCallbackType(t *testing.T) {
	sim, err := NewSimulation(1, FIFOPolicy{})
	if err != nil {
		t.Fatalf("NewSimulation error: %v", err)
	}

	called := false
	var ev Event = func(*Simulation) { called = true }
	sim.ScheduleAt(NewSimTime(1), ev)
	sim.Run()

	if !called {
		t.Fatalf("expected scheduled event callback to run")
	}
}

func TestScheduleInUsesSaturatingArithmetic(t *testing.T) {
	sim, err := NewSimulation(1, FIFOPolicy{})
	if err != nil {
		t.Fatalf("NewSimulation error: %v", err)
	}

	sim.ScheduleAt(NewSimTime(math.MaxInt64-1), func(sim *Simulation) {
		sim.ScheduleIn(100, func(sim *Simulation) {
			if sim.Now() != SimTime(math.MaxInt64) {
				t.Fatalf("expected saturation to MaxInt64, got %d", sim.Now())
			}
		})
	})

	sim.Run()
}

func TestRootRNGSeedContract(t *testing.T) {
	run := func(seed int64) []uint64 {
		sim, err := NewSimulation(seed, FIFOPolicy{})
		if err != nil {
			t.Fatalf("NewSimulation error: %v", err)
		}
		out := make([]uint64, 0, 6)
		for range 6 {
			out = append(out, sim.RNG().Uint64())
		}
		return out
	}

	simtest.AssertDeterministic(t, 8, 101, run)
}

func TestSubstreamDeterminismByName(t *testing.T) {
	run := func(seed int64) []uint64 {
		sim, err := NewSimulation(seed, FIFOPolicy{})
		if err != nil {
			t.Fatalf("NewSimulation error: %v", err)
		}
		a := sim.Substream("actor:A")
		b := sim.Substream("actor:B")
		out := make([]uint64, 0, 6)
		for range 3 {
			out = append(out, a.Uint64())
			out = append(out, b.Uint64())
		}
		return out
	}

	simtest.AssertDeterministic(t, 8, 202, run)
}

func TestFrontierPolicyDefaultFIFO(t *testing.T) {
	sim, err := NewSimulation(9, FIFOPolicy{})
	if err != nil {
		t.Fatalf("NewSimulation error: %v", err)
	}
	trace := make([]string, 0, 3)

	sim.ScheduleAt(NewSimTime(3), func(*Simulation) { trace = append(trace, "first") })
	sim.ScheduleAt(NewSimTime(3), func(*Simulation) { trace = append(trace, "second") })
	sim.ScheduleAt(NewSimTime(3), func(*Simulation) { trace = append(trace, "third") })

	sim.Run()

	got := simtest.HashTrace(trace)
	want := simtest.HashTrace([]string{"first", "second", "third"})
	if got != want {
		t.Fatalf("expected FIFO equal-time ordering, got trace=%v", trace)
	}
}

func TestSeedMappingDoesNotAliasNegativeAndPositiveSeeds(t *testing.T) {
	a, err := NewSimulation(1, FIFOPolicy{})
	if err != nil {
		t.Fatalf("NewSimulation error: %v", err)
	}
	b, err := NewSimulation(-1, FIFOPolicy{})
	if err != nil {
		t.Fatalf("NewSimulation error: %v", err)
	}

	x := a.RNG().Uint64()
	y := b.RNG().Uint64()
	if x == y {
		t.Fatalf("expected distinct streams for seeds 1 and -1, both got %d", x)
	}
}

func TestSeededRandomPolicyDeterministic(t *testing.T) {
	run := func(seed int64) string {
		sim, err := NewSimulation(seed, &SeededRandomPolicy{Seed: 404})
		if err != nil {
			t.Fatalf("NewSimulation error: %v", err)
		}
		trace := make([]int64, 0, 5)
		for i := range 5 {
			i := int64(i)
			sim.ScheduleAt(NewSimTime(1), func(*Simulation) {
				trace = append(trace, i)
			})
		}
		sim.Run()
		return simtest.HashTrace(trace)
	}
	simtest.AssertDeterministic(t, 8, 404, run)
}

func TestSeededRandomPolicyDiffersFromFIFO(t *testing.T) {
	fifo, _ := NewSimulation(7, FIFOPolicy{})
	seeded, _ := NewSimulation(7, &SeededRandomPolicy{Seed: 7})

	for i := 0; i < 200; i++ {
		i := i
		fifo.ScheduleAt(0, func(*Simulation) { _ = i })
		seeded.ScheduleAt(0, func(*Simulation) { _ = i })
	}

	var fifoOrder []int
	var seededOrder []int
	for i := 0; i < 200; i++ {
		i := i
		fifo.ScheduleAt(0, func(*Simulation) { fifoOrder = append(fifoOrder, i) })
		seeded.ScheduleAt(0, func(*Simulation) { seededOrder = append(seededOrder, i) })
	}

	fifo.Run()
	seeded.Run()

	same := true
	for i := range fifoOrder {
		if fifoOrder[i] != seededOrder[i] {
			same = false
			break
		}
	}
	if same {
		t.Fatal("seeded-random should differ from FIFO")
	}
}

func TestScheduleAtReturnsNonZeroTaskHandle(t *testing.T) {
	sim, _ := NewSimulation(1, FIFOPolicy{})
	h := sim.ScheduleAt(NewSimTime(5), func(*Simulation) {})
	if h.ID() == 0 {
		t.Fatal("expected non-zero TaskID from ScheduleAt")
	}
	sim.Run()
}

func TestScheduleInReturnsNonZeroTaskHandle(t *testing.T) {
	sim, _ := NewSimulation(1, FIFOPolicy{})
	h := sim.ScheduleIn(3, func(*Simulation) {})
	if h.ID() == 0 {
		t.Fatal("expected non-zero TaskID from ScheduleIn")
	}
	sim.Run()
}

func TestScheduleAtWithNilEventReturnsZeroHandle(t *testing.T) {
	sim, _ := NewSimulation(1, FIFOPolicy{})
	h := sim.ScheduleAt(NewSimTime(5), nil)
	if h.ID() != 0 {
		t.Fatalf("expected zero TaskID for nil event, got %d", h.ID())
	}
}

func TestScheduleAtExPreservesExplicitTaskID(t *testing.T) {
	sim, _ := NewSimulation(1, FIFOPolicy{})
	customID := TaskID(99)
	h := sim.ScheduleAtEx(NewSimTime(5), func(*Simulation) {}, ScheduleOptions{TaskID: customID, Kind: KindTask})
	if h.ID() != customID {
		t.Fatalf("expected TaskID %d, got %d", customID, h.ID())
	}
	sim.Run()
}

func TestConsecutiveScheduleAtGetsDistinctTaskIDs(t *testing.T) {
	sim, _ := NewSimulation(1, FIFOPolicy{})
	h1 := sim.ScheduleAt(NewSimTime(5), func(*Simulation) {})
	h2 := sim.ScheduleAt(NewSimTime(5), func(*Simulation) {})
	if h1.ID() == h2.ID() {
		t.Fatalf("expected distinct TaskIDs, both = %d", h1.ID())
	}
	sim.Run()
}

// TestScheduleAtExKindInference verifies the Kind defaults:
//   - No opts → KindCallback (via ScheduleAt)
//   - TaskID set, Kind unset → KindTask (chain reschedule convention)
//   - Kind set explicitly → used as-is
func TestScheduleAtExKindInference(t *testing.T) {
	sim, _ := NewSimulation(1, FIFOPolicy{})

	// ScheduleAt → KindCallback
	sim.ScheduleAt(NewSimTime(1), func(*Simulation) {})

	// ScheduleAtEx with TaskID only → KindTask (chain reschedule)
	sim.ScheduleAtEx(NewSimTime(2), func(*Simulation) {}, ScheduleOptions{TaskID: 42})

	// ScheduleAtEx with Kind=KindCallback + TaskID → KindCallback
	sim.ScheduleAtEx(NewSimTime(3), func(*Simulation) {}, ScheduleOptions{TaskID: 43, Kind: KindCallback})

	// ScheduleAtEx with Kind=KindTask, no TaskID → KindTask, auto TaskID
	sim.ScheduleAtEx(NewSimTime(4), func(*Simulation) {}, ScheduleOptions{Kind: KindTask})

	sim.Run()
}
