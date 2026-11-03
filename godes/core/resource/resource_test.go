package resource

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

func TestResourceAcquireRelease(t *testing.T) {
	sim := newSim(t, 1)
	r := NewResource(sim, 2)
	if !r.TryAcquire() || !r.TryAcquire() {
		t.Fatal("both slots should be acquirable")
	}
	if r.TryAcquire() {
		t.Fatal("third acquire should fail")
	}
	if r.Used() != 2 || r.Free() != 0 {
		t.Fatalf("Used=%d Free=%d want 2/0", r.Used(), r.Free())
	}
	r.Release() // frees one slot -> now 1 used, 1 free
	if r.Used() != 1 || r.Free() != 1 {
		t.Fatalf("Used=%d Free=%d want 1/1 after release", r.Used(), r.Free())
	}
	if !r.TryAcquire() {
		t.Fatal("one slot free, acquire should succeed")
	}
}

func TestResourceAcquireQueuesWhenFull(t *testing.T) {
	sim := newSim(t, 1)
	r := NewResource(sim, 1)
	if !r.TryAcquire() {
		t.Fatal("first acquire should succeed")
	}
	woke := 0
	r.Acquire(func(*core.Simulation) { woke++ })
	if woke != 0 {
		t.Fatal("acquire on full resource must not grant immediately")
	}
	if r.Waits() != 1 {
		t.Fatalf("Waits() = %d, want 1", r.Waits())
	}
	r.Release()
	if woke != 1 {
		t.Fatalf("release should wake the waiter, woke=%d", woke)
	}
	// The granted slot stays held (level unchanged), so a new acquire waits.
	woke2 := 0
	r.Acquire(func(*core.Simulation) { woke2++ })
	if woke2 != 0 {
		t.Fatal("resource still held by granted waiter")
	}
}

func TestResourceFIFOWakeOrder(t *testing.T) {
	sim := newSim(t, 2)
	r := NewResource(sim, 1)
	r.TryAcquire()
	var order []int
	r.Acquire(func(*core.Simulation) { order = append(order, 1) })
	r.Acquire(func(*core.Simulation) { order = append(order, 2) })
	r.Acquire(func(*core.Simulation) { order = append(order, 3) })
	r.Release() // grants waiter 1
	r.Release() // grants waiter 2
	r.Release() // grants waiter 3
	if len(order) != 3 || order[0] != 1 || order[1] != 2 || order[2] != 3 {
		t.Fatalf("wake order %v, want [1 2 3]", order)
	}
}

func TestResourceGrantedWaiterHoldsSlot(t *testing.T) {
	sim := newSim(t, 3)
	r := NewResource(sim, 1)
	r.TryAcquire()
	var granted bool
	r.Acquire(func(*core.Simulation) { granted = true })
	r.Release() // grants the waiter; slot remains held by it
	if !granted {
		t.Fatal("waiter should be granted on release")
	}
	if r.Used() != 1 {
		t.Fatalf("granted waiter should hold the slot, Used()=%d", r.Used())
	}
}

func TestResourceStats(t *testing.T) {
	sim := newSim(t, 4)
	r := NewResource(sim, 1)
	r.TryAcquire()
	r.Acquire(func(*core.Simulation) {})
	r.Acquire(func(*core.Simulation) {})
	r.Release()
	r.Release()
	// 1 immediate (TryAcquire) + 2 waited.
	if r.Granted() != 1 || r.Waits() != 2 {
		t.Fatalf("Granted=%d Waits=%d want 1/2", r.Granted(), r.Waits())
	}
	if c := r.Contention(); c != 2.0/3.0 {
		t.Fatalf("Contention() = %v, want 2/3", c)
	}
}

func TestResourcePanicsOnInvalid(t *testing.T) {
	for _, cap := range []int{0, -1} {
		func(c int) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("expected panic for capacity", c)
				}
			}()
			NewResource(newSim(t, 1), c)
		}(cap)
	}
}
