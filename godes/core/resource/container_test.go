package resource

import (
	"math"
	"testing"

	"github.com/lawRathod/queuesim/godes/core"
)

func TestContainerPutTake(t *testing.T) {
	sim := newSim(t, 1)
	c := NewContainer(sim, 10, 0)
	if !c.TryPut(4) {
		t.Fatal("put 4 should fit")
	}
	if c.Level() != 4 || c.Free() != 6 {
		t.Fatalf("Level=%v Free=%v want 4/6", c.Level(), c.Free())
	}
	if !c.TryTake(3) {
		t.Fatal("take 3 should be available")
	}
	if c.Level() != 1 {
		t.Fatalf("Level=%v want 1", c.Level())
	}
	if c.TryPut(10) {
		t.Fatal("put 10 should not fit (1+10>10)")
	}
	if c.TryTake(5) {
		t.Fatal("take 5 should not be available (1<5)")
	}
}

func TestContainerBlockingPut(t *testing.T) {
	sim := newSim(t, 2)
	c := NewContainer(sim, 5, 4)
	woke := 0
	c.Put(3, func(*core.Simulation) { woke++ }) // 4+3>5 -> queue
	if woke != 0 {
		t.Fatal("put beyond capacity must not grant immediately")
	}
	c.Take(4, func(*core.Simulation) {}) // frees 4 -> level 0, frees space for the putter
	if woke != 1 {
		t.Fatalf("take should have unblocked the putter, woke=%d", woke)
	}
	if c.Level() != 3 {
		t.Fatalf("Level=%v want 3 (putter's 3 placed)", c.Level())
	}
}

func TestContainerBlockingTake(t *testing.T) {
	sim := newSim(t, 3)
	c := NewContainer(sim, 10, 1)
	woke := 0
	c.Take(3, func(*core.Simulation) { woke++ }) // 1<3 -> queue
	if woke != 0 {
		t.Fatal("take beyond level must not grant immediately")
	}
	c.Put(5, func(*core.Simulation) {}) // level 1+5=6 >= 3 -> grants the taker
	if woke != 1 {
		t.Fatalf("put should have unblocked the taker, woke=%d", woke)
	}
	// Take 3 removed from level 6 -> 3.
	if math.Abs(c.Level()-3) > 1e-9 {
		t.Fatalf("Level=%v want 3", c.Level())
	}
}

func TestContainerFIFOAmountAware(t *testing.T) {
	sim := newSim(t, 4)
	c := NewContainer(sim, 10, 2)
	// A big taker (needs 8) queued first, then a small taker (needs 2).
	var order []int
	c.Take(8, func(*core.Simulation) { order = append(order, 1) })
	c.Take(2, func(*core.Simulation) { order = append(order, 2) })
	// Add 6 -> level 8: serve() grants takers FIFO from the head, so the
	// small taker (needs 2, now at head after the big one remains blocked)
	// is granted first from level 8 -> 6, then the big taker (needs 8)
	// cannot be granted from 6 and stays blocked. Order: [2].
	c.Put(6, func(*core.Simulation) {}) // level 2+6=8
	if len(order) != 1 || order[0] != 2 {
		t.Fatalf("small taker should be granted first (FIFO head), order=%v", order)
	}
	// Level is now 6. Add 2 -> 8 -> big taker (needs 8) granted.
	c.Put(2, func(*core.Simulation) {})
	if len(order) != 2 || order[1] != 1 {
		t.Fatalf("big taker should be granted second, order=%v", order)
	}
}

func TestContainerStatsAndAccessors(t *testing.T) {
	sim := newSim(t, 5)
	c := NewContainer(sim, 10, 3)
	c.TryPut(2)
	c.TryTake(1)
	if c.Puts() != 1 || c.Takes() != 1 {
		t.Fatalf("Puts=%d Takes=%d want 1/1", c.Puts(), c.Takes())
	}
	if math.Abs(c.Capacity()-10) > 1e-9 || math.Abs(c.Level()-4) > 1e-9 {
		t.Fatalf("accessors wrong: Capacity=%v Level=%v", c.Capacity(), c.Level())
	}
}

func TestContainerPanicsOnInvalid(t *testing.T) {
	tests := []struct {
		name string
		cap  float64
		lvl  float64
	}{
		{name: "zero capacity", cap: 0, lvl: 0},
		{name: "negative capacity", cap: -1, lvl: 0},
		{name: "negative level", cap: 10, lvl: -1},
		{name: "level above capacity", cap: 10, lvl: 11},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("expected panic")
				}
			}()
			NewContainer(newSim(t, 1), tt.cap, tt.lvl)
		})
	}
}
