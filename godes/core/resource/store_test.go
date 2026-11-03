package resource

import (
	"testing"

	"github.com/lawRathod/queuesim/godes/core"
)

func TestStorePutGet(t *testing.T) {
	sim := newSim(t, 1)
	s := NewStore(sim, 2)
	if !s.TryPut("a") || !s.TryPut("b") {
		t.Fatal("two puts should fit in capacity 2")
	}
	if s.TryPut("c") {
		t.Fatal("third put should fail")
	}
	if s.Len() != 2 {
		t.Fatalf("Len=%d want 2", s.Len())
	}
	v, ok := s.TryGet()
	if !ok || v != "a" {
		t.Fatalf("TryGet = %v,%v want a,true (FIFO)", v, ok)
	}
	v, ok = s.TryGet()
	if !ok || v != "b" {
		t.Fatalf("TryGet = %v,%v want b,true", v, ok)
	}
	if _, ok := s.TryGet(); ok {
		t.Fatal("empty store must not get")
	}
}

func TestStoreBlockingPut(t *testing.T) {
	sim := newSim(t, 2)
	s := NewStore(sim, 1)
	s.TryPut("full")
	woke := 0
	s.Put("queued", func(*core.Simulation) { woke++ })
	if woke != 0 {
		t.Fatal("put on full store must not grant immediately")
	}
	s.Get(func(v any) {})
	// serve(): getter gets "full", then putter's "queued" is placed.
	if woke != 1 {
		t.Fatalf("get should unblock the putter, woke=%d", woke)
	}
	if s.Len() != 1 {
		t.Fatalf("Len=%d want 1 (queued item placed)", s.Len())
	}
	v, _ := s.TryGet()
	if v != "queued" {
		t.Fatalf("popped %v, want queued", v)
	}
}

func TestStoreBlockingGet(t *testing.T) {
	sim := newSim(t, 3)
	s := NewStore(sim, 2)
	var got any
	s.Get(func(v any) { got = v })
	if got != nil {
		t.Fatal("get on empty store must not grant immediately")
	}
	s.Put("hello", func(*core.Simulation) {})
	if got != "hello" {
		t.Fatalf("put should unblock the getter, got=%v", got)
	}
}

func TestStoreFIFOGetOrder(t *testing.T) {
	sim := newSim(t, 4)
	s := NewStore(sim, 3)
	var order []string
	s.Get(func(v any) { order = append(order, v.(string)) })
	s.Get(func(v any) { order = append(order, v.(string)) })
	s.Put("x", func(*core.Simulation) {})
	s.Put("y", func(*core.Simulation) {})
	if len(order) != 2 || order[0] != "x" || order[1] != "y" {
		t.Fatalf("order %v, want [x y]", order)
	}
}

func TestStoreStats(t *testing.T) {
	sim := newSim(t, 5)
	s := NewStore(sim, 2)
	s.TryPut(1)
	s.TryPut(2)
	s.TryGet()
	if s.Puts() != 2 || s.Gets() != 1 {
		t.Fatalf("Puts=%d Gets=%d want 2/1", s.Puts(), s.Gets())
	}
	if s.Free() != 1 || s.Len() != 1 {
		t.Fatalf("Free=%d Len=%d want 1/1", s.Free(), s.Len())
	}
}

func TestStorePanicsOnInvalid(t *testing.T) {
	for _, cap := range []int{0, -1} {
		func(c int) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("expected panic for capacity", c)
				}
			}()
			NewStore(newSim(t, 1), c)
		}(cap)
	}
}
