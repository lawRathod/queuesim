package model

import (
	"testing"

	"github.com/law/GoDES/core/dists"
)

func TestNetworkDeliveryOrder(t *testing.T) {
	sim := newSim(t)
	src := dists.FuncSource(func() float64 { return 0.5 })
	nw := NewNetwork(sim, NetworkConfig{Name: "link", DelayMs: 2, LinkSrc: src})
	nw.Transmit(Task{ID: 1}, sim.Now())
	nw.Transmit(Task{ID: 2}, sim.Now())
	if nw.Pending() != 2 {
		t.Fatalf("Pending=%d want 2", nw.Pending())
	}
	// Before due time, nothing delivers.
	got := nw.DrainToTick(sim.Now())
	if len(got) != 0 {
		t.Fatal("nothing should deliver at t=0")
	}
	// At t=2 both are due (FIFO).
	got = nw.DrainToTick(sim.Now().Add(2))
	if len(got) != 2 || got[0].ID != 1 || got[1].ID != 2 {
		t.Fatalf("delivered=%v want [1 2]", got)
	}
	if nw.Pending() != 0 {
		t.Fatalf("Pending=%d want 0 after full drain", nw.Pending())
	}
}

func TestNetworkDrop(t *testing.T) {
	sim := newSim(t)
	dropSrc := dists.FuncSource(func() float64 { return 0.1 })
	nw := NewNetwork(sim, NetworkConfig{Name: "link", DropRate: 0.5, LinkSrc: dropSrc})
	if nw.Transmit(Task{ID: 1}, sim.Now()) {
		t.Fatal("drop source < 0.5 should drop")
	}
	if nw.Dropped() != 1 || nw.Sent() != 1 {
		t.Fatalf("Dropped=%d Sent=%d want 1/1", nw.Dropped(), nw.Sent())
	}
	// A non-dropping source.
	okSrc := dists.FuncSource(func() float64 { return 0.9 })
	nw2 := NewNetwork(sim, NetworkConfig{Name: "link2", DelayMs: 1, DropRate: 0.5, LinkSrc: okSrc})
	if !nw2.Transmit(Task{ID: 2}, sim.Now()) {
		t.Fatal("0.9 >= 0.5 should not drop")
	}
	if nw2.Dropped() != 0 {
		t.Fatalf("Dropped=%d want 0", nw2.Dropped())
	}
}

func TestNetworkJitter(t *testing.T) {
	sim := newSim(t)
	// Jitter source always 0.5 => delay 1 + 0.5*2 = 2 (round to 2).
	src := dists.FuncSource(func() float64 { return 0.5 })
	nw := NewNetwork(sim, NetworkConfig{Name: "link", DelayMs: 1, JitterMs: 2, LinkSrc: src})
	nw.Transmit(Task{ID: 1}, sim.Now())
	// At t=1 (base delay), not yet due.
	if got := nw.DrainToTick(sim.Now().Add(1)); len(got) != 0 {
		t.Fatal("jitter should push delivery past base delay")
	}
	// At t=2 due.
	if got := nw.DrainToTick(sim.Now().Add(2)); len(got) != 1 {
		t.Fatal("delivery should occur at t=2 with jitter")
	}
}

func TestNetworkDeterminism(t *testing.T) {
	run := func() uint64 {
		sim := newSim(t)
		src := dists.FuncSource(func() float64 { return 0.5 })
		nw := NewNetwork(sim, NetworkConfig{Name: "link", DelayMs: 1, LinkSrc: src})
		for i := 0; i < 5; i++ {
			nw.Transmit(Task{ID: uint64(i)}, sim.Now())
		}
		return uint64(len(nw.DrainToTick(sim.Now().Add(5))))
	}
	if run() != run() {
		t.Fatal("determinism violation")
	}
}

func TestNetworkPanics(t *testing.T) {
	sim := newSim(t)
	src := dists.FuncSource(func() float64 { return 0.5 })
	tests := []struct {
		name string
		cfg  NetworkConfig
	}{
		{name: "negative delay", cfg: NetworkConfig{Name: "n", DelayMs: -1}},
		{name: "negative jitter", cfg: NetworkConfig{Name: "n", JitterMs: -1}},
		{name: "drop above 1", cfg: NetworkConfig{Name: "n", DropRate: 1.1, LinkSrc: src}},
		{name: "drop below 0", cfg: NetworkConfig{Name: "n", DropRate: -0.1, LinkSrc: src}},
		{name: "drop without source", cfg: NetworkConfig{Name: "n", DropRate: 0.5}},
		{name: "jitter without source", cfg: NetworkConfig{Name: "n", JitterMs: 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("expected panic")
				}
			}()
			NewNetwork(sim, tt.cfg)
		})
	}
}
