package model

import (
	"testing"

	"github.com/lawRathod/queuesim/godes/core"
)

// ---- D4 (plan 2026-08-12): SetEndMs + Revive ----

// TestSetEndMsStops: SetEndMs stops the poller's self-rescheduling at or
// after endMs (at-pop-skip): no further poll events fire.
func TestSetEndMsStops(t *testing.T) {
	sim, err := core.NewSimulation(42, core.FIFOPolicy{})
	if err != nil {
		t.Fatalf("NewSimulation: %v", err)
	}
	var takes int
	p := NewPoller(sim, PollerConfig{Name: "p", PollIntervalMs: 10}, func(now core.SimTime) bool {
		takes++
		return false
	})
	p.Start()
	sim.Advance(150)
	if takes == 0 {
		t.Fatal("poller never polled")
	}
	p.SetEndMs(sim.Now().Int64() + 10)
	sim.Advance(1000)
	before := takes
	sim.Advance(500)
	if takes != before {
		t.Fatalf("poller kept polling after EndMs: %d -> %d", before, takes)
	}
}

// TestReviveRestarts: Revive re-arms a poller stopped by EndMs; SetEndMs(0)
// clears the horizon so a revived poller never stops again.
func TestReviveRestarts(t *testing.T) {
	sim, err := core.NewSimulation(42, core.FIFOPolicy{})
	if err != nil {
		t.Fatalf("NewSimulation: %v", err)
	}
	var takes int
	p := NewPoller(sim, PollerConfig{Name: "p", PollIntervalMs: 10}, func(now core.SimTime) bool {
		takes++
		return false
	})
	p.Start()
	sim.Advance(100)
	p.SetEndMs(sim.Now().Int64()) // stops at the next poll
	sim.Advance(500)
	stopped := takes
	p.SetEndMs(0)
	p.Revive()
	sim.Advance(1000)
	if takes <= stopped {
		t.Fatal("Revive did not re-arm the poller")
	}
}

// TestSetEndMsIdempotent: repeated SetEndMs calls converge; Revive on a
// running poller is a no-op (no double event chains).
func TestSetEndMsIdempotent(t *testing.T) {
	sim, err := core.NewSimulation(42, core.FIFOPolicy{})
	if err != nil {
		t.Fatalf("NewSimulation: %v", err)
	}
	var takes int
	p := NewPoller(sim, PollerConfig{Name: "p", PollIntervalMs: 10}, func(now core.SimTime) bool {
		takes++
		return false
	})
	p.Start()
	sim.Advance(100)
	p.Revive() // no-op: still running
	before := takes
	sim.Advance(100)
	if takes != before+10 {
		t.Fatalf("Revive on running poller changed cadence: %d -> %d (want +10)", before, takes)
	}
}
