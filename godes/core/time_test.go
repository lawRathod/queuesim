package core

import (
	"math"
	"testing"
)

func TestNewSimTime_ClampsNegativeToZero(t *testing.T) {
	got := NewSimTime(-10)
	if got != SimTime(0) {
		t.Fatalf("expected 0, got %d", got)
	}
}

func TestSimTimeAdd_ClampsAtMax(t *testing.T) {
	start := NewSimTime(math.MaxInt64 - 2)
	got := start.Add(10)
	if got != SimTime(math.MaxInt64) {
		t.Fatalf("expected MaxInt64 clamp, got %d", got)
	}
}

func TestSimTimeSub_ClampsAtZero(t *testing.T) {
	start := NewSimTime(3)
	got := start.Sub(10)
	if got != SimTime(0) {
		t.Fatalf("expected zero clamp, got %d", got)
	}
}

func TestSimTimeAdd_MinInt64DoesNotRecurse(t *testing.T) {
	start := NewSimTime(10)
	got := start.Add(math.MinInt64)
	if got != SimTime(0) {
		t.Fatalf("expected zero clamp for MinInt64 delta, got %d", got)
	}
}

func TestSimTimeSub_MinInt64DoesNotRecurse(t *testing.T) {
	start := NewSimTime(10)
	got := start.Sub(math.MinInt64)
	if got != SimTime(math.MaxInt64) {
		t.Fatalf("expected max clamp for MinInt64 delta, got %d", got)
	}
}

func TestSimTimeMonotonicScheduleGuard(t *testing.T) {
	sim, err := NewSimulation(1, FIFOPolicy{})
	if err != nil {
		t.Fatalf("NewSimulation error: %v", err)
	}

	sim.ScheduleAt(NewSimTime(5), func(sim *Simulation) {
		now := sim.Now()
		if now != NewSimTime(5) {
			t.Fatalf("expected now=5, got %d", now)
		}
	})
	sim.Run()

	if sim.Now() != NewSimTime(5) {
		t.Fatalf("expected now to remain at last event time, got %d", sim.Now())
	}
}
