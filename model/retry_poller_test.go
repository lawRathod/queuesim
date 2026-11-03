package model

import (
	"math"
	"testing"

	"github.com/lawRathod/queuesim/godes/core"
)

// TestRetryBackoffDegenerateMath: non-finite coefficients collapse to the
// documented clamps — never NaN, never Inf, never negative.
func TestRetryBackoffDegenerateMath(t *testing.T) {
	nan := RetryController{InitialIntervalMs: 100, Coefficient: math.NaN()}
	if got := nan.BackoffMs(5); got != 60000 {
		t.Fatalf("NaN coefficient BackoffMs(5) = %d, want 60000", got)
	}
	inf := RetryController{InitialIntervalMs: 100, Coefficient: math.Inf(1)}
	if got := inf.BackoffMs(5); got != 60000 {
		t.Fatalf("+Inf coefficient BackoffMs(5) = %d, want 60000", got)
	}
	neg := RetryController{InitialIntervalMs: 100, Coefficient: -2.0}
	if got := neg.BackoffMs(2); got != 100 {
		t.Fatalf("negative coefficient BackoffMs(2) = %d, want 100 (lower clamp on odd exponent)", got)
	}
	zero := RetryController{InitialIntervalMs: 100, Coefficient: 0}
	if got := zero.BackoffMs(5); got != 100 {
		t.Fatalf("zero coefficient BackoffMs(5) = %d, want 100 (lower clamp)", got)
	}
}

// TestRetryBackoffMath: InitialIntervalMs * Coefficient^(attempt-1), clamped
// to [min(initial, 60000), 60000]. No RNG anywhere in the controller.
func TestRetryBackoffMath(t *testing.T) {
	c := RetryController{InitialIntervalMs: 100, Coefficient: 1.0, MaxAttempts: 0}
	if got := c.BackoffMs(1); got != 100 {
		t.Fatalf("BackoffMs(1) = %d, want 100", got)
	}
	if got := c.BackoffMs(5); got != 100 {
		t.Fatalf("BackoffMs(5) = %d, want 100 (coeff 1.0)", got)
	}

	c2 := RetryController{InitialIntervalMs: 100, Coefficient: 2.0, MaxAttempts: 0}
	want := []int64{100, 200, 400, 800, 1600} // initial * coeff^(attempt-1)
	for attempt := 1; attempt <= 5; attempt++ {
		if got := c2.BackoffMs(int32(attempt)); got != want[attempt-1] {
			t.Fatalf("coeff 2.0 BackoffMs(%d) = %d, want %d", attempt, got, want[attempt-1])
		}
	}
}

// TestRetryBackoffClamp: backoff never leaves [initial, 60000].
func TestRetryBackoffClamp(t *testing.T) {
	// Upper clamp.
	c := RetryController{InitialIntervalMs: 100, Coefficient: 3.0}
	if got := c.BackoffMs(20); got != 60000 {
		t.Fatalf("BackoffMs(20) = %d, want 60000 (clamped)", got)
	}
	// Initial above the cap: both bounds collapse to 60000.
	c2 := RetryController{InitialIntervalMs: 120000, Coefficient: 1.0}
	if got := c2.BackoffMs(1); got != 60000 {
		t.Fatalf("BackoffMs(1) = %d, want 60000 (initial capped)", got)
	}
	// Degenerate: zero initial → immediate retry, no backoff.
	c3 := RetryController{InitialIntervalMs: 0, Coefficient: 2.0}
	if got := c3.BackoffMs(3); got != 0 {
		t.Fatalf("BackoffMs(3) = %d, want 0 (zero initial)", got)
	}
}

// TestRetryCanRetry: MaxAttempts=0 is unlimited; otherwise the next attempt
// is allowed only while the completed attempt count is below the cap.
func TestRetryCanRetry(t *testing.T) {
	unlimited := RetryController{InitialIntervalMs: 100, Coefficient: 1.0, MaxAttempts: 0}
	if !unlimited.CanRetry(1) || !unlimited.CanRetry(1000) {
		t.Fatal("MaxAttempts=0 must be unlimited")
	}
	three := RetryController{InitialIntervalMs: 100, Coefficient: 1.0, MaxAttempts: 3}
	if !three.CanRetry(1) || !three.CanRetry(2) {
		t.Fatal("attempts 1-2 must be retryable under MaxAttempts=3")
	}
	if three.CanRetry(3) {
		t.Fatal("attempt 3 must NOT be retryable under MaxAttempts=3")
	}
}

// TestPollerCadence: a poller self-reschedules every PollIntervalMs until
// EndMs and calls take on each poll (respecting the in-flight budget).
func TestPollerCadence(t *testing.T) {
	sim, err := core.NewSimulation(1, core.FIFOPolicy{})
	if err != nil {
		t.Fatalf("NewSimulation: %v", err)
	}
	var polls []int64
	p := NewPoller(sim, PollerConfig{
		Name: "p0", PollIntervalMs: 10, InFlightBudget: 1, EndMs: 50,
	}, func(now core.SimTime) bool {
		polls = append(polls, now.Int64())
		return false // never takes: cadence-only observation
	})
	p.Start()
	sim.Run()
	want := []int64{0, 10, 20, 30, 40} // no poll at or after EndMs=50
	if len(polls) != len(want) {
		t.Fatalf("polls = %v, want %v", polls, want)
	}
	for i := range want {
		if polls[i] != want[i] {
			t.Fatalf("polls = %v, want %v", polls, want)
		}
	}
	if p.PollCount() != 5 {
		t.Fatalf("PollCount = %d, want 5", p.PollCount())
	}
}

// TestPollerBudget: take is not called once in-flight reaches the budget;
// Release frees a slot.
func TestPollerBudget(t *testing.T) {
	sim, err := core.NewSimulation(1, core.FIFOPolicy{})
	if err != nil {
		t.Fatalf("NewSimulation: %v", err)
	}
	taken := 0
	p := NewPoller(sim, PollerConfig{
		Name: "p0", PollIntervalMs: 10, InFlightBudget: 2, EndMs: 30,
	}, func(now core.SimTime) bool {
		taken++
		return true
	})
	p.Start()
	sim.Run()
	if taken != 2 { // budget 2: only two polls may take before Release
		t.Fatalf("taken = %d, want 2 (budget cap)", taken)
	}
	if p.InFlight() != 2 {
		t.Fatalf("InFlight = %d, want 2", p.InFlight())
	}
	p.Release()
	p.Release()
	if p.InFlight() != 0 {
		t.Fatalf("InFlight after release = %d, want 0", p.InFlight())
	}
	// Release below zero is a no-op.
	p.Release()
	if p.InFlight() != 0 {
		t.Fatalf("InFlight after over-release = %d, want 0", p.InFlight())
	}
}

// TestPollerDeterminism: same seed, same config → identical poll sequence.
func TestPollerDeterminism(t *testing.T) {
	run := func() []int64 {
		sim, _ := core.NewSimulation(9, core.FIFOPolicy{})
		var polls []int64
		p := NewPoller(sim, PollerConfig{Name: "p", PollIntervalMs: 7, InFlightBudget: 3, EndMs: 100}, func(now core.SimTime) bool {
			polls = append(polls, now.Int64())
			return now.Int64()%21 == 0
		})
		p.Start()
		sim.Run()
		return polls
	}
	first := run()
	for i := 0; i < 5; i++ {
		got := run()
		if len(got) != len(first) {
			t.Fatalf("run %d: poll count %d vs %d", i, len(got), len(first))
		}
		for j := range got {
			if got[j] != first[j] {
				t.Fatalf("run %d: poll[%d] = %d vs %d", i, j, got[j], first[j])
			}
		}
	}
}
