package simclock

import (
	"testing"
	"time"

	"github.com/lawRathod/queuesim/godes/core"
	"github.com/lawRathod/queuesim/godes/simtest"
)

// newTestSim creates a core.Simulation for testing. Fails the test on error.
func newTestSim(t *testing.T) *core.Simulation {
	t.Helper()
	sim, err := core.NewSimulation(42, core.FIFOPolicy{})
	if err != nil {
		t.Fatalf("core.NewSimulation: %v", err)
	}
	return sim
}

func TestSimClock_Now_ReturnsZeroAtStart(t *testing.T) {
	sim := newTestSim(t)
	c := NewSimClock(sim)
	got := c.Now()
	want := time.UnixMilli(0)
	if !got.Equal(want) {
		t.Errorf("SimClock.Now() at start = %v, want %v", got, want)
	}
}

func TestSimClock_Now_AdvancesWithSim(t *testing.T) {
	sim := newTestSim(t)
	c := NewSimClock(sim)

	// Schedule an event at tick 100 and run.
	sim.ScheduleAt(core.NewSimTime(100), func(*core.Simulation) {})
	sim.Run()

	got := c.Now()
	want := time.UnixMilli(100)
	if !got.Equal(want) {
		t.Errorf("SimClock.Now() after advance to 100 = %v, want %v", got, want)
	}
}

func TestSimClock_Since_PositiveDuration(t *testing.T) {
	sim := newTestSim(t)
	c := NewSimClock(sim)

	start := c.Now() // tick 0
	sim.ScheduleAt(core.NewSimTime(500), func(*core.Simulation) {})
	sim.Run()

	got := c.Since(start)
	want := 500 * time.Millisecond
	if got != want {
		t.Errorf("SimClock.Since(start) = %v, want %v", got, want)
	}
}

func TestSimClock_Since_FutureTimeReturnsZero(t *testing.T) {
	sim := newTestSim(t)
	c := NewSimClock(sim)

	// Sim is at tick 0; ask Since a time in the future (tick 1000).
	future := time.UnixMilli(1000)
	got := c.Since(future)
	if got != 0 {
		t.Errorf("SimClock.Since(future) = %v, want 0", got)
	}
}

func TestSimClock_Sleep_AdvancesSim(t *testing.T) {
	sim := newTestSim(t)
	c := NewSimClock(sim)

	c.Sleep(50 * time.Millisecond)

	got := c.Now()
	want := time.UnixMilli(50)
	if !got.Equal(want) {
		t.Errorf("after Sleep(50ms), Now() = %v, want %v", got, want)
	}
}

func TestSimClock_Sleep_FiresScheduledEvents(t *testing.T) {
	sim := newTestSim(t)
	c := NewSimClock(sim)

	fired := false
	sim.ScheduleAt(core.NewSimTime(30), func(*core.Simulation) { fired = true })

	c.Sleep(50 * time.Millisecond)

	if !fired {
		t.Error("Sleep(50ms) did not fire event scheduled at tick 30")
	}
	got := c.Now()
	want := time.UnixMilli(50)
	if !got.Equal(want) {
		t.Errorf("after Sleep, Now() = %v, want %v", got, want)
	}
}

func TestSimClock_Sleep_NegativeDurationNoop(t *testing.T) {
	sim := newTestSim(t)
	c := NewSimClock(sim)

	c.Sleep(-5 * time.Millisecond)

	got := c.Now()
	want := time.UnixMilli(0)
	if !got.Equal(want) {
		t.Errorf("after Sleep(-5ms), Now() = %v, want %v", got, want)
	}
}

func TestSimClock_Sleep_ZeroDurationNoop(t *testing.T) {
	sim := newTestSim(t)
	c := NewSimClock(sim)

	c.Sleep(0)

	got := c.Now()
	want := time.UnixMilli(0)
	if !got.Equal(want) {
		t.Errorf("after Sleep(0), Now() = %v, want %v", got, want)
	}
}

func TestNewSimClock_ImplementsClockInterface(t *testing.T) {
	sim := newTestSim(t)
	c := NewSimClock(sim)
	var _ Clock = c // compile-time check
}

func TestNewSimClock_PanicsOnNil(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("NewSimClock(nil) did not panic")
		}
	}()
	NewSimClock(nil)
}

func TestDurationToMilliseconds(t *testing.T) {
	tests := []struct {
		name string
		d    time.Duration
		want int64
	}{
		{"zero", 0, 0},
		{"1ms", 1 * time.Millisecond, 1},
		{"100ms", 100 * time.Millisecond, 100},
		{"1s", 1 * time.Second, 1000},
		{"999us", 999 * time.Microsecond, 0},
		{"1500us", 1500 * time.Microsecond, 1},
		{"negative", -5 * time.Millisecond, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := durationToMilliseconds(tt.d)
			if got != tt.want {
				t.Errorf("durationToMilliseconds(%v) = %d, want %d", tt.d, got, tt.want)
			}
		})
	}
}

func TestSimClock_Determinism(t *testing.T) {
	simtest.AssertDeterministic(t, 50, 42, func(seed int64) time.Time {
		sim, _ := core.NewSimulation(seed, core.FIFOPolicy{})
		c := NewSimClock(sim)
		sim.ScheduleAt(core.NewSimTime(100), func(*core.Simulation) {})
		sim.Run()
		return c.Now()
	})
}
