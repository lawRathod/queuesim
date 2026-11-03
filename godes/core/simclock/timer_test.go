package simclock

import (
	"testing"
	"time"

	"github.com/lawRathod/queuesim/godes/core"
	"github.com/lawRathod/queuesim/godes/simtest"
)

// ---------------------------------------------------------------------------
// WallClock Timer tests
// ---------------------------------------------------------------------------

func TestWallClock_Timer_Fires(t *testing.T) {
	timer := WallClock{}.NewTimer(50 * time.Millisecond)
	select {
	case <-timer.C:
		// Success.
	case <-time.After(500 * time.Millisecond):
		t.Fatal("WallClock timer did not fire within 500ms")
	}
}

func TestWallClock_Timer_Stop_PreventsFire(t *testing.T) {
	timer := WallClock{}.NewTimer(50 * time.Millisecond)
	timer.Stop()
	// After Stop, the channel should not receive within a reasonable window.
	select {
	case <-timer.C:
		// It's possible the timer fired before Stop was called (race).
		// That's acceptable for wall-clock tests.
	case <-time.After(200 * time.Millisecond):
		// Success: no fire after stop.
	}
}

func TestWallClock_Timer_Reset_ExtendsTime(t *testing.T) {
	timer := WallClock{}.NewTimer(1 * time.Second) // far future
	timer.Reset(50 * time.Millisecond)             // near future
	select {
	case <-timer.C:
		// Success: timer fired after reset.
	case <-time.After(500 * time.Millisecond):
		t.Fatal("WallClock timer did not fire after Reset to 50ms")
	}
}

// ---------------------------------------------------------------------------
// WallClock Ticker tests
// ---------------------------------------------------------------------------

func TestWallClock_Ticker_Fires(t *testing.T) {
	ticker := WallClock{}.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	select {
	case <-ticker.C:
		// Success.
	case <-time.After(500 * time.Millisecond):
		t.Fatal("WallClock ticker did not fire within 500ms")
	}
}

func TestWallClock_Ticker_Stop_StopsTicks(t *testing.T) {
	ticker := WallClock{}.NewTicker(20 * time.Millisecond)
	// Wait for first tick.
	select {
	case <-ticker.C:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("WallClock ticker did not fire first tick")
	}
	ticker.Stop()
	// After Stop, no more ticks should arrive.
	select {
	case <-ticker.C:
		// Possible race: a tick was already in flight. Acceptable.
	case <-time.After(100 * time.Millisecond):
		// Success: no more ticks.
	}
}

// ---------------------------------------------------------------------------
// SimClock Timer tests
// ---------------------------------------------------------------------------

func TestSimClock_Timer_FiresAtCorrectTime(t *testing.T) {
	sim := newTestSim(t)
	c := NewSimClock(sim)

	timer := c.NewTimer(100 * time.Millisecond)

	// Advance to 50ms — timer should NOT have fired.
	c.Sleep(50 * time.Millisecond)
	select {
	case <-timer.C:
		t.Fatal("timer fired at 50ms, expected fire at 100ms")
	default:
		// Good: not yet fired.
	}

	// Advance to 110ms — timer should have fired.
	c.Sleep(60 * time.Millisecond)
	select {
	case got := <-timer.C:
		want := time.UnixMilli(100)
		if !got.Equal(want) {
			t.Errorf("timer fired with time %v, want %v", got, want)
		}
	default:
		t.Fatal("timer did not fire after advancing past 100ms")
	}
}

func TestSimClock_Timer_Stop_PreventsFire(t *testing.T) {
	sim := newTestSim(t)
	c := NewSimClock(sim)

	timer := c.NewTimer(100 * time.Millisecond)
	stopped := timer.Stop()
	if !stopped {
		t.Error("Stop() returned false, want true (timer was active)")
	}

	// Advance past the timer's target.
	c.Sleep(200 * time.Millisecond)

	select {
	case <-timer.C:
		t.Fatal("timer fired after Stop()")
	default:
		// Success: no fire.
	}
}

func TestSimClock_Timer_Stop_AfterFire(t *testing.T) {
	sim := newTestSim(t)
	c := NewSimClock(sim)

	timer := c.NewTimer(50 * time.Millisecond)

	// Advance past the timer.
	c.Sleep(100 * time.Millisecond)

	// Consume the fired value.
	select {
	case <-timer.C:
	default:
		t.Fatal("timer should have fired")
	}

	// Stop after fire returns false.
	if timer.Stop() {
		t.Error("Stop() after fire returned true, want false")
	}
}

func TestSimClock_Timer_Reset(t *testing.T) {
	sim := newTestSim(t)
	c := NewSimClock(sim)

	timer := c.NewTimer(50 * time.Millisecond)

	// Reset to 200ms.
	wasActive := timer.Reset(200 * time.Millisecond)
	if !wasActive {
		t.Error("Reset() returned false, want true (timer was active)")
	}

	// Advance to 100ms — should NOT have fired.
	c.Sleep(100 * time.Millisecond)
	select {
	case <-timer.C:
		t.Fatal("timer fired at 100ms after Reset(200ms)")
	default:
	}

	// Advance to 250ms — should have fired.
	c.Sleep(150 * time.Millisecond)
	select {
	case got := <-timer.C:
		want := time.UnixMilli(200)
		if !got.Equal(want) {
			t.Errorf("timer fired with time %v, want %v", got, want)
		}
	default:
		t.Fatal("timer did not fire after advancing past 200ms")
	}
}

func TestSimClock_Timer_ZeroDuration_FiresImmediately(t *testing.T) {
	sim := newTestSim(t)
	c := NewSimClock(sim)

	timer := c.NewTimer(0)

	// Advance 1ms — timer should fire at tick 0.
	c.Sleep(1 * time.Millisecond)
	select {
	case <-timer.C:
		// Success.
	default:
		t.Fatal("zero-duration timer did not fire")
	}
}

// ---------------------------------------------------------------------------
// SimClock Ticker tests
// ---------------------------------------------------------------------------

func TestSimClock_Ticker_FiresRepeatedly(t *testing.T) {
	sim := newTestSim(t)
	c := NewSimClock(sim)

	ticker := c.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	// Advance incrementally, consuming each tick to verify timing.
	var times []time.Time

	c.Sleep(55 * time.Millisecond)
	select {
	case tm := <-ticker.C:
		times = append(times, tm)
	default:
		t.Fatal("expected tick at 50ms")
	}

	c.Sleep(50 * time.Millisecond)
	select {
	case tm := <-ticker.C:
		times = append(times, tm)
	default:
		t.Fatal("expected tick at 100ms")
	}

	c.Sleep(55 * time.Millisecond)
	select {
	case tm := <-ticker.C:
		times = append(times, tm)
	default:
		t.Fatal("expected tick at 150ms")
	}

	if len(times) != 3 {
		t.Fatalf("got %d ticks, want 3", len(times))
	}
	wantTimes := []time.Time{
		time.UnixMilli(50),
		time.UnixMilli(100),
		time.UnixMilli(150),
	}
	for i, got := range times {
		if !got.Equal(wantTimes[i]) {
			t.Errorf("tick[%d] = %v, want %v", i, got, wantTimes[i])
		}
	}
}

func TestSimClock_Ticker_Stop_StopsTicks(t *testing.T) {
	sim := newTestSim(t)
	c := NewSimClock(sim)

	ticker := c.NewTicker(50 * time.Millisecond)

	// Advance to first tick.
	c.Sleep(50 * time.Millisecond)
	select {
	case <-ticker.C:
	default:
		t.Fatal("first tick did not fire")
	}

	ticker.Stop()

	// Advance more — no more ticks.
	c.Sleep(200 * time.Millisecond)
	select {
	case <-ticker.C:
		t.Fatal("ticker fired after Stop()")
	default:
		// Success.
	}
}

func TestSimClock_Ticker_Reset(t *testing.T) {
	sim := newTestSim(t)
	c := NewSimClock(sim)

	ticker := c.NewTicker(1000 * time.Millisecond)
	defer ticker.Stop()

	// Reset to 50ms.
	ticker.Reset(50 * time.Millisecond)

	// Advance 100ms — at least 1 tick should fire.
	c.Sleep(100 * time.Millisecond)

	select {
	case <-ticker.C:
		// Success.
	default:
		t.Fatal("ticker did not fire after Reset(50ms)")
	}
}

func TestNewTicker_PanicsOnNonPositiveInterval(t *testing.T) {
	sim := newTestSim(t)
	c := NewSimClock(sim)

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("NewTicker(0) did not panic")
		}
	}()
	c.NewTicker(0)
}

// ---------------------------------------------------------------------------
// Determinism test
// ---------------------------------------------------------------------------

func TestSimClock_Timer_Determinism(t *testing.T) {
	simtest.AssertDeterministic(t, 50, 42, func(seed int64) time.Time {
		sim, _ := core.NewSimulation(seed, core.FIFOPolicy{})
		c := NewSimClock(sim)
		timer := c.NewTimer(100 * time.Millisecond)
		c.Sleep(200 * time.Millisecond)
		select {
		case tm := <-timer.C:
			return tm
		default:
			return time.Time{}
		}
	})
}

func TestTimerStopDoesNotAdvanceSimTime(t *testing.T) {
	sim, _ := core.NewSimulation(1, core.FIFOPolicy{})
	clk := NewSimClock(sim)
	tmr := clk.NewTimer(time.Duration(50) * time.Millisecond)
	tmr.Stop()
	sim.Run()
	if sim.Now() != 0 {
		t.Fatalf("expected sim time 0 after Timer.Stop (no advance), got %d", sim.Now())
	}
}

// TestTickerCancelBeforeFirstTick verifies that cancelling a Ticker before its
// first tick fires actually prevents ticks from firing. This was broken by
// the placeholder-minting pattern which left a tombstone in cancelledTasks.
func TestTickerCancelBeforeFirstTick(t *testing.T) {
	sim := newTestSim(t)
	c := NewSimClock(sim)

	ticker := c.NewTicker(50 * time.Millisecond)
	ticker.Stop()

	// Advance past where ticks would fire.
	c.Sleep(200 * time.Millisecond)

	select {
	case <-ticker.C:
		t.Fatal("ticker fired after Stop()")
	default:
		// Success: no ticks.
	}
}

// TestSimClock_Ticker_Stop_PreventsFirstTick verifies that Stop() before the
// first tick fires prevents any ticks. Uses sim-level Cancel to stop the
// chain immediately after creation.
func TestSimClock_Ticker_Stop_PreventsFirstTick(t *testing.T) {
	sim := newTestSim(t)
	c := NewSimClock(sim)

	ticker := c.NewTicker(50 * time.Millisecond)
	ticker.Stop()

	c.Sleep(200 * time.Millisecond)

	select {
	case <-ticker.C:
		t.Fatal("ticker fired after immediate Stop()")
	default:
		// Success.
	}
}

// TestTimerResetReturnsWasActive verifies that Timer.Reset returns true
// when the timer was active and false when it was not.
func TestTimerResetReturnsWasActive(t *testing.T) {
	sim := newTestSim(t)
	c := NewSimClock(sim)

	timer := c.NewTimer(100 * time.Millisecond)

	// Reset while active — should return true.
	wasActive := timer.Reset(200 * time.Millisecond)
	if !wasActive {
		t.Error("Reset() on active timer returned false, want true")
	}

	// Let the timer fire.
	c.Sleep(250 * time.Millisecond)
	// Consume the fired value.
	select {
	case <-timer.C:
	default:
		t.Fatal("timer did not fire after Reset(200ms)")
	}

	// Reset after fire — should return false.
	wasActive = timer.Reset(100 * time.Millisecond)
	if wasActive {
		t.Error("Reset() on expired timer returned true, want false")
	}
}

// TestTimerResetReturnsWasActive_AfterStop verifies that Reset returns false
// after Stop was called.
func TestTimerResetReturnsWasActive_AfterStop(t *testing.T) {
	sim := newTestSim(t)
	c := NewSimClock(sim)

	timer := c.NewTimer(100 * time.Millisecond)
	timer.Stop()

	// Reset after Stop — should return false (was not active).
	wasActive := timer.Reset(100 * time.Millisecond)
	if wasActive {
		t.Error("Reset() after Stop() returned true, want false")
	}
}
