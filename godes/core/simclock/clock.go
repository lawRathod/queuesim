package simclock

import (
	"context"
	"time"
)

// Clock is the single source of truth for simulation time.
// Any system that uses this interface instead of the standard
// library's time.Now / context.WithDeadline / time.After can
// run deterministically inside a GoDES simulation.
type Clock interface {
	// Now returns the current time. For SimClock, this is sim-time
	// encoded as time.Time (Unix epoch + sim-ticks-as-milliseconds).
	// For WallClock, this delegates to time.Now().
	Now() time.Time

	// Since returns the time elapsed since t. For SimClock, this is
	// sim-time elapsed; for WallClock, it delegates to time.Since.
	Since(t time.Time) time.Duration

	// NewTimer creates a timer that fires after duration d.
	// The returned Timer supports Stop() and Reset() semantics
	// equivalent to time.Timer.
	NewTimer(d time.Duration) *Timer

	// NewTicker creates a ticker that fires every duration d.
	// The returned Ticker supports Stop() semantics.
	NewTicker(d time.Duration) *Ticker

	// Sleep pauses the caller until duration d has elapsed.
	// For SimClock, this advances the simulation; for WallClock,
	// it delegates to time.Sleep.
	Sleep(d time.Duration)

	// WithDeadline returns a context that is cancelled at the given
	// deadline. For SimClock, the deadline is checked in sim-time;
	// for WallClock, it delegates to context.WithDeadline.
	WithDeadline(parent context.Context, deadline time.Time) (context.Context, context.CancelFunc)

	// WithTimeout returns a context that is cancelled after the given
	// timeout duration. For SimClock, the timeout is in sim-time;
	// for WallClock, it delegates to context.WithTimeout.
	WithTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc)
}

// Timer is the simclock equivalent of time.Timer.
// For WallClock, it wraps a real time.Timer.
// For SimClock, it fires when the simulation advances past the target time.
type Timer struct {
	// C delivers the time at which the timer fired.
	C <-chan time.Time

	// stop prevents the timer from firing. Returns true if the call
	// stops the timer, false if it was already expired or stopped.
	stop func() bool

	// reset reschedules the timer to fire after duration d.
	reset func(d time.Duration) bool
}

// Stop prevents the Timer from firing. It returns true if the call
// stops the timer, false if the timer was already expired or stopped.
func (t *Timer) Stop() bool {
	if t == nil || t.stop == nil {
		return false
	}
	return t.stop()
}

// Reset changes the timer to fire after duration d. It returns true
// if the timer had been active, false if it had expired or been stopped.
func (t *Timer) Reset(d time.Duration) bool {
	if t == nil || t.reset == nil {
		return false
	}
	return t.reset(d)
}

// Ticker is the simclock equivalent of time.Ticker.
// For WallClock, it wraps a real time.Ticker.
// For SimClock, it fires each time the simulation advances past a tick boundary.
type Ticker struct {
	// C delivers the time at each tick.
	C <-chan time.Time

	// stop halts the ticker.
	stop func()

	// reset reschedules the ticker to fire every duration d.
	reset func(d time.Duration)
}

// Stop turns off the ticker. After Stop, no more ticks will be sent.
func (t *Ticker) Stop() {
	if t == nil || t.stop == nil {
		return
	}
	t.stop()
}

// Reset stops the ticker and resets it to fire every duration d.
func (t *Ticker) Reset(d time.Duration) {
	if t == nil || t.reset == nil {
		return
	}
	t.reset(d)
}

// WallClock delegates all time operations to the real time and context
// packages. Use WallClock for production code that runs outside a
// simulation.
type WallClock struct{}

// Compile-time check that WallClock implements Clock.
var _ Clock = WallClock{}

// Now returns the current wall-clock time via time.Now().
func (WallClock) Now() time.Time { return time.Now() }

// Since returns the wall-clock duration since t via time.Since(t).
func (WallClock) Since(t time.Time) time.Duration { return time.Since(t) }

// NewTimer creates a real time.Timer and wraps it in a simclock Timer.
func (WallClock) NewTimer(d time.Duration) *Timer {
	rt := time.NewTimer(d)
	return &Timer{
		C: rt.C,
		stop: func() bool {
			return rt.Stop()
		},
		reset: func(d time.Duration) bool {
			return rt.Reset(d)
		},
	}
}

// NewTicker creates a real time.Ticker and wraps it in a simclock Ticker.
func (WallClock) NewTicker(d time.Duration) *Ticker {
	rt := time.NewTicker(d)
	return &Ticker{
		C: rt.C,
		stop: func() {
			rt.Stop()
		},
		reset: func(d time.Duration) {
			rt.Reset(d)
		},
	}
}

// Sleep pauses the current goroutine for duration d via time.Sleep.
func (WallClock) Sleep(d time.Duration) { time.Sleep(d) }

// WithDeadline returns a context cancelled at the given wall-clock deadline
// via context.WithDeadline.
func (WallClock) WithDeadline(parent context.Context, deadline time.Time) (context.Context, context.CancelFunc) {
	return context.WithDeadline(parent, deadline)
}

// WithTimeout returns a context cancelled after the given wall-clock timeout
// via context.WithTimeout.
func (WallClock) WithTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, timeout)
}
