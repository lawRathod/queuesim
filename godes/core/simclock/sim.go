package simclock

import (
	"context"
	"time"

	"github.com/lawRathod/queuesim/godes/core"
)

// SimClock implements Clock using a core.Simulation for all time operations.
// Time is encoded as milliseconds since the Unix epoch: sim tick N corresponds
// to time.UnixMilli(N). This allows SimClock to satisfy the Clock interface
// while keeping all time deterministic and replayable.
//
// A zero-value SimClock (with nil Simulation) will panic on use; always
// construct via NewSimClock.
type SimClock struct {
	sim *core.Simulation
}

// NewSimClock creates a SimClock backed by sim. Panics if sim is nil.
func NewSimClock(sim *core.Simulation) *SimClock {
	if sim == nil {
		panic("simclock: NewSimClock called with nil Simulation")
	}
	return &SimClock{sim: sim}
}

// Now returns the current simulation time as a time.Time.
// Sim tick N is encoded as time.UnixMilli(N).
func (c *SimClock) Now() time.Time {
	return time.UnixMilli(int64(c.sim.Now()))
}

// Since returns the sim-time elapsed since t. If t is in the future
// (relative to the current sim time), Since returns 0.
func (c *SimClock) Since(t time.Time) time.Duration {
	now := c.Now()
	if t.After(now) {
		return 0
	}
	return now.Sub(t)
}

// Sleep advances the simulation by the given duration. The duration is
// truncated to milliseconds (1 tick = 1ms); sub-millisecond durations
// are no-ops. Negative and zero durations are also no-ops.
func (c *SimClock) Sleep(d time.Duration) {
	ms := durationToMilliseconds(d)
	if ms <= 0 {
		return
	}
	c.sim.Advance(ms)
}

// NewTimer creates a Timer that fires after d sim-time has elapsed.
// The timer is implemented as a DES event scheduled at sim.Now() + d.
func (c *SimClock) NewTimer(d time.Duration) *Timer {
	return newSimTimer(c, d)
}

// NewTicker creates a Ticker that fires every d sim-time.
// The ticker is implemented as a chain of DES events.
func (c *SimClock) NewTicker(d time.Duration) *Ticker {
	return newSimTicker(c, d)
}

// WithDeadline returns a context.Context whose Err() reports
// context.DeadlineExceeded once the sim-time clock crosses deadline.
// The standard context.WithDeadline uses wall time; this uses sim time.
func (c *SimClock) WithDeadline(parent context.Context, deadline time.Time) (context.Context, context.CancelFunc) {
	return newDeadlineContext(c, deadline, parent)
}

// WithTimeout returns a context.Context whose Err() reports
// context.DeadlineExceeded once the sim-time clock advances past
// Now() + timeout.
func (c *SimClock) WithTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return c.WithDeadline(parent, c.Now().Add(timeout))
}

// durationToMilliseconds converts a time.Duration to milliseconds,
// saturating at 0 for negative durations.
func durationToMilliseconds(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64(d / time.Millisecond)
}
