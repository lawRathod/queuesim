package simclock

import (
	"context"
	"time"
)

// deadlineContext is a context.Context whose Err() reports whether the
// configured sim-time deadline has been crossed. The standard
// context.WithDeadline uses wall time, which doesn't match SimClock's
// "sim time as wall time" encoding. This custom context is the
// sim-time deadline check (ADR-042).
//
// Err() returns DeadlineExceeded iff the Clock has advanced past the
// configured Deadline. Done() returns nil because the deadline is
// driven by sim-time advance, not by a separate goroutine.
type deadlineContext struct {
	clock    Clock
	deadline time.Time
	parent   context.Context
}

// newDeadlineContext creates a context.Context whose Err() reports
// DeadlineExceeded once the clock crosses the deadline. The returned
// CancelFunc is a no-op because the deadline is sim-time-driven:
// cancellation can only occur when the simulation advances past the
// deadline, not via an external cancel call.
func newDeadlineContext(clk Clock, deadline time.Time, parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	dc := &deadlineContext{
		clock:    clk,
		deadline: deadline,
		parent:   parent,
	}
	return dc, func() {}
}

// Deadline returns the configured deadline and true.
func (c *deadlineContext) Deadline() (time.Time, bool) {
	if c == nil {
		return time.Time{}, false
	}
	return c.deadline, true
}

// Done returns nil. The sim-time deadline context is never cancelled
// by an external party; only the Clock's advance can trigger the
// deadline. Channels would require a separate goroutine to signal,
// which would violate the "no goroutines in the hot path" invariant.
func (c *deadlineContext) Done() <-chan struct{} { return nil }

// Err returns DeadlineExceeded iff the Clock (sim time) has crossed
// the configured deadline. If the parent context has an error (e.g.,
// Canceled), that error is returned instead.
func (c *deadlineContext) Err() error {
	if c == nil {
		return nil
	}
	// Check parent first.
	if err := c.parent.Err(); err != nil {
		return err
	}
	if c.clock.Now().After(c.deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

// Value delegates to the parent context.
func (c *deadlineContext) Value(key any) any {
	if c == nil || c.parent == nil {
		return nil
	}
	return c.parent.Value(key)
}
