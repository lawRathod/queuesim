package simclock

import (
	"time"

	"github.com/lawRathod/queuesim/godes/core"
)

// newSimTimer creates a Timer backed by a DES event. The timer fires
// when the simulation advances to sim.Now() + d. Stop cancels the
// event via sim.Cancel (at-pop-skip, ADR-044); Reset reschedules.
func newSimTimer(clk *SimClock, d time.Duration) *Timer {
	// Buffer size 1 with non-blocking send: prevents blocking the sim's
	// event handler if no consumer is reading from the channel. Matches
	// time.Timer behavior where a missed tick is dropped.
	ch := make(chan time.Time, 1)

	ms := durationToMilliseconds(d)
	target := clk.sim.Now().Add(ms)
	handle := clk.sim.ScheduleAtEx(target, func(s *core.Simulation) {
		// Non-blocking send.
		select {
		case ch <- clk.Now():
		default:
		}
	}, core.ScheduleOptions{Kind: core.KindTask})

	t := &Timer{C: ch}
	t.stop = func() bool {
		return clk.sim.Cancel(handle)
	}
	t.reset = func(d time.Duration) bool {
		// Cancel the old event (at-pop-skip); returns whether it was active.
		wasActive := clk.sim.Cancel(handle)
		// Schedule a new event.
		ms := durationToMilliseconds(d)
		target := clk.sim.Now().Add(ms)
		handle = clk.sim.ScheduleAtEx(target, func(s *core.Simulation) {
			select {
			case ch <- clk.Now():
			default:
			}
		}, core.ScheduleOptions{Kind: core.KindTask})
		return wasActive
	}
	return t
}

// newSimTicker creates a Ticker backed by chained DES events. The
// ticker fires every d sim-time units. Stop cancels the chain via
// sim.Cancel (at-pop-skip); Reset stops the old chain and starts a
// new one. Panics if d <= 0.
func newSimTicker(clk *SimClock, d time.Duration) *Ticker {
	if d <= 0 {
		panic("simclock: NewTicker requires positive interval")
	}

	ch := make(chan time.Time, 1)

	// chainID is minted from the first real schedule call, avoiding
	// placeholder tombstones that would block Cancel before the first tick.
	var chainID core.TaskID

	var scheduleNext func(interval time.Duration)
	scheduleNext = func(interval time.Duration) {
		ms := durationToMilliseconds(interval)
		if ms <= 0 {
			ms = 1 // minimum 1 tick to avoid infinite loop
		}
		target := clk.sim.Now().Add(ms)
		opts := core.ScheduleOptions{Kind: core.KindTask}
		if chainID != 0 {
			opts.TaskID = chainID // re-schedule under same ID for whole-chain Cancel
		}
		th := clk.sim.ScheduleAtEx(target, func(s *core.Simulation) {
			// Non-blocking send.
			select {
			case ch <- clk.Now():
			default:
			}
			scheduleNext(interval)
		}, opts)
		if chainID == 0 {
			// First schedule: mint the chainID from the returned handle.
			chainID = th.ID()
		}
	}

	scheduleNext(d)

	t := &Ticker{C: ch}
	t.stop = func() {
		// Cancel all events with this chainID (at-pop-skip).
		clk.sim.Cancel(core.NewTaskHandle(chainID))
	}
	t.reset = func(newD time.Duration) {
		if newD <= 0 {
			panic("simclock: Ticker.Reset requires positive interval")
		}
		// Stop old chain.
		clk.sim.Cancel(core.NewTaskHandle(chainID))
		// Reset chainID so the next scheduleNext call mints a fresh one.
		chainID = 0
		scheduleNext(newD)
	}
	return t
}
