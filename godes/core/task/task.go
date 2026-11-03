// Package task provides deterministic task helpers (Timeout, Retry, Periodic)
// with cancellation support for the GoDES simulation framework.
//
// All tasks are scheduled using simulation time only — no wall-clock
// dependencies. Cancellation is handled via the simulation's Cancel
// method (at-pop-skip semantics, ADR-044). The task Handle wraps a
// core.TaskHandle and delegates Cancel to sim.Cancel.
package task

import "github.com/lawRathod/queuesim/godes/core"

// Manager scopes task scheduling methods. Create with NewManager().
//
// Manager is not goroutine-safe. It must only be used within a single
// simulation execution context (GOMAXPROCS=1 model per ADR-001/ADR-003).
type Manager struct{}

// NewManager creates a new Manager.
func NewManager() *Manager {
	return &Manager{}
}

// Handle allows cancellation of a scheduled task. It wraps a core.TaskHandle
// and delegates Cancel to the simulation's Cancel method (at-pop-skip).
type Handle struct {
	h   core.TaskHandle
	sim *core.Simulation
}

// Cancel marks the task as cancelled via sim.Cancel (at-pop-skip).
// The scheduled callback is never invoked; sim time is not advanced.
// Returns true if the task was pending and is now cancelled.
//
// Cancel must only be called from within the simulation's single-threaded
// execution context (i.e., from within a scheduled callback or during setup).
func (h *Handle) Cancel() bool {
	if h == nil || h.sim == nil {
		return false
	}
	return h.sim.Cancel(h.h)
}

// ID returns the unique identifier for this task handle.
func (h *Handle) ID() TaskID { return TaskID(h.h.ID()) }

// taskHandle returns the underlying core.TaskHandle for passing to sim.Cancel.
func (h *Handle) taskHandle() core.TaskHandle { return h.h }

// TaskID is a type alias for core.TaskID.
type TaskID = core.TaskID

// Timeout schedules fn to fire once after delay (in simulation time units).
// Returns a Handle that can be used to cancel the timeout before it fires.
func (m *Manager) Timeout(sim *core.Simulation, delay int64, fn func(*core.Simulation)) *Handle {
	if sim == nil {
		panic("task: Timeout requires non-nil simulation")
	}
	th := sim.ScheduleInEx(delay, fn, core.ScheduleOptions{Kind: core.KindTask})
	return &Handle{h: th, sim: sim}
}

// Retry schedules fn to run repeatedly with backoff delay between attempts.
// fn should return true when the operation succeeds (stops retrying).
// The function will retry up to maxAttempts times with the given backoff
// delay between each attempt. Returns a Handle for cancellation.
//
// The chain's TaskID is minted from the first real schedule call, avoiding
// placeholder tombstones that would block Cancel before the first attempt.
func (m *Manager) Retry(sim *core.Simulation, initialDelay int64, maxAttempts int, backoff int64, fn func(*core.Simulation) bool) *Handle {
	if sim == nil {
		panic("task: Retry requires non-nil simulation")
	}
	attempts := 0
	var chainID core.TaskID
	var h *Handle
	var schedule func(delay int64)
	schedule = func(delay int64) {
		opts := core.ScheduleOptions{Kind: core.KindTask}
		if chainID != 0 {
			opts.TaskID = chainID // re-schedule under same ID for whole-chain Cancel
		}
		th := sim.ScheduleInEx(delay, func(s *core.Simulation) {
			attempts++
			if fn(s) {
				return
			}
			if attempts < maxAttempts {
				schedule(backoff)
			}
		}, opts)
		if chainID == 0 {
			// First schedule: mint the chainID from the returned handle.
			chainID = th.ID()
			h = &Handle{sim: sim, h: th}
		}
	}
	schedule(initialDelay)
	return h
}

// Periodic schedules fn to fire count times at the given interval.
// If count <= 0, the task fires indefinitely (until cancelled).
// Returns a Handle for cancellation.
//
// The chain's TaskID is minted from the first real schedule call, avoiding
// placeholder tombstones that would block Cancel before the first tick.
func (m *Manager) Periodic(sim *core.Simulation, interval int64, count int, fn func(*core.Simulation)) *Handle {
	if sim == nil {
		panic("task: Periodic requires non-nil simulation")
	}
	fired := 0
	var chainID core.TaskID
	var h *Handle
	var schedule func()
	schedule = func() {
		opts := core.ScheduleOptions{Kind: core.KindTask}
		if chainID != 0 {
			opts.TaskID = chainID // re-schedule under same ID for whole-chain Cancel
		}
		th := sim.ScheduleInEx(interval, func(s *core.Simulation) {
			fired++
			fn(s)
			if count <= 0 || fired < count {
				schedule()
			}
		}, opts)
		if chainID == 0 {
			// First schedule: mint the chainID from the returned handle.
			chainID = th.ID()
			h = &Handle{sim: sim, h: th}
		}
	}
	schedule()
	return h
}
