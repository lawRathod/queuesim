// Package resource provides deterministic resource-modeling primitives —
// Resource (capacity-limited with a FIFO acquire queue), Container
// (SimPy-style tank: capacity + fluid level with put/take), and
// Store (generic item buffer with put/get). All built on the GoDES core
// scheduler with no goroutines and no wall-clock dependence.
//
// API style: operations that may need to wait take a continuation
// (func(*core.Simulation)) that is invoked — immediately on a grant, or
// later at the current sim time when the blocking condition clears.
// Waiters are served strictly FIFO by registration order, making wake
// order deterministic.
package resource

import (
	"github.com/lawRathod/queuesim/godes/core"
)

// waiter is a queued continuation awaiting a resource, container, or
// store. A release/put/take that frees the resource pops the oldest
// waiter and runs it (at the same sim time). For timeout-capable waits
// (Resource.AcquireOrTimeout), done marks the waiter resolved so a late
// grant/timeout is a no-op.
type waiter struct {
	fn   func(*core.Simulation)
	done bool
}

func runWaiter(fn func(*core.Simulation), sim *core.Simulation) {
	if fn != nil {
		fn(sim)
	}
}

// Resource is a capacity-limited resource with a FIFO acquirer queue,
// mirroring SimPy's Resource. Acquire either succeeds immediately (a
// capacity slot is free) or queues the acquirer until a Release frees a
// slot. Waiters are woken strictly in registration (FIFO) order.
type Resource struct {
	sim      *core.Simulation
	capacity int
	level    int
	waiters  []*waiter
	waits    uint64 // total queued waits
	granted  uint64 // total immediate grants
}

// NewResource creates a Resource with the given capacity. Panics if
// capacity <= 0 or sim is nil.
func NewResource(sim *core.Simulation, capacity int) *Resource {
	if sim == nil {
		panic("resource: NewResource requires a non-nil simulation")
	}
	if capacity <= 0 {
		panic("resource: capacity must be > 0")
	}
	return &Resource{sim: sim, capacity: capacity, waiters: []*waiter{}}
}

// TryAcquire acquires a slot without waiting. Returns true if a slot was
// taken, false if the resource is fully held.
func (r *Resource) TryAcquire() bool {
	if r.level >= r.capacity {
		return false
	}
	r.level++
	r.granted++
	return true
}

// Acquire acquires a slot, invoking fn immediately if a slot is free, or
// queueing fn (in FIFO order) until a Release grants it. When granted,
// fn is invoked with the sim; the slot is considered held by the caller,
// who must call Release when done. fn may be nil (observe-only wait).
func (r *Resource) Acquire(fn func(*core.Simulation)) {
	if r.level < r.capacity {
		r.level++
		r.granted++
		runWaiter(fn, r.sim)
		return
	}
	r.waits++
	r.waiters = append(r.waiters, &waiter{fn: fn})
}

// AcquireOrTimeout acquires a slot with a patience: if the slot is
// granted before timeoutMs of sim time elapse, onGranted runs (the caller
// holds the slot and must Release it); if the patience expires first, the
// waiter reneges and onTimeout runs instead (no slot held). Exactly one
// of the two continuations ever runs — the other is cancelled, whether by
// the grant arriving after the timeout or vice versa. Deterministic: the
// timeout is a scheduled sim event; a tie at the same sim tick resolves
// by event order (the timeout was scheduled at acquire time, so it fires
// first at equal times under the FIFO frontier policy).
func (r *Resource) AcquireOrTimeout(timeoutMs int64, onGranted, onTimeout func(*core.Simulation)) {
	if r.level < r.capacity {
		r.level++
		r.granted++
		runWaiter(onGranted, r.sim)
		return
	}
	r.waits++
	w := &waiter{}
	r.waiters = append(r.waiters, w)
	// Resolve reports the outcome once; the loser becomes a no-op.
	var resolve func(granted bool, sim *core.Simulation)
	resolve = func(granted bool, sim *core.Simulation) {
		if w.done {
			return
		}
		w.done = true
		if granted {
			runWaiter(onGranted, sim)
		} else {
			runWaiter(onTimeout, sim)
		}
	}
	// Timeout event: fires after timeoutMs unless already resolved.
	r.sim.ScheduleIn(timeoutMs, func(s *core.Simulation) {
		if w.done {
			return
		}
		// Remove the waiter from the queue (it reneged).
		for i, q := range r.waiters {
			if q == w {
				r.waiters = append(r.waiters[:i], r.waiters[i+1:]...)
				break
			}
		}
		resolve(false, s)
	})
	// Grant path: wrap fn so a late Release grant is detected and ignored.
	w.fn = func(s *core.Simulation) {
		resolve(true, s)
	}
}

// Release frees one held slot. If acquirers are waiting, the oldest one is
// granted (its slot transitions, so level is unchanged); otherwise level
// is decremented. Releasing when nothing is held is a no-op.
func (r *Resource) Release() {
	if len(r.waiters) > 0 {
		w := r.waiters[0]
		r.waiters = r.waiters[1:]
		runWaiter(w.fn, r.sim)
		return
	}
	if r.level > 0 {
		r.level--
	}
}

// Used returns the number of currently held slots.
func (r *Resource) Used() int { return r.level }

// Capacity returns the configured capacity.
func (r *Resource) Capacity() int { return r.capacity }

// Free returns the number of available slots.
func (r *Resource) Free() int { return r.capacity - r.level }

// Waits returns the total number of acquires that had to wait.
func (r *Resource) Waits() uint64 { return r.waits }

// Granted returns the total number of immediate (non-waiting) grants.
func (r *Resource) Granted() uint64 { return r.granted }

// Contention returns the fraction of acquires that waited.
func (r *Resource) Contention() float64 {
	total := r.waits + r.granted
	if total == 0 {
		return 0
	}
	return float64(r.waits) / float64(total)
}
