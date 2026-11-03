package model

import (
	"github.com/lawRathod/queuesim/godes/core"
)

// WorkerConfig configures a Worker fleet.
type WorkerConfig struct {
	// Name is the worker identity (substream keys, diagnostics).
	Name string
	// Queue is the task queue the workers long-poll.
	Queue *TaskQueue
	// ServiceMs is the per-task execution time (0 = instant).
	ServiceMs int64
	// Budget bounds concurrent executions per worker (0 = unlimited).
	Budget int
	// Count is the number of workers in the fleet.
	Count int
	// LongPollMs is the long-poll expiry (server default ~1min; 0 = the
	// caller bounds parking via the topology).
	LongPollMs int64
}

// Worker is a fleet of long-polling workers that pull tasks from a
// TaskQueue (builder improvement 2026-08-23). Each worker parks on the
// queue (sync-match rendezvous or backlog dispatch), executes a task after
// ServiceMs, and re-parks. This brings the v2 model's worker semantics
// into the general builder.
//
// Determinism: single-goroutine, pump-driven (the topology calls ParkUpTo /
// Tick); no goroutines, no locks.
type Worker struct {
	sim       *core.Simulation
	cfg       WorkerConfig
	inFlight  int
	completed uint64
	onDone    func()
}

// NewWorker creates a Worker fleet bound to sim. Panics on nil sim/queue.
func NewWorker(sim *core.Simulation, cfg WorkerConfig) *Worker {
	if sim == nil {
		panic("model: NewWorker requires a non-nil simulation")
	}
	if cfg.Queue == nil {
		panic("model: NewWorker requires a queue")
	}
	if cfg.ServiceMs < 0 {
		cfg.ServiceMs = 0
	}
	if cfg.Budget < 0 {
		cfg.Budget = 0
	}
	if cfg.Count <= 0 {
		cfg.Count = 1
	}
	if cfg.LongPollMs <= 0 {
		cfg.LongPollMs = 60000
	}
	return &Worker{sim: sim, cfg: cfg}
}

// Name returns the worker fleet name.
func (w *Worker) Name() string { return w.cfg.Name }

// ParkUpTo keeps at least n workers parked on the queue (long-poll), then
// dispatches buffered backlog tasks to parked workers (the queue's
// async-read path). The in-flight budget bounds concurrent executions.
// Deterministic.
func (w *Worker) ParkUpTo(nowMs int64) {
	target := w.cfg.Count
	for w.cfg.Queue.Parked() < target {
		w.cfg.Queue.Park(nowMs+w.cfg.LongPollMs, func(t Task) { w.take(t) }, func() {
			// long-poll expired: re-park (the worker's cycle continues).
			w.cfg.Queue.Park(w.sim.Now().Int64()+w.cfg.LongPollMs, func(t Task) { w.take(t) })
		})
	}
	// Dispatch backlog: hand buffered tasks to parked workers (the queue's
	// reader path — a parked worker takes the oldest backlog task).
	for w.cfg.Queue.Parked() > 0 && w.cfg.Queue.Len() > 0 {
		if w.cfg.Budget > 0 && w.inFlight >= w.cfg.Budget {
			break // all workers busy
		}
		// Take the oldest buffered task and hand it to the oldest parked
		// worker (deterministic FIFO).
		w.cfg.Queue.dispatchOldest(func(t Task) { w.take(t) })
	}
}

// take handles a task handed to a worker (sync-match or dispatch): it
// starts execution (in-flight++) and schedules completion after ServiceMs.
func (w *Worker) take(t Task) {
	if w.cfg.Budget > 0 && w.inFlight >= w.cfg.Budget {
		// Over budget: the task can't run now. For the general builder,
		// this should not happen (ParkUpTo bounds parking by budget) — but
		// if it does, re-buffer the task.
		w.cfg.Queue.Add(t)
		return
	}
	w.inFlight++
	w.sim.ScheduleIn(w.cfg.ServiceMs, func(*core.Simulation) {
		w.inFlight--
		w.completed++
		if w.onDone != nil {
			w.onDone()
		}
		// Re-park: the worker's cycle continues (under the budget gate).
		if w.cfg.Budget == 0 || w.inFlight < w.cfg.Budget {
			w.cfg.Queue.Park(w.sim.Now().Int64()+w.cfg.LongPollMs, func(t Task) { w.take(t) })
		}
	})
}

// SetOnDone installs the completion hook (topology observability).
func (w *Worker) SetOnDone(fn func()) { w.onDone = fn }

// InFlight returns the current execution count.
func (w *Worker) InFlight() int { return w.inFlight }

// Completed returns the number of completed executions.
func (w *Worker) Completed() uint64 { return w.completed }
