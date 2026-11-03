package model

import (
	"math"

	"github.com/lawRathod/queuesim/godes/core"
)

// TaskClass routes a task to its per-class queue partition (plan 2026-08-12
// D3): activity-class tasks flow matching-act, workflow-class tasks flow
// matching-wf.
type TaskClass int

const (
	// TaskActivity is an activity-class task (AddActivityTask RPC).
	TaskActivity TaskClass = iota
	// TaskWfSchedule is a workflow-class task: after workflow execution it
	// schedules the activity (AddActivityTask). Used for workflow starts
	// and retry round trips.
	TaskWfSchedule
	// TaskWfComplete is a workflow-class task: after workflow execution the
	// workflow completes. Used for the post-activity-success wf task.
	TaskWfComplete
)

// Task is a unit of work flowing through the queue network. ID identifies
// the logical workflow/job; Attempt is the 1-based execution attempt of that
// task (incremented on each retry re-add). Class routes the task to its
// per-class queue partition (zero value = activity class). BornMs is the
// schedule-to-start clock override (plan 2026-08-12 D2): when > 0, a
// MaxWaitMs expiry at drain measures against BornMs instead of the queue
// enqueue time (0 = enqueue time — zero-change for generic queue users).
// Task is a unit of work flowing through the queue network. ID identifies
// the logical workflow/job; Attempt is the 1-based execution attempt of that
// task (incremented on each retry re-add). Class routes the task to its
// per-class queue partition (zero value = activity class). BornMs is the
// schedule-to-start clock override (plan 2026-08-12 D2): when > 0, a
// MaxWaitMs expiry at drain measures against BornMs instead of the queue
// enqueue time (0 = enqueue time — zero-change for generic queue users).
//
// The Task struct is shared with the tasqueue model (models/tasqueue) and
// the sync-match Worker primitive — the queue hands a task directly to a
// parked worker via Add, or buffers it.
type Task struct {
	ID      uint64
	Attempt int32
	Class   TaskClass
	BornMs  int64
}

// TaskQueueConfig configures a TaskQueue.
type TaskQueueConfig struct {
	// Name is the queue identity; it names the RNG substream used for
	// drain-time fault draws.
	Name string
	// Capacity bounds the buffered task count. 0 = unbounded. When full,
	// Add/AddAt reject the newest task (DropNewest → RejectedAdds
	// semantics); capacity frees as tasks drain.
	Capacity int
	// substreamPrefix is the hierarchical path prefix for the queue's
	// RNG substream (e.g. "frontend" or "frontend.matching"). It is
	// constructor-only and must not be mutated after construction;
	// the TopologyBuilder sets it during Build's post-order pass.
	// Empty means no prefix (root-level, backward compatible).
	// Unexported to prevent late mutation after construction which would
	// silently change the substream name (AD-4).
	substreamPrefix string
	// SubstreamPrefix is deprecated, use substreamPrefix via builder.
	// Kept for backward compatibility of struct literal with exported field
	// but ignored for substream key (use substreamPrefix). Will be removed.
	SubstreamPrefix string
}

// queuedTask is a buffered delivery with its absolute due time. deferred
// marks a task whose delivery was already postponed once by SetDelay —
// delay applies exactly once per task. enqueuedMs is the sim time of the
// Add/AddAt call — the schedule-to-start deadline clock (D2: MaxWaitMs).
type queuedTask struct {
	task       Task
	atMs       core.SimTime
	deferred   bool
	enqueuedMs int64
}

// TaskQueue is a buffered FIFO delivery queue driven by the DrainToTick
// pump contract (ADR-041/068): tasks are buffered by Add/AddAt with an
// absolute due time and delivered by DrainToTick once the pump reaches it.
//
// Fault semantics (ADR-058 layer-3 style): the drop-rate decision is
// evaluated AT DRAIN TIME, in FIFO order, drawing from a per-queue
// substream. A drop rate of 0 performs zero RNG draws and never creates
// the substream. Capacity is enforced at Add time (the RejectedAdds
// boundary). Transfer delay (SetDelay/SetDelayActive) postpones each due
// task once by a fixed amount — the transfer-queue slowdown primitive.
// The MaxWaitMs deadline (D2) expires tasks whose schedule-to-start wait
// exceeds the deadline at drain — terminal, non-retryable.
//
// Determinism: no goroutines, no mutexes, no map iteration; all decisions
// are pure functions of drain order and substream draws.
type TaskQueue struct {
	sim *core.Simulation
	cfg TaskQueueConfig

	items []queuedTask
	minAt core.SimTime // smallest atMs across items; math.MaxInt64 = empty

	dropRate  float64
	rng       core.RNG // drop substream; nil unless dropRate > 0
	delayMs   int64    // transfer-delay amount; 0 = off
	delayOn   bool     // delay active flag, toggled via SetDelayActive
	maxWaitMs int64    // schedule-to-start deadline (D2); 0 = off

	// Sync-match (builder improvement, 2026-08-23): parked workers waiting
	// on this queue. A parked worker is a cancelable scheduled event; Add
	// hands a task to a parked worker directly (rendezvous) before
	// buffering — the mechanism the v2 model has, now in the general
	// builder.
	parked  []*parkedWorker
	nextPID uint64

	rejected uint64 // capacity rejects at Add/AddAt
	dropped  uint64 // drain-time fault drops
}

// NewTaskQueue creates a TaskQueue backed by sim. The config name is used
// verbatim in the substream key; distinct names must be unique per sim.
func NewTaskQueue(sim *core.Simulation, cfg TaskQueueConfig) *TaskQueue {
	if sim == nil {
		panic("model: nil simulation in NewTaskQueue")
	}
	return &TaskQueue{
		sim:   sim,
		cfg:   cfg,
		minAt: core.SimTime(math.MaxInt64),
	}
}

// Len returns the number of buffered tasks.
func (q *TaskQueue) Len() int { return len(q.items) }

// Name returns the queue's configured name.
func (q *TaskQueue) Name() string { return q.cfg.Name }

// Rejected returns the total number of capacity rejects.
func (q *TaskQueue) Rejected() uint64 { return q.rejected }

// Dropped returns the total number of drain-time fault drops.
func (q *TaskQueue) Dropped() uint64 { return q.dropped }

// parkedWorker is a worker long-polling this queue. onTake runs inline
// when the worker receives a task (sync-match or dispatch). The wake event
// fires at expiryMs if the worker is still parked (long-poll timeout).
type parkedWorker struct {
	id       uint64
	expiryMs int64
	onTake   func(Task)
	wake     core.TaskHandle
	consumed bool
}

// Park registers a long-polling worker: it waits until expiryMs for a
// task. onTake runs inline when a task is handed over (sync-match). On
// expiry without a task, onExpire (if any) runs — the worker re-parks.
// Returns the park token (for Unpark).
func (q *TaskQueue) Park(expiryMs int64, onTake func(Task), onExpire ...func()) *parkedWorker {
	p := &parkedWorker{id: q.nextPID, expiryMs: expiryMs, onTake: onTake}
	q.nextPID++
	q.parked = append(q.parked, p)
	p.wake = q.sim.ScheduleAt(core.SimTime(expiryMs), func(*core.Simulation) {
		if p.consumed {
			return
		}
		q.removeParked(p)
		if len(onExpire) > 0 && onExpire[0] != nil {
			onExpire[0]()
		}
	})
	return p
}

// Unpark cancels a worker's park without handing it a task.
func (q *TaskQueue) Unpark(p *parkedWorker) bool {
	if p == nil || p.consumed {
		return false
	}
	p.consumed = true
	q.sim.Cancel(p.wake)
	q.removeParked(p)
	return true
}

// Parked returns the number of workers currently parked (long-polling).
func (q *TaskQueue) Parked() int { return len(q.parked) }

// dispatchOldest hands the oldest buffered task to the oldest parked
// worker (FIFO, deterministic). Returns true when a task was dispatched.
// Used by the Worker primitive's backlog-dispatch path.
func (q *TaskQueue) dispatchOldest(onTake func(Task)) bool {
	if len(q.items) == 0 || len(q.parked) == 0 {
		return false
	}
	// Oldest buffered item (FIFO): find the item with the smallest atMs.
	idx := 0
	for i := 1; i < len(q.items); i++ {
		if q.items[i].atMs < q.items[idx].atMs {
			idx = i
		}
	}
	t := q.items[idx].task
	q.items = append(q.items[:idx], q.items[idx+1:]...)
	// Oldest parked worker.
	p := q.parked[0]
	q.parked = q.parked[1:]
	p.consumed = true
	q.sim.Cancel(p.wake)
	onTake(t)
	return true
}

func (q *TaskQueue) removeParked(p *parkedWorker) {
	for i, e := range q.parked {
		if e == p {
			q.parked = append(q.parked[:i], q.parked[i+1:]...)
			return
		}
	}
}

// Add buffers a task for immediate delivery (due at the current sim time).
// Returns false when the queue is at capacity (the add is rejected and
// counted). Sync-match: if a worker is parked, the task is handed to it
// directly (inline) and returns true (buffered=false semantics — the
// caller can distinguish via the returned syncMatched flag on AddSync).
func (q *TaskQueue) Add(t Task) bool {
	_, accepted := q.AddSync(t)
	return accepted
}

// AddSync adds with sync-match semantics: if a worker is parked, the task
// is handed off directly (returns syncMatched=true, accepted=true).
// Otherwise it buffers (syncMatched=false, accepted as before). This is
// the general-builder equivalent of the v2 MatchQueue.Add rendezvous.
func (q *TaskQueue) AddSync(t Task) (syncMatched, accepted bool) {
	if n := len(q.parked); n > 0 {
		p := q.parked[0]
		q.parked = q.parked[1:]
		p.consumed = true
		q.sim.Cancel(p.wake)
		p.onTake(t) // inline handoff at the producer's sim instant
		return true, true
	}
	return false, q.AddAt(t, q.sim.Now())
}

// AddAt buffers a task for delivery at the absolute sim time atMs. Returns
// false when the queue is at capacity (the add is rejected and counted).
// An atMs at or before the current sim time is delivered at the next drain.
func (q *TaskQueue) AddAt(t Task, atMs core.SimTime) bool {
	if q.cfg.Capacity > 0 && len(q.items) >= q.cfg.Capacity {
		q.rejected++
		return false
	}
	q.items = append(q.items, queuedTask{task: t, atMs: atMs, enqueuedMs: atMs.Int64()})
	if atMs < q.minAt {
		q.minAt = atMs
	}
	return true
}

// SetDropRate sets the drain-time fault drop probability, clamped to
// [0,1]. A rate > 0 creates the queue's drop substream; a rate of 0
// disables drops with zero RNG draws.
func (q *TaskQueue) SetDropRate(rate float64) {
	if rate < 0 {
		rate = 0
	}
	if rate > 1 {
		rate = 1
	}
	q.dropRate = rate
	if rate > 0 && q.rng == nil {
		q.rng = q.sim.Substream(q.substreamKey())
	}
}

// SubstreamPrefix returns the hierarchical prefix for this queue's RNG
// substream ("" for root).
func (q *TaskQueue) SubstreamPrefix() string {
	if q.cfg.substreamPrefix != "" {
		return q.cfg.substreamPrefix
	}
	return q.cfg.SubstreamPrefix
}

// substreamKey returns the hierarchical RNG substream name for this queue.
func (q *TaskQueue) substreamKey() string {
	prefix := q.SubstreamPrefix()
	if prefix != "" {
		return "model.taskqueue." + prefix + "." + q.cfg.Name + ".drop"
	}
	return "model.taskqueue." + q.cfg.Name + ".drop"
}

// SetDelay sets the transfer-delay amount in sim ms (0 = off).
func (q *TaskQueue) SetDelay(ms int64) {
	if ms < 0 {
		ms = 0
	}
	q.delayMs = ms
}

// SetDelayActive toggles the transfer-delay window. The delay amount is
// unchanged; the flag gates whether deferral applies at drain time.
func (q *TaskQueue) SetDelayActive(on bool) { q.delayOn = on }

// SetMaxWaitMs sets the schedule-to-start deadline in sim ms (plan
// 2026-08-12 D2): tasks whose queue residency exceeds it expire at drain
// (non-retryable — the caller decides the terminal semantics). 0 = off.
// Expiry is pure time math — no RNG draws.
func (q *TaskQueue) SetMaxWaitMs(ms int64) {
	if ms < 0 {
		ms = 0
	}
	q.maxWaitMs = ms
}

// MaxWaitMs returns the current schedule-to-start deadline (0 = off) — the
// take-time expiry seam (the topology re-checks the deadline when a
// dispatched task is polled, mirroring the server's IsTaskExpired at poll,
// physical_task_queue_manager.go:486-497).
func (q *TaskQueue) MaxWaitMs() int64 { return q.maxWaitMs }

// DrainToTick delivers every buffered task with atMs <= target in FIFO
// order. Drain-time fault drops are evaluated per task in drain order —
// dropped tasks are removed from the queue and returned in the dropped
// slice (the caller decides the retry semantics: the Temporal topology
// re-attempts them like the bubble's caller-side AddTask retry). Tasks
// deferred by an active transfer delay are re-buffered to target + delayMs
// exactly once — the deferral target is strictly beyond target (delayMs >
// 0), so a single pass is sufficient. Tasks whose queue residency exceeds
// the MaxWaitMs deadline EXPIRE: removed, returned in the expired slice,
// non-retryable (the caller's terminal semantics). Expiry is checked
// before the drop draw — pure time math, zero RNG draws.
func (q *TaskQueue) DrainToTick(target core.SimTime) (delivered, dropped, expired []Task) {
	// Fast path: nothing due.
	if q.minAt == math.MaxInt64 || q.minAt > target {
		return nil, nil, nil
	}
	ready := make([]queuedTask, 0, len(q.items))
	remaining := make([]queuedTask, 0, len(q.items))
	for _, it := range q.items {
		if it.atMs <= target {
			ready = append(ready, it)
		} else {
			remaining = append(remaining, it)
		}
	}
	// Deferred items move to remaining (delivery postponed exactly once);
	// the rest are delivered.
	var deliver []queuedTask
	for _, it := range ready {
		if q.delayOn && q.delayMs > 0 && !it.deferred {
			it.atMs = target.Add(q.delayMs)
			it.deferred = true
			remaining = append(remaining, it)
			continue
		}
		deliver = append(deliver, it)
	}
	out := make([]Task, 0, len(deliver))
	var dropOut, expOut []Task
	for _, it := range deliver {
		if q.maxWaitMs > 0 {
			born := it.enqueuedMs
			if it.task.BornMs > 0 {
				born = it.task.BornMs // D2: attempt-cycle clock override
			}
			if target.Int64()-born > q.maxWaitMs {
				expOut = append(expOut, it.task)
				continue
			}
		}
		if q.dropRate > 0 {
			if q.dropRate >= 1 || q.rng.Float64() < q.dropRate {
				q.dropped++
				dropOut = append(dropOut, it.task)
				continue
			}
		}
		out = append(out, it.task)
	}
	q.items = remaining
	if len(remaining) == 0 {
		q.minAt = core.SimTime(math.MaxInt64)
	} else {
		newMin := remaining[0].atMs
		for _, it := range remaining[1:] {
			if it.atMs < newMin {
				newMin = it.atMs
			}
		}
		q.minAt = newMin
	}
	return out, dropOut, expOut
}
