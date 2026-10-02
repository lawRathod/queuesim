package faults

import (
	"github.com/lawRathod/queuesim/model"
)

// QueueTarget adapts a model.TaskQueue to the fault Target interface.
// Every knob maps 1:1 onto a TaskQueue setter, so the injected fault is
// exactly the queue's own deterministic semantics (drain-time drop draws
// from the queue's per-name substream; delay defers delivery once; timeout
// expires tasks whose residency exceeds the deadline).
type QueueTarget struct {
	q *model.TaskQueue
}

// NewQueueTarget wraps a TaskQueue as a fault target.
func NewQueueTarget(q *model.TaskQueue) *QueueTarget {
	return &QueueTarget{q: q}
}

// ApplyDrop sets the queue's drain-time drop rate (0 = off).
func (t *QueueTarget) ApplyDrop(rate float64) { t.q.SetDropRate(rate) }

// ApplyDelay sets the queue's per-task delay and enables it. Setting 0
// disables the delay.
func (t *QueueTarget) ApplyDelay(ms int64) {
	t.q.SetDelay(ms)
	t.q.SetDelayActive(ms > 0)
}

// ApplyTimeout sets the queue's residency deadline (0 = off).
func (t *QueueTarget) ApplyTimeout(ms int64) { t.q.SetMaxWaitMs(ms) }

// TargetName returns the queue's name.
func (t *QueueTarget) TargetName() string { return t.q.Name() }
