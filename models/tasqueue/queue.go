package tasqueue

import (
	"github.com/lawRathod/queuesim/model"
)

// queueService returns the broker Queue service: the pending-job queue
// "tasqueue:tasks" (capacity, drain-time drop rate, schedule-to-start
// MaxWaitMs deadline) plus the internal timer queue used for ETA /
// EnqueueScheduled jobs. The service owns the real TaskQueues (constructed
// directly on the shared runtime's simulation below and bound to the shared
// runtime); queue declares NO ports and wires NO builder-queue shells — the
// runtime loop (timer→q→dispatch, drop/expire, retry re-entry) drives the
// real broker queue directly, so a schematic port edge would render without
// a single task traversing it (the honest-schematics principle: if the code
// doesn't traverse it, the diagram doesn't draw it).
//
// Broker divergence (documented): the in-memory broker does NOT implement
// EnqueueScheduled (brokers/in-memory), so ETA support here is an
// abstraction of the Redis-broker behavior: burst jobs with ETAOffsetMs > 0
// are parked in the internal timer queue and re-enter tasqueue:tasks when
// their ETA arrives (pump step 1).
func queueService(rt *runtime) model.Service {
	return model.Service{
		Name:  "queue",
		Kind:  "tasqueue.queue",
		Label: "broker: tasqueue:tasks queue + ETA timer",
		Build: func(b *model.TopologyBuilder) error {
			// The real broker queue. Constructed directly (not via the
			// builder) so the drop/maxwait knobs are applied at
			// construction time with the root-level substream prefix —
			// late SetDropRate after builder prefix assignment would panic
			// (AD-4) and would move the drop substream key.
			rt.q = model.NewTaskQueue(rt.sim, model.TaskQueueConfig{Name: "tasqueue:tasks", Capacity: rt.cfg.QueueCapacity})
			if rt.cfg.DropRate > 0 {
				// Drain-time delivery drop. The runtime models a drop as an
				// attempt-level failure consuming one retry (config.go
				// DropRate) — the substream is created here, at
				// construction, with the root-level prefix.
				rt.q.SetDropRate(rt.cfg.DropRate)
			}
			if rt.cfg.MaxWaitMs > 0 {
				rt.q.SetMaxWaitMs(rt.cfg.MaxWaitMs)
			}
			// Internal ETA/scheduled queue (EnqueueScheduled abstraction).
			rt.timer = model.NewTaskQueue(rt.sim, model.TaskQueueConfig{Name: "tasqueue:timer"})
			return nil
		},
	}
}
