package core

// TaskID is the stable identity of a scheduled event. It is used for
// cancellation (Cancel) and for replay matching (FrontierEvent.TaskID).
//
// The TaskID is co-indexed with the scheduler's `seq` counter (ADR-006)
// — i.e., TaskID(seqN) is the canonical mapping. Re-scheduling a
// Retry/Periodic chain under the same TaskID is supported via
// ScheduleOptions.TaskID in ScheduleAtEx / ScheduleInEx.
type TaskID uint64

// TaskHandle is a cancellation-capable reference to a scheduled event.
//
// The zero TaskHandle is invalid (returned only when a nil Event is
// passed to ScheduleAt / ScheduleIn). Cancellation is a privileged
// single-threaded op (ADR-003) — callers must not race Cancel with the
// scheduler's pop loop.
type TaskHandle struct {
	id TaskID
}

// ID returns the underlying TaskID. Stable across re-runs with the same seed.
func (h TaskHandle) ID() TaskID { return h.id }

// NewTaskHandle creates a TaskHandle from a TaskID. This is used by
// sub-packages (core/task, core/simclock) that mint their own TaskIDs
// for chain cancellation.
func NewTaskHandle(id TaskID) TaskHandle { return TaskHandle{id: id} }
