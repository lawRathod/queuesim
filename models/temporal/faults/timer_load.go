package faults

// applyTimerLoad applies the per-workflow timer (KindTimerLoad): while
// active, every new workflow is routed through the history shard's timer
// queue (timerForced ms) before its transfer task is enqueued
// (startWorkflow, history.go) — loading the timer processor ahead of the
// activity. The member's rate is the forced timer delay in ms; -1 = no
// active member → the normal persist→transfer path. Fault-control
// (taxonomy in models/temporal/doc.go): a test knob at the history
// timer-queue routing chokepoint, not a server pin.
func applyTimerLoad(t Target, timerForced int64) {
	t.SetWfTimerMs(timerForced)
}
