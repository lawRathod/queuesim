package faults

// Target abstracts every chokepoint the decision-vector faults touch on
// the temporal model's runtime: the matching queues' knobs, the runtime's
// fault-state fields, and the activity-failure decisions. The fault files
// apply ONLY through this interface — the temporal model implements it
// with small adapter methods that forward to the queues/fields
// (fault_target.go), so this package never reaches into the model's
// internals. Traceability role: Fault-control plumbing (taxonomy in
// models/temporal/doc.go) — this interface exposes the chokepoints the
// test knobs turn; it pins no server source itself.
type Target interface {
	// SetSpoolDrop sets the spool-time drop rate on the matching-wf and
	// matching-act queues (0 = no drops).
	SetSpoolDrop(wfRate, actRate float64)
	// SetDispatchRate sets the token-bucket dispatch rate and burst on
	// both matching queues.
	SetDispatchRate(rate, burst float64)
	// SetMaxWaitAct sets the matching-act MaxWaitMs schedule-to-start
	// deadline.
	SetMaxWaitAct(ms int64)
	// SetDispatchDelay sets the dispatch delay on both matching queues
	// (0 = no delay).
	SetDispatchDelay(ms int64)
	// SetPollerForced forces the active poller count for both classes
	// (-1 = no force; the configured fleet applies).
	SetPollerForced(n int)
	// SetWfTimerMs forces the per-workflow timer delay (-1 = off; the
	// normal persist→transfer path applies).
	SetWfTimerMs(ms int64)
	// SetPersistMult sets the persistence-latency multiplier (1 = no
	// change).
	SetPersistMult(rate float64)
	// SetPollJitterP sets the poller long-poll early-expiry probability
	// (0 = off).
	SetPollJitterP(p float64)
	// SetS2cMs sets the ScheduleToClose override (0 = the config value
	// applies).
	SetS2cMs(ms int64)
	// ActivityFailsAt reports whether an activity completing at nowMs
	// fails.
	ActivityFailsAt(nowMs int64) bool
	// ActivityFailsFor reports whether a SPECIFIC workflow (by ID) fails
	// its activity at nowMs (cascade mixed-population mode).
	ActivityFailsFor(id uint64, nowMs int64) bool
}
