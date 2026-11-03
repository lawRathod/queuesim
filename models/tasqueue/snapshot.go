package tasqueue

// Snapshot is one observational sample of the model's live state. It is a
// pure read: zero RNG draws, zero simulation mutation. Attach a sampler via
// Topology.System().AddTickHook before Run and the digest does not move.
type Snapshot struct {
	TMs int64 `json:"t_ms"`

	// Broker backlog: the pending-job queue plus the runtime's dispatch
	// buffer (tasks drained from the broker awaiting a poller take).
	QueueDepth    int `json:"queue_depth"`
	DispatchDepth int `json:"dispatch_depth"`
	TimerDepth    int `json:"timer_depth"`
	// PollerInFlight is the number of handler executions in flight.
	PollerInFlight int    `json:"poller_inflight"`
	PollCount      uint64 `json:"poll_count"`

	// Cumulative lifecycle counters.
	Started        uint64 `json:"started"`
	Completed      uint64 `json:"completed"`
	Retried        uint64 `json:"retried"`
	Failed         uint64 `json:"failed"`
	Dropped        uint64 `json:"dropped"`
	Expired        uint64 `json:"expired"`
	RejectedAdds   uint64 `json:"rejected_adds"`
	AttemptsTotal  uint64 `json:"attempts_total"`
	ChainStepsDone uint64 `json:"chain_steps_done"`
	GroupJobsDone  uint64 `json:"group_jobs_done"`
	OnErrorSpawned uint64 `json:"on_error_spawned"`
}

// Snapshot samples the live model state. Call it between pump ticks.
func (t *Topology) Snapshot() Snapshot {
	if t == nil || t.rt == nil || t.rt.sim == nil {
		return Snapshot{}
	}
	rt := t.rt
	if rt.q == nil || rt.timer == nil || rt.poller == nil {
		return Snapshot{TMs: rt.sim.Now().Int64()}
	}
	return Snapshot{
		TMs:            rt.sim.Now().Int64(),
		QueueDepth:     rt.q.Len(),
		DispatchDepth:  len(rt.dispatch),
		TimerDepth:     rt.timer.Len(),
		PollerInFlight: rt.poller.InFlight(),
		PollCount:      rt.poller.PollCount(),
		Started:        rt.stats.Started,
		Completed:      rt.stats.Completed,
		Retried:        rt.stats.Retried,
		Failed:         rt.stats.Failed,
		Dropped:        rt.stats.Dropped,
		Expired:        rt.stats.Expired,
		RejectedAdds:   rt.stats.RejectedAdds,
		AttemptsTotal:  rt.stats.AttemptsTotal,
		ChainStepsDone: rt.stats.ChainStepsDone,
		GroupJobsDone:  rt.stats.GroupJobsDone,
		OnErrorSpawned: rt.stats.OnErrorSpawned,
	}
}
