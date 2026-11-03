package temporal

import "github.com/lawRathod/queuesim/models/temporal/components/matcher"

// Snapshot is one observational sample of the model's live mechanism state.
// It is a pure read: zero RNG draws, zero simulation mutation. The model is
// unchanged whether or not a caller samples it, so a sampler may be attached
// via Topology.System().AddTickHook before Run without moving the digest.
//
// Depths are instantaneous; the counters are cumulative. Head-age values are
// milliseconds of the oldest resident task in each matching partition (0
// when empty). All values are JSON-ready for the visualization server.
type Snapshot struct {
	TMs int64 `json:"t_ms"`

	// Matching partitions. Wf is the workflow-task queue, Act the activity
	// ROOT partition (the SDK poller fleet parks here), Child the activity
	// write partition that absorbs adds when forwarding is unreliable.
	WfDepth    int `json:"wf_depth"`
	ActDepth   int `json:"act_depth"`
	ChildDepth int `json:"child_depth"`

	WfHeadAgeMs    int64 `json:"wf_head_age_ms"`
	ActHeadAgeMs   int64 `json:"act_head_age_ms"`
	ChildHeadAgeMs int64 `json:"child_head_age_ms"`

	// Poller fleets: parked long-pollers and executions in flight.
	WfPollers   int `json:"wf_pollers"`
	ActPollers  int `json:"act_pollers"`
	WfInflight  int `json:"wf_inflight"`
	ActInflight int `json:"act_inflight"`

	// History: sticky affinity buffers and the shard queues
	// (transfer = entering matching, timer = scheduled work, resched =
	// failed executions waiting out the retry backoff).
	StickyDepth   int `json:"sticky_depth"`
	ShardTransfer int `json:"shard_transfer"`
	ShardTimer    int `json:"shard_timer"`
	ShardResched  int `json:"shard_resched"`

	// Cumulative lifecycle counters.
	Started          uint64 `json:"started"`
	ActivityAdds     uint64 `json:"activity_adds"`
	AttemptsExecuted uint64 `json:"attempts_executed"`
	Completed        uint64 `json:"completed"`
	Retries          uint64 `json:"retries"`
	Failed           uint64 `json:"failed"`

	// Cumulative matching-boundary tallies (summed across partitions).
	SyncMatched uint64 `json:"sync_matched"`
	Spooled     uint64 `json:"spooled"`
	Dispatched  uint64 `json:"dispatched"`
	Rejected    uint64 `json:"rejected"`
	Dropped     uint64 `json:"dropped"`
	Expired     uint64 `json:"expired"`
}

// Snapshot samples the live model state. Call it between pump ticks (e.g.
// from a System tick hook) — it reads queue heads and counters only.
func (t *Topology) Snapshot() Snapshot {
	if t == nil || t.rt == nil || t.rt.sim == nil {
		return Snapshot{}
	}
	rt := t.rt
	now := rt.sim.Now()
	// The mechanism components are materialized by System.Build (inside Run).
	// A caller that samples before Run gets an empty snapshot instead of a
	// nil-pointer panic.
	if rt.wfQ == nil || rt.actQ == nil || rt.childQ == nil || rt.wfPool == nil || rt.actPool == nil || rt.sticky == nil {
		return Snapshot{TMs: now.Int64()}
	}
	s := Snapshot{
		TMs:              now.Int64(),
		WfDepth:          rt.wfQ.Len(),
		ActDepth:         rt.actQ.Len(),
		ChildDepth:       rt.childQ.Len(),
		WfHeadAgeMs:      rt.wfQ.HeadAge(now),
		ActHeadAgeMs:     rt.actQ.HeadAge(now),
		ChildHeadAgeMs:   rt.childQ.HeadAge(now),
		WfPollers:        rt.wfQ.Parked(),
		ActPollers:       rt.actQ.Parked(),
		WfInflight:       rt.wfPool.InFlight(),
		ActInflight:      rt.actPool.InFlight(),
		StickyDepth:      rt.sticky.Len(),
		Started:          rt.stats.Started,
		ActivityAdds:     rt.stats.ActivityAdds,
		AttemptsExecuted: rt.stats.AttemptsExecuted,
		Completed:        rt.stats.Completed,
		Retries:          rt.stats.Retries,
		Failed:           rt.stats.Failed,
	}
	for _, sh := range rt.shards {
		s.ShardTransfer += sh.LenTransfer()
		s.ShardTimer += sh.LenTimer()
		s.ShardResched += sh.LenResched()
	}
	for _, q := range []*matcher.MatchQueue{rt.wfQ, rt.actQ, rt.childQ} {
		synced, spooled, rejected, dispatched, _, dropped := q.Stats()
		s.SyncMatched += synced
		s.Spooled += spooled
		s.Rejected += rejected
		s.Dispatched += dispatched
		s.Dropped += dropped
		for _, e := range q.ExpiredCount() {
			s.Expired += e
		}
	}
	return s
}
