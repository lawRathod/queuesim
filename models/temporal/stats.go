package temporal

import (
	"github.com/lawRathod/queuesim/model/monitor"
	"github.com/lawRathod/queuesim/models/temporal/components/matcher"
	"github.com/law/GoDES/observe"
)

// TopologyStats is the run-level counter set of a temporal model execution.
type TopologyStats struct {
	Started          uint64 // workflows entered (initial burst + arrivals)
	AttemptedAdds    uint64 // add attempts at the matching boundary (incl. rejected)
	ActivityAdds     uint64 // ACTIVITY add attempts only (AddActivityTask RPCs, incl. dropped/rescheduled)
	RejectedAdds     uint64 // capacity rejects at the matching boundary
	ActivityAttempts uint64 // activity execution attempts (the bubble's attempt counter)
	AttemptsExecuted uint64 // completed activity executions (all outcomes)
	Retries          uint64 // failed attempts re-added with backoff
	Completed        uint64 // successful activity executions (workflow completions)
	Failed           uint64 // workflows exhausted their retry budget or expired
	MaxQueue         int    // peak matching depth
	PollEvents       uint64 // tasks handed to parked pollers (sync-match + dispatch + sticky)
	SyncMatched      uint64 // tasks sync-matched (never spooled)
	Spooled          uint64 // tasks buffered to the backlog
	Expired          uint64 // tasks expired (all stages)
	Timeouts         uint64 // dispatch-time expiries only (matcher.ExpireAtPoll: waited out MaxWaitMs at dispatch)
	Dropped          uint64 // spool-time fault drops
}

// AttemptsPerStart is the retry-amplification headline metric (mirrors
// the bubble's AttemptedAdds/UniqueStarts). Counts ACTIVITY add attempts
// (calm = 1.0).
func (s TopologyStats) AttemptsPerStart() float64 {
	if s.Started == 0 {
		return 0
	}
	if s.ActivityAdds > 0 {
		return float64(s.ActivityAdds) / float64(s.Started)
	}
	return float64(s.ActivityAttempts) / float64(s.Started)
}

// RetryAmp is retries-per-completion.
func (s TopologyStats) RetryAmp() float64 {
	if s.Completed == 0 {
		return 0
	}
	return float64(s.Retries) / float64(s.Completed)
}

// RunRates holds a completed run's request/attempt lifecycle rates (WP6/B5).
// The per-second rates divide an existing cumulative counter by the simulated
// elapsed time; the ratio rates are dimensionless. Every field is derived from
// the EXISTING TopologyStats counters — no new machinery — and every
// zero-denominator case returns 0 (never NaN, never Inf). Pure: no RNG, no
// mutation.
type RunRates struct {
	// Goodput is successful workflow completions per simulated second
	// (Completed / elapsed seconds) — the useful-output rate. Mirrors
	// DesCartes des-metrics RequestTracker::goodput.
	Goodput float64
	// Throughput is completed activity executions per simulated second
	// (AttemptsExecuted / elapsed seconds) — raw work, retries included.
	// Mirrors DesCartes des-metrics RequestTracker::throughput.
	Throughput float64
	// RetryRate is retries per completed workflow (the RetryAmp convention).
	// 0 when no workflow completed. Attempts-per-start remains the separate
	// AttemptsPerStart headline.
	RetryRate float64
	// SuccessRate is completed workflows divided by started workflows; 0 when
	// nothing started. SuccessRate+ErrorRate may be < 1 when workflows were
	// still in flight at the horizon.
	SuccessRate float64
	// ErrorRate is failed workflows divided by started workflows; 0 when
	// nothing started.
	ErrorRate float64
	// TimeoutRate is dispatch-time schedule-to-start expiries (a task waited
	// out its MaxWaitMs residency deadline) divided by started workflows; 0
	// when nothing started. Sourced from the matcher's per-stage expiry
	// counter (Timeouts = matcher.ExpireAtPoll), NOT the all-stage Expired
	// aggregate.
	TimeoutRate float64
}

// Rates summarizes the run's lifecycle rates from the existing counters.
// elapsedMs is the simulated run duration in milliseconds (<= 0 ⇒ the
// per-second rates are 0). Pure — no RNG, no mutation.
func (s TopologyStats) Rates(elapsedMs int64) RunRates {
	var r RunRates
	if elapsedMs > 0 {
		secs := float64(elapsedMs) / 1000
		r.Goodput = float64(s.Completed) / secs
		r.Throughput = float64(s.AttemptsExecuted) / secs
	}
	r.RetryRate = s.RetryAmp()
	if s.Started > 0 {
		st := float64(s.Started)
		r.SuccessRate = float64(s.Completed) / st
		r.ErrorRate = float64(s.Failed) / st
		r.TimeoutRate = float64(s.Timeouts) / st
	}
	return r
}

// ratePermille scales a non-negative lifecycle rate to integer permille for
// the int64-only gauge surface (1000× a ratio; 1000× a per-second rate).
// Truncation is deterministic; rates are bounded far below int64 range.
func ratePermille(rate float64) int64 {
	if rate <= 0 {
		return 0
	}
	return int64(rate * 1000)
}

// Result bundles a run's outcome. The historical fields (Stats/Status/
// DrainMs) are joined by the System's free-monitor metrics and digest:
// Metrics carries the automatic S2S latency percentile histogram
// (MetricS2SLatency: p50/p90/p99/max), the throughput/outcome counters
// (MetricStarted/MetricActivityAdds/MetricCompleted/MetricRetries/
// MetricFailed), and the queue-depth gauges (matching/history surfaces) —
// zero configuration; Digest is the deterministic serialization of
// Metrics.
type Result struct {
	Stats  TopologyStats
	Status observe.MonitorStatus
	// DrainMs is the sim time when the matching backlog LAST transitioned
	// from non-empty to empty after the run started — the SLA-recovery
	// signal. 0 = the backlog never drained.
	DrainMs int64
	// Metrics are the System's free-monitor metrics (empty when monitoring
	// is disabled; Digest is the deterministic serialization of Metrics).
	Metrics monitor.Metrics
	Digest  string
	// SimEndMs is the ending simulation time.
	SimEndMs int64
}

// finalizeStats single-sources the queue-accounted counters (sync/spool/
// reject/dispatch/drop/expire) from the queues' own tallies; the attempt
// and outcome counters (Started, AttemptedAdds, ActivityAdds,
// ActivityAttempts, AttemptsExecuted, Retries, Completed, Failed) are
// live-only and untouched here.
//
// The activity path has TWO queues (CHILD rt.childQ + ROOT rt.actQ): the
// SyncMatched/Spooled/Dispatched counts are summed across both. This
// deliberately double-counts a task that spooled on the child and was then
// re-spooled through the root drain (it counts once per partition it
// traversed) — a partition-spool-event counter, not a unique-task count;
// it does not affect aps/throughput (those use the live ActivityAdds).
func (rt *runtime) finalizeStats() {
	var synced, spooled, rejected, dispatched, dropped, expired uint64
	for _, q := range []*matcher.MatchQueue{rt.wfQ, rt.actQ, rt.childQ} {
		if q == nil {
			continue
		}
		s, sp, rj, dp, _, dr := q.Stats()
		synced += s
		spooled += sp
		rejected += rj
		dispatched += dp
		dropped += dr
		for _, e := range q.ExpiredCount() {
			expired += e
		}
	}
	rt.stats.SyncMatched = synced
	rt.stats.Spooled = spooled
	rt.stats.RejectedAdds = rejected
	rt.stats.PollEvents = dispatched
	rt.stats.Dropped = dropped
	rt.stats.Expired = expired
	// Timeouts is the PER-STAGE dispatch-time expiry (S2S deadline wait-out,
	// matcher.ExpireAtPoll) — the only stage the model ever increments today.
	// It is kept distinct from Expired (the all-stage sum) so the conflation
	// seen in models/tasqueue/runtime.go:31 does not propagate: a future
	// ExpireAtSpool/ExpireAtRead stage would move Expired without moving
	// Timeouts.
	rt.stats.Timeouts = rt.liveDispatchTimeouts()
}

// liveDispatchTimeouts is the run-to-date dispatch-time (schedule-to-start)
// expiry count across all matching partitions — the PER-STAGE
// matcher.ExpireAtPoll tally, NOT the all-stage Expired aggregate. It is a
// pure read (zero RNG, zero mutation). It is the LIVE source for the
// temporal.lifecycle.timeout_rate gauge: finalizeStats runs only AFTER the
// system's final Sample, so a gauge reading TopologyStats.Timeouts would be
// constant zero.
func (rt *runtime) liveDispatchTimeouts() uint64 {
	var n uint64
	for _, q := range []*matcher.MatchQueue{rt.wfQ, rt.actQ, rt.childQ} {
		if q != nil {
			n += q.ExpiredCount()[matcher.ExpireAtPoll]
		}
	}
	return n
}
