package temporal

import (
	"strconv"

	"github.com/lawRathod/queuesim/model/monitor"
	"github.com/lawRathod/queuesim/models/temporal/components/worker"
)

// attachLabeledMetrics registers the OPT-IN labeled/attributed metric
// surfaces (Config.LabeledMetrics). Every metric here is exactly one leg of
// the load definition (model/monitor load vocabulary):
//
//   - arrivals  → history.shards.throughput{shard,kind} — the history-
//     boundary INTAKE counter (the offered-load rate; WP3, the highest-value
//     surface: it makes "load on history vs matching" defensible)
//   - occupancy → history.shards.queue_depth{shard,kind} (per-shard WAITING
//     depth — the correctly-named counterpart to the misnamed aggregate
//     history.shards.inflight, see attachMetrics)
//   - service rate → matching.utilization{queue} = 1000 × InFlight / Budget.
//     A pool with an UNLIMITED budget (Budget() <= 0) has no meaningful
//     ratio, so its surface is OMITTED — a negative sample must never reach
//     the monitor's unsigned gauge accumulator.
//
// Cost (ADR-115): counters retain no per-tick series (one uint64 read per
// Sample); every GAUGE retains a full per-tick series (model/monitor doc.go).
// With the default one shard this registers 3 intake counters + 3 shard-depth
// gauges + up to 2 matching-utilization gauges + 6 temporal.lifecycle.* rate
// gauges = 14 series, of which up to 11 are gauges ⇒ ~11×60k retained samples
// (~5.3 MB) at PumpMs=1 over a 60 s run.
//
// OFF by default (Config.LabeledMetrics false): nothing here runs and the
// six pinned determinism digests stay unmoved. The closures are pure reads —
// zero RNG draws, zero simulation mutation.
func (rt *runtime) attachLabeledMetrics(m *monitor.Monitor) {
	for i, sh := range rt.shards {
		i, sh := i, sh
		labels := func(kind string) []monitor.Label {
			return []monitor.Label{{Key: "shard", Value: strconv.Itoa(i)}, {Key: "kind", Value: kind}}
		}
		intake := monitor.LoadName("history.shards", monitor.LoadThroughput)
		depth := monitor.LoadName("history.shards", monitor.LoadQueueDepth)
		m.AddCounterKey(intake, labels("transfer"), sh.IntakeTransfer)
		m.AddCounterKey(intake, labels("timer"), sh.IntakeTimer)
		m.AddCounterKey(intake, labels("resched"), sh.IntakeResched)
		m.AddGaugeKey(depth, labels("transfer"), func() int64 { return int64(sh.LenTransfer()) })
		m.AddGaugeKey(depth, labels("timer"), func() int64 { return int64(sh.LenTimer()) })
		m.AddGaugeKey(depth, labels("resched"), func() int64 { return int64(sh.LenResched()) })
	}
	for _, p := range []struct {
		queue string
		pool  *worker.Pool
	}{{"wf", rt.wfPool}, {"act", rt.actPool}} {
		p := p
		if _, ok := monitor.UtilizationPermille(0, p.pool.Budget()); !ok {
			continue // unlimited budget: utilization undefined — omit the surface
		}
		m.AddGaugeKey(monitor.LoadName("matching", monitor.LoadUtilization),
			[]monitor.Label{{Key: "queue", Value: p.queue}},
			func() int64 {
				// Budget() is immutable after NewPool (no setter), so the
				// attach-time budget>0 gate above makes ok always true here
				// — the discarded bool can never hide a negative sample.
				v, _ := monitor.UtilizationPermille(p.pool.InFlight(), p.pool.Budget())
				return v
			})
	}
	// Lifecycle rates (WP6/B5): the run-to-date request/attempt rates as
	// sampled gauges. Gauges are int64-only, so each rate is scaled by 1000
	// (permille — the same integer-ratio convention as UtilizationPermille).
	// LABELED ({scope=run}) so the unlabeled-equivalent projection still
	// equals the flag-off digest (constraint 2). Each closure is a pure read
	// of the existing counters via TopologyStats.Rates — zero RNG, zero
	// mutation.
	now := func() int64 { return rt.sim.Now().Int64() }
	regRate := func(leaf string, pick func(RunRates) float64) {
		m.AddGaugeKey("temporal.lifecycle."+leaf,
			[]monitor.Label{{Key: "scope", Value: "run"}},
			func() int64 { return ratePermille(pick(rt.stats.Rates(now()))) })
	}
	regRate("goodput", func(r RunRates) float64 { return r.Goodput })
	regRate("throughput", func(r RunRates) float64 { return r.Throughput })
	regRate("retry_rate", func(r RunRates) float64 { return r.RetryRate })
	regRate("success_rate", func(r RunRates) float64 { return r.SuccessRate })
	regRate("error_rate", func(r RunRates) float64 { return r.ErrorRate })
	// timeout_rate reads the LIVE per-stage expiry tally directly, not
	// TopologyStats.Timeouts: finalizeStats (and thus Timeouts) runs only
	// after the system's final Sample, so routing the gauge through
	// RunRates would make it a constant zero. See liveDispatchTimeouts.
	m.AddGaugeKey("temporal.lifecycle.timeout_rate",
		[]monitor.Label{{Key: "scope", Value: "run"}},
		func() int64 {
			if rt.stats.Started == 0 {
				return 0
			}
			return ratePermille(float64(rt.liveDispatchTimeouts()) / float64(rt.stats.Started))
		})
}
