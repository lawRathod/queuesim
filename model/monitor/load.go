package monitor

// Load vocabulary (WP3, ADR-115). "Load" on a modelled surface is exactly
// one of three things, and every load metric added by the observability
// port is one of those three:
//
//   - arrivals into the surface        → a cumulative counter (throughput)
//   - service rate out of the surface  → a ratio gauge (utilization)
//   - occupancy of the surface         → an instantaneous count gauge
//     (queue_depth = work waiting; inflight = work in service)
//
// The four leaf names below are shared so models/temporal and
// models/tasqueue cannot drift. They are NEW names: no existing metric is
// renamed (a rename would move both models' determinism digests and defeat
// the off-by-default gate — ADR-115). Build a namespaced name with
// LoadName.
const (
	// LoadQueueDepth is the occupancy leaf for work WAITING on a surface
	// (backlog / queue depth).
	LoadQueueDepth = "queue_depth"
	// LoadUtilization is the service-rate leaf: work in service divided by
	// the service capacity, reported as integer permille (may exceed 1000).
	LoadUtilization = "utilization"
	// LoadInflight is the occupancy leaf for work currently IN SERVICE. It
	// is defined here so both models spell the in-service occupancy the same
	// way; the existing unlabeled matching.*.inflight gauges predate the
	// vocabulary and are deliberately not renamed (additive-only).
	LoadInflight = "inflight"
	// LoadThroughput is the arrivals leaf: a cumulative count of work
	// admitted into a surface (the arrival leg of load).
	LoadThroughput = "throughput"
)

// LoadName builds a namespaced load-metric name: prefix + "." + leaf. The
// separator lives here so the two models cannot spell the shared vocabulary
// differently.
func LoadName(prefix, leaf string) string {
	return prefix + "." + leaf
}

// UtilizationPermille returns a pool's service utilization as integer
// permille (1000 × inflight / budget). It reports ok=false when budget <= 0
// (an unlimited budget has no meaningful ratio); a caller MUST NOT register
// the result as a gauge sample in that case — a negative value would wrap
// the monitor's unsigned gauge accumulator, and 0 would read as idle. Gate
// on ok and OMIT the surface instead. This is the service-rate leg of the
// load definition. Pure integer math — no RNG, no mutation. The result may
// exceed 1000 when inflight exceeds the per-poller budget.
func UtilizationPermille(inflight, budget int) (int64, bool) {
	if budget <= 0 {
		return 0, false
	}
	return 1000 * int64(inflight) / int64(budget), true
}

// NonEmptyFraction returns the fraction of samples in which a gauge series
// held a positive value — a queue-non-empty fraction. Fold an existing
// unlabeled gauge series (Monitor.GaugeSeries) to get an honest OCCUPANCY
// measure for a surface whose model has no per-tick server:
//
//	occ := monitor.NonEmptyFraction(m.GaugeSeries("history.shards.transfer"))
//
// It is a pure function of the series (no RNG, no mutation); 0 for an
// empty series. This is deliberately NOT worker utilization — a
// busy-worker gauge would assert a service mechanism the history model does
// not simulate (components/history/shard.go: the shard worker counts have no
// per-tick effect).
func NonEmptyFraction(series []int64) float64 {
	if len(series) == 0 {
		return 0
	}
	var nonEmpty int
	for _, v := range series {
		if v > 0 {
			nonEmpty++
		}
	}
	return float64(nonEmpty) / float64(len(series))
}
