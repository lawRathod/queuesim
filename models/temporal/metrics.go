package temporal

// Automatic monitor metric names. These are registered on the System's
// free monitor by attachMetrics when monitoring is enabled — zero model
// configuration (they appear because the model is built on the library).
// They flow into Result.Metrics, where the monitor.Metrics finders
// (Counter/Gauge/Histogram) surface them by name.
const (
	// MetricS2SLatency is the schedule-to-start latency histogram: one
	// sample per activity execution, measured as take-time − BornMs — the
	// same clock the model's activity S2S timeout uses (task added to
	// matching-act → taken by a poller). Percentile summary via
	// monitor.HistogramSummary (p50/p90/p99/max).
	MetricS2SLatency = "temporal.s2s.latency"

	// MetricStarted counts workflows entered (initial burst + arrivals).
	MetricStarted = "temporal.started"
	// MetricActivityAdds counts AddActivityTask attempts at the matching
	// boundary (incl. dropped/rescheduled) — the retry-amplification
	// numerator.
	MetricActivityAdds = "temporal.activity_adds"
	// MetricCompleted counts successful workflow completions.
	MetricCompleted = "temporal.completed"
	// MetricRetries counts failed attempts re-added with backoff.
	MetricRetries = "temporal.retries"
	// MetricFailed counts workflows that exhausted their retry budget or
	// expired.
	MetricFailed = "temporal.failed"
)

// s2sCollector accumulates schedule-to-start latency samples between
// monitor samples. It is created by attachMetrics ONLY when monitoring is
// enabled; onTake appends when it is non-nil, and the monitor's
// AddHistogram closure drains it at every Sample. Purely deterministic:
// zero RNG draws, zero simulation mutation (the sim clock minus the task's
// birth clock).
type s2sCollector struct {
	samples []int64
}
