package lab

import (
	"fmt"
	"time"

	"github.com/lawRathod/queuesim/godes/core"
	"github.com/lawRathod/queuesim/godes/seed"
	"github.com/lawRathod/queuesim/models/tasqueue"
	"github.com/lawRathod/queuesim/models/temporal"
)

// traceBuilder accumulates a downsampled timeline. It samples at most
// ~maxFrames points regardless of the simulation's native tick cadence.
type traceBuilder struct {
	stride int64
	next   int64
	t      []int64
	v      map[string][]float64
	keys   []string
}

func newTrace(series []SeriesMeta, horizonMs int64) *traceBuilder {
	stride := int64(1)
	if horizonMs > 0 {
		if s := horizonMs / maxFrames; s > 1 {
			stride = s
		}
	}
	keys := make([]string, 0, len(series))
	v := make(map[string][]float64, len(series))
	for _, s := range series {
		keys = append(keys, s.Key)
		v[s.Key] = nil
	}
	return &traceBuilder{stride: stride, t: make([]int64, 0, maxFrames+2), v: v, keys: keys}
}

// maybe samples at nowMs when the stride has elapsed, calling frame to read
// the model state.
func (b *traceBuilder) maybe(nowMs int64, frame func() map[string]float64) {
	if nowMs < b.next {
		return
	}
	b.add(nowMs, frame())
	b.next = nowMs + b.stride
}

func (b *traceBuilder) add(nowMs int64, vals map[string]float64) {
	b.t = append(b.t, nowMs)
	for _, k := range b.keys {
		b.v[k] = append(b.v[k], vals[k])
	}
}

// finish derives per-second rates from the counter timelines. Deltas are
// measured over a trailing window (about 1/50th of the trace ceiling 64
// samples) so rare events — a single arrival between two samples — read as
// a rate instead of a 0/1000 spike.
func (b *traceBuilder) finish(rateSources map[string]string) {
	n := len(b.t)
	win := n / 50
	if win < 1 {
		win = 1
	}
	if win > 64 {
		win = 64
	}
	for dst, src := range rateSources {
		srcVals := b.v[src]
		out := make([]float64, len(srcVals))
		for i := 0; i < len(srcVals); i++ {
			j := i - win
			if j < 0 {
				j = 0
			}
			dt := float64(b.t[i]-b.t[j]) / 1000
			if dt > 0 {
				out[i] = (srcVals[i] - srcVals[j]) / dt
			}
		}
		b.v[dst] = out
	}
	// Rate series are not in the sampling key set; initialize any that a
	// very short run left nil so JSON always carries an array.
	for _, s := range append(temporalSeries, tasqueueSeries...) {
		if _, ok := b.v[s.Key]; !ok {
			b.v[s.Key] = make([]float64, len(b.t))
		}
	}
}

func (b *traceBuilder) peak(keys ...string) float64 {
	var max float64
	for _, k := range keys {
		for _, v := range b.v[k] {
			if v > max {
				max = v
			}
		}
	}
	return max
}

// peakSum returns the maximum value of the per-frame sum of keys — the
// total backlog at one instant, not the largest single queue.
func (b *traceBuilder) peakSum(keys ...string) float64 {
	var max float64
	for i := range b.t {
		var sum float64
		for _, k := range keys {
			if a := b.v[k]; a != nil {
				sum += a[i]
			}
		}
		if sum > max {
			max = sum
		}
	}
	return max
}

// mean returns the average of a sampled series (0 for an empty series).
func (b *traceBuilder) mean(key string) float64 {
	a := b.v[key]
	if len(a) == 0 {
		return 0
	}
	var sum float64
	for _, v := range a {
		sum += v
	}
	return sum / float64(len(a))
}

// executeTemporal runs one Temporal experiment and returns its trace.
func executeTemporal(vals map[string]float64, requestedSeed int64) (out *Run, err error) {
	defer func() {
		if r := recover(); r != nil {
			out, err = nil, fmt.Errorf("simulation failed: %v", r)
		}
	}()

	cfg := applyTemporal(vals)
	cfg.Seed = requestedSeed
	top, err := temporal.NewTopology(cfg)
	if err != nil {
		return nil, err
	}
	tb := newTrace(temporalSeries, cfg.SimEndMs)
	top.System().AddTickHook(func(now core.SimTime) {
		tb.maybe(now.Int64(), func() map[string]float64 {
			s := top.Snapshot()
			return map[string]float64{
				"wf_depth":          float64(s.WfDepth),
				"act_depth":         float64(s.ActDepth),
				"child_depth":       float64(s.ChildDepth),
				"sticky_depth":      float64(s.StickyDepth),
				"shard_transfer":    float64(s.ShardTransfer),
				"shard_timer":       float64(s.ShardTimer),
				"shard_resched":     float64(s.ShardResched),
				"wf_pollers":        float64(s.WfPollers),
				"act_pollers":       float64(s.ActPollers),
				"wf_inflight":       float64(s.WfInflight),
				"act_inflight":      float64(s.ActInflight),
				"wf_head_age_ms":    float64(s.WfHeadAgeMs),
				"act_head_age_ms":   float64(s.ActHeadAgeMs),
				"child_head_age_ms": float64(s.ChildHeadAgeMs),
				"started":           float64(s.Started),
				"activity_adds":     float64(s.ActivityAdds),
				"attempts_executed": float64(s.AttemptsExecuted),
				"completed":         float64(s.Completed),
				"retries":           float64(s.Retries),
				"failed":            float64(s.Failed),
				"sync_matched":      float64(s.SyncMatched),
				"spooled":           float64(s.Spooled),
				"dispatched":        float64(s.Dispatched),
				"rejected":          float64(s.Rejected),
				"dropped":           float64(s.Dropped),
				"expired":           float64(s.Expired),
			}
		})
	})

	start := time.Now()
	res := top.Run()
	wall := time.Since(start).Milliseconds()
	tb.finish(temporalRateSources)

	st := res.Stats
	// Rates use the pump window, not Result.SimEndMs: long-poll expiries can
	// push the tail far past the horizon and would understate goodput.
	elapsed := int64(vals["sim_end_ms"])
	if n := len(tb.t); n > 0 {
		elapsed = tb.t[n-1]
	}
	rates := st.Rates(elapsed)

	peakBacklog := tb.peakSum("wf_depth", "act_depth", "child_depth")
	peakHeadAge := tb.peak("wf_head_age_ms", "act_head_age_ms", "child_head_age_ms")

	metrics := []Metric{
		{Key: "started", Label: "Workflows started", Value: float64(st.Started), Unit: "wf", Precision: 0,
			Help: "Workflows that entered the system (initial burst + arrivals)."},
		{Key: "completed", Label: "Completed", Value: float64(st.Completed), Unit: "wf", Precision: 0,
			Help: "Workflows whose final task finished successfully."},
		{Key: "failed", Label: "Failed", Value: float64(st.Failed), Unit: "wf", Precision: 0, Tone: badIfPositive(float64(st.Failed)),
			Help: "Workflows that exhausted the retry budget or expired."},
		{Key: "retries", Label: "Retries", Value: float64(st.Retries), Unit: "attempts", Precision: 0, Tone: warnIfPositive(float64(st.Retries)),
			Help: "Failed attempts re-added to matching with backoff."},
		{Key: "attempts_per_start", Label: "Attempts per start", Value: st.AttemptsPerStart(), Unit: "×", Precision: 2, Tone: warnAbove(st.AttemptsPerStart(), 1.01),
			Help: "Retry amplification: activity attempts ÷ workflows started. Calm is 1.0."},
		{Key: "goodput", Label: "Goodput", Value: rates.Goodput, Unit: "/s", Precision: 2,
			Help: "Successful workflow completions per simulated second during the run."},
		{Key: "peak_backlog", Label: "Peak backlog", Value: peakBacklog, Unit: "tasks", Precision: 0, Tone: warnIfPositive(peakBacklog),
			Help: "Largest combined wf + act + child backlog at any sampled instant."},
		{Key: "peak_head_age", Label: "Peak backlog head age", Value: peakHeadAge, Unit: "ms", Precision: 0, Tone: warnAbove(peakHeadAge, 1),
			Help: "Longest wait endured by the oldest task in a backlog."},
	}
	if syncShare, ok := typedShare(st.SyncMatched, st.SyncMatched+st.Spooled); ok {
		metrics = append(metrics, Metric{Key: "sync_share", Label: "Served by sync-match", Value: syncShare * 100, Unit: "%", Precision: 1,
			Help: "Tasks handed straight to a parked poller instead of spooling."})
	}
	if res.DrainMs > 0 {
		metrics = append(metrics, Metric{Key: "drain_ms", Label: "Last drain", Value: float64(res.DrainMs), Unit: "ms", Precision: 0,
			Help: "When the matching backlog last emptied."})
	}

	var pcts []Percentile
	if h, ok := res.Metrics.Histogram(temporal.MetricS2SLatency); ok {
		pcts = append(pcts, Percentile{Name: h.Name, Label: "Schedule-to-start latency", Count: h.Count,
			P50Ms: h.P50Ms, P90Ms: h.P90Ms, P99Ms: h.P99Ms, MaxMs: h.MaxMs})
	}

	status := &Status{
		Metastable:     res.Status.Metastable,
		Recovered:      res.Status.Recovered,
		MetastableAtMs: res.Status.MetastableAtMs,
	}
	if res.Status.RecoveryTimeMs != nil {
		status.RecoveryTimeMs = *res.Status.RecoveryTimeMs
	}

	r := &Run{
		Model:       "temporal",
		Seed:        seed.Resolve(requestedSeed),
		SimEndMs:    res.SimEndMs,
		WallMs:      wall,
		Annotations: temporalAnnotations(vals),
		Series:      temporalSeries,
		Charts:      temporalCharts,
		T:           tb.t,
		V:           tb.v,
		Metrics:     metrics,
		Percentiles: pcts,
		Status:      status,
		Digest:      res.Digest,
	}
	r.Insights = temporalInsights(vals, metrics, tb, peakBacklog, peakHeadAge)
	r.Notes = runNotes(tb, r)
	return r, nil
}

// executeTasqueue runs one Tasqueue experiment and returns its trace.
func executeTasqueue(vals map[string]float64, requestedSeed int64) (out *Run, err error) {
	defer func() {
		if r := recover(); r != nil {
			out, err = nil, fmt.Errorf("simulation failed: %v", r)
		}
	}()

	cfg := applyTasqueue(vals)
	cfg.Seed = requestedSeed
	top, err := tasqueue.NewTopology(cfg)
	if err != nil {
		return nil, err
	}
	tb := newTrace(tasqueueSeries, cfg.SimEndMs)
	top.System().AddTickHook(func(now core.SimTime) {
		tb.maybe(now.Int64(), func() map[string]float64 {
			s := top.Snapshot()
			return map[string]float64{
				"queue_depth":      float64(s.QueueDepth),
				"dispatch_depth":   float64(s.DispatchDepth),
				"timer_depth":      float64(s.TimerDepth),
				"poller_inflight":  float64(s.PollerInFlight),
				"started":          float64(s.Started),
				"completed":        float64(s.Completed),
				"retried":          float64(s.Retried),
				"failed":           float64(s.Failed),
				"dropped":          float64(s.Dropped),
				"expired":          float64(s.Expired),
				"rejected_adds":    float64(s.RejectedAdds),
				"attempts_total":   float64(s.AttemptsTotal),
				"chain_steps_done": float64(s.ChainStepsDone),
				"group_jobs_done":  float64(s.GroupJobsDone),
			}
		})
	})

	start := time.Now()
	res := top.Run()
	wall := time.Since(start).Milliseconds()
	tb.finish(tasqueueRateSources)

	st := res.Stats
	peakBacklog := tb.peakSum("queue_depth", "dispatch_depth", "timer_depth")

	metrics := []Metric{
		{Key: "started", Label: "Jobs started", Value: float64(st.Started), Unit: "jobs", Precision: 0,
			Help: "Jobs enqueued (burst + scheduled + OnError)."},
		{Key: "completed", Label: "Completed", Value: float64(st.Completed), Unit: "jobs", Precision: 0,
			Help: "Jobs that finished: plain success, chain tail, or group join."},
		{Key: "failed", Label: "Failed", Value: float64(st.Failed), Unit: "jobs", Precision: 0, Tone: badIfPositive(float64(st.Failed)),
			Help: "Jobs that exhausted retries or expired in the queue."},
		{Key: "retried", Label: "Retried", Value: float64(st.Retried), Unit: "attempts", Precision: 0, Tone: warnIfPositive(float64(st.Retried)),
			Help: "Failed attempts re-enqueued immediately."},
		{Key: "attempts_per_start", Label: "Attempts per start", Value: st.AttemptsPerStart(), Unit: "×", Precision: 2, Tone: warnAbove(st.AttemptsPerStart(), 1.01),
			Help: "Handler attempts ÷ jobs started. Chain steps inflate this by design."},
		{Key: "peak_backlog", Label: "Peak backlog", Value: peakBacklog, Unit: "jobs", Precision: 0, Tone: warnIfPositive(peakBacklog),
			Help: "Largest combined broker + dispatch + scheduled backlog."},
	}
	if st.RejectedAdds > 0 {
		metrics = append(metrics, Metric{Key: "rejected", Label: "Rejected adds", Value: float64(st.RejectedAdds), Unit: "jobs", Precision: 0, Tone: "bad"})
	}
	if st.Dropped > 0 {
		metrics = append(metrics, Metric{Key: "dropped", Label: "Dropped", Value: float64(st.Dropped), Unit: "jobs", Precision: 0, Tone: "bad"})
	}
	if st.Expired > 0 {
		metrics = append(metrics, Metric{Key: "expired", Label: "Expired", Value: float64(st.Expired), Unit: "jobs", Precision: 0, Tone: "bad"})
	}
	if st.ChainStepsDone > 0 {
		metrics = append(metrics, Metric{Key: "chain_steps", Label: "Chain steps", Value: float64(st.ChainStepsDone), Precision: 0})
	}
	if st.GroupJobsDone > 0 {
		metrics = append(metrics, Metric{Key: "group_jobs", Label: "Group members done", Value: float64(st.GroupJobsDone), Precision: 0})
	}

	var pcts []Percentile
	if h, ok := res.Metrics.Histogram(tasqueue.MetricCompletionLatency); ok {
		pcts = append(pcts, Percentile{Name: h.Name, Label: "Enqueue → completion latency", Count: h.Count,
			P50Ms: h.P50Ms, P90Ms: h.P90Ms, P99Ms: h.P99Ms, MaxMs: h.MaxMs})
	}

	r := &Run{
		Model:       "tasqueue",
		Seed:        seed.Resolve(requestedSeed),
		SimEndMs:    res.SimEndMs,
		WallMs:      wall,
		Annotations: nil,
		Series:      tasqueueSeries,
		Charts:      tasqueueCharts,
		T:           tb.t,
		V:           tb.v,
		Metrics:     metrics,
		Percentiles: pcts,
		Digest:      res.Digest,
	}
	r.Insights = tasqueueInsights(vals, metrics, tb, peakBacklog)
	r.Notes = runNotes(tb, r)
	return r, nil
}

func temporalAnnotations(vals map[string]float64) []Annotation {
	var out []Annotation
	start, end := vals["fault_start_ms"], vals["fault_end_ms"]
	if start > 0 || end > 0 {
		out = append(out, Annotation{Label: "activity failure window", StartMs: int64(start), EndMs: int64(end), Kind: "fault"})
	}
	if vals["arrival_switch_ms"] > 0 {
		out = append(out, Annotation{Label: "arrival rate changes", StartMs: int64(vals["arrival_switch_ms"]),
			EndMs: int64(vals["arrival_switch_ms"]), Kind: "load"})
	}
	return out
}

func runNotes(tb *traceBuilder, r *Run) []string {
	notes := []string{fmt.Sprintf("Sampled every %d simulated ms (%d points).", tb.stride, len(tb.t))}
	if g, ok := seed.Global(); ok {
		notes = append(notes, fmt.Sprintf("DSTSEED=%d is set and overrides the seed field.", g))
	}
	return notes
}

// Execute runs a scenario with optional knob overrides.
func Execute(req Request) (*Run, error) {
	sc, ok := scenarios[req.Scenario]
	if !ok {
		return nil, fmt.Errorf("unknown scenario %q", req.Scenario)
	}
	if err := validateKnobs(sc.defs, req.Knobs); err != nil {
		return nil, err
	}
	vals := effectiveValues(sc.defs, sc.preset, req.Knobs)
	if err := validateKnobs(sc.defs, vals); err != nil {
		return nil, err
	}
	requested := sc.defaultSeed
	if req.Seed != nil {
		requested = *req.Seed
	}
	r, err := sc.run(vals, requested)
	if err != nil {
		return nil, err
	}
	r.Scenario = sc.id
	r.Knobs = vals
	return r, nil
}

// Sweep runs a scenario once per seed and returns the digest-bearing
// summary points. The same config with the same seed must always produce
// the same digest; different seeds trace different random streams.
func Sweep(scenarioID string, knobs map[string]float64, seeds []int64) ([]SweepPoint, error) {
	if len(seeds) == 0 {
		return nil, fmt.Errorf("sweep needs at least one seed")
	}
	out := make([]SweepPoint, 0, len(seeds))
	for _, s := range seeds {
		s := s
		r, err := Execute(Request{Scenario: scenarioID, Knobs: knobs, Seed: &s})
		if err != nil {
			return nil, err
		}
		out = append(out, SweepPoint{
			Seed:      r.Seed,
			Digest:    r.Digest,
			Value:     metricValue(r, "attempts_per_start"),
			Completed: uint64(metricValue(r, "completed")),
			Failed:    uint64(metricValue(r, "failed")),
			MaxDepth:  metricValue(r, "peak_backlog"),
		})
	}
	return out, nil
}

func metricValue(r *Run, key string) float64 {
	for _, m := range r.Metrics {
		if m.Key == key {
			return m.Value
		}
	}
	return 0
}

func badIfPositive(v float64) string {
	if v > 0 {
		return "bad"
	}
	return ""
}

func warnIfPositive(v float64) string {
	if v > 0 {
		return "warn"
	}
	return ""
}

func warnAbove(v, threshold float64) string {
	if v > threshold {
		return "warn"
	}
	return ""
}

// typedShare returns part/whole when whole > 0.
func typedShare(part, whole uint64) (float64, bool) {
	if whole == 0 {
		return 0, false
	}
	return float64(part) / float64(whole), true
}
