package temporal

import (
	"testing"

	"github.com/lawRathod/queuesim/model/monitor"
)

// stormMetricsConfig is the WP3 reference storm: an activity-failure window
// on the default config drives retry amplification (spooled retries pile
// into matching-act), which is what the S2S latency histogram must expose.
func stormMetricsConfig() Config {
	cfg := DefaultConfig()
	cfg.Seed = 1
	cfg.BurstCount = 50
	cfg.SimEndMs = 60000
	cfg.MatchingCapacity = 1000
	cfg.FaultStartMs = 1
	cfg.FaultEndMs = 10001
	return cfg
}

// TestAutomaticMetricsRegistered is the WP3 zero-config surface gate: a
// plain run (no metric configuration anywhere) yields the automatic S2S
// latency histogram, the throughput/outcome counters, and the queue-depth
// gauges on the interesting surfaces. The counters must mirror the live
// TopologyStats at finalize (the final monitor sample covers events past
// the last pump tick), and the S2S histogram must have exactly one sample
// per activity execution.
func TestAutomaticMetricsRegistered(t *testing.T) {
	top, err := NewTopology(stormMetricsConfig())
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res := top.Run()
	s := res.Stats

	for _, tc := range []struct {
		name string
		want uint64
	}{
		{MetricStarted, s.Started},
		{MetricActivityAdds, s.ActivityAdds},
		{MetricCompleted, s.Completed},
		{MetricRetries, s.Retries},
		{MetricFailed, s.Failed},
	} {
		got, ok := res.Metrics.Counter(tc.name)
		if !ok {
			t.Fatalf("missing automatic counter %q in metrics:\n%s", tc.name, res.Digest)
		}
		if got.Value != tc.want {
			t.Fatalf("counter %s = %d, want %d (must mirror Stats)", tc.name, got.Value, tc.want)
		}
	}

	h, ok := res.Metrics.Histogram(MetricS2SLatency)
	if !ok {
		t.Fatalf("missing automatic S2S histogram %q:\n%s", MetricS2SLatency, res.Digest)
	}
	if h.Count != s.AttemptsExecuted {
		t.Fatalf("S2S histogram count = %d, want %d (one sample per activity execution)", h.Count, s.AttemptsExecuted)
	}
	if h.MaxMs < 0 || h.P50Ms < 0 || h.P90Ms < 0 || h.P99Ms < 0 {
		t.Fatalf("S2S histogram negative percentile: %+v", h)
	}

	for _, g := range []string{
		"matching.sticky.depth", "matching.wf.inflight", "matching.act.inflight",
		"history.shards.transfer", "history.shards.timer", "history.shards.resched",
	} {
		if _, ok := res.Metrics.Gauge(g); !ok {
			t.Fatalf("missing automatic gauge %q:\n%s", g, res.Digest)
		}
	}
}

// TestCalmS2SImmediate pins the calm behavior: with no faults every add
// sync-matches a parked poller at the instant it lands, so the S2S latency
// histogram records exactly one 0ms sample per activity add.
func TestCalmS2SImmediate(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 1
	cfg.BurstCount = 50
	cfg.SimEndMs = 60000
	cfg.MatchingCapacity = 1000
	cfg.FaultStartMs = 0
	cfg.FaultEndMs = 0
	cfg.Vector = nil

	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res := top.Run()
	h, ok := res.Metrics.Histogram(MetricS2SLatency)
	if !ok {
		t.Fatalf("missing S2S histogram on a calm run:\n%s", res.Digest)
	}
	if h.Count != res.Stats.ActivityAdds {
		t.Fatalf("calm S2S count = %d, want %d (every activity add records a sample)", h.Count, res.Stats.ActivityAdds)
	}
	// Sync-match is instant: the poller takes the task at its land time.
	if h.MaxMs != 0 {
		t.Fatalf("calm S2S max = %d, want 0 (sync-match, no queue wait)", h.MaxMs)
	}
}

// TestStormS2SLatencyElevated is the educated-decisions story: the SAME
// model with an activity-failure window shows measurably elevated S2S
// latency (spooled retries wait in matching-act), so the percentile
// histogram makes the storm's effect obvious from the run result alone.
func TestStormS2SLatencyElevated(t *testing.T) {
	storm := Run(stormMetricsConfig())
	calm := Run(DefaultConfig()) // identical except no fault window
	hc, _ := calm.Metrics.Histogram(MetricS2SLatency)
	hs, _ := storm.Metrics.Histogram(MetricS2SLatency)

	if hs.Count == 0 {
		t.Fatal("storm S2S histogram is empty (test config invalid)")
	}
	if hs.MaxMs <= hc.MaxMs {
		t.Fatalf("storm S2S max %d not above calm %d", hs.MaxMs, hc.MaxMs)
	}
	if hs.P90Ms <= 0 {
		t.Fatalf("storm S2S p90 = %d, want > 0 (retries must wait in matching-act)", hs.P90Ms)
	}
}

// TestS2SHistogramDeterministic is the WP3 determinism gate on the new
// metric: N=5 identical storm runs produce a byte-identical digest and an
// identical S2S percentile summary (the histogram math is pure integer
// bucket math over a deterministically-ordered sample stream).
func TestS2SHistogramDeterministic(t *testing.T) {
	run := func() (monitor.HistogramSummary, string) {
		top, err := NewTopology(stormMetricsConfig())
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		res := top.Run()
		h, ok := res.Metrics.Histogram(MetricS2SLatency)
		if !ok {
			t.Fatalf("missing S2S histogram:\n%s", res.Digest)
		}
		return h, res.Digest
	}
	first, firstDigest := run()
	for i := 1; i < 5; i++ {
		h, digest := run()
		if digest != firstDigest {
			t.Fatalf("run %d: digest mismatch\n%q\n%q", i, digest, firstDigest)
		}
		if h != first {
			t.Fatalf("run %d: S2S summary mismatch\n%+v\n%+v", i, h, first)
		}
	}
	if first.Count == 0 {
		t.Fatal("S2S histogram empty (test config invalid)")
	}
}
