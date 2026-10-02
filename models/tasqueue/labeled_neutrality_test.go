package tasqueue

import (
	"testing"

	"github.com/lawRathod/queuesim/model/monitor"
)

// TestLabeledMetricsZeroDefault pins the neutral default: the WP2 flag is
// off in DefaultConfig, so no labeled surface is registered by default
// (ADR-115).
func TestLabeledMetricsZeroDefault(t *testing.T) {
	if got := DefaultConfig().LabeledMetrics; got {
		t.Fatal("DefaultConfig().LabeledMetrics = true, want false (off by default)")
	}
}

// unlabeledEquivalent projects a Metrics summary down to its UNLABELED
// entries — every metric the flag adds is labeled, so this strips exactly
// the labeled surfaces.
func unlabeledEquivalent(ms monitor.Metrics) monitor.Metrics {
	out := monitor.Metrics{}
	for _, c := range ms.Counters {
		if c.LabelSet == "" {
			out.Counters = append(out.Counters, c)
		}
	}
	for _, g := range ms.Gauges {
		if g.LabelSet == "" {
			out.Gauges = append(out.Gauges, g)
		}
	}
	for _, h := range ms.Histograms {
		if h.LabelSet == "" {
			out.Histograms = append(out.Histograms, h)
		}
	}
	return out
}

// TestLabeledMetricsFlagNeutral is the tasqueue labeled-path neutrality gate
// (mandatory constraint 2): with Config.LabeledMetrics on, the unlabeled-
// equivalent projection of the digest equals the flag-off digest byte for
// byte and the simulation outcome is unchanged. DefaultConfig draws RNG
// (FailP 0.30), so any extra draw in the labeled path would shift the trace.
// Non-vacuous: the labeled digest must differ from the flag-off digest.
func TestLabeledMetricsFlagNeutral(t *testing.T) {
	type outcome struct {
		digest    string
		simEnd    int64
		started   uint64
		completed uint64
		failed    uint64
		metrics   monitor.Metrics
	}
	run := func(labeled bool) outcome {
		cfg := DefaultConfig()
		cfg.BurstCount = 20
		cfg.SimEndMs = 2000
		cfg.LabeledMetrics = labeled
		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		res := top.Run()
		o := outcome{digest: res.Digest, simEnd: res.SimEndMs, metrics: res.Metrics}
		if c, ok := res.Metrics.Counter(MetricStarted); ok {
			o.started = c.Value
		}
		if c, ok := res.Metrics.Counter(MetricCompleted); ok {
			o.completed = c.Value
		}
		if c, ok := res.Metrics.Counter(MetricFailed); ok {
			o.failed = c.Value
		}
		return o
	}

	for i := 0; i < 3; i++ {
		on := run(true)
		off := run(false)
		if on.digest == off.digest {
			t.Fatalf("run %d: labeled-on digest equals labeled-off — the flag registered nothing", i)
		}
		found := false
		for _, g := range on.metrics.Gauges {
			if g.Name == "tasqueue.queue_depth" && g.LabelSet != "" {
				found = true
			}
		}
		if !found {
			t.Fatalf("run %d: labeled surface tasqueue.queue_depth missing with LabeledMetrics on", i)
		}
		if on.simEnd != off.simEnd || on.started != off.started ||
			on.completed != off.completed || on.failed != off.failed {
			t.Fatalf("run %d: labeled registration perturbed the simulation:\non  %+v\noff %+v", i, on, off)
		}
		if got := unlabeledEquivalent(on.metrics).String(); got != off.digest {
			t.Fatalf("run %d: labeled run's unlabeled-equivalent digest differs from flag-off:\nprojected %q\noff       %q",
				i, got, off.digest)
		}
	}
}
