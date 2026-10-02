package temporal

import (
	"strings"
	"testing"

	"github.com/lawRathod/queuesim/model/monitor"
)

// TestLabeledMetricsNeutral is the WP1 labeled-path neutrality gate
// (mandatory constraint 2). Registering an additional LABELED metric on the
// free monitor must be purely observational: the simulation outcome must be
// byte-identical to the run whose equivalent metric is registered UNLABELED
// (the "unlabeled-equivalent registration"), and the digest must differ only
// by the label decoration.
//
// The RNG-drawing storm config makes this a zero-additional-draws gate: an
// extra draw in the monitor would shift the arrival/spool/jitter substreams
// and move the mirror counters. model/monitor imports no engine and no RNG
// (TestMonitorZeroDrawsStructural), so the labeled path cannot draw at all.
//
// Known limitation (WP1 scope): the labeled probes are registered by hand
// on the monitor because WP1 ships only the additive API — the model has no
// label flag yet. WP2 turns this into a run-level gate behind
// Config.LabeledMetrics: see TestLabeledMetricsFlagNeutral, which covers the
// REAL registration path (flag on vs off), not a hand-registered probe.
func TestLabeledMetricsNeutral(t *testing.T) {
	type outcome struct {
		digest    string
		simEnd    int64
		started   uint64
		completed uint64
		retries   uint64
		failed    uint64
	}
	run := func(labeled bool) outcome {
		sys := BuildSystem(rngConfig())
		if err := sys.Build(); err != nil {
			t.Fatalf("Build: %v", err)
		}
		at := func() int64 { return sys.Sim().Now().Int64() }
		if labeled {
			sys.Monitor().AddGaugeKey("probe.wp1.cursor", []monitor.Label{{Key: "leg", Value: "model"}}, at)
		} else {
			sys.Monitor().AddGauge("probe.wp1.cursor", at)
		}
		res := sys.Run()
		o := outcome{digest: res.Digest, simEnd: res.SimEndMs}
		if c, ok := res.Metrics.Counter(MetricStarted); ok {
			o.started = c.Value
		}
		if c, ok := res.Metrics.Counter(MetricCompleted); ok {
			o.completed = c.Value
		}
		if c, ok := res.Metrics.Counter(MetricRetries); ok {
			o.retries = c.Value
		}
		if c, ok := res.Metrics.Counter(MetricFailed); ok {
			o.failed = c.Value
		}
		return o
	}

	for i := 0; i < 3; i++ {
		lab := run(true)
		base := run(false)
		if lab.simEnd != base.simEnd || lab.started != base.started ||
			lab.completed != base.completed || lab.retries != base.retries ||
			lab.failed != base.failed {
			t.Fatalf("run %d: labeled registration perturbed the simulation:\nlab  %+v\nbase %+v", i, lab, base)
		}
		// The labeled digest, with its label decoration stripped, must equal
		// the unlabeled-equivalent digest byte for byte.
		normalized := strings.Replace(lab.digest, "probe.wp1.cursor{leg=model}", "probe.wp1.cursor", 1)
		if normalized != base.digest {
			t.Fatalf("run %d: labeled digest is not the unlabeled digest + the label decoration:\nnormalized %q\nunlabeled  %q",
				i, normalized, base.digest)
		}
	}
}

// TestLabeledMetricsZeroDefault pins the neutral default: the flag is off in
// DefaultConfig, so no labeled surface is registered by default (ADR-115).
func TestLabeledMetricsZeroDefault(t *testing.T) {
	if got := DefaultConfig().LabeledMetrics; got {
		t.Fatal("DefaultConfig().LabeledMetrics = true, want false (off by default)")
	}
}

// unlabeledEquivalent projects a Metrics summary down to its UNLABELED
// entries — the "unlabeled-equivalent" registration. Every metric the WP2
// flag adds is labeled, so this strips exactly the labeled surfaces and
// leaves the pre-existing (unlabeled) digest content.
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

func hasLabeledCounter(ms monitor.Metrics, name string) bool {
	for _, c := range ms.Counters {
		if c.Name == name && c.LabelSet != "" {
			return true
		}
	}
	return false
}

func hasLabeledGauge(ms monitor.Metrics, name string) bool {
	for _, g := range ms.Gauges {
		if g.Name == name && g.LabelSet != "" {
			return true
		}
	}
	return false
}

// TestLabeledMetricsFlagNeutral is the WP2 labeled-path neutrality gate
// (mandatory constraint 2) on the REAL registration path. A run with
// Config.LabeledMetrics = true registers the attributed surfaces; its
// UNLABELED-EQUIVALENT projection must equal the flag-off digest byte for
// byte, and the simulation outcome (mirror counters, SimEndMs) must be
// identical. This is the zero-additional-RNG-draws proof: the rngConfig
// storm consumes arrival/spool/jitter draws, so ANY extra draw in the
// labeled path would shift the substreams and move the trace. The test is
// non-vacuous — the labeled digest must differ from the flag-off digest.
func TestLabeledMetricsFlagNeutral(t *testing.T) {
	type outcome struct {
		digest    string
		simEnd    int64
		started   uint64
		completed uint64
		retries   uint64
		failed    uint64
		metrics   monitor.Metrics
	}
	run := func(labeled bool) outcome {
		cfg := rngConfig()
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
		if c, ok := res.Metrics.Counter(MetricRetries); ok {
			o.retries = c.Value
		}
		if c, ok := res.Metrics.Counter(MetricFailed); ok {
			o.failed = c.Value
		}
		return o
	}

	for i := 0; i < 3; i++ {
		on := run(true)
		off := run(false)

		// Non-vacuous: the flag must have registered something.
		if on.digest == off.digest {
			t.Fatalf("run %d: labeled-on digest equals labeled-off — the flag registered nothing", i)
		}
		// The attributed surfaces must be present under the flag.
		for _, name := range []string{"history.shards.throughput", "history.shards.queue_depth"} {
			if !hasLabeledCounter(on.metrics, name) && !hasLabeledGauge(on.metrics, name) {
				t.Fatalf("run %d: labeled surface %q missing with LabeledMetrics on", i, name)
			}
		}
		if !hasLabeledGauge(on.metrics, "matching.utilization") {
			t.Fatalf("run %d: labeled surface matching.utilization missing with LabeledMetrics on", i)
		}

		// Simulation outcome must be unchanged (mirror counters + horizon).
		if on.simEnd != off.simEnd || on.started != off.started ||
			on.completed != off.completed || on.retries != off.retries || on.failed != off.failed {
			t.Fatalf("run %d: labeled registration perturbed the simulation:\non  %+v\noff %+v", i, on, off)
		}

		// The unlabeled-equivalent projection must be byte-identical to the
		// flag-off digest — zero additional RNG draws, zero mutation.
		if got := unlabeledEquivalent(on.metrics).String(); got != off.digest {
			t.Fatalf("run %d: labeled run's unlabeled-equivalent digest differs from flag-off:\nprojected %q\noff       %q",
				i, got, off.digest)
		}
	}
}

// TestLabeledMetricsMultiShard covers the multi-shard label fan-out: with
// Shards > 1 the per-shard labeled surfaces must carry distinct label sets
// (no duplicate-key panic) and the run must stay determinism-neutral.
func TestLabeledMetricsMultiShard(t *testing.T) {
	run := func(labeled bool) Result {
		cfg := DefaultConfig()
		cfg.BurstCount = 30
		cfg.SimEndMs = 5000
		cfg.Shards = 3
		cfg.LabeledMetrics = labeled
		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		return top.Run()
	}
	for i := 0; i < 3; i++ {
		on := run(true)
		off := run(false)
		shards := map[string]bool{}
		for _, c := range on.Metrics.Counters {
			// one transfer-intake counter per shard ⇒ 3 distinct shard labels.
			if c.Name == "history.shards.throughput" && strings.Contains(c.LabelSet, "kind=transfer") {
				shards[c.LabelSet] = true
			}
		}
		if len(shards) != 3 {
			t.Fatalf("run %d: got %d distinct shard label sets for history.shards.throughput, want 3", i, len(shards))
		}
		if on.SimEndMs != off.SimEndMs {
			t.Fatalf("run %d: 3-shard SimEndMs moved: on=%d off=%d", i, on.SimEndMs, off.SimEndMs)
		}
		if got := unlabeledEquivalent(on.Metrics).String(); got != off.Digest {
			t.Fatalf("run %d: 3-shard labeled projection differs from flag-off:\nprojected %q\noff       %q", i, got, off.Digest)
		}
	}
}

// TestLabeledMetricsUnlimitedBudget pins the reviewer-flagged edge: with an
// unlimited pool budget (PollerBudget 0) the utilization surface is OMITTED,
// so no negative sample can reach the monitor's UNSIGNED gauge accumulator
// (a -1 would wrap the mean). No gauge may report a negative last value.
func TestLabeledMetricsUnlimitedBudget(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BurstCount = 10
	cfg.SimEndMs = 2000
	cfg.PollerBudget = 0 // unlimited budget: utilization is undefined (DefaultConfig is 1)
	cfg.LabeledMetrics = true
	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res := top.Run()
	for _, g := range res.Metrics.Gauges {
		if g.Name == "matching.utilization" {
			t.Fatalf("matching.utilization registered with unlimited budget: %+v", g)
		}
		if g.Last < 0 || g.Mean < 0 {
			t.Fatalf("gauge %s%s last=%d mean=%d, want >= 0 (unsigned accumulator)", g.Name, g.LabelSet, g.Last, g.Mean)
		}
	}
}

// TestHistoryOccupancyFraction exercises the summary-time history-occupancy
// helper (WP3): the queue-non-empty fraction folded from the existing gauge
// series is always a valid fraction, and a nil/unrun topology yields the
// zero value (no panic).
func TestHistoryOccupancyFraction(t *testing.T) {
	var nilTop *Topology
	if got := nilTop.HistoryOccupancy(); got != (HistoryOccupancyEvidence{}) {
		t.Fatalf("nil topology occupancy = %+v, want zero", got)
	}

	cfg := DefaultConfig()
	cfg.BurstCount = 50
	cfg.SimEndMs = 5000
	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	top.Run()
	occ := top.HistoryOccupancy()
	for _, c := range []struct {
		name string
		f    float64
	}{{"transfer", occ.Transfer}, {"timer", occ.Timer}, {"resched", occ.Resched}} {
		if c.f < 0 || c.f > 1 {
			t.Fatalf("%s occupancy = %v, want a fraction in [0,1]", c.name, c.f)
		}
	}
}
