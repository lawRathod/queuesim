package temporal

import (
	"math"
	"strings"
	"testing"
)

// TestTopologyStatsRatesKnown pins the lifecycle-rate arithmetic against
// hand-computed values on a small synthetic counter set (WP6/B5).
func TestTopologyStatsRatesKnown(t *testing.T) {
	s := TopologyStats{
		Started:          100,
		ActivityAdds:     250,
		ActivityAttempts: 230,
		AttemptsExecuted: 220,
		Retries:          120,
		Completed:        90,
		Failed:           10,
		Timeouts:         7,
	}
	r := s.Rates(20000) // 20 simulated seconds

	checks := []struct {
		name string
		got  float64
		want float64
	}{
		{"Goodput", r.Goodput, 90.0 / 20.0},          // completions/s
		{"Throughput", r.Throughput, 220.0 / 20.0},   // executions/s
		{"RetryRate", r.RetryRate, 120.0 / 90.0},     // retries/completion
		{"SuccessRate", r.SuccessRate, 90.0 / 100.0}, // completed/started
		{"ErrorRate", r.ErrorRate, 10.0 / 100.0},     // failed/started
		{"TimeoutRate", r.TimeoutRate, 7.0 / 100.0},  // timeouts/started
	}
	for _, c := range checks {
		if math.Abs(c.got-c.want) > 1e-12 {
			t.Fatalf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

// TestTopologyStatsRatesConsistentWithHelpers pins the convention shared
// with the existing helpers: RetryRate reuses RetryAmp's retries-per-
// completion ratio.
func TestTopologyStatsRatesConsistentWithHelpers(t *testing.T) {
	s := TopologyStats{Completed: 40, Retries: 7, Started: 100}
	if got, want := s.Rates(1000).RetryRate, s.RetryAmp(); got != want {
		t.Fatalf("RetryRate = %v, want RetryAmp() = %v", got, want)
	}
}

// TestTopologyStatsRatesZeroDenominator pins the zero-denominator contract:
// an empty / zero-elapsed run yields all-zero rates and never NaN or Inf.
func TestTopologyStatsRatesZeroDenominator(t *testing.T) {
	for _, elapsed := range []int64{0, -5} {
		r := TopologyStats{}.Rates(elapsed)
		for _, f := range []float64{r.Goodput, r.Throughput, r.RetryRate, r.SuccessRate, r.ErrorRate, r.TimeoutRate} {
			if math.IsNaN(f) || math.IsInf(f, 0) {
				t.Fatalf("elapsed=%d: rate = %v, want finite 0", elapsed, f)
			}
			if f != 0 {
				t.Fatalf("elapsed=%d: rate = %v, want 0", elapsed, f)
			}
		}
	}
	// Non-zero elapsed but zero starts/completions: ratio rates stay 0.
	r := TopologyStats{AttemptsExecuted: 5}.Rates(1000)
	if r.Goodput != 0 || r.RetryRate != 0 || r.SuccessRate != 0 || r.ErrorRate != 0 || r.TimeoutRate != 0 {
		t.Fatalf("zero-start run produced non-zero ratio: %+v", r)
	}
	if r.Throughput != 5 {
		t.Fatalf("Throughput = %v, want 5 (5 executions / 1 s)", r.Throughput)
	}
}

// TestDispatchTimeoutCounterPopulated pins the countable dispatch-time
// (schedule-to-start) timeout path: the S2S-expiry config records a
// non-zero Timeouts counter and a non-zero timeout rate. The counter is
// sourced from the matcher's per-stage expiry array (matcher.ExpireAtPoll),
// the only stage the model ever increments.
func TestDispatchTimeoutCounterPopulated(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 1
	cfg.BurstCount = 50
	cfg.SimEndMs = 60000
	cfg.MatchingCapacity = 1000
	cfg.PollersWF = 1
	cfg.PollersAct = 1
	cfg.ActivityMs = 50
	cfg.ActivityS2STimeoutMs = 30
	cfg.FaultStartMs = 0
	cfg.FaultEndMs = 0

	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res := top.Run()
	if res.Stats.Timeouts == 0 {
		t.Fatalf("Timeouts = 0; dispatch-time S2S expiry not surfaced (Expired=%d)", res.Stats.Expired)
	}
	if res.Stats.Timeouts != res.Stats.Expired {
		// Today only matcher.ExpireAtPoll increments, so the per-stage Timeouts
		// tally equals the all-stage Expired sum. If a future ExpireAtSpool /
		// ExpireAtRead stage is added this pin fires — revisit the conflation.
		t.Fatalf("Timeouts (%d) != Expired (%d): a non-Poll expiry stage now increments; re-check the stage split", res.Stats.Timeouts, res.Stats.Expired)
	}
	if r := res.Stats.Rates(res.SimEndMs); r.TimeoutRate <= 0 {
		t.Fatalf("TimeoutRate = %v, want > 0", r.TimeoutRate)
	}
}

// TestLifecycleTimeoutRateGaugeLive pins the reviewer-flagged live-source
// requirement: under Config.LabeledMetrics the timeout_rate gauge must read
// the LIVE per-stage expiry tally, not the post-run TopologyStats.Timeouts
// (finalizeStats runs after the last Sample, so that would be constant zero).
func TestLifecycleTimeoutRateGaugeLive(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 1
	cfg.BurstCount = 50
	cfg.SimEndMs = 60000
	cfg.MatchingCapacity = 1000
	cfg.PollersWF = 1
	cfg.PollersAct = 1
	cfg.ActivityMs = 50
	cfg.ActivityS2STimeoutMs = 30
	cfg.FaultStartMs = 0
	cfg.FaultEndMs = 0
	cfg.LabeledMetrics = true

	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res := top.Run()
	if res.Stats.Timeouts == 0 {
		t.Fatal("precondition: run produced no timeouts")
	}
	for _, g := range res.Metrics.Gauges {
		if g.Name == "temporal.lifecycle.timeout_rate" {
			if g.Last <= 0 || g.Max <= 0 {
				t.Fatalf("timeout_rate gauge last=%d max=%d, want > 0 (live per-stage count)", g.Last, g.Max)
			}
			return
		}
	}
	t.Fatal("temporal.lifecycle.timeout_rate gauge not registered with LabeledMetrics on")
}

// TestLifecycleRatesGatedByLabeledMetrics pins the off-by-default contract
// for the rate gauges: flag off registers no temporal.lifecycle.* gauge and
// two flag-off runs are byte-identical; flag on registers exactly the six
// rate gauges and its unlabeled-equivalent projection still equals the
// flag-off digest (zero additional RNG draws, zero mutation).
func TestLifecycleRatesGatedByLabeledMetrics(t *testing.T) {
	run := func(labeled bool) Result {
		cfg := DefaultConfig()
		cfg.BurstCount = 20
		cfg.SimEndMs = 4000
		cfg.LabeledMetrics = labeled
		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		return top.Run()
	}

	off1 := run(false)
	off2 := run(false)
	if off1.Digest != off2.Digest {
		t.Fatalf("two flag-off runs differ: %q vs %q", off1.Digest, off2.Digest)
	}
	for _, g := range off1.Metrics.Gauges {
		if strings.HasPrefix(g.Name, "temporal.lifecycle.") {
			t.Fatalf("rate gauge %s registered with LabeledMetrics off", g.Name)
		}
	}

	on := run(true)
	var n int
	for _, g := range on.Metrics.Gauges {
		if strings.HasPrefix(g.Name, "temporal.lifecycle.") {
			n++
		}
	}
	if n != 6 {
		t.Fatalf("flag-on registered %d temporal.lifecycle.* gauges, want 6", n)
	}
	if on.SimEndMs != off1.SimEndMs {
		t.Fatalf("SimEndMs moved with the flag: on=%d off=%d", on.SimEndMs, off1.SimEndMs)
	}
	if got := unlabeledEquivalent(on.Metrics).String(); got != off1.Digest {
		t.Fatalf("labeled run's unlabeled-equivalent digest differs from flag-off:\nprojected %q\noff       %q", got, off1.Digest)
	}
}
