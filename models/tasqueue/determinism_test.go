package tasqueue

import (
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/lawRathod/queuesim/model/capture"
)

// gateConfig returns the determinism-gate configuration: it exercises every
// stochastic substream (failP draw, drain-time drop) and every DAG
// mechanism within one run — group fan-out (GroupSize, with the group-fail
// suppression path under FailP), OnError spawns from terminal group
// members, ETA timer routing, and the MaxWaitMs deadline (armed; its
// firing is pinned by TestGroupExpiredMemberNeverJoins — the broker fully
// drains each pump tick, so queue residency is ~0 and expiry only fires
// under a manually advanced clock). Chains are exercised by contract tests
// (TestChain*), so the gate keeps the exclusive-with-groups GroupSize knob.
func gateConfig() Config {
	cfg := DefaultConfig()
	cfg.Seed = 7
	cfg.BurstCount = 30
	cfg.SimEndMs = 5000
	cfg.FailP = 0.3
	cfg.DropRate = 0.05
	cfg.MaxRetries = 1
	cfg.GroupSize = 3
	cfg.OnErrorJobs = 2
	cfg.ETAOffsetMs = 10
	cfg.MaxWaitMs = 50
	return cfg
}

// TestCaptureDeterminismN5 is the capture determinism gate: capture ON under
// the full stochastic+DAG gate config yields a bit-identical event trace
// (the JSONL a caller would flush) across N=5 runs — the recorder assigns
// Seq in single-goroutine sim order, so the sim order IS the sequence order.
func TestCaptureDeterminismN5(t *testing.T) {
	run := func() (Result, string) {
		cfg := gateConfig()
		cfg.Capture = true
		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		res := top.Run()
		return res, fmt.Sprintf("%+v", top.Capture().Events())
	}
	var firstDigest, firstTrace string
	for i := 0; i < 5; i++ {
		res, trace := run()
		if i == 0 {
			firstDigest = res.Digest
			firstTrace = trace
		}
		if res.Digest != firstDigest {
			t.Fatalf("run %d: digest mismatch (capture on)\n%q\n%q", i, res.Digest, firstDigest)
		}
		if trace != firstTrace {
			t.Fatalf("run %d: capture trace mismatch\n%s\n%s", i, trace, firstTrace)
		}
	}
	t.Logf("capture-on digest (run 0): %q", firstDigest)
}

// TestCaptureDigestMatchesOff is the sim-digest-unchanged gate: with capture
// ON the digest is byte-identical to capture OFF under the SAME
// stochastic+DAG gate config — the recorder is observational and never
// perturbs the sim or the monitor.
func TestCaptureDigestMatchesOff(t *testing.T) {
	run := func(captureOn bool) (string, int) {
		cfg := gateConfig()
		cfg.Capture = captureOn
		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		res := top.Run()
		if captureOn {
			return res.Digest, len(top.Capture().Events())
		}
		return res.Digest, 0
	}
	offDigest, _ := run(false)
	onDigest, n := run(true)
	if offDigest == "" {
		t.Fatal("expected non-empty digest")
	}
	if offDigest != onDigest {
		t.Fatalf("capture perturbed digest under gate config:\noff=%q\non =%q", offDigest, onDigest)
	}
	if n == 0 {
		t.Fatal("expected captured events")
	}
}

// TestStormRetriesMatchRetryRecords is the capture-honesty gate for the
// tasqueue failure path: with a plain (no chain/group) config, the number
// of exec→load retry records equals Stats.Retried and the number of
// exec→terminal complete records equals Stats.Completed — the mechanism
// map's edges are the observed counts, not inventions.
func TestStormRetriesMatchRetryRecords(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 7
	cfg.BurstCount = 40
	cfg.SimEndMs = 10000
	cfg.FailP = 0.4
	cfg.MaxRetries = 2
	cfg.ChainLen = 0
	cfg.GroupSize = 0
	cfg.OnErrorJobs = 0
	cfg.Capture = true
	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res := top.Run()
	g := capture.Aggregate(top.Capture().Events())
	countKind := func(kind string) int {
		n := 0
		for _, e := range g.Edges {
			if e.Kind == kind {
				n += e.Count
			}
		}
		return n
	}
	if int(res.Stats.Retried) != countKind("retry") {
		t.Errorf("retry records = %d, want Stats.Retried = %d", countKind("retry"), res.Stats.Retried)
	}
	if int(res.Stats.Completed) != countKind("complete") {
		t.Errorf("complete records = %d, want Stats.Completed = %d", countKind("complete"), res.Stats.Completed)
	}
}

// TestDeterminismN5 is the determinism gate: same model + same seed →
// bit-identical Result across N=5 runs. The digest is the System's
// free-monitor serialization; Stats must be field-for-field identical; the
// ending sim time (queue-exhaustion clock) must match. The gate config
// exercises both RNG consumers (failP draw and drain-time drop), the ETA
// timer queue, the OnError DAG, the group join/fail machinery and the
// MaxWaitMs deadline — every stochastic and DAG mechanism — so the gate
// proves the whole model is seed-deterministic.
func TestDeterminismN5(t *testing.T) {
	run := func() Result {
		top, err := NewTopology(gateConfig())
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		return top.Run()
	}
	var firstDigest, firstStats string
	var firstEnd int64
	for i := 0; i < 5; i++ {
		res := run()
		if i == 0 {
			firstDigest = res.Digest
			firstStats = fmt.Sprintf("%+v", res.Stats)
			firstEnd = res.SimEndMs
		}
		if res.Digest != firstDigest {
			t.Fatalf("run %d: digest mismatch (RNG draws)\n%q\n%q", i, res.Digest, firstDigest)
		}
		if s := fmt.Sprintf("%+v", res.Stats); s != firstStats {
			t.Fatalf("run %d: stats mismatch (RNG draws)\n%s\n%s", i, s, firstStats)
		}
		if res.SimEndMs != firstEnd {
			t.Fatalf("run %d: SimEndMs = %d, want %d", i, res.SimEndMs, firstEnd)
		}
	}
	t.Logf("determinism digest (run 0): %q", firstDigest)
	t.Logf("sha256(digest)=%x", sha256.Sum256([]byte(firstDigest)))
	t.Logf("stats (run 0): %s", firstStats)
	t.Logf("SimEndMs (queue exhaustion) = %d", firstEnd)
}

// TestDeterminismN5ZeroDraw is the zero-draw determinism gate: a config
// with every fault OFF (FailP 0, DropRate 0, no ETA, no MaxWaitMs) makes
// zero RNG draws and is bit-identical across N=5 runs. The digest is
// non-empty (the free-monitor surface is registered and sampled even with
// zero samples in the histogram).
func TestDeterminismN5ZeroDraw(t *testing.T) {
	run := func() Result {
		cfg := DefaultConfig()
		cfg.Seed = 11
		cfg.BurstCount = 20
		cfg.SimEndMs = 2000
		// Faults off: no fail draw, no drop substream, no ETA timer, no
		// max-wait deadline — the RNG must never be consulted.
		cfg.FailP = 0
		cfg.DropRate = 0
		cfg.MaxRetries = 0
		cfg.MaxWaitMs = 0
		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		return top.Run()
	}
	var firstDigest, firstStats string
	var firstEnd int64
	for i := 0; i < 5; i++ {
		res := run()
		if i == 0 {
			firstDigest = res.Digest
			firstStats = fmt.Sprintf("%+v", res.Stats)
			firstEnd = res.SimEndMs
		}
		if res.Digest != firstDigest {
			t.Fatalf("run %d: digest mismatch (zero draws)\n%q\n%q", i, res.Digest, firstDigest)
		}
		if s := fmt.Sprintf("%+v", res.Stats); s != firstStats {
			t.Fatalf("run %d: stats mismatch (zero draws)\n%s\n%s", i, s, firstStats)
		}
		if res.SimEndMs != firstEnd {
			t.Fatalf("run %d: SimEndMs = %d, want %d", i, res.SimEndMs, firstEnd)
		}
	}
	t.Logf("zero-draw digest (run 0): %q", firstDigest)
	t.Logf("sha256(digest)=%x", sha256.Sum256([]byte(firstDigest)))
	t.Logf("stats (run 0): %s", firstStats)
}

// TestMonitorOffEqualOn is the monitor-neutrality gate (determinism
// contract): the mechanism result with the System's free monitor ON is
// bit-identical to monitoring OFF — under the RNG-drawing gate config, so
// the neutrality property is proven on the stochastic path too. Only the
// metrics/digest differ (the off-run carries none) — the monitor never
// perturbs the simulation.
func TestMonitorOffEqualOn(t *testing.T) {
	run := func(off bool) Result {
		cfg := gateConfig()
		cfg.DisableMonitor = off
		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		return top.Run()
	}
	on := run(false)
	off := run(true)

	if fmt.Sprintf("%+v", on.Stats) != fmt.Sprintf("%+v", off.Stats) {
		t.Fatalf("stats differ between monitor on/off:\non  %+v\noff %+v", on.Stats, off.Stats)
	}
	if on.SimEndMs != off.SimEndMs {
		t.Fatalf("SimEndMs differs: on=%d off=%d", on.SimEndMs, off.SimEndMs)
	}
	if on.Digest == "" || off.Digest != "" {
		t.Fatalf("unexpected digests: on=%q off=%q", on.Digest, off.Digest)
	}
	// The on-run's digest must be stable across repeated identical runs —
	// determinism evidence beyond N=5.
	again := run(false)
	if again.Digest != on.Digest {
		t.Fatalf("digest not stable: %q vs %q", on.Digest, again.Digest)
	}
}
