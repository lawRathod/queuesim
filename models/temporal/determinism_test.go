package temporal

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	"github.com/lawRathod/queuesim/model"
	"github.com/lawRathod/queuesim/model/capture"
)

// rngConfig returns a config that exercises every stochastic substream in
// the model: exponential arrivals (arrivalRNG), spool-time drops
// (dropRng on the matching queues) and poll-jitter early-expiry (jitterRng).
func rngConfig() Config {
	cfg := DefaultConfig()
	cfg.Seed = 7
	cfg.BurstCount = 20
	cfg.SimEndMs = 20000
	cfg.FaultStartMs = 0
	cfg.FaultEndMs = 0
	cfg.ArrivalRate = 40
	cfg.DispatchRate = 10
	cfg.DispatchBurst = 2
	cfg.Vector = &model.DecisionVector{Name: "add_drop+jitter", Compose: []model.DecisionVector{
		{Name: "add_drop", Kind: model.KindAddDrop, Target: "matching", StartMs: 1000, DurationMs: 10000, Rate: 0.3},
		{Name: "poll_jitter", Kind: model.KindPollJitter, Target: "matching", StartMs: 1000, DurationMs: 10000, Rate: 0.5},
	}}
	return cfg
}

// TestDeterminismN5 is the determinism gate: same model + same seed →
// bit-identical result (monitor ON), N=5. The digest is the System's
// free-monitor serialization; Stats must be field-for-field identical.
func TestDeterminismN5(t *testing.T) {
	run := func() (Result, string) {
		cfg := DefaultConfig()
		cfg.Seed = 7
		cfg.BurstCount = 30
		cfg.SimEndMs = 20000
		cfg.FaultStartMs = 1
		cfg.FaultEndMs = 5001
		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		res := top.Run()
		return res, fmt.Sprintf("%+v", res.Stats)
	}
	var firstDigest string
	var firstStats string
	var firstEnd int64
	for i := 0; i < 5; i++ {
		res, stats := run()
		if i == 0 {
			firstDigest = res.Digest
			firstStats = stats
			firstEnd = res.SimEndMs
		}
		if res.Digest != firstDigest {
			t.Fatalf("run %d: digest mismatch\n%q\n%q", i, res.Digest, firstDigest)
		}
		if stats != firstStats {
			t.Fatalf("run %d: stats mismatch\n%s\n%s", i, stats, firstStats)
		}
		// The sim runs to queue exhaustion (events scheduled past the pump
		// horizon still fire — e.g. long-poll wakes), so SimEndMs is the
		// deterministic exhaustion time, not the configured horizon.
		if res.SimEndMs != firstEnd {
			t.Fatalf("run %d: SimEndMs = %d, want %d", i, res.SimEndMs, firstEnd)
		}
		if res.SimEndMs < 20000 {
			t.Fatalf("run %d: SimEndMs = %d below horizon 20000", i, res.SimEndMs)
		}
	}
	t.Logf("determinism digest (run 0): %q", firstDigest)
	t.Logf("sha256(digest)=%x", sha256.Sum256([]byte(firstDigest)))
	t.Logf("SimEndMs (queue exhaustion) = %d", firstEnd)
}

// TestDeterminismN5WithDraws is the RNG-drawing determinism gate: the same
// storm config WITH stochastic substreams (arrival + spool-drop +
// poll-jitter) produces a bit-identical digest and stats across N=5 runs.
func TestDeterminismN5WithDraws(t *testing.T) {
	run := func() Result {
		top, err := NewTopology(rngConfig())
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		return top.Run()
	}
	var firstDigest string
	var firstStats string
	for i := 0; i < 5; i++ {
		res := run()
		if i == 0 {
			firstDigest = res.Digest
			firstStats = fmt.Sprintf("%+v", res.Stats)
		}
		if res.Digest != firstDigest {
			t.Fatalf("run %d: digest mismatch (RNG draws)\n%q\n%q", i, res.Digest, firstDigest)
		}
		if s := fmt.Sprintf("%+v", res.Stats); s != firstStats {
			t.Fatalf("run %d: stats mismatch (RNG draws)\n%s\n%s", i, s, firstStats)
		}
	}
	t.Logf("draws digest (run 0): %q", firstDigest)
	t.Logf("sha256(draws digest)=%x", sha256.Sum256([]byte(firstDigest)))
}

// TestStormRetriesMatchActRetryCount is the capture-honesty gate for the
// storm path: the number of exec→history act_retry records equals the
// model's Retries counter — the mechanism map's retry edge is not an
// invention, it is the observed retry count.
func TestStormRetriesMatchActRetryCount(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 7
	cfg.BurstCount = 40
	cfg.SimEndMs = 20000
	cfg.FaultStartMs = 0
	cfg.FaultEndMs = 10000
	cfg.RetryInitialMs = 100
	cfg.RetryCoeff = 1.0
	cfg.RetryMaxAttempts = 3
	cfg.Capture = true
	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res := top.Run()
	g := capture.Aggregate(top.Capture().Events())
	if int(res.Stats.Retries) != countKind(g, "act_retry") {
		t.Errorf("act_retry records = %d, want Stats.Retries = %d", countKind(g, "act_retry"), res.Stats.Retries)
	}
	// And the completed count matches the workflow-complete records.
	if res.Stats.Completed != uint64(countKind(g, "wf_complete")) {
		t.Errorf("wf_complete records = %d, want Stats.Completed = %d", countKind(g, "wf_complete"), res.Stats.Completed)
	}
}

// TestCaptureDeterminismN5 is the capture determinism gate: capture ON
// under an RNG-drawing storm yields a bit-identical event trace (the JSONL
// a caller would flush) across N=5 runs — the recorder assigns Seq in
// single-goroutine sim order, so the sim order IS the sequence order.
func TestCaptureDeterminismN5(t *testing.T) {
	run := func() (Result, string) {
		cfg := rngConfig()
		cfg.Capture = true
		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		res := top.Run()
		return res, fmt.Sprintf("%+v", top.Capture().Events())
	}
	var firstDigest string
	var firstTrace string
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
// RNG-drawing storm — the recorder is observational and never perturbs the
// sim or the monitor.
func TestCaptureDigestMatchesOff(t *testing.T) {
	run := func(captureOn bool) (string, int) {
		cfg := rngConfig()
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
		t.Fatalf("capture perturbed digest under RNG storm:\noff=%q\non =%q", offDigest, onDigest)
	}
	if n == 0 {
		t.Fatal("expected captured events")
	}
}

// TestMonitorOffEqualOn is the monitor-neutrality gate (determinism
// contract): the mechanism result with the System's free monitor ON is
// bit-identical to monitoring OFF — under an RNG-drawing storm (arrival +
// spool-drop + poll-jitter), so the neutrality property is proven on the
// stochastic path too. Only the metrics/digest differ (the off-run carries
// none) — the monitor never perturbs the simulation.
func TestMonitorOffEqualOn(t *testing.T) {
	run := func(off bool) Result {
		cfg := rngConfig()
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
	if on.Status != off.Status {
		t.Fatalf("status differs between monitor on/off: %+v vs %+v", on.Status, off.Status)
	}
	if on.DrainMs != off.DrainMs {
		t.Fatalf("DrainMs differs: on=%d off=%d", on.DrainMs, off.DrainMs)
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

// TestBuildSystemProgrammaticPath: the BuildSystem path exposes the free
// monitor (automatic per-service gauges registered by the library) and the
// run digest includes the real mechanism-depth gauges attached on the first
// tick (matching.wf.depth etc.), so the programmatic and shim surfaces
// share the same monitoring contract.
func TestBuildSystemProgrammaticPath(t *testing.T) {
	cfg := Config{Seed: 3, BurstCount: 10, PollersWF: 4, PollersAct: 4, SimEndMs: 5000, MatchingCapacity: 100}
	sys := BuildSystem(cfg)
	if sys == nil {
		t.Fatal("BuildSystem returned nil")
	}
	if err := sys.Build(); err != nil {
		t.Fatalf("Build: %v", err)
	}
	if sys.Monitor() == nil {
		t.Fatal("expected a monitor (monitoring on by default)")
	}
	res := sys.Run()
	if res.Digest == "" {
		t.Fatal("expected a metrics digest")
	}
	for _, want := range []string{"matching.wf.depth", "matching.act.depth", "matching.wf.pollers", "matching.act.pollers", "history.shards.inflight"} {
		if !strings.Contains(res.Digest, want) {
			t.Fatalf("digest missing %q:\n%s", want, res.Digest)
		}
	}
	// Determinism: same config twice → same digest.
	sys2 := BuildSystem(cfg)
	if err := sys2.Build(); err != nil {
		t.Fatal(err)
	}
	if got := sys2.Run().Digest; got != res.Digest {
		t.Fatalf("programmatic digest not stable: %q vs %q", got, res.Digest)
	}
}
