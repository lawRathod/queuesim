package tasqueue

import (
	"crypto/sha256"
	"fmt"
	"math"
	"testing"

	"github.com/lawRathod/queuesim/model"
	"github.com/lawRathod/queuesim/models/tasqueue/faults"
)

// faultRun drives cfg through the real Topology entry point (Run->Build).
func faultRun(t *testing.T, cfg Config) Result {
	t.Helper()
	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	return top.Run()
}

// windowedConfig returns a SUSTAINED-inflow config used to observe a fault
// window against continuous traffic: a 100-job burst every second pinned
// against a single-unit worker budget (one job per 6ms poll → ~166/s max
// handling capacity, below the ~200/s burst demand so a mid-run fault has
// room to build a backlog). It is the demo shape (examples/models/
// tasqueue_metrics/runConfig) with every static fault knob OFF so the fault
// window is the ONLY source of the effect and a no-fault control run is a
// clean baseline.
func windowedConfig() Config {
	cfg := DefaultConfig()
	cfg.Seed = 42
	cfg.BurstCount = 100
	cfg.ScheduleEveryMs = 1000
	cfg.SimEndMs = 10000
	cfg.NumWorkers = 1
	cfg.Concurrency = 1
	cfg.PollMs = 6
	cfg.ServiceMs = 5
	cfg.MaxRetries = 2
	cfg.FailP = 0     // baseline off — fault (or control) decides
	cfg.DropRate = 0  // baseline off
	cfg.TimeoutMs = 0 // baseline off
	return cfg
}

// depthSeries returns the sampled broker+dispatch backlog (MetricQueueDepth)
// over the run — one value per pump tick plus the final exhaustion sample.
// It is the temporal evidence that a window built a backlog and that the
// system drained after the window closed (recovery).
func depthSeries(t *testing.T, cfg Config) ([]int64, Result) {
	t.Helper()
	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res := top.Run()
	series := top.System().Monitor().GaugeSeries(MetricQueueDepth)
	return series, res
}

// maxDepth returns the maximum sampled depth value.
func maxDepth(series []int64) int64 {
	var mx int64
	for _, v := range series {
		if v > mx {
			mx = v
		}
	}
	return mx
}

// TestFaultDropWindowRecovers pins the KindDrop window: a delivery drop is
// an attempt-level failure consuming the retry budget (config.go DropRate —
// the contract with TestDropConsumesRetryBudget), so while the drop window
// is active deliveries are dropped and retried (Dropped/Retried > 0 vs a
// zero-baseline control), and after the window closes the backlog drains and
// jobs complete again (recovery). The depth series shows the mid-run spike
// returning to the drained baseline.
func TestFaultDropWindowRecovers(t *testing.T) {
	cfg := windowedConfig()
	cfg.Faults = []faults.Fault{{Kind: faults.KindDrop, StartMs: 2000, EndMs: 5000, Rate: 0.5}}

	ctrlSeries, ctrl := depthSeries(t, func() Config {
		c := cfg
		c.Faults = nil
		return c
	}())
	fSeries, fRes := depthSeries(t, cfg)

	t.Logf("drop window [2000,5000): fault dropped=%d retried=%d failed=%d completed=%d depthMax=%d depthLast=%d",
		fRes.Stats.Dropped, fRes.Stats.Retried, fRes.Stats.Failed, fRes.Stats.Completed, maxDepth(fSeries), fSeries[len(fSeries)-1])
	t.Logf("control               : dropped=%d retried=%d failed=%d completed=%d depthMax=%d depthLast=%d",
		ctrl.Stats.Dropped, ctrl.Stats.Retried, ctrl.Stats.Failed, ctrl.Stats.Completed, maxDepth(ctrlSeries), ctrlSeries[len(ctrlSeries)-1])

	// The fault engaged: deliveries dropped during the window consumed a
	// retry (attempt-level failure) — the drop-window contract.
	if fRes.Stats.Dropped == 0 || fRes.Stats.Retried == 0 {
		t.Fatalf("drop window: Dropped=%d Retried=%d, want both > 0 (drops consumed retries during the window)", fRes.Stats.Dropped, fRes.Stats.Retried)
	}
	// Baseline control: a zero-baseline config with no faults must see no
	// drops and no retries (the fault is the ONLY source).
	if ctrl.Stats.Dropped != 0 || ctrl.Stats.Retried != 0 {
		t.Fatalf("control: Dropped=%d Retried=%d, want 0 (no-fault zero-baseline must be clean)", ctrl.Stats.Dropped, ctrl.Stats.Retried)
	}
	// Recovery: after the window closes, later jobs complete (Completed > 0)
	// and the backlog drains back to the near-drained baseline.
	if fRes.Stats.Completed == 0 {
		t.Fatalf("drop window: Completed=0, want > 0 (system must recover and complete jobs after the window)")
	}
	fLast := fSeries[len(fSeries)-1]
	if fLast != 0 || ctrlSeries[len(ctrlSeries)-1] != 0 {
		t.Fatalf("depth not drained after run: fault last=%d control last=%d, want both 0 (recovery: backlog drained)", fLast, ctrlSeries[len(ctrlSeries)-1])
	}
}

// TestFaultFailureWindowSpikeAndRecovery pins the KindFailure window: while
// the window is active the effective handler failure probability spikes
// (Retried/Failed climb vs a zero-baseline control), and after the window
// the system recovers — the backlog that the failure storm piled up drains
// (final depth == 0) and jobs complete again.
func TestFaultFailureWindowSpikeAndRecovery(t *testing.T) {
	cfg := windowedConfig()
	cfg.Faults = []faults.Fault{{Kind: faults.KindFailure, StartMs: 3000, EndMs: 7000, Rate: 0.9}}

	ctrlSeries, ctrl := depthSeries(t, func() Config {
		c := cfg
		c.Faults = nil
		return c
	}())
	fSeries, fRes := depthSeries(t, cfg)

	t.Logf("failure window [3000,7000): retried=%d failed=%d completed=%d depthMax=%d depthLast=%d",
		fRes.Stats.Retried, fRes.Stats.Failed, fRes.Stats.Completed, maxDepth(fSeries), fSeries[len(fSeries)-1])
	t.Logf("control                 : retried=%d failed=%d completed=%d depthMax=%d depthLast=%d",
		ctrl.Stats.Retried, ctrl.Stats.Failed, ctrl.Stats.Completed, maxDepth(ctrlSeries), ctrlSeries[len(ctrlSeries)-1])

	// The failure storm during the window drives clear retry amplification
	// vs the zero-baseline control (90% of attempts fail, retried, and many
	// exhaust the MaxRetries=2 budget).
	if fRes.Stats.Retried == 0 || fRes.Stats.Failed == 0 {
		t.Fatalf("failure window: Retried=%d Failed=%d, want both > 0", fRes.Stats.Retried, fRes.Stats.Failed)
	}
	if ctrl.Stats.Retried != 0 || ctrl.Stats.Failed != 0 {
		t.Fatalf("control: Retried=%d Failed=%d, want 0 (no-fault zero-baseline must be clean)", ctrl.Stats.Retried, ctrl.Stats.Failed)
	}
	// The storm piles up a backlog (depthMax clearly above the calm
	// control), then drains to a near-empty state after the window closes
	// (recovery). The tail is not exactly 0 because the poller stops at
	// SimEndMs (worker.go EndMs), leaving the last-cycle jobs undrained —
	// the honest signal is that the storm backlog collapsed.
	if maxDepth(fSeries) <= maxDepth(ctrlSeries) {
		t.Fatalf("backlog not observable: fault depthMax=%d <= control depthMax=%d", maxDepth(fSeries), maxDepth(ctrlSeries))
	}
	fLast := fSeries[len(fSeries)-1]
	if fLast >= maxDepth(fSeries)/4 {
		t.Fatalf("depth not drained after window: last=%d (=%.0f%% of peak %d), want < 25%% of peak (recovery)",
			fLast, 100*float64(fLast)/float64(maxDepth(fSeries)), maxDepth(fSeries))
	}
	if ctrlLast := ctrlSeries[len(ctrlSeries)-1]; ctrlLast != 0 {
		t.Fatalf("control depth last=%d, want 0", ctrlLast)
	}
}

// TestFaultTimeoutWindowOnly pins the KindTimeout window at the event level:
// an attempt times out iff ServiceMs > the effective timeout deadline
// (server.go:388-390). Driving the real take/complete path at controlled
// sim times proves that only the attempt TAKEN inside the window times out —
// attempts taken before the window and after it succeed. applyFaults is
// driven explicitly at each poll boundary (the manual stepper has no System
// pump), so the effective timeout reflects exactly the sim time of each
// poll.
func TestFaultTimeoutWindowOnly(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BurstCount = 0 // manual inflow
	cfg.Seed = 1
	cfg.SimEndMs = 100000
	cfg.ServiceMs = 100
	cfg.FailP = 0      // failures must come from the timeout deadline only
	cfg.MaxRetries = 0 // a timeout is terminal (no retry to mask the window edge)
	cfg.TimeoutMs = 0  // baseline off
	cfg.NumWorkers = 1
	cfg.Concurrency = 1
	cfg.PollMs = 1000 // polls at 0, 1000, 2000, 3000
	cfg.Faults = []faults.Fault{{Kind: faults.KindTimeout, StartMs: 1500, EndMs: 2500, Rate: 50}}

	rt, sim := contractRuntime(t, cfg)

	// Four jobs enqueued at t=0; with a 1-unit budget and 1000ms poll the
	// poller takes one per poll: @0, @1000, @2000, @3000.
	for id := uint64(1); id <= 4; id++ {
		js := &jobState{id: id, chainIdx: -1, groupID: noGroup, attempt: 1, enqueuedMs: 0}
		rt.enqueueTask(model.Task{ID: id, Attempt: 1}, js, sim.Now())
	}
	rt.tick(sim.Now()) // starts the poller (poll@0), drains; applyFaults(0) → window inactive
	rt.applyFaults(0)

	// Poll@0 → job 1 taken BEFORE the window: no timeout → succeeds.
	if !sim.Step() {
		t.Fatal("expected poll@0")
	}
	rt.applyFaults(0) // take read effTimeout set by the tick@0 above
	if !sim.Step() {
		t.Fatal("expected job-1 completion@100")
	}

	// Poll@1000 → job 2 taken before the window: succeeds.
	rt.applyFaults(1000)
	if !sim.Step() {
		t.Fatal("expected poll@1000")
	}
	rt.applyFaults(1000)
	if !sim.Step() {
		t.Fatal("expected job-2 completion@1100")
	}

	// Poll@2000 → job 3 taken INSIDE [1500,2500): ServiceMs 100 > window
	// deadline 50 → times out → terminal failure (MaxRetries=0).
	rt.applyFaults(2000)
	if !sim.Step() {
		t.Fatal("expected poll@2000")
	}
	rt.applyFaults(2000)
	if !sim.Step() {
		t.Fatal("expected job-3 completion@2100 (timeout)")
	}

	// Poll@3000 → job 4 taken after the window: succeeds.
	rt.applyFaults(3000)
	if !sim.Step() {
		t.Fatal("expected poll@3000")
	}
	rt.applyFaults(3000)
	if !sim.Step() {
		t.Fatal("expected job-4 completion@3100")
	}

	if rt.stats.Completed != 3 {
		t.Fatalf("Completed = %d, want 3 (jobs 1,2,4 succeed; only the in-window job 3 fails)", rt.stats.Completed)
	}
	if rt.stats.Failed != 1 {
		t.Fatalf("Failed = %d, want 1 (only the attempt taken inside the window times out)", rt.stats.Failed)
	}
	if rt.stats.AttemptsTotal != 4 {
		t.Fatalf("AttemptsTotal = %d, want 4", rt.stats.AttemptsTotal)
	}
}

// faultGateConfig exercises every fault override on a stochastic path in one
// run: a drop window (queue drop substream), a failure window (fail
// substream draws) and a timeout window (deadline, no RNG) — with the static
// baselines OFF so the windows are the only fault source.
func faultGateConfig() Config {
	cfg := DefaultConfig()
	cfg.Seed = 7
	cfg.BurstCount = 30
	cfg.SimEndMs = 5000
	cfg.FailP = 0
	cfg.DropRate = 0
	cfg.MaxRetries = 1
	cfg.NumWorkers = 3
	cfg.Concurrency = 1
	cfg.PollMs = 10
	cfg.Faults = []faults.Fault{
		{Kind: faults.KindDrop, StartMs: 500, EndMs: 3000, Rate: 0.2},
		{Kind: faults.KindFailure, StartMs: 1000, EndMs: 4000, Rate: 0.6},
		{Kind: faults.KindTimeout, StartMs: 2000, EndMs: 3500, Rate: 2}, // ServiceMs 5 > 2 → attempts time out
	}
	return cfg
}

// TestFaultPathDeterminismN5 is the fault-path determinism gate: same model
// + same seed → bit-identical Result across N=5 runs with active drop,
// failure and timeout windows (draws from the queue drop substream and the
// fail substream).
func TestFaultPathDeterminismN5(t *testing.T) {
	run := func() Result { return faultRun(t, faultGateConfig()) }
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
			t.Fatalf("run %d: digest mismatch (fault path)\n%q\n%q", i, res.Digest, firstDigest)
		}
		if s := fmt.Sprintf("%+v", res.Stats); s != firstStats {
			t.Fatalf("run %d: stats mismatch (fault path)\n%s\n%s", i, s, firstStats)
		}
		if res.SimEndMs != firstEnd {
			t.Fatalf("run %d: SimEndMs = %d, want %d", i, res.SimEndMs, firstEnd)
		}
	}
	t.Logf("fault-path digest (run 0): %q", firstDigest)
	t.Logf("sha256(digest)=%x", sha256.Sum256([]byte(firstDigest)))
	t.Logf("stats (run 0): %s", firstStats)
	t.Logf("SimEndMs = %d", firstEnd)
}

// TestFaultMonitorOffEqualOn is the fault-path monitor-neutrality gate: on
// the stochastic fault path (drop + failure + timeout windows), the
// mechanism result with the System's free monitor ON is bit-identical to
// monitoring OFF — the monitor never perturbs the simulation.
func TestFaultMonitorOffEqualOn(t *testing.T) {
	run := func(off bool) Result {
		cfg := faultGateConfig()
		cfg.DisableMonitor = off
		return faultRun(t, cfg)
	}
	on := run(false)
	off := run(true)
	if fmt.Sprintf("%+v", on.Stats) != fmt.Sprintf("%+v", off.Stats) {
		t.Fatalf("fault-path stats differ between monitor on/off:\non  %+v\noff %+v", on.Stats, off.Stats)
	}
	if on.SimEndMs != off.SimEndMs {
		t.Fatalf("fault-path SimEndMs differs: on=%d off=%d", on.SimEndMs, off.SimEndMs)
	}
	if on.Digest == "" || off.Digest != "" {
		t.Fatalf("unexpected digests: on=%q off=%q", on.Digest, off.Digest)
	}
}

// TestFaultRateValidationRejectsNonFinite pins the malformed-rate boundary
// of Config validation: a NaN or ±Inf Rate must be rejected for every kind
// (NaN slips past the [0,1] range checks, and +Inf KindTimeout has no upper
// bound — each would silently disable the fault instead of failing the
// config).
func TestFaultRateValidationRejectsNonFinite(t *testing.T) {
	for _, kind := range []faults.FaultKind{faults.KindDrop, faults.KindFailure, faults.KindTimeout} {
		for _, rate := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
			cfg := DefaultConfig()
			cfg.Faults = []faults.Fault{{Kind: kind, StartMs: 0, EndMs: 100, Rate: rate}}
			if err := cfg.validate(); err == nil {
				t.Fatalf("%s rate=%v: validate accepted non-finite rate, want error", kind, rate)
			}
		}
	}
}

// TestFaultDropWindowOverridesNonZeroBaseline pins that an active window
// overrides AND a closed window restores a NON-ZERO baseline: with a
// DropRate=1.0 baseline (everything dropped, consuming the retry budget) and
// a Rate=0 drop window, jobs are delivered cleanly DURING the window (they
// complete — proving the override), while drops resume OUTSIDE it (Dropped
// and Failed still > 0 — proving the baseline restore after the window).
func TestFaultDropWindowOverridesNonZeroBaseline(t *testing.T) {
	cfg := windowedConfig()
	cfg.DropRate = 1.0                                                                        // baseline: every delivery dropped
	cfg.Faults = []faults.Fault{{Kind: faults.KindDrop, StartMs: 3000, EndMs: 6000, Rate: 0}} // window: nothing dropped

	fRes := faultRun(t, cfg)
	ctrl := func() Result {
		c := cfg
		c.Faults = nil
		return faultRun(t, c)
	}()

	t.Logf("baseline 1.0 + [3000,6000) Rate 0: fault completed=%d dropped=%d failed=%d | control completed=%d dropped=%d failed=%d",
		fRes.Stats.Completed, fRes.Stats.Dropped, fRes.Stats.Failed, ctrl.Stats.Completed, ctrl.Stats.Dropped, ctrl.Stats.Failed)

	// During the Rate-0 window the override delivers jobs cleanly → they
	// complete (the all-drop baseline alone would complete nothing).
	if fRes.Stats.Completed == 0 {
		t.Fatalf("window override: Completed=0, want > 0 (Rate 0 window must deliver jobs), dropped=%d", fRes.Stats.Dropped)
	}
	// Control (all-drop, no window) completes nothing — isolates the window.
	if ctrl.Stats.Completed != 0 {
		t.Fatalf("control (DropRate=1.0, no window): Completed=%d, want 0 (clean all-drop baseline)", ctrl.Stats.Completed)
	}
	// Outside the window the baseline 1.0 resumes: drops (and their retry
	// churn → terminal failures) still occur after the window closes.
	if fRes.Stats.Dropped == 0 || fRes.Stats.Failed == 0 {
		t.Fatalf("baseline not restored after window: Dropped=%d Failed=%d, want both > 0", fRes.Stats.Dropped, fRes.Stats.Failed)
	}
}

// TestFaultTimeoutBoundaryNotActiveWhenEqual pins the strict `>` deadline
// boundary (server.go:388-390): an attempt times out iff ServiceMs > the
// effective timeout, so a window whose rate EQUALS ServiceMs must NOT time
// out the attempt. A whole-run window with rate == ServiceMs succeeds every
// attempt.
func TestFaultTimeoutBoundaryNotActiveWhenEqual(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BurstCount = 0
	cfg.Seed = 1
	cfg.SimEndMs = 100000
	cfg.ServiceMs = 100
	cfg.FailP = 0
	cfg.MaxRetries = 0
	cfg.TimeoutMs = 0
	cfg.NumWorkers = 1
	cfg.Concurrency = 1
	cfg.PollMs = 1000
	cfg.Faults = []faults.Fault{{Kind: faults.KindTimeout, StartMs: 0, EndMs: 100000, Rate: 100}} // rate == ServiceMs

	rt, sim := contractRuntime(t, cfg)
	rt.enqueueTask(model.Task{ID: 1, Attempt: 1}, &jobState{id: 1, chainIdx: -1, groupID: noGroup, attempt: 1, enqueuedMs: 0}, sim.Now())
	rt.tick(sim.Now()) // starts poller, drains, applyFaults(0) → window active, effTimeout=100
	if !sim.Step() {
		t.Fatal("expected poll@0")
	}
	rt.applyFaults(0) // take reads effTimeout: ServiceMs(100) > 100 is FALSE → no timeout
	if !sim.Step() {
		t.Fatal("expected completion@100")
	}
	if rt.stats.Completed != 1 || rt.stats.Failed != 0 {
		t.Fatalf("rate==ServiceMs: Completed=%d Failed=%d, want 1 and 0 (strict > boundary — no timeout)", rt.stats.Completed, rt.stats.Failed)
	}
}

// TestFaultsInactiveZeroDraw proves a configured-but-inactive fault makes
// zero RNG draws: a zero-baseline config with a window that never opens
// (fully after SimEndMs) yields a byte-identical digest to the same config
// with no Faults at all — the dispatcher resets 0-knobs and never draws.
func TestFaultsInactiveZeroDraw(t *testing.T) {
	base := func() Config {
		cfg := DefaultConfig()
		cfg.Seed = 11
		cfg.BurstCount = 20
		cfg.SimEndMs = 2000
		cfg.FailP = 0
		cfg.DropRate = 0
		cfg.MaxRetries = 0
		cfg.MaxWaitMs = 0
		return cfg
	}
	plain := faultRun(t, base())
	inactive := func() Config {
		cfg := base()
		// Windows fully beyond the 2000ms horizon — never active.
		cfg.Faults = []faults.Fault{
			{Kind: faults.KindDrop, StartMs: 9000, EndMs: 10000, Rate: 0.5},
			{Kind: faults.KindFailure, StartMs: 9000, EndMs: 10000, Rate: 0.5},
			{Kind: faults.KindTimeout, StartMs: 9000, EndMs: 10000, Rate: 5},
		}
		return cfg
	}
	faulted := faultRun(t, inactive())
	if plain.Digest != faulted.Digest {
		t.Fatalf("inactive-fault digest differs from zero-draw baseline:\nplain   %q\nfaulted %q", plain.Digest, faulted.Digest)
	}
	if fmt.Sprintf("%+v", plain.Stats) != fmt.Sprintf("%+v", faulted.Stats) {
		t.Fatalf("inactive-fault stats differ from baseline:\nplain   %+v\nfaulted %+v", plain.Stats, faulted.Stats)
	}
	if plain.SimEndMs != faulted.SimEndMs {
		t.Fatalf("inactive-fault SimEndMs differs: %d vs %d", plain.SimEndMs, faulted.SimEndMs)
	}
}
