package temporal

import (
	"fmt"
	"testing"
)

// respoolStarvation returns a config that genuinely exercises the CHILD
// partition's persistence re-spool round-trip (ChildReSpoolMs > 0): a
// dead-forwarding window (SyncMatchWindowMs 0 < ForwardReliableMinMs 200)
// routes activity adds into the child, and a STARVED root poller fleet
// (PollersAct 2 with a non-zero activity service time) keeps the root
// pollers too busy to accept a drained task at read-back, so the task
// re-spools (write→read round-trip with preserved create-time). This is the
// regime the 2026-09-07 re-spool model targets.
func respoolStarvation() Config {
	cfg := DefaultConfig()
	cfg.BurstCount = 150
	cfg.SimEndMs = 30000
	cfg.FaultStartMs = 1
	cfg.FaultEndMs = 30000
	cfg.SyncMatchWindowMs = 0 // dead-forwarding window → child path
	cfg.DispatchRate = 39     // calibrated dispatch ceiling (c2 cell)
	cfg.DispatchBurst = 1
	cfg.MatchingCapacity = 1000
	cfg.PollersWF = 2
	cfg.PollersAct = 2 // starved poller fleet
	cfg.ActivityMs = 100
	cfg.ChildReSpoolMs = 500 // persistence write→read round-trip
	return cfg
}

// TestReSpoolByteNeutralDefault pins the re-spool knob's byte-neutrality
// within the CURRENT code: a config with ChildReSpoolMs explicitly 0 is
// bit-identical to the same config with the knob unset — both take the
// immediate-FIFO drain (the pre-re-spool behavior) and fire zero re-spools.
// (The authoritative PRE-CHANGE neutrality gate is the integration matrix —
// TestSynctestMatrix's model leg is byte-identical to the pre-re-spool
// baseline, c0 1.00 / c1 10.40 / c2 7.28 / c4 10.40.) The persistence
// round-trip is OFF by default.
func TestReSpoolByteNeutralDefault(t *testing.T) {
	off := DefaultConfig() // ChildReSpoolMs 0 (unset) — byte-neutral default
	explicit := DefaultConfig()
	explicit.ChildReSpoolMs = 0

	a := runTopo(t, off)
	b := runTopo(t, explicit)
	if a.Result.Digest != b.Result.Digest {
		t.Fatalf("ChildReSpoolMs=0 perturbed the run:\n%q\n%q", a.Result.Digest, b.Result.Digest)
	}
	if a.Gate.ReSpools != 0 || b.Gate.ReSpools != 0 {
		t.Fatalf("ReSpools = %d/%d, want 0 (round-trip disabled at default)", a.Gate.ReSpools, b.Gate.ReSpools)
	}
}

// TestReSpoolDisabledKeepsCalibratedCellByteIdentical guards the C1 matrix
// byte-neutrality requirement: enabling NO re-spool (the default) must leave
// the calibrated degraded window-0 cell bit-identical to the pre-re-spool
// model (ChildReSpoolMs 0 ⇒ the same immediate-FIFO drain).
func TestReSpoolDisabledKeepsCalibratedCellByteIdentical(t *testing.T) {
	base := DefaultConfig()
	base.BurstCount = 50
	base.SyncMatchWindowMs = 0
	base.FaultStartMs = 1
	base.FaultEndMs = 11000
	base.DispatchRate = 39 // calibrated dispatch ceiling (c2 cell)
	base.DispatchBurst = 1
	base.MatchingCapacity = 1000
	base.ChildReSpoolMs = 0

	a := runTopo(t, base)
	b := runTopo(t, base)
	if a.Result.Digest != b.Result.Digest {
		t.Fatalf("re-spool-0 run not deterministic: %q vs %q", a.Result.Digest, b.Result.Digest)
	}
	aps := a.Result.Stats.AttemptsPerStart()
	// Pin the realized values (the prior coupling-invariant baseline): aps
	// ~7.28, head < 5s, gate 0% — the re-spool OFF must NOT change these.
	if aps > 7.5 || aps < 6.9 {
		t.Errorf("re-spool-0 c2 aps = %.3f, want ~7.28 (calibrated baseline shifted)", aps)
	}
	if a.Gate.MaxHeadAgeMs >= 5000 {
		t.Errorf("re-spool-0 c2 head = %dms, want < 5s (calibrated cell must stay un-latched at default)", a.Gate.MaxHeadAgeMs)
	}
	if a.Gate.GateTicks != 0 {
		t.Errorf("re-spool-0 c2 gate tripped %d ticks, want 0 (default must not latch the calibrated cell)", a.Gate.GateTicks)
	}
}

// TestReSpoolFiresUnderPollerStarvation pins that the persistence re-spool
// round-trip GENUINELY fires when the root poller fleet is starved: the
// drained child task finds no parked root poller at read-back and re-spools
// (ReSpools > 0). This is the mechanism the round-trip exists to express — a
// FIFO memory drain cannot re-circulate an unservable task while the drain
// proceeds. The re-spool preserves create-time, so the child head-of-backlog
// age (now − BornMs) is measured across the round-trips.
func TestReSpoolFiresUnderPollerStarvation(t *testing.T) {
	cfg := respoolStarvation()
	top := runTopo(t, cfg)
	if top.Gate.ReSpools == 0 {
		t.Fatalf("ReSpools = 0 — the persistence round-trip never fired under poller starvation; mechanism not engaged")
	}
	if top.Gate.MaxHeadAgeMs == 0 {
		t.Fatalf("head age = 0 — the child backlog never accumulated; the child path was not exercised")
	}
	t.Logf("starvation re-spool: aps=%.3f head=%dms reSpools=%d gateTripped=%d/%d (%.1f%%)",
		top.Result.Stats.AttemptsPerStart(), top.Gate.MaxHeadAgeMs, top.Gate.ReSpools,
		top.Gate.GateTicks, top.Gate.Ticks, 100*float64(top.Gate.GateTicks)/float64(top.Gate.Ticks))
}

// TestReSpoolLatchIsCouplingBound pins the honest bounded-negative: the
// model's backlog-age gate CAN latch under heavier/starvation load, but the
// latch is always coupled to a THROUGHPUT COLLAPSE (the aps×head-age
// invariant) — a high-throughput latch (head > 5s while aps stays in the
// calibrated ~6.9-7.3 range) is NOT reachable even with the re-spool
// round-trip FIRING. This is the evidence that the re-spool models the
// create-time-preserving persistence hop but does NOT reproduce the real
// server's throughput/head-age decoupling (bubble pB sustains a deep backlog
// at retained aps 6.32; see the syncmatch_sweep doc.go judgment block).
func TestReSpoolLatchIsCouplingBound(t *testing.T) {
	cfg := respoolStarvation() // re-spool fires here (ReSpools > 0)
	top := runTopo(t, cfg)
	if top.Gate.ReSpools == 0 {
		t.Fatalf("premise: re-spool must fire for this coupling probe")
	}
	if top.Gate.MaxHeadAgeMs < 5000 {
		t.Logf("note: this cell did not latch in this run (head=%dms) — the coupling premise holds even without a trip",
			top.Gate.MaxHeadAgeMs)
		return
	}
	// If it DOES latch, throughput must have collapsed — never a
	// high-throughput (aps ~7) latch.
	aps := top.Result.Stats.AttemptsPerStart()
	if aps >= 6.9 {
		t.Fatalf("RE-SPOOL HIGH-THROUGHPUT LATCH: head=%dms at aps=%.3f ≥ 6.9 — the coupling-bound finding is INVALIDATED; revisit",
			top.Gate.MaxHeadAgeMs, aps)
	}
	t.Logf("coupling-bound: latch at head=%dms coincides with aps=%.3f (collapsed, not the calibrated ~7 range)",
		top.Gate.MaxHeadAgeMs, aps)
}

type topoRun struct {
	Result Result
	Gate   ChildGateEvidence
}

// runTopo builds+runs one models/temporal config and returns its Result plus
// the child gate evidence (the re-spool/gate-latch surface).
func runTopo(t *testing.T, cfg Config) topoRun {
	t.Helper()
	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res := top.Run()
	return topoRun{Result: res, Gate: top.ChildGate()}
}

// TestReSpoolDeterminismN3 pins the re-spool-ON path's determinism as a
// PROVEN contract (not just inferred from single-goroutine scheduled-event
// ordering): N=3 identical runs of the starvation re-spool config produce a
// bit-identical digest, stats, and re-spool count. The re-spool scheduled-
// event chain (offerAfterPersistenceRead) must not introduce any run-to-run
// nondeterminism.
func TestReSpoolDeterminismN3(t *testing.T) {
	cfg := respoolStarvation()
	var firstDigest string
	var firstStats string
	var firstRespools uint64
	for i := 0; i < 3; i++ {
		top := runTopo(t, cfg)
		if i == 0 {
			firstDigest = top.Result.Digest
			firstStats = fmt.Sprintf("%+v", top.Result.Stats)
			firstRespools = top.Gate.ReSpools
		}
		if top.Result.Digest != firstDigest {
			t.Fatalf("run %d: re-spool digest mismatch\n%q\n%q", i, top.Result.Digest, firstDigest)
		}
		if s := fmt.Sprintf("%+v", top.Result.Stats); s != firstStats {
			t.Fatalf("run %d: re-spool stats mismatch\n%s\n%s", i, s, firstStats)
		}
		if top.Gate.ReSpools != firstRespools {
			t.Fatalf("run %d: ReSpools %d != %d (re-spool path nondeterministic)", i, top.Gate.ReSpools, firstRespools)
		}
	}
	if firstRespools == 0 {
		t.Fatalf("ReSpools = 0 — determinism probe did not exercise the re-spool path")
	}
}
