package faults

import "testing"

// fakeTarget is a recording seam stub: it captures the last knob written
// per chokepoint and exposes the baselines the appliers restore, so tests
// can assert exactly what Apply hands off (the model's runtime adapter
// implements the same interface by forwarding to the queue and the runtime
// fields — fault_target.go).
type fakeTarget struct {
	dropRate     float64
	effFailP     float64
	effTimeoutMs int64

	dropBase float64
	failBase float64
	toBase   int64
}

func (f *fakeTarget) SetDropRate(rate float64)       { f.dropRate = rate }
func (f *fakeTarget) SetEffectiveFailP(p float64)    { f.effFailP = p }
func (f *fakeTarget) SetEffectiveTimeoutMs(ms int64) { f.effTimeoutMs = ms }
func (f *fakeTarget) DropBaseline() float64          { return f.dropBase }
func (f *fakeTarget) FailPBaseline() float64         { return f.failBase }
func (f *fakeTarget) TimeoutBaseline() int64         { return f.toBase }

// newFakeTarget returns a seam stub with the static baselines a real
// runtime carries (DropRate 0.05, FailP 0.3, TimeoutMs 1000).
func newFakeTarget() *fakeTarget {
	return &fakeTarget{dropBase: 0.05, failBase: 0.3, toBase: 1000}
}

// TestApplyEmptyNoop pins the neutrality contract: an empty fault list
// early-returns — zero chokepoint writes, zero RNG draws (the run is
// byte-identical to a config with no faults; pinned end-to-end by
// TestFaultsInactiveZeroDraw).
func TestApplyEmptyNoop(t *testing.T) {
	f := newFakeTarget()
	Apply(f, nil, 0)
	if f.dropRate != 0 || f.effFailP != 0 || f.effTimeoutMs != 0 {
		t.Fatalf("empty fault list touched knobs: %+v", f)
	}
}

// TestApplyInactiveRestoresBaselines pins the per-tick reset semantics: a
// configured-but-inactive list still runs the tail appliers, restoring the
// static baselines (SetDropRate of the baseline, effFailP = FailP,
// effTimeoutMs = TimeoutMs) — the O(1) idempotent knob-reset path with
// zero RNG draws.
func TestApplyInactiveRestoresBaselines(t *testing.T) {
	f := newFakeTarget()
	Apply(f, []Fault{
		{Kind: KindDrop, StartMs: 9000, EndMs: 10000, Rate: 0.5},
		{Kind: KindFailure, StartMs: 9000, EndMs: 10000, Rate: 0.5},
		{Kind: KindTimeout, StartMs: 9000, EndMs: 10000, Rate: 5},
	}, 0)
	if f.dropRate != f.dropBase {
		t.Fatalf("inactive drop window: rate=%v, want baseline %v (restore)", f.dropRate, f.dropBase)
	}
	if f.effFailP != f.failBase {
		t.Fatalf("inactive failure window: failP=%v, want baseline %v (restore)", f.effFailP, f.failBase)
	}
	if f.effTimeoutMs != f.toBase {
		t.Fatalf("inactive timeout window: timeoutMs=%d, want baseline %d (restore)", f.effTimeoutMs, f.toBase)
	}
}

// TestApplyActiveOverrides pins the active-window semantics: each active
// window overrides its baseline with the fault's rate, and the timeout
// deadline truncates a fractional rate to whole sim ms (int64(rate)).
func TestApplyActiveOverrides(t *testing.T) {
	f := newFakeTarget()
	Apply(f, []Fault{
		{Kind: KindDrop, StartMs: 0, EndMs: 100, Rate: 0.8},
		{Kind: KindFailure, StartMs: 0, EndMs: 100, Rate: 0.6},
		{Kind: KindTimeout, StartMs: 0, EndMs: 100, Rate: 50.9},
	}, 50)
	if f.dropRate != 0.8 {
		t.Fatalf("drop override: rate=%v, want 0.8", f.dropRate)
	}
	if f.effFailP != 0.6 {
		t.Fatalf("failure override: failP=%v, want 0.6", f.effFailP)
	}
	if f.effTimeoutMs != 50 {
		t.Fatalf("timeout override: timeoutMs=%d, want 50 (int64(50.9) floors)", f.effTimeoutMs)
	}
}

// TestApplyMixedActiveRestoresInactive pins the per-kind composition within
// a single Apply call: an active window overrides its knob while the other
// kinds' inactive windows still restore their baselines.
func TestApplyMixedActiveRestoresInactive(t *testing.T) {
	f := newFakeTarget()
	Apply(f, []Fault{
		{Kind: KindDrop, StartMs: 0, EndMs: 100, Rate: 0.8},
		{Kind: KindFailure, StartMs: 9000, EndMs: 10000, Rate: 0.9},
		{Kind: KindTimeout, StartMs: 9000, EndMs: 10000, Rate: 5},
	}, 50)
	if f.dropRate != 0.8 {
		t.Fatalf("active drop: rate=%v, want 0.8 (override)", f.dropRate)
	}
	if f.effFailP != f.failBase {
		t.Fatalf("inactive failure: failP=%v, want baseline %v (restore)", f.effFailP, f.failBase)
	}
	if f.effTimeoutMs != f.toBase {
		t.Fatalf("inactive timeout: timeoutMs=%d, want baseline %d (restore)", f.effTimeoutMs, f.toBase)
	}
}

// TestActiveFaultRateLastWins pins the same-kind overlap semantics of the
// dispatcher: when two faults of the SAME kind are active at once, the LAST
// one in the list governs (mirrors model/faults Injector's last-active-wins
// and the temporal scalar "last wins"). The slice iteration is
// insertion-order, so the result is deterministic.
func TestActiveFaultRateLastWins(t *testing.T) {
	faults := []Fault{
		{Kind: KindDrop, StartMs: 0, EndMs: 100, Rate: 0.2},
		{Kind: KindDrop, StartMs: 50, EndMs: 150, Rate: 0.8},
		{Kind: KindFailure, StartMs: 0, EndMs: 200, Rate: 0.3},
	}
	// At 75 both drop windows are active → the later (0.8) governs.
	if r, ok := activeFaultRate(faults, KindDrop, 75); !ok || r != 0.8 {
		t.Fatalf("overlap@75: rate=%v ok=%v, want 0.8 true (last active wins)", r, ok)
	}
	// At 0 only the first drop window is active → 0.2.
	if r, ok := activeFaultRate(faults, KindDrop, 0); !ok || r != 0.2 {
		t.Fatalf("single@0: rate=%v ok=%v, want 0.2 true", r, ok)
	}
	// At 200 no drop window is active → not found.
	if r, ok := activeFaultRate(faults, KindDrop, 200); ok {
		t.Fatalf("none@200: rate=%v ok=true, want not found", r)
	}
	// Kinds are isolated: the failure fault's rate never leaks into drop.
	if r, ok := activeFaultRate(faults, KindDrop, 100); !ok || r != 0.8 {
		t.Fatalf("kind isolation: rate=%v ok=%v, want 0.8 true", r, ok)
	}
}
