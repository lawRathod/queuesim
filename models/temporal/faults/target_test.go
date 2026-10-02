package faults

import (
	"testing"

	"github.com/lawRathod/queuesim/model"
)

// fakeTarget is a recording seam stub: it captures the last knob written
// per chokepoint so tests can assert exactly what Apply hands off (the
// model's runtime adapter implements the same interface by forwarding to
// the queues/fields — fault_target.go).
type fakeTarget struct {
	spoolWf      float64
	spoolAct     float64
	rate         float64
	burst        float64
	rateCalled   bool
	maxWaitAct   int64
	delay        int64
	pollerForced int
	wfTimerMs    int64
	persistMult  float64
	pollJitterP  float64
	s2cMs        int64
	failsAt      bool
	failsFor     bool
}

func (f *fakeTarget) SetSpoolDrop(wfRate, actRate float64) { f.spoolWf, f.spoolAct = wfRate, actRate }
func (f *fakeTarget) SetDispatchRate(rate, burst float64) {
	f.rate, f.burst, f.rateCalled = rate, burst, true
}
func (f *fakeTarget) SetMaxWaitAct(ms int64)                       { f.maxWaitAct = ms }
func (f *fakeTarget) SetDispatchDelay(ms int64)                    { f.delay = ms }
func (f *fakeTarget) SetPollerForced(n int)                        { f.pollerForced = n }
func (f *fakeTarget) SetWfTimerMs(ms int64)                        { f.wfTimerMs = ms }
func (f *fakeTarget) SetPersistMult(rate float64)                  { f.persistMult = rate }
func (f *fakeTarget) SetPollJitterP(p float64)                     { f.pollJitterP = p }
func (f *fakeTarget) SetS2cMs(ms int64)                            { f.s2cMs = ms }
func (f *fakeTarget) ActivityFailsAt(nowMs int64) bool             { return f.failsAt }
func (f *fakeTarget) ActivityFailsFor(id uint64, nowMs int64) bool { return f.failsFor }

// TestApplyNilVectorIsNoop pins the neutrality contract: a nil vector
// touches no chokepoint (zero knobs, zero draws).
func TestApplyNilVectorIsNoop(t *testing.T) {
	f := &fakeTarget{}
	Apply(f, Spec{Vector: nil}, 0)
	if f.rateCalled || f.spoolWf != 0 || f.spoolAct != 0 || f.maxWaitAct != 0 || f.delay != 0 {
		t.Fatalf("nil vector touched knobs: %+v", f)
	}
}

// TestApplyInactiveMembersReset pins the per-tick reset semantics: with no
// ACTIVE member the tail applications still run, restoring the queues'
// defaults (spool 0, MaxWaitMs to the config default, delay 0) and the
// runtime fields' inactive sentinels (-1 poller force, -1 timer). The
// dispatch rate is left untouched (its constructed rate) and the
// per-member-only kinds (persistence/jitter/s2c) do not write.
func TestApplyInactiveMembersReset(t *testing.T) {
	spec := Spec{
		Vector: &model.DecisionVector{
			Name: "future",
			Compose: []model.DecisionVector{
				{Name: "ad", Kind: model.KindAddDrop, StartMs: 999999, DurationMs: 0, Rate: 0.5},
				{Name: "pm", Kind: model.KindPollerMin, StartMs: 999999, DurationMs: 0, Rate: 2},
				{Name: "tl", Kind: model.KindTimerLoad, StartMs: 999999, DurationMs: 0, Rate: 777},
				{Name: "s2s", Kind: model.KindS2STimeout, StartMs: 999999, DurationMs: 0, Rate: 555},
				{Name: "td", Kind: model.KindTransferDelay, StartMs: 999999, DurationMs: 0, Rate: 33},
				{Name: "pd", Kind: model.KindPollDrop, StartMs: 999999, DurationMs: 0, Rate: 0.3},
				{Name: "ps", Kind: model.KindPersistenceStall, StartMs: 999999, DurationMs: 0, Rate: 4},
				{Name: "pj", Kind: model.KindPollJitter, StartMs: 999999, DurationMs: 0, Rate: 0.75},
				{Name: "s2c", Kind: model.KindS2CShorten, StartMs: 999999, DurationMs: 0, Rate: 99},
			},
		},
		ActivityS2STimeoutMs: 1234,
	}
	f := &fakeTarget{}
	Apply(f, spec, 0)

	if f.spoolWf != 0 || f.spoolAct != 0 {
		t.Fatalf("inactive members must reset spool drop to 0: wf=%v act=%v", f.spoolWf, f.spoolAct)
	}
	if f.maxWaitAct != 1234 {
		t.Fatalf("inactive s2s must reset MaxWaitAct to the config default: %d, want 1234", f.maxWaitAct)
	}
	if f.delay != 0 {
		t.Fatalf("inactive transfer delay must reset to 0: %d", f.delay)
	}
	if f.pollerForced != -1 || f.wfTimerMs != -1 {
		t.Fatalf("inactive members must restore inactive sentinels: poller=%d timer=%d, want -1/-1", f.pollerForced, f.wfTimerMs)
	}
	if f.rateCalled {
		t.Fatal("inactive poll_drop must NOT touch the dispatch rate (constructed rate applies)")
	}
	if f.persistMult != 0 || f.pollJitterP != 0 || f.s2cMs != 0 {
		t.Fatalf("per-member-only kinds must not write when inactive: %v/%v/%d", f.persistMult, f.pollJitterP, f.s2cMs)
	}
}

// TestApplyActiveAccumulation exercises the dispatcher's accumulation with
// every active kind: max-rate for the drop kinds (add_drop folds into
// wf_drop via wfSpoolRate), last-wins for the scalar kinds, the computed
// dispatch rate, and the per-member persistence/jitter/s2c applications.
func TestApplyActiveAccumulation(t *testing.T) {
	spec := Spec{
		Vector: &model.DecisionVector{
			Name: "all",
			Compose: []model.DecisionVector{
				{Name: "ad1", Kind: model.KindAddDrop, StartMs: 0, DurationMs: 0, Rate: 0.2},
				{Name: "ad2", Kind: model.KindAddDrop, StartMs: 0, DurationMs: 0, Rate: 0.4}, // max wins
				{Name: "wf", Kind: model.KindWfDrop, StartMs: 0, DurationMs: 0, Rate: 0.6},
				{Name: "pd", Kind: model.KindPollDrop, StartMs: 0, DurationMs: 0, Rate: 0.3},
				{Name: "s2s", Kind: model.KindS2STimeout, StartMs: 0, DurationMs: 0, Rate: 555},
				{Name: "td", Kind: model.KindTransferDelay, StartMs: 0, DurationMs: 0, Rate: 33},
				{Name: "pm", Kind: model.KindPollerMin, StartMs: 0, DurationMs: 0, Rate: 2},
				{Name: "tl", Kind: model.KindTimerLoad, StartMs: 0, DurationMs: 0, Rate: 777},
				{Name: "ps", Kind: model.KindPersistenceStall, StartMs: 0, DurationMs: 0, Rate: 4},
				{Name: "pj", Kind: model.KindPollJitter, StartMs: 0, DurationMs: 0, Rate: 0.75},
				{Name: "s2c", Kind: model.KindS2CShorten, StartMs: 0, DurationMs: 0, Rate: 99},
			},
		},
		DispatchRate:  10,
		DispatchBurst: 2,
	}
	f := &fakeTarget{}
	Apply(f, spec, 0)

	// add_drop max 0.4 wins; act queue gets add_drop alone; wf queue folds
	// in wf_drop: max(0.4, 0.6) = 0.6.
	if f.spoolWf != 0.6 || f.spoolAct != 0.4 {
		t.Fatalf("spool drop: wf=%v act=%v, want 0.6/0.4", f.spoolWf, f.spoolAct)
	}
	// poll_drop: rate = 10·(1−0.3) (same expression → same bits).
	if want := 10 * (1 - 0.3); f.rate != want || f.burst != 2 || !f.rateCalled {
		t.Fatalf("dispatch rate: %v burst %v called %v, want %v/2", f.rate, f.burst, f.rateCalled, want)
	}
	if f.maxWaitAct != 555 {
		t.Fatalf("MaxWaitAct = %d, want 555", f.maxWaitAct)
	}
	if f.delay != 33 {
		t.Fatalf("dispatch delay = %d, want 33", f.delay)
	}
	if f.pollerForced != 2 || f.wfTimerMs != 777 {
		t.Fatalf("forced poller/timer = %d/%d, want 2/777", f.pollerForced, f.wfTimerMs)
	}
	if f.persistMult != 4 || f.pollJitterP != 0.75 || f.s2cMs != 99 {
		t.Fatalf("persist/jitter/s2c = %v/%v/%d, want 4/0.75/99", f.persistMult, f.pollJitterP, f.s2cMs)
	}
}

// TestApplyPlainVector exercises the plain (non-composed) vector path:
// Members() returns the vector itself, so the Kind sits on the top-level
// vector (the composed-members dispatch is covered by
// TestApplyActiveAccumulation).
func TestApplyPlainVector(t *testing.T) {
	spec := Spec{
		Vector:       &model.DecisionVector{Name: "single", Kind: model.KindTransferDelay, StartMs: 0, DurationMs: 0, Rate: 42},
		DispatchRate: 7,
	}
	f := &fakeTarget{}
	Apply(f, spec, 0)
	if f.delay != 42 {
		t.Fatalf("dispatch delay = %d, want 42 (plain vector kind)", f.delay)
	}
	if f.rateCalled {
		t.Fatal("no poll_drop member: dispatch rate must stay untouched")
	}
	if f.spoolWf != 0 || f.spoolAct != 0 || f.maxWaitAct != 0 {
		t.Fatalf("unrelated knobs must reset to defaults: %+v", f)
	}
}

// TestActivityFailsAtConfigWindow pins the failure-decision semantics:
// the config window applies only for the add_drop vector (the bubble
// merged-config caveat); other vectors suppress it; the vector's own
// activity_failure window is the only failure source for that kind.
func TestActivityFailsAtConfigWindow(t *testing.T) {
	base := Spec{FaultStartMs: 100, FaultEndMs: 200}
	if ActivityFailsAt(base, 50) {
		t.Fatal("before window must not fail")
	}
	if !ActivityFailsAt(base, 150) {
		t.Fatal("inside window must fail")
	}
	if ActivityFailsAt(base, 200) {
		t.Fatal("at EndMs must not fail (half-open)")
	}
	if ActivityFailsAt(Spec{FaultStartMs: 0, FaultEndMs: 0}, 150) {
		t.Fatal("zero window must not fail")
	}
	open := Spec{FaultStartMs: 100, FaultEndMs: 0}
	if !ActivityFailsAt(open, 99999) {
		t.Fatal("open-ended window must fail")
	}

	// add_drop vector KEEPS the config window (the merged-config caveat).
	addDrop := Spec{Vector: &model.DecisionVector{Kind: model.KindAddDrop, StartMs: 0, DurationMs: 0}, FaultStartMs: 100, FaultEndMs: 200}
	if !ActivityFailsAt(addDrop, 150) {
		t.Fatal("add_drop must keep the config window")
	}
	// Any other vector suppresses it (the vector's own window only).
	other := Spec{Vector: &model.DecisionVector{Kind: model.KindPollDrop, StartMs: 0, DurationMs: 0}, FaultStartMs: 100, FaultEndMs: 200}
	if ActivityFailsAt(other, 150) {
		t.Fatal("non-add_drop vector must suppress the config window")
	}
	// activity_failure vector: its own window is the failure source.
	af := Spec{Vector: &model.DecisionVector{Kind: model.KindActivityFailure, StartMs: 100, DurationMs: 50}}
	if !ActivityFailsAt(af, 120) || ActivityFailsAt(af, 200) {
		t.Fatal("activity_failure window must be the only source")
	}
}

// TestActivityFailsForCascade pins the cascade FNV: bad actors (ID hash <
// fraction) fail inside the window, healthy workflows never fail, and the
// decision is deterministic (same ID → same verdict).
func TestActivityFailsForCascade(t *testing.T) {
	spec := Spec{
		FaultStartMs:       0,
		FaultEndMs:         1000,
		CascadeOn:          true,
		CascadeBadFraction: 0.5,
	}
	bad, good := uint64(0), uint64(0)
	foundBad, foundGood := false, false
	for id := uint64(0); id < 200 && (!foundBad || !foundGood); id++ {
		if ActivityFailsFor(spec, id, 500) {
			if !foundBad {
				bad, foundBad = id, true
			}
		} else if !foundGood {
			good, foundGood = id, true
		}
	}
	if !foundBad || !foundGood {
		t.Fatal("cascade population must contain both bad actors and healthy workflows")
	}
	// Determinism: repeated calls give the same verdict.
	if ActivityFailsFor(spec, bad, 500) != ActivityFailsFor(spec, bad, 500) {
		t.Fatal("cascade verdict must be deterministic per ID")
	}
	// Healthy workflows never fail inside the window.
	if ActivityFailsFor(spec, good, 500) {
		t.Fatal("healthy workflow must never fail")
	}
	// Outside the window nobody fails; cascade off = the global predicate.
	if ActivityFailsFor(spec, bad, 2000) {
		t.Fatal("outside the window no workflow may fail")
	}
	off := Spec{FaultStartMs: 0, FaultEndMs: 1000, CascadeOn: false}
	if !ActivityFailsFor(off, bad, 500) || !ActivityFailsAt(off, 500) {
		t.Fatal("cascade off must match the global predicate")
	}
}
