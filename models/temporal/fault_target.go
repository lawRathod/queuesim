package temporal

import (
	"github.com/lawRathod/queuesim/godes/faultsched"
	"github.com/lawRathod/queuesim/model"
	"github.com/lawRathod/queuesim/models/temporal/faults"
)

// applyVector is the pump-time fault entry point: with NO schedule set it
// builds the fault Spec from the runtime's config and delegates to the
// faults subpackage, which applies the active decision-vector members
// through this runtime's Target adapter (ADR-078). With a schedule set the
// schedule is the active fault source and applySchedule handles the tick.
// Called once per pump tick (tick, step 4) and once at start (the
// construction-time knob application — the duplicate per tick is
// intentional and idempotent).
func (rt *runtime) applyVector(nowMs int64) {
	if rt.faultSchedule == nil {
		// Nil schedule: the byte-neutral cfg.Vector-only path — today's
		// exact behavior, bit-for-bit (the WP1 neutrality gate).
		faults.Apply(rt, rt.faultSpec(), nowMs)
		return
	}
	rt.applySchedule(nowMs)
}

// applySchedule applies the active shared fault schedule (ADR-100) at
// nowMs. It queries the schedule's pure State(seed, nowMs) and feeds the
// returned ActiveFaults through the SAME faults.Apply dispatcher the vector
// path uses: it synthesizes a composed model.DecisionVector whose members
// carry the active windows' kind/rate and runs it through Apply. State
// already resolved same-kind overlap (LAST active window wins) and returns
// at most one ActiveFault per kind, so the synthesized member list is
// exactly the "members active at nowMs" set the vector dispatcher would
// iterate — Apply then performs the vector path's exact accumulation
// (max-rate for the drop kinds, last-wins for the scalar kinds) and the
// same tail apply* calls.
//
// When State returns NO active faults the synthesized vector is empty but
// NON-NIL, so Apply still runs its reset-to-defaults tail (Members() on an
// empty-compose vector yields the vector itself — Kind "" matches no case —
// and the tail restores spool 0, MaxWaitAct to the config default, delay 0,
// poller -1, timer -1) — byte-identical to an inactive-vector tick
// (faults/target_test.go TestApplyInactiveMembersReset). Per-member-only
// kinds (persistence_stall/poll_jitter/s2c_shorten) only write while an
// active member carries them, exactly as the vector path behaves.
//
// activity_failure is NOT a pump-time knob on either path: it is excluded
// from the synthesized members (matching faults.Apply's no-op case) and
// handled on the lifecycle path. Its active state is recorded on the
// runtime (schedActFail) for ActivityFailsAt/ActivityFailsFor.
//
// The three RESERVED kinds (persistence_fail/duplicate/partition) have no
// behavior on this leg yet: State may return them and they become
// members, but faults.Apply's switch has no case for them, so they no-op
// (their model mechanisms are a separate later WP — the schedule surface
// accepts them cleanly per ADR-100).
//
// cfg.Vector is nil on this path (mutual-exclusion validation in
// newRuntime), so the synthesized vector never merges with a config vector.
func (rt *runtime) applySchedule(nowMs int64) {
	active := rt.faultSchedule.State(rt.cfg.Seed, nowMs)
	rt.cfg.ScheduleDecisionLog.Add(nowMs, active) // ADR-112: nil receiver = off; raw nowMs, never quantized
	actFail := false
	members := make([]model.DecisionVector, 0, len(active))
	for _, f := range active {
		if f.Kind == faultsched.KindActivityFailure {
			actFail = true // the schedule's activity_failure state (lifecycle path)
			continue
		}
		members = append(members, model.DecisionVector{
			Kind: model.VectorKind(f.Kind),
			Rate: f.Rate,
		})
	}
	rt.schedActFail = actFail
	spec := rt.faultSpec()
	spec.Vector = &model.DecisionVector{Name: "schedule", Compose: members}
	faults.Apply(rt, spec, nowMs)
}

// schedActFailAt reports whether the schedule's activity_failure window is
// active AT nowMs — a per-decision query of the pure State function (no RNG
// draws, no sim mutation, no map iteration — faultsched/doc.go). The
// lifecycle path (complete, history.go) evaluates the failure decision at
// the completion event's own time, exactly as the vector path evaluates its
// window predicate per decision, so window-boundary semantics are exact
// (a 1-tick-lagged pump field would be stale for events firing at the
// closing ms before that tick).
func (rt *runtime) schedActFailAt(nowMs int64) bool {
	for _, f := range rt.faultSchedule.State(rt.cfg.Seed, nowMs) {
		if f.Kind == faultsched.KindActivityFailure {
			return true
		}
	}
	return false
}

// faultSpec projects the runtime's config onto the fault-relevant fields
// the faults subpackage reads. Built fresh on each delegation; the config
// is immutable in these fields, so the projection is stable for a run.
func (rt *runtime) faultSpec() faults.Spec {
	return faults.Spec{
		Vector:               rt.cfg.Vector,
		FaultStartMs:         rt.cfg.FaultStartMs,
		FaultEndMs:           rt.cfg.FaultEndMs,
		CascadeOn:            rt.cascadeOn,
		CascadeBadFraction:   rt.cfg.CascadeBadFraction,
		DispatchRate:         rt.cfg.DispatchRate,
		DispatchBurst:        rt.cfg.DispatchBurst,
		ActivityS2STimeoutMs: rt.cfg.ActivityS2STimeoutMs,
	}
}

// The faults subpackage applies every knob through the Target interface;
// these adapter methods forward to the matching queues and the runtime's
// fault-state fields, so the subpackage never touches the model's
// unexported internals.
var _ faults.Target = (*runtime)(nil)

// SetSpoolDrop forwards the spool-time drop rates to both matching queues.
func (rt *runtime) SetSpoolDrop(wfRate, actRate float64) {
	rt.wfQ.SetSpoolDrop(wfRate)
	rt.actQ.SetSpoolDrop(actRate)
}

// SetDispatchRate forwards the token-bucket dispatch rate and burst to both
// matching queues.
func (rt *runtime) SetDispatchRate(rate, burst float64) {
	rt.wfQ.SetRate(rate, burst)
	rt.actQ.SetRate(rate, burst)
}

// SetMaxWaitAct forwards the matching-act schedule-to-start deadline.
func (rt *runtime) SetMaxWaitAct(ms int64) {
	rt.actQ.SetMaxWaitMs(ms)
}

// SetDispatchDelay forwards the dispatch delay to both matching queues.
func (rt *runtime) SetDispatchDelay(ms int64) {
	rt.wfQ.SetDispatchDelay(ms)
	rt.actQ.SetDispatchDelay(ms)
}

// SetPollerForced stores the forced active poller count for both classes.
func (rt *runtime) SetPollerForced(n int) {
	rt.pollerForced = n
}

// SetWfTimerMs stores the forced per-workflow timer delay.
func (rt *runtime) SetWfTimerMs(ms int64) {
	rt.wfTimerMs = ms
}

// SetPersistMult stores the persistence-latency multiplier.
func (rt *runtime) SetPersistMult(rate float64) {
	rt.persistMult = rate
}

// SetPollJitterP stores the poller long-poll early-expiry probability.
func (rt *runtime) SetPollJitterP(p float64) {
	rt.pollJitterP = p
}

// SetS2cMs stores the ScheduleToClose override.
func (rt *runtime) SetS2cMs(ms int64) {
	rt.s2cMs = ms
}

// ActivityFailsAt reports whether an activity completing at nowMs fails.
// With NO schedule this delegates to the faults subpackage (byte-identical
// semantics — same FNV, same config-window logic, incl. the add_drop
// caveat). Under a schedule the config-window fallback does NOT apply: the
// schedule's activity_failure state is the ONLY failure source, evaluated
// at the decision time itself (schedActFailAt — exact boundary semantics).
// This is a DELIBERATE semantic difference from the vector path's add_drop
// caveat (activity_failure.go:27-29 keeps the config window for add_drop
// vectors) — ADR-100 decision 7 makes the schedule the sole source; do not
// "fix" this back into parity. The runtime's schedActFail field is
// refreshed to the decision so it stays truthful for any reader. Called
// from the lifecycle path (complete, history.go).
func (rt *runtime) ActivityFailsAt(nowMs int64) bool {
	if rt.faultSchedule != nil {
		rt.schedActFail = rt.schedActFailAt(nowMs)
		return rt.schedActFail
	}
	return faults.ActivityFailsAt(rt.faultSpec(), nowMs)
}

// ActivityFailsFor reports whether a SPECIFIC workflow (by ID) fails its
// activity at nowMs. Under a schedule the schedule's activity-failure state
// (evaluated per decision, as in ActivityFailsAt) is the only gate, and
// cascade semantics stay as today: bad actors (ID hash <
// CascadeBadFraction) fail while the state is active, healthy workflows
// never fail — the faults subpackage's FNV32a-of-ID split is reused with an
// always-active activity_failure marker (the schedule gate above already
// decided the window). With no schedule this delegates to the faults
// subpackage.
func (rt *runtime) ActivityFailsFor(id uint64, nowMs int64) bool {
	if rt.faultSchedule != nil {
		if !rt.ActivityFailsAt(nowMs) {
			return false
		}
		spec := rt.faultSpec()
		spec.Vector = &model.DecisionVector{Kind: model.KindActivityFailure} // always-active marker: the schedule gate decided the window
		return faults.ActivityFailsFor(spec, id, nowMs)
	}
	return faults.ActivityFailsFor(rt.faultSpec(), id, nowMs)
}
