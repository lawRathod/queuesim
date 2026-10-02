package temporal

import (
	"github.com/lawRathod/queuesim/models/temporal/components/matcher"
	"github.com/lawRathod/queuesim/models/temporal/components/worker"
)

// matching_scale.go is the S3 seam home (poller wait + decisions cap): the
// maintain/scale/park region maintainWorkers/pollForward/scaleElastic/
// actRateOverload/scaleDownAct/scaleDownWf/parkUpTo/parkWithJitter/parkOne/repoll*.
// maintainWorkers parks the configured poller count on each queue (long
// poll), where a parked poller is a worker NOT currently executing. The
// fleet is fixed at the configured count: parked + in-flight = target. A
// poller_min vector forces the count down during its window. Elastic
// matching scales the target up when backlog persists (see scaleElastic),
// and poll-forwarding lends waiting root pollers to the child backlog once
// per tick (see pollForward).

// ---------------------------------------------------------------------------
// S3 seam — server-side poller-scaling cap (WP3, ADR-106 / plan
// 2026-09-07-four-grounded-performance-seams-phase1.md). The two zero-default
// config fields cfg.PollerScaleWaitMs (wait gate) and cfg.PollerDecisionsPerSec
// (decisions/s token bucket) cap the elastic decisions scaleElastic issues;
// both at zero = off = today's exact behavior.
//
// Server mechanics this models (temporal submodule @ ffddf6fdc): the physical
// task queue manager issues at most one scaling decision per poll and bounds
// decision cadence — makePollerScalingDecisionImpl
// (service/matching/physical_task_queue_manager.go:851-898) suggests -1 when a
// poller has been idle >= matching.pollerScalingWaitTime (default 1s,
// common/dynamicconfig/constants.go:1360-1363), and otherwise every suggestion
// passes a token limiter sized matching.pollerScalingDecisionsPerSecond
// (default 10, constants.go:1366-1371; the server multiplies by 1e6 and
// divides each permit by the recent poller count so the per-poller budget
// holds — the model applies the per-queue rate at its single root seam). The
// model exposes the two knobs as cfg.PollerScaleWaitMs / cfg.PollerDecisionsPerSec:
//
//   - wait gate: at most ONE scale decision per PollerScaleWaitMs — the
//     interval BETWEEN decisions (the first decision after a quiet stretch
//     still fires at the 200ms backlog age: there is no prior decision to
//     space). One gate per runtime, shared across the act/wf classes,
//     mirroring the model's single-per-tick elastic evaluation.
//   - decisions cap: a burst-1 sim-time token bucket at PollerDecisionsPerSec
//     decisions/s — at most DecisionsPerSec actual decisions per second (a
//     non-binding ceiling above the natural 200ms cadence of 5/s). The
//     wave-two `decide` bucket (matching.go, matchDecisionRatePerSec) is a
//     separate per-tick ATTEMPT limiter, constant-rate and unlimited by
//     default; it is left untouched — the seam gates decisions, not attempts.
//
// Both gates are enforced AT THE DECISION SITES: only an actual ++/-- on a
// poller target consumes a token or re-arms the wait clock, so no-op
// evaluations never spend the budget. Scale-down decrements
// (scaleDownAct/scaleDownWf) count as decisions too — the gate also spaces
// down-decisions and prevents up/down oscillation inside one wait window.
//
// Wait-knob remap (explicit divergence): the real WaitTime triggers
// scale-down on idle ≥1s; the model reuses the knob to space UP decisions
// ≥1s apart (approximation; relief cadence at server defaults ~1/s vs the
// real up-decision bound 10/s×pollers,
// physical_task_queue_manager.go:869).
//
// SDK divergence (recorded, never silently fixed — ADR-102): the real server
// issues the suggestion only on a MATCHED task and the SDK executes it only
// when its ingestion-growth gate passes (sdk/internal_worker_base.go:733-783,
// 100ms report interval). Neither gate is modeled — the model may relieve
// faster than the real server; that divergence is a first-class C7 finding,
// not a defect. Median-only envelope comparisons later; no bit-identity claim
// on scaling decisions (ADR-102).
//
// Determinism: zero new RNG draws at any setting, no wall clock, no
// goroutines — the bucket refills lazily on sim time (matcher.TokenBucket)
// and the registry below is lookup-only on the single simulation goroutine
// (the same process-lifetime lifecycle as the wave-two matchWaves registry in
// matching.go; runtimes are never reused across runs, so no reset hook is
// needed). Neutrality: with both fields at zero every guard short-circuits
// BEFORE the registry is touched — no state is created and the scaleElastic
// decision path is byte-identical to the pre-seam behavior (proven by
// TestS3ZeroOff and the pinned digests in seams_s3_test.go / seams_test.go).
type s3GateState struct {
	decide *matcher.TokenBucket // PollerDecisionsPerSec bucket; created lazily on the first decision attempt
	lastMs int64                // sim ms of the last issued scale decision; -1 = none yet
}

// s3Gates is the per-runtime S3 gate registry (see above). Created only when
// a runtime first attempts a decision with a seam knob nonzero; never
// iterated; entries are process-lifetime like the sim itself.
var s3Gates = map[*runtime]*s3GateState{}

// s3GateFor returns the S3 gate record for rt, creating it on first use.
func s3GateFor(rt *runtime) *s3GateState {
	if g, ok := s3Gates[rt]; ok {
		return g
	}
	g := &s3GateState{lastMs: -1}
	s3Gates[rt] = g
	return g
}

// scaleDecisionAllowed reports whether an elastic scale decision may be
// issued at nowMs under the S3 seam: the wait gate is open (no decision
// within the last PollerScaleWaitMs, or none yet) and the decisions/s token
// bucket has a token. Both knobs at zero = always allowed — the byte-neutral
// off path (returns before the registry is touched, so no state is ever
// created). Callers invoke it only immediately before an unconditional
// poller-target change, so a grant is always spent on a real decision.
func (rt *runtime) scaleDecisionAllowed(nowMs int64) bool {
	wait := rt.cfg.PollerScaleWaitMs
	rate := rt.cfg.PollerDecisionsPerSec
	if wait <= 0 && rate <= 0 {
		return true // seam off — today's behavior, zero state, zero draws
	}
	g := s3GateFor(rt)
	if wait > 0 && g.lastMs >= 0 && nowMs-g.lastMs < wait {
		return false // wait gate: decisions at most every PollerScaleWaitMs
	}
	if rate > 0 {
		if g.decide == nil {
			g.decide = matcher.NewTokenBucket(rate, 1) // burst 1: one decision per 1000/rate ms
		}
		return g.decide.Consume(nowMs)
	}
	return true
}

// scaleDecisionAt records that a scale decision was issued at nowMs —
// re-arming the wait gate (the decision clock). No-op while the seam is off
// (both knobs zero), so the off path never touches the registry.
func (rt *runtime) scaleDecisionAt(nowMs int64) {
	if rt.cfg.PollerScaleWaitMs <= 0 && rt.cfg.PollerDecisionsPerSec <= 0 {
		return
	}
	s3GateFor(rt).lastMs = nowMs
}

func (rt *runtime) maintainWorkers(nowMs int64) {
	rt.scaleElastic(nowMs)
	wf := rt.pollerTargetWf
	act := rt.pollerTargetAct
	if rt.pollerForced >= 0 {
		wf = rt.pollerForced
		act = rt.pollerForced
	}
	rt.parkUpTo(rt.wfPool, rt.wfQ, wf, nowMs)
	rt.parkUpTo(rt.actPool, rt.actQ, act, nowMs)
	if rt.cfg.PollForwardEnabled {
		rt.pollForward()
	}
}

// pollForward moves one parked-poller token child→root per tick, one
// direction only (the real server's poll forwarding: a child-partition
// backlog borrows root pollers). Preconditions: the child holds backlog and
// the root holds a parked poller. The handoff pops one child head and spools
// it to the root ahead of the same tick's dispatchFrom(actQ) (tick order:
// maintainWorkers runs before dispatch), so a waiting root poller receives
// it within the same tick — same-tick handoff, no goroutines, one task per
// tick (maintainWorkers runs once per pump tick). The task keeps its
// identity (BornMs preserved), so a forwarded+fromBacklog flag set at
// child-absorb survives into the root; the flag is cleared here (the task
// reached the root — the MustOffer wait has nothing left to protect).
// No-op when the child is empty (healthy windows) — zero RNG, zero events,
// byte-neutral.
func (rt *runtime) pollForward() {
	if rt.childQ == nil || rt.actQ == nil {
		return
	}
	if rt.childQ.Len() == 0 || rt.actQ.Parked() == 0 {
		return
	}
	tk, ok := rt.childQ.PopHead()
	if !ok {
		return
	}
	delete(matchWaveFor(rt).forwarded, matchFwdFlagOf(tk))
	now := rt.sim.Now()
	if !rt.actQ.SpoolOnly(tk) {
		rt.rec("matching", "matching", "act_root_reject", now.Int64(), tk)
		rt.shardFor(tk.ID).EnqueueTransfer(tk)
		return
	}
	rt.rec("matching", "matching", "act_poll_forward", now.Int64(), tk)
}

// scaleElastic grows the active poller target when the matching backlog
// persists past PollerScaleUpBacklogMs, up to PollerScaleMax — the real
// server's makePollerScalingDecisionImpl (backlog age >
// PollerScalingBacklogAgeScaleUp → +1 poller;
// service/matching/physical_task_queue_manager.go:876-884). This is what
// lets the model absorb sustained arrival like real Temporal's elastic
// matching. Wave-two completeness: scaling decisions pass the decide token
// bucket (rate-limit; unlimited by default), an empty backlog persisting
// past matchScaleDownWaitMs scales the target back down toward the
// configured floor (scale-down; disabled by default), and the act (root)
// class gains a second scale-up trigger when the arrival rate exceeds 1.2×
// the dispatch rate (root-only). The 200ms threshold branches are kept
// verbatim; with both knobs at their off defaults the only live addition is
// the rate trigger, which is false whenever the dispatch rate is unlimited
// or no activity adds have flowed. S3 seam (WP3): each actual ++/-- decision
// additionally passes the cfg-driven wait gate + decisions/s cap (see the
// S3 section above); at the seam's zero defaults nothing changes.
func (rt *runtime) scaleElastic(nowMs int64) {
	if rt.cfg.PollerScaleMax <= rt.cfg.PollersAct && rt.cfg.PollerScaleMax <= rt.cfg.PollersWF {
		return // scaling disabled (max <= configured fleet)
	}
	if !matchWaveFor(rt).decide.Consume(nowMs) {
		return // decision rate-limit (token bucket); off by default
	}
	// Act class (the polled ROOT partition).
	if rt.cfg.PollerScaleMax > rt.cfg.PollersAct {
		if rt.actQ.Len() > 0 {
			matchWaveFor(rt).emptyInitAct = false
			if rt.backlogSinceAct < 0 {
				rt.backlogSinceAct = nowMs
			} else if nowMs-rt.backlogSinceAct >= rt.cfg.PollerScaleUpBacklogMs && rt.pollerTargetAct < rt.cfg.PollerScaleMax && rt.scaleDecisionAllowed(nowMs) {
				// S3 gate (WP3): the decision passes the wait gate + decisions cap.
				rt.pollerTargetAct++
				rt.backlogSinceAct = nowMs // re-arm for the next increment
				rt.scaleDecisionAt(nowMs)
			} else if rt.pollerTargetAct < rt.cfg.PollerScaleMax && rt.actRateOverload(nowMs) && rt.scaleDecisionAllowed(nowMs) {
				// Second trigger (root-only): arrival/dispatch > 1.2.
				rt.pollerTargetAct++
				rt.backlogSinceAct = nowMs // re-arm for the next increment
				rt.scaleDecisionAt(nowMs)
			}
		} else {
			rt.backlogSinceAct = -1
			rt.scaleDownAct(nowMs)
		}
	}
	// Wf class (threshold branch kept verbatim; no rate trigger: root-only).
	if rt.cfg.PollerScaleMax > rt.cfg.PollersWF {
		if rt.wfQ.Len() > 0 {
			matchWaveFor(rt).emptyInitWf = false
			if rt.backlogSinceWf < 0 {
				rt.backlogSinceWf = nowMs
			} else if nowMs-rt.backlogSinceWf >= rt.cfg.PollerScaleUpBacklogMs && rt.pollerTargetWf < rt.cfg.PollerScaleMax && rt.scaleDecisionAllowed(nowMs) {
				// S3 gate (WP3): the decision passes the wait gate + decisions cap.
				rt.pollerTargetWf++
				rt.backlogSinceWf = nowMs // re-arm for the next increment
				rt.scaleDecisionAt(nowMs)
			}
		} else {
			rt.backlogSinceWf = -1
			rt.scaleDownWf(nowMs)
		}
	}
}

// actRateOverload is the root-only second scale-up trigger: the windowed
// activity arrival rate exceeds matchRateOverloadRatio (1.2×) the configured
// dispatch rate. The window opens on the first evaluation (baseline sample)
// and rates are cumulative averages since — deterministic, zero RNG. False
// whenever the dispatch rate is unlimited (no rate pressure to outrun) or
// the window has no span yet.
func (rt *runtime) actRateOverload(nowMs int64) bool {
	rate := rt.cfg.DispatchRate
	if rate <= 0 {
		return false
	}
	ws := matchWaveFor(rt)
	if !ws.rateInit {
		ws.rateAdds = rt.stats.ActivityAdds
		ws.rateMs = nowMs
		ws.rateInit = true
		return false
	}
	dt := nowMs - ws.rateMs
	if dt <= 0 {
		return false
	}
	addRate := float64(rt.stats.ActivityAdds-ws.rateAdds) / float64(dt) * 1000.0
	return addRate/rate > matchRateOverloadRatio
}

// scaleDownAct scales the act target back toward the configured floor after
// an empty backlog persists past matchScaleDownWaitMs (the
// PollerScalingWaitTime analog). Disabled when matchScaleDownWaitMs <= 0
// (legacy: targets never shrink) — the off-by-default knob.
func (rt *runtime) scaleDownAct(nowMs int64) {
	if matchScaleDownWaitMs <= 0 {
		return
	}
	ws := matchWaveFor(rt)
	if !ws.emptyInitAct {
		ws.emptySinceAct = nowMs
		ws.emptyInitAct = true
		return
	}
	if nowMs-ws.emptySinceAct >= matchScaleDownWaitMs && rt.pollerTargetAct > rt.cfg.PollersAct && rt.scaleDecisionAllowed(nowMs) {
		// S3 gate (WP3): a scale-down is a decision — wait gate + decisions cap apply.
		rt.pollerTargetAct--
		ws.emptySinceAct = nowMs // re-arm for the next decrement
		rt.scaleDecisionAt(nowMs)
	}
}

// scaleDownWf scales the wf target back toward the configured floor (same
// semantics as scaleDownAct; likewise disabled by default).
func (rt *runtime) scaleDownWf(nowMs int64) {
	if matchScaleDownWaitMs <= 0 {
		return
	}
	ws := matchWaveFor(rt)
	if !ws.emptyInitWf {
		ws.emptySinceWf = nowMs
		ws.emptyInitWf = true
		return
	}
	if nowMs-ws.emptySinceWf >= matchScaleDownWaitMs && rt.pollerTargetWf > rt.cfg.PollersWF && rt.scaleDecisionAllowed(nowMs) {
		// S3 gate (WP3): a scale-down is a decision — wait gate + decisions cap apply.
		rt.pollerTargetWf--
		ws.emptySinceWf = nowMs // re-arm for the next decrement
		rt.scaleDecisionAt(nowMs)
	}
}

// parkUpTo parks exactly the right number of workers so that parked +
// in-flight = n (the fixed fleet size). It parks no more than n - inFlight
// fresh pollers; busy workers are NOT parked. Poll jitter shortens the
// long-poll with probability pollJitterP (worker reconnect churn).
func (rt *runtime) parkUpTo(pool *worker.Pool, q *matcher.MatchQueue, n int, nowMs int64) {
	idle := n - pool.InFlight()
	if idle < 0 {
		idle = 0
	}
	for q.Parked() < idle {
		rt.parkWithJitter(pool, q, nowMs)
	}
}

// parkWithJitter parks one poller, honoring the poll_jitter early-expiry
// probability (0 = off, zero draws).
func (rt *runtime) parkWithJitter(pool *worker.Pool, q *matcher.MatchQueue, nowMs int64) {
	expiry := nowMs + rt.cfg.LongPollMs
	if rt.pollJitterP > 0 {
		if rt.jitterRNG == nil {
			rt.jitterRNG = substream(rt.sim, "poll-jitter")
		}
		if rt.jitterRNG.Float64() < rt.pollJitterP {
			expiry = nowMs + rt.cfg.LongPollMs/10 // early expiry (reconnect)
		}
	}
	q.Park(expiry, func(tk Task) { rt.onTake(tk) })
}

// parkOne parks a single poller (the completion re-poll hook).
func (rt *runtime) parkOne(pool *worker.Pool, q *matcher.MatchQueue, nowMs int64) {
	rt.parkWithJitter(pool, q, nowMs)
}

// repollWf re-parks a wf worker on completion. The completing worker just
// became idle, so it must re-park (unless the fleet ceiling or force count
// prevents it).
func (rt *runtime) repollWf() {
	now := rt.sim.Now().Int64()
	target := rt.cfg.PollersWF
	if rt.pollerForced >= 0 {
		target = rt.pollerForced
	}
	// Re-park when under the in-flight budget (the SDK poll loop allows a
	// poller to run MANY concurrent executions up to its budget) AND under
	// the fleet ceiling.
	budgetOK := rt.wfPool.InFlight() < rt.wfPool.Budget() || rt.wfPool.Budget() == 0
	if budgetOK && rt.wfQ.Parked() < target {
		rt.parkOne(rt.wfPool, rt.wfQ, now)
	}
}

// repollAct re-parks an act worker on completion / after a take (same
// budget-aware semantics as repollWf).
func (rt *runtime) repollAct() {
	now := rt.sim.Now().Int64()
	target := rt.cfg.PollersAct
	if rt.pollerForced >= 0 {
		target = rt.pollerForced
	}
	budgetOK := rt.actPool.InFlight() < rt.actPool.Budget() || rt.actPool.Budget() == 0
	if budgetOK && rt.actQ.Parked() < target {
		rt.parkOne(rt.actPool, rt.actQ, now)
	}
}
