package temporal

import (
	"github.com/lawRathod/queuesim/model"
	"github.com/lawRathod/queuesim/models/temporal/components/matcher"
	"github.com/lawRathod/queuesim/models/temporal/components/worker"
)

// Wave-two state (this file hosts the shared matchWaves registry).
//
// File-disjoint ownership (the second-wave plan) spread the wave-two
// regions across the three seam homes plus this file: matching_add.go owns
// the add boundary (S2 append threshold), matching_offer.go owns routing
// and the offer/forward region (S1 forwarder caps —
// isForwardingAllowed/offerOrTimeout/offerForwarded/onTransfer/onTimer),
// and matching_scale.go owns the maintain/scale/park region (S3 poller
// wait + decisions cap — maintainWorkers/pollForward/scaleElastic/
// scaleDownAct/scaleDownWf). matching.go no longer owns elastic scaling or
// forwarding; it retains the dispatch-expiry split
// (dispatchFrom/expireDispatch/dispatchSticky) and the wave-two mutable
// state the homes share: a per-runtime record keyed by *runtime in the
// matchWaves registry. runtime.go, config.go, queue.go, history.go, and
// shard.go belong to concurrent tracks. Access is lookup-only (never
// iterated), on the single simulation goroutine, with no wall-clock, no go
// statements, and no RNG draws — deterministic under a fixed seed. Entries
// are one small record per Topology (process-lifetime, like the sim
// itself); runtimes are never reused across runs, so no reset hook is
// needed.
const (
	// matchForwardRatePerSec sizes the child-to-root forward token bucket
	// (the dynamic forwarding rate limit — the real server throttles
	// per-partition forwarding). 0 = unlimited: forwarding is gated only by
	// the live window check + the backlog-age gate (the legacy static
	// condition, now evaluated per add). Off by default.
	matchForwardRatePerSec = 0
	// matchForwardBurst is the forward bucket capacity.
	matchForwardBurst = 1
	// matchDecisionRatePerSec rate-limits elastic scaling decisions via a
	// token bucket (the real server rate-limits
	// makePollerScalingDecisionImpl). 0 = unlimited: decide every tick
	// (legacy). Off by default.
	matchDecisionRatePerSec = 0
	// matchScaleDownWaitMs is the PollerScalingWaitTime analog: an empty
	// backlog persisting this long scales the active poller target back down
	// toward the configured floor. 0 = scale-down disabled (legacy: targets
	// only grow). Off by default.
	matchScaleDownWaitMs int64 = 0
	// matchRateOverloadRatio is the second scale-up trigger (root partition
	// only): arrival rate / dispatch rate above this scales the target up
	// even before the backlog-age threshold trips.
	matchRateOverloadRatio = 1.2
)

// matchFwdKey side-cars the forwarded+fromBacklog flag through the child
// drain: partition.go preserves ID/Attempt/BornMs on every hop (including the
// create-time-preserving re-spool round-trip), so the key survives the
// write→read path without touching the Task struct (owned by the matcher
// package) or the drain (owned by partition.go).
type matchFwdKey struct {
	id      uint64
	attempt int32
	born    int64
}

// matchWaveState is the per-runtime wave-two record. Both buckets are
// deterministic matcher.TokenBuckets (sim-time refill, zero RNG).
type matchWaveState struct {
	fwd    *matcher.TokenBucket // child-to-root forward rate limit
	decide *matcher.TokenBucket // elastic decision rate limit
	// forwarded marks tasks absorbed into the child that arrived from the
	// history DB backlog (Rescheduled re-adds): the MustOffer set. Cleared
	// when the MustOffer wait is armed, when poll-forwarding hands the task
	// to the root, or when a root Offer sync-matches; entries for executed
	// tasks otherwise linger process-lifetime (bounded: one small key per
	// backlog task).
	forwarded map[matchFwdKey]bool
	// Rate window for the root-only second trigger.
	rateAdds uint64 // ActivityAdds at the window start
	rateMs   int64  // window start (ms)
	rateInit bool
	// Scale-down emptiness tracking per class (ms since backlog drained).
	emptySinceAct int64
	emptyInitAct  bool
	emptySinceWf  int64
	emptyInitWf   bool
}

// matchWaves is the per-runtime wave-two registry (see above).
var matchWaves = map[*runtime]*matchWaveState{}

// matchWaveFor returns the wave-two record for rt, creating it on first use.
// Lookup-only (never iterated); single-goroutine; zero RNG.
func matchWaveFor(rt *runtime) *matchWaveState {
	if ws, ok := matchWaves[rt]; ok {
		return ws
	}
	ws := &matchWaveState{
		fwd:       matcher.NewTokenBucket(matchForwardRatePerSec, matchForwardBurst),
		decide:    matcher.NewTokenBucket(matchDecisionRatePerSec, 1),
		forwarded: map[matchFwdKey]bool{},
	}
	matchWaves[rt] = ws
	return ws
}

// matchFwdFlagOf keys a task for the forwarded set.
func matchFwdFlagOf(tk Task) matchFwdKey {
	return matchFwdKey{id: tk.ID, attempt: tk.Attempt, born: tk.BornMs}
}

// matchingService returns the matching Service: one MatchQueue per task
// class (sync-match first, then spool; token-bucket dispatch; three-point
// expiry) plus the poller fleets (long-poll parking, in-flight budget,
// elastic scaling). The real mechanism (the MatchQueues and worker pools)
// is constructed directly on the shared runtime's simulation below and
// driven by the runtime tick — matching declares NO ports and wires NO
// builder queues: a port shell would render in the DOT schematic without a
// single task traversing it (the honest-schematics principle: if the code
// doesn't traverse it, the diagram doesn't draw it).
func matchingService(rt *runtime) model.Service {
	return model.Service{
		Name:  "matching",
		Kind:  "temporal.matching",
		Label: "task queues + poller fleets (sync-match first, then spool)",
		Build: func(b *model.TopologyBuilder) error {
			// Matching queues: one per class. MaxWaitMs from the config
			// (activity S2S on the act queue, workflow-task timeout on the
			// wf queue).
			// C1 sync-match mechanism wiring (SOURCE-CONFIRMED 2026-09-07,
			// RESTRUCTURED 2026-09-07): the activity path now models TWO
			// distinct partitions with distinct drain capacity — a CHILD
			// (write/persistence) partition (rt.childQ, where history adds
			// land when the child cannot offload, and where the
			// isBacklogNegligible backlog-age gate lives) and a ROOT
			// (read/polled) partition (rt.actQ, which the SDK poller fleet
			// drains). Mechanism B (ForwardReliableMinMs) is the routing
			// driver in THIS model: when the sync-match window is below the
			// reliable floor, the child CANNOT forward to the root's pollers
			// (TrySyncMatch's expired context kills forwarding —
			// matcher.go:130-168), so adds are absorbed into the child and
			// accumulate, and the child's own local drain (the drain-bound
			// residual, partition.go) is the only way they leave. Wave-two
			// evaluates this per add in isForwardingAllowed (live window
			// check + backlog-age gate + forward-token acquire) instead of
			// the Build-cached bool; the field is still assigned below for
			// compatibility but is no longer a routing input.
			//
			// Mechanism A (the backlog-age gate) is implemented, unit-tested
			// (it trips on an old head, matcher.go:109-116 / constants.go:
			// 1277-1279), and MEASURED every tick (Topology.ChildGate →
			// partition.go). It is deliberately OBSERVATIONAL in this model,
			// not a routing input: at a healthy window the child is always
			// empty (so the gate is trivially clear, byte-neutral), and at a
			// dead-forwarding window the CHILD path is already selected by
			// Mechanism B regardless of the gate. The gate therefore does NOT
			// behaviorally drive the drop in this cell — the drop is carried
			// by Mechanism B + the child's structural drain. This is the
			// honest companion to doc.go's gate-latch finding: the gate is
			// sound but does not engage at the calibrated aps here.
			//
			// This restructure replaces the retired ForwardDeadRateFactor
			// proxy, which faked the drop on the collapsed single queue as a
			// window-conditional rate multiplier; the new structure requires
			// the child/root split to reproduce the degradation. The child
			// drain magnitude (ChildDrainFraction) remains a calibrated
			// scalar — structural, not the old proxy, but still calibrated.
			//
			// At the healthy default (window=200 >= ForwardReliableMinMs)
			// the child CAN forward, so every add is offered straight to the
			// root's pollers and the root behaves exactly like the collapsed
			// single queue — byte-neutral (bit-exact digest). The child
			// stays empty; the gate never trips; zero extra RNG/events.
			forwardReliable := rt.cfg.ForwardReliableMinMs == 0 || rt.cfg.SyncMatchWindowMs >= rt.cfg.ForwardReliableMinMs
			// Compat only: retained so external readers of the field keep
			// working; routing consults isForwardingAllowed (live per-add
			// evaluation), never this cached bool.
			rt.forwardReliableAct = forwardReliable
			rt.wfQ = matcher.NewMatchQueueWithPrefix(rt.sim, "", "matching-wf", rt.cfg.MatchingCapacity, rt.cfg.WorkflowTaskTimeoutMs, 1, rt.cfg.DispatchRate, float64(rt.cfg.DispatchBurst), rt.cfg.SyncMatchWindowMs)
			// ROOT activity partition: the SDK poller fleet parks and drains
			// here. Dispatch rate is ALWAYS the configured rate (no window
			// proxy attenuation) — the degradation comes from the child being
			// drain-bound, not from throttling the root.
			rt.actQ = matcher.NewMatchQueueWithPrefix(rt.sim, "", "matching-act", rt.cfg.MatchingCapacity, rt.cfg.ActivityS2STimeoutMs, 1, rt.cfg.DispatchRate, float64(rt.cfg.DispatchBurst), rt.cfg.SyncMatchWindowMs)
			// CHILD activity partition: history adds land here when
			// forwarding is dead (or the child gate has latched). It has NO
			// pollers and NO dispatch rate — tasks leave only via the child's
			// own drain (rt.drainChildToRoot, bounded by childDrainBucket).
			// capacity 0 = unlimited (the persistence-backed child absorbs
			// everything; rejection is the root's job), no expiry, forward-
			// irrelevant (its adds never sync).
			rt.childQ = matcher.NewMatchQueueWithPrefix(rt.sim, "", "matching-act-child", 0, 0, 1, 0, 0, rt.cfg.SyncMatchWindowMs)
			// The child's structural local drain: ChildDrainFraction ×
			// DispatchRate, the drain-bound residual (partition.go).
			rt.childDrainBucket = matcher.NewTokenBucket(childDrainRate(rt.cfg.DispatchRate, rt.cfg.ChildDrainFraction), float64(rt.cfg.DispatchBurst))
			rt.wfQ.SetBacklogGate(rt.cfg.BacklogGateMs)
			rt.actQ.SetBacklogGate(rt.cfg.BacklogGateMs)
			rt.childQ.SetBacklogGate(rt.cfg.BacklogGateMs)
			rt.childQ.SetForwardReliable(false)

			// Worker pools (long-poll parking, in-flight budget, service
			// time). The wf fleet executes via history shards (runWF), so
			// its service time is 0; the act fleet runs the activity's own
			// service time.
			rt.wfPool = worker.NewPool(rt.sim, rt.wfQ, 0, rt.cfg.PollerBudget)
			rt.actPool = worker.NewPool(rt.sim, rt.actQ, rt.cfg.ActivityMs, rt.cfg.PollerBudget)
			// When an execution completes, the worker immediately re-parks
			// (long polls again) — the server's worker loop. This is what
			// makes sync-match dominate under calm load.
			rt.wfPool.SetOnDone(func() { rt.repollWf() })
			rt.actPool.SetOnDone(func() { rt.repollAct() })
			return nil
		},
	}
}

// dispatchFrom hands backlog tasks to parked pollers (batched by the
// pump). A dispatch-time expiry routes by class (see expireDispatch): an
// activity S2S expiry is terminal (the workflow fails —
// service/history/workflow/retry.go:115-133 — only START_TO_CLOSE and
// HEARTBEAT timeouts are retryable), while a workflow-task expiry
// reschedules via history on the same attempt. A spool-time
// dropped task re-enters via history with the queues-framework reschedule
// backoff (common/util.go:67-69), on the SAME attempt — a transport drop
// never consumes the activity retry budget (the attempt only increments
// when the activity actually starts and fails). Spool accounting is
// single-sourced from the queue at finalize; only attempt/failure counters
// are live here.
func (rt *runtime) dispatchFrom(q *matcher.MatchQueue) {
	for {
		beforeExp := q.ExpiredCount()[matcher.ExpireAtPoll]
		beforeDrop := q.DroppedCount()
		tk, ok := q.TryDispatch(rt.sim.Now())
		if !ok {
			return
		}
		if q.ExpiredCount()[matcher.ExpireAtPoll] > beforeExp {
			rt.expireDispatch(tk)
			continue
		}
		if q.DroppedCount() > beforeDrop {
			// A spool-time drop is the SAME transport-drop semantic as the
			// add-boundary drop (the queue's TryDispatch spool-drop draw —
			// add_drop/wf_drop): the queues framework NACKs and resubmits
			// on its own timer without touching the activity attempt
			// counter (temporal/queues/executable.go Nack → rescheduler).
			// The task re-enters via the shard rescheduler on the SAME
			// attempt — no CanRetry gate and no Failed++: a drop NEVER
			// terminates a workflow (the retry budget is only consumed by
			// real activity failures in complete()). The reschedule
			// backoff stays flat (rescheduleBackoffMs keys off attempt,
			// which no longer grows on drops) — the model lumps the
			// server's separate escalating reschedule counter into the
			// flat policy; acceptable since the drop rate is bounded per
			// re-add.
			backoff := rescheduleBackoffMs(tk.Attempt)
			rt.shardFor(tk.ID).RescheduleAt(Task{ID: tk.ID, Attempt: tk.Attempt, Class: tk.Class, BornMs: rt.sim.Now().Int64(), Depth: tk.Depth, ParentID: tk.ParentID, Rescheduled: true}, rt.sim.Now().Int64()+backoff)
			// Capture: matching → history — a spool-time dropped task is
			// rescheduled into history (retryable).
			rt.rec("matching", "history", "drop_resched", rt.sim.Now().Int64(), tk)
			continue
		}
	}
}

// expireDispatch routes a dispatch-time expiry by class (the dispatch-expiry
// split): a TaskActivity S2S expiry is terminal (Failed++ — the retry policy
// does not retry SCHEDULE_TO_START, service/history/workflow/retry.go:
// 115-133); a TaskWfStart/TaskWfComplete expiry reschedules via the history
// transfer queue on the SAME attempt (the workflow-task timeout re-add, not
// a workflow failure — it re-enters matching-wf through onTransfer). Task
// identity (BornMs/Depth/ParentID) is preserved. Distinct capture kinds per
// outcome (act_expire vs wf_expire_resched).
func (rt *runtime) expireDispatch(tk Task) {
	now := rt.sim.Now().Int64()
	if tk.Class == TaskActivity {
		rt.stats.Failed++
		// Capture: matching → terminal — the activity expired at dispatch
		// (S2S deadline); the workflow fails.
		rt.rec("matching", "terminal", "act_expire", now, tk)
		return
	}
	rt.shardFor(tk.ID).EnqueueTransfer(Task{ID: tk.ID, Attempt: tk.Attempt, Class: tk.Class, BornMs: tk.BornMs, Depth: tk.Depth, ParentID: tk.ParentID})
	// Capture: matching → history — an expired workflow task reschedules
	// via the transfer queue (same attempt, retryable).
	rt.rec("matching", "history", "wf_expire_resched", now, tk)
}

// dispatchSticky drains the per-worker sticky buffers to parked wf pollers.
// A worker's poll checks its sticky queue BEFORE the normal queue (the real
// sticky-task-queue loop); the model's poller fleet is anonymous, so any
// parked poller may take a sticky task. MarkActive refreshes the worker's
// cache residency. Tasks with no parked poller wait for the next tick.
func (rt *runtime) dispatchSticky() {
	if !rt.cfg.StickyEnabled {
		return
	}
	nowMs := rt.sim.Now().Int64()
	for w := uint64(0); w < uint64(rt.cfg.PollersWF); w++ {
		for {
			tk, ok := rt.sticky.Peek(w)
			if !ok {
				break
			}
			if !rt.wfQ.DispatchTask(tk) {
				return // no parked poller; the sticky task waits
			}
			// Capture: matching → exec — a sticky task was handed straight
			// to a poller (bypasses the class backlog).
			rt.rec("matching", "exec", "sticky_dispatch", nowMs, tk)
			rt.sticky.Poll(w)
			rt.sticky.MarkActive(w, nowMs)
		}
	}
}
