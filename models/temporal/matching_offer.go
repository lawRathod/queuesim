package temporal

import (
	"github.com/lawRathod/queuesim/godes/core"
)

// matching_offer.go is the S1 seam home (forwarder caps): the offer/forward
// region isForwardingAllowed/offerOrTimeout/offerForwarded/onTransfer/onTimer.
// isForwardingAllowed reports per add whether an activity add may be offered
// to the ROOT partition: the live window check (SyncMatchWindowMs vs
// ForwardReliableMinMs, read from the live config — the retired static
// forwardReliableAct Build bool is never consulted) + the child backlog-age
// gate + the S1 forwarder-cap gate + one forward-token acquire. A refused
// forward denies the ROOT path and the add is absorbed into the child (the
// child partition is the queue — backpressure, never a drop). The rate
// bucket is unlimited by default (matchForwardRatePerSec = 0), so at zero
// seam config this verdict equals the legacy static condition evaluated
// live — byte-neutral — while the seam rate-limits forwarding once sized.
//
// # S1 — forwarder caps (Phase 1, ADR-106 / plan 2026-09-07; WP1)
//
// The real server's partition forwarder keeps TWO token pools — inflight
// forwarded addTask/queryTask calls and inflight forwarded polls
// (service/matching/forwarder.go:36-42,84-91: addReqToken/pollReqToken
// channels each buffered to the dynamic-config maximum;
// matching.forwarderMaxOutstandingTasks = 1 and
// matching.forwarderMaxOutstandingPolls = 1,
// temporal/common/dynamicconfig/constants.go:1220-1229). A caller without a
// token does NOT forward — the item stays queued where it is (backpressure,
// nothing is dropped). The task pool gates the child→root ForwardTask call
// (matcher.go:146-157); the poll pool gates ForwardPoll (matcher.go:
// 488-498).
//
// This model has NO per-partition structure (the honest single-seam
// approximation the plan pre-registers): ONE collapsed child→root forward
// seam stands in for the whole partition tree, so the cap applies at the one
// forward seam, not per forwarder. At that seam the ONLY forwarded item that
// stays in flight across sim events is a forwarded DB-backlog add held in
// the ROOT's poller rendezvous — the offerOrTimeout wait below, which
// reproduces the real root's block-trying-to-match-a-poller
// (matcher.go:159-165,172-187; a forwarded task the root cannot spool is
// returned to the child, task_queue_partition_manager.go:376-378, so the
// child's forwarder holds its single task token for the whole rendezvous).
// Every other forward (a sync-match or an immediate spool/reject) resolves
// within the event that issues it — like a real ForwardTask RPC that the
// parent answers instantly — and therefore never occupies the pool. The S1
// TASK pool (Config.ForwarderMaxOutstandingTasks) therefore counts exactly
// the armed offerOrTimeout waits: while the pool is exhausted (the ROOT is
// holding `cap` forwarded adds in its poller rendezvous) isForwardingAllowed
// refuses new forwards and the adds are absorbed into the child queue —
// backpressure, exactly like a child whose single forward token is held by a
// blocked ForwardTask RPC. 0 = off = unbounded (today's behavior,
// byte-neutral).
//
// The S1 POLL pool (Config.ForwarderMaxOutstandingPolls) has NO binding site
// at this seam: the model's SDK poller fleet parks directly on the ROOT
// partition, so the model never emits a child→root ForwardPoll (the real
// poll-forward structure — a partition borrowing pollers to drain another
// partition's backlog — is the model's pollForward, matching_scale.go:157,
// OFF by default and uncalibrated). A nonzero polls cap is therefore
// byte-neutral at the offer seam; it is read, validated (WP0), and pinned to
// its server default below, and its live binding (pollForward) is reported
// for Manager arbitration — WP1's file ownership is the offer seam only.

// s1State is the per-runtime S1 forwarder-cap record: the in-flight count
// of forwarded adds the forward seam has sent to the ROOT whose disposition
// is still pending across sim events (armed offerOrTimeout waits). One small
// record per Topology, process-lifetime (runtimes are never reused across
// runs — no reset hook needed), accessed only from the single simulation
// goroutine: registry lookup, never iterated, zero RNG — deterministic
// (same pattern as the matchWaves registry in matching.go).
type s1State struct {
	// outTasks is the number of forwarded adds currently held in the ROOT's
	// poller rendezvous (offerOrTimeout waits armed, not yet resolved).
	// Bounded by the sim horizon: every armed wait resolves at or before
	// now + SyncMatchWindowMs, or the run ends.
	outTasks int
}

// s1Waves is the per-runtime S1 forwarder-cap registry (see s1State).
var s1Waves = map[*runtime]*s1State{}

// s1For returns the S1 forwarder-cap record for rt, creating it on first
// use. Lookup-only (never iterated); single-goroutine; zero RNG.
func s1For(rt *runtime) *s1State {
	if st, ok := s1Waves[rt]; ok {
		return st
	}
	st := &s1State{}
	s1Waves[rt] = st
	return st
}

func (rt *runtime) isForwardingAllowed(now core.SimTime) bool {
	if rt.cfg.ForwardReliableMinMs != 0 && rt.cfg.SyncMatchWindowMs < rt.cfg.ForwardReliableMinMs {
		return false
	}
	if rt.childQ != nil && rt.childQ.GateTripped(now) {
		return false
	}
	// S1 task cap (WP1, ADR-106): while the forwarder's in-flight forwarded-
	// task pool is exhausted — the ROOT holds `cap` forwarded adds in its
	// poller rendezvous (offerOrTimeout waits) and the real forwarder's
	// single addReqToken is therefore held by a blocked ForwardTask RPC —
	// refuse the forward and let the caller absorb the add into the child
	// queue (backpressure, never dropped). 0 = off = unbounded (today).
	if cap := rt.cfg.ForwarderMaxOutstandingTasks; cap > 0 && s1For(rt).outTasks >= cap {
		return false
	}
	return matchWaveFor(rt).fwd.Consume(now.Int64())
}

// offerOrTimeout is the MustOffer exception for forwarded DB-backlog tasks
// (temporal/service/matching/matcher.go MustOffer semantics): a forwarded
// task from the history backlog (flagged at child-absorb, flag carried
// through the child drain by task identity) that meets a poller-less root
// does NOT spool immediately — it blocks to the sync-match deadline via a
// sim ScheduleAt wait, then re-offers with its create-time preserved (so
// residency age keeps counting across the wait, like the queue's own Offer
// wait). The flag is cleared when the wait is armed (single shot). No
// wall-clock, no go statements; one sim event at most. With no positive
// window the wait has no deadline to run out and the task takes the
// immediate Offer instead.
//
// S1 (WP1): arming the wait acquires one slot of the forwarder's in-flight
// task pool — the forwarded add's disposition is now pending across events
// (the real root holds the child's ForwardTask RPC in offerOrTimeout,
// matcher.go:172-187, so the child's single addReqToken is held for the
// whole rendezvous). The slot is released when the wait resolves in
// offerForwarded. At cap 0 the slot bookkeeping is skipped entirely
// (byte-neutral).
//
// Drain-side note: the child drain (partition.go offerChildToRoot, owned by
// another track) still spools drained tasks immediately without consulting
// the flag; threading offerOrTimeout through that one call is the owning
// worker's one-line follow-up — the flag already survives the drain, so no
// state change is needed there, only the call swap. Until that lands, the
// MustOffer wait never arms in a full run (verified: zero act_mustoffer_wait
// events across healthy/dead-window/small-capacity storm cells) and the S1
// task cap is exercised at the seam level only — the same arbitration
// boundary as the poll pool above.
func (rt *runtime) offerOrTimeout(tk Task, now core.SimTime) {
	delete(matchWaveFor(rt).forwarded, matchFwdFlagOf(tk))
	if rt.cfg.SyncMatchWindowMs <= 0 {
		rt.offerForwarded(tk, now, false)
		return
	}
	if rt.cfg.ForwarderMaxOutstandingTasks > 0 {
		s1For(rt).outTasks++ // the forward is now in flight at the ROOT
	}
	deadline := now.Add(rt.cfg.SyncMatchWindowMs)
	rt.rec("history", "matching", "act_mustoffer_wait", now.Int64(), tk)
	rt.sim.ScheduleAt(deadline, func(*core.Simulation) {
		rt.offerForwarded(tk, rt.sim.Now(), true)
	})
}

// offerForwarded re-offers a MustOffer-waited task to the root at its
// deadline: sync-match first, then spool; a reject (dead today —
// persistence-backed queues never reject) releases the forward token and
// re-enters via the history transfer queue. held reports whether this call
// resolves an armed offerOrTimeout wait (the wait that acquired the S1
// in-flight slot); held=false is the no-window immediate path, which never
// occupied the pool. The slot is released on entry — the ROOT has resolved
// the forwarded add (accepted or bounced), so the forwarder's token is free
// again before the re-offer itself runs.
func (rt *runtime) offerForwarded(tk Task, now core.SimTime, held bool) {
	if held && rt.cfg.ForwarderMaxOutstandingTasks > 0 {
		// Release the in-flight slot. The > 0 floor guards the count (a
		// mispaired release can never drive it negative); pairing is exact —
		// every armed offerOrTimeout schedules exactly one held resolution.
		if s1For(rt).outTasks > 0 {
			s1For(rt).outTasks--
		}
	}
	sm, acc := rt.actQ.Add(tk, now)
	if sm {
		rt.rec("history", "matching", "act_add", now.Int64(), tk)
		return
	}
	if !acc {
		matchWaveFor(rt).fwd.Recycle()
		rt.rec("history", "matching", "act_reject", now.Int64(), tk)
		rt.shardFor(tk.ID).EnqueueTransfer(tk)
		return
	}
	rt.rec("history", "matching", "act_spool", now.Int64(), tk)
}

// onTransfer processes a transfer-queue task (rejected adds re-enter via
// the matching boundary; timer tasks re-enter their queue). Re-entered
// tasks pass through the add boundary again, so they re-attempt the forward
// under the S1 cap like any fresh add (addAct/addActSpoolOnly consult
// isForwardingAllowed): a bounced forwarded add whose slot was released
// re-forwards when the pool has room and queues in the child when it does
// not — the real child taskReader's one-forward-at-a-time backlog relief.
func (rt *runtime) onTransfer(tk Task, now core.SimTime) {
	switch tk.Class {
	case TaskWfStart, TaskWfComplete:
		// Capture: history → matching — a transfer-queue task re-enters
		// matching-wf (matching reject/drop → history transfer → re-add).
		rt.rec("history", "matching", "transfer_wf", now.Int64(), tk)
		rt.addWf(tk, now.Add(rt.cfg.NetworkMs))
	case TaskActivity:
		if tk.Rescheduled {
			rt.rec("history", "matching", "transfer_act", now.Int64(), tk)
			rt.addActSpoolOnly(tk, now.Add(rt.cfg.NetworkMs))
		} else {
			rt.rec("history", "matching", "transfer_act", now.Int64(), tk)
			rt.addAct(tk, now.Add(rt.cfg.NetworkMs))
		}
	default:
		rt.rec("history", "matching", "transfer_act", now.Int64(), tk)
		rt.addAct(tk, now.Add(rt.cfg.NetworkMs))
	}
}

// onTimer processes a timer event (ActivityRetryTimerTask): the activity
// retry re-enters matching-act via the SPOOL path (no sync-match), matching
// the real server's backoff-rescheduled tasks.
func (rt *runtime) onTimer(tk Task, now core.SimTime) {
	if tk.Class == TaskWfStart || tk.Class == TaskWfComplete {
		// Capture: history → matching — a timer task re-enters matching-wf.
		rt.rec("history", "matching", "timer_wf", now.Int64(), tk)
		rt.addWf(tk, now.Add(rt.cfg.NetworkMs))
		return
	}
	// Capture: history → matching — an activity retry timer re-enters
	// matching-act on the spool path.
	rt.rec("history", "matching", "timer_act", now.Int64(), tk)
	rt.addActSpoolOnly(tk, now.Add(rt.cfg.NetworkMs))
}
