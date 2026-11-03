package temporal

import (
	"math"

	"github.com/lawRathod/queuesim/godes/core"
	"github.com/lawRathod/queuesim/models/temporal/components/matcher"
)

// matching_add.go is the S2 seam home (append threshold): the add-boundary
// region addWf/addAct/addActSpoolOnly/addActWithMode.
// addWf attempts an add at the matching-wf boundary (AddWorkflowTask RPC):
// sync-match first, then spool; capacity reject re-enters via the history
// transfer queue. The spool/reject/drop counters are single-sourced from
// the queue at finalize (finalizeStats) — only attempt counters are live.
//
// S2 (WP2, ADR-106:2; model leg PARTIAL per ADR-103:2 — see the plan's S2
// section): when cfg.AppendThreshold > 0, an add whose destination queue's
// append occupancy is at/over the cap is shed with ResourceExhausted
// semantics — the server's full task-writer channel (task_writer.go:103-121)
// — and re-enters history on the resource-exhausted ESCALATION reschedule
// (util.go:82-84; executable.go:776-805), never the transfer queue. The
// shed/re-add helpers and per-task escalation state live at the bottom of
// this file. 0 = off = today's exact behavior (zero RNG, byte-neutral).
func (rt *runtime) addWf(tk Task, atMs core.SimTime) {
	rt.stats.AttemptedAdds++
	if rt.s2AppendFull(rt.wfQ) {
		rt.s2Reject(tk, atMs)
		return
	}
	sm, acc := rt.wfQ.Add(tk, atMs)
	rt.s2REClear(tk)
	if sm {
		// Capture: history → matching — the wf task sync-matched a parked
		// poller (the server's unbuffered rendezvous handoff).
		rt.rec("history", "matching", "wf_add", atMs.Int64(), tk)
		return
	}
	if !acc {
		// Capture: history → matching — capacity reject; re-enters via the
		// history transfer queue.
		rt.rec("history", "matching", "wf_reject", atMs.Int64(), tk)
		rt.shardFor(tk.ID).EnqueueTransfer(tk)
		return
	}
	// Spooled to the wf backlog (no parked poller in the sync window).
	rt.rec("history", "matching", "wf_spool", atMs.Int64(), tk)
}

// addAct attempts an add at the matching-act boundary (AddActivityTask RPC).
func (rt *runtime) addAct(tk Task, atMs core.SimTime) {
	rt.addActWithMode(tk, atMs, false)
}

// addActSpoolOnly adds an activity task WITHOUT sync-match: it always
// spools to the backlog (the rescheduled/retry-timer re-add path).
func (rt *runtime) addActSpoolOnly(tk Task, atMs core.SimTime) {
	rt.addActWithMode(tk, atMs, true)
}

func (rt *runtime) addActWithMode(tk Task, atMs core.SimTime, spoolOnly bool) {
	rt.stats.AttemptedAdds++
	rt.stats.ActivityAdds++
	if rt.actQ.ShouldDropOnAdd() {
		// A drop is the AddTask TRANSPORT drop (the bubble's clean
		// Unavailable to history): the queues framework NACKs and
		// resubmits on its OWN queue-level timer — the activity attempt
		// counter is NOT touched (temporal/queues/executable.go Nack →
		// rescheduler). The task re-enters via the shard rescheduler with
		// the queues-framework reschedule backoff, on the SAME attempt: a
		// task dropped 100 times still runs as attempt 1 when it finally
		// gets through, and a drop NEVER terminates a workflow (no
		// CanRetry gate here — the retry budget is only consumed by real
		// activity failures in complete()). The drop consumes one
		// reschedule backoff at the flat 1s interval regardless of
		// attempt — the model lumps the server's separate reschedule
		// counter (which does escalate) into the flat policy; the
		// tradeoff is acceptable: the transport-drop rate is bounded per
		// re-add, so repeated drops of one task are rare.
		backoff := rescheduleBackoffMs(tk.Attempt)
		rt.shardFor(tk.ID).RescheduleAt(Task{ID: tk.ID, Attempt: tk.Attempt, Class: TaskActivity, BornMs: atMs.Int64(), Depth: tk.Depth, ParentID: tk.ParentID, Rescheduled: true}, atMs.Int64()+backoff)
		// Capture: history → matching — the add was dropped at the
		// matching boundary and the task is rescheduled into history
		// (retryable).
		rt.rec("history", "matching", "act_drop", atMs.Int64(), tk)
		// A transport drop is a NON-ResourceExhausted retryable error: the
		// server resets the executable's resource-exhausted count on any
		// non-RE error (executable.go:464) — clear the task's S2 shed count
		// (its BornMs resets here too, so the count could not carry anyway).
		rt.s2REClear(tk)
		return
	}
	if rt.isForwardingAllowed(atMs) {
		// ROOT path — forwarded to the root's poller fleet (the forward
		// token was acquired inside isForwardingAllowed: the ForwardTask to
		// root). At healthy windows with an open gate this is byte-identical
		// to the pre-split collapsed single queue (the root behaves exactly
		// as the old actQ). Adds are offered to the root's poller fleet.
		if rt.s2AppendFull(rt.actQ) {
			// S2: the root's append channel is at cap — shed the add
			// (ResourceExhausted, task_writer.go:103-121) instead of
			// appending. Release the forward token: the add did not land
			// (the !acc path below recycles on the same principle).
			matchWaveFor(rt).fwd.Recycle()
			rt.s2Reject(tk, atMs)
			return
		}
		if spoolOnly {
			if !rt.actQ.SpoolOnly(tk) {
				matchWaveFor(rt).fwd.Recycle() // release on failure
				rt.rec("history", "matching", "act_reject", atMs.Int64(), tk)
				rt.shardFor(tk.ID).EnqueueTransfer(tk)
				return
			}
			rt.s2REClear(tk)
			rt.rec("history", "matching", "act_spool", atMs.Int64(), tk)
			return
		}
		if ws := matchWaveFor(rt); tk.Rescheduled && ws.forwarded[matchFwdFlagOf(tk)] && rt.actQ.Parked() == 0 {
			// MustOffer exception: a forwarded DB-backlog task meeting a
			// poller-less root blocks to the sync-match deadline via sim
			// wait (offerOrTimeout), not the immediate Offer.
			rt.s2REClear(tk) // the add leaves the boundary into the wait
			rt.offerOrTimeout(tk, atMs)
			return
		}
		sm, acc := rt.actQ.Add(tk, atMs)
		delete(matchWaveFor(rt).forwarded, matchFwdFlagOf(tk))
		rt.s2REClear(tk)
		if sm {
			rt.rec("history", "matching", "act_add", atMs.Int64(), tk)
			return
		}
		if !acc {
			matchWaveFor(rt).fwd.Recycle() // release on failure
			rt.rec("history", "matching", "act_reject", atMs.Int64(), tk)
			rt.shardFor(tk.ID).EnqueueTransfer(tk)
			return
		}
		rt.rec("history", "matching", "act_spool", atMs.Int64(), tk)
		return
	}
	// CHILD path — drain-bound: selected when the child cannot forward to
	// the root (short/no window, closed gate, or forward-token exhaustion —
	// the dynamic isForwardingAllowed verdict) OR — latent, only if a
	// healthy window ever had a non-empty child — the isBacklogNegligible
	// gate. Local sync first: childQ.Add attempts the sync-match fast path
	// whenever it is available and spools otherwise; the child partition has
	// no poller fleet and forwarding dead, so under current wiring this
	// spools — the child-absorb (token exhaustion lands here too). The add
	// accumulates in the CHILD partition's backlog and can only leave via
	// the child's own drain (rt.drainChildToRoot) at the child's structural
	// drain rate (ChildDrainFraction × DispatchRate), or via poll-forwarding
	// (rt.pollForward, one task per tick to a waiting root poller). In THIS
	// cell the CHILD branch is driven by the window/token verdict, not by
	// the gate — the gate is observational here (see matchingService and
	// doc.go). The degradation is reproduced by the child being drain-bound;
	// the child drain magnitude is a calibrated scalar, not a
	// window-conditional multiplier (the retired ForwardDeadRateFactor).
	// Flag forwarded+fromBacklog: a Rescheduled re-add arrives from the
	// history DB backlog; side-car the flag by task identity so it survives
	// the child drain (which preserves ID/Attempt/BornMs) into the root,
	// where the MustOffer exception consults it.
	// S2: the CHILD (write/persistence) partition's append channel is at
	// cap — shed BEFORE landing (and before the forwarded-flag side-car
	// below, so a shed task never arms it; its BornMs resets at the shed, so
	// a stale pre-existing flag under the old born can never re-arm either).
	if rt.s2AppendFull(rt.childQ) {
		rt.s2Reject(tk, atMs)
		return
	}
	// Flag forwarded+fromBacklog: a Rescheduled re-add arrives from the
	// history DB backlog; side-car the flag by task identity so it survives
	// the child drain (which preserves ID/Attempt/BornMs) into the root,
	// where the MustOffer exception consults it.
	if tk.Rescheduled {
		matchWaveFor(rt).forwarded[matchFwdFlagOf(tk)] = true
	}
	if spoolOnly {
		if !rt.childQ.SpoolOnly(tk) {
			rt.rec("history", "matching", "act_reject", atMs.Int64(), tk)
			rt.shardFor(tk.ID).EnqueueTransfer(tk)
			return
		}
		rt.s2REClear(tk)
		rt.rec("history", "matching", "act_child", atMs.Int64(), tk)
		return
	}
	if _, acc := rt.childQ.Add(tk, atMs); !acc {
		rt.rec("history", "matching", "act_reject", atMs.Int64(), tk)
		rt.shardFor(tk.ID).EnqueueTransfer(tk)
		return
	}
	rt.s2REClear(tk)
	rt.rec("history", "matching", "act_child", atMs.Int64(), tk)
}

// ---- S2 append-threshold seam (WP2; ADR-106:2; plan S2 section). The
// model leg is PARTIAL by design (ADR-103:2, pre-registered): it carries
// reject-at-cap + escalation-backoff re-add; it makes NO verdict claim —
// saturation is decided by the C11 G0 gate on the bubble, not here. ----

// s2Key is the identity key of the S2 shed counter: task ID + attempt +
// class. BornMs is deliberately excluded: an S2 shed reschedules the task
// with a fresh BornMs (the drop-path convention — a shed add was never
// scheduled, so its residency/S2S clock restarts), while the escalation
// counter must survive the shed→re-add cycle.
type s2Key struct {
	id      uint64
	attempt int32
	class   TaskClass
}

func s2KeyOf(tk Task) s2Key {
	return s2Key{id: tk.ID, attempt: tk.Attempt, class: tk.Class}
}

// appendSeamState is the per-runtime S2 shed state.
type appendSeamState struct {
	// reCounts holds each task's consecutive ResourceExhausted reject count
	// (1-based) — the server's executableImpl.resourceExhaustedCount, which
	// increments once per ResourceExhausted Nack BEFORE the backoff is
	// computed (executable.go:455-457) and resets on any non-RE retryable
	// error (executable.go:464). The count therefore persists only across
	// consecutive sheds; every non-shed outcome clears it (s2REClear).
	reCounts map[s2Key]int
}

// appendSeams is the per-runtime S2 registry — the same lookup-only
// registry pattern as matchWaves (matching.go): never iterated,
// single-goroutine, zero RNG. Entries are created lazily on the first shed
// (cfg.AppendThreshold > 0) and live process-lifetime (runtimes are never
// reused across runs), so a run with the seam OFF leaves the registry
// empty — zero perturbation.
var appendSeams = map[*runtime]*appendSeamState{}

func s2State(rt *runtime) *appendSeamState {
	if st, ok := appendSeams[rt]; ok {
		return st
	}
	st := &appendSeamState{reCounts: map[s2Key]int{}}
	appendSeams[rt] = st
	return st
}

// s2AppendFull reports whether the add's destination queue is at the append
// cap (S2 reject-at-cap). cfg.AppendThreshold == 0 → always false (off =
// today's exact behavior — the neutrality contract). Zero RNG, pure read.
//
// Partial-leg approximation (documented, not hidden — ADR-103:2): the
// model has no R2 write→read hop for matching appends (ADR-104
// unallocated; the C11 bubble carries the real writer), so the task-writer
// channel occupancy the server bounds (task_writer.go:103-121) is carried
// by the destination partition's resident backlog — tasks appended
// (spooled) but not yet handed to a poller. The gate is a HARD boundary at
// the add RPC: when the channel is full the RPC is refused wholesale — the
// model cannot peek Add's internal sync verdict (parked pollers + dispatch
// token state) before landing, so even a would-be sync-match is shed.
// Over-shed relative to the real server (which sync-matches without the
// writer); bounded by the seam being OFF at the zero default.
func (rt *runtime) s2AppendFull(q *matcher.MatchQueue) bool {
	return rt.cfg.AppendThreshold > 0 && q != nil && q.Len() >= rt.cfg.AppendThreshold
}

// s2Reject sheds an add with ResourceExhausted semantics (S2): the task is
// rescheduled into history at the resource-exhausted ESCALATION backoff and
// re-enters matching via the shard rescheduler (onResched — the same
// Nack→reschedule path the transport drop uses), never the transfer queue.
// The server treats ResourceExhausted as retryable with a LONGER backoff:
// shouldResubmitOnNack (executable.go:749-774) lets it resubmit;
// backoffDuration (executable.go:776-805) picks max(generic policy on
// attempt, taskResourceExhuastedReschedulePolicy on the shed count —
// 3s×1.5 cap 5min, util.go:82-84). Zero RNG; one deterministic future
// reschedule event.
func (rt *runtime) s2Reject(tk Task, atMs core.SimTime) {
	st := s2State(rt)
	k := st.reCounts[s2KeyOf(tk)] + 1
	st.reCounts[s2KeyOf(tk)] = k
	backoff := rescheduleResourceExhaustedMs(tk.Attempt, k)
	rt.shardFor(tk.ID).RescheduleAt(Task{ID: tk.ID, Attempt: tk.Attempt, Class: tk.Class, BornMs: atMs.Int64(), Depth: tk.Depth, ParentID: tk.ParentID, Rescheduled: true}, atMs.Int64()+backoff)
	// Capture: history → matching — the add was shed at the append channel
	// (ResourceExhausted / SYSTEM_OVERLOADED). The _re suffix keeps it
	// distinct from the legacy capacity-reject kind (act_reject/wf_reject —
	// dead today: persistence-backed queues never reject) — the model-side
	// mirror of the bubble's ResourceExhaustedRejects code-split
	// (ADR-103:1).
	kind := "act_reject_re"
	if tk.Class != TaskActivity {
		kind = "wf_reject_re"
	}
	rt.rec("history", "matching", kind, atMs.Int64(), tk)
}

// s2REClear resets the task's shed counter on every outcome that is not
// another shed — an accepted add (the task left the shed cycle; any later
// re-add of the same identity is a fresh executable with count 0) or a
// transport drop (a non-RE error resets the server count,
// executable.go:464). No-op when no shed has happened (no registry entry
// for this runtime).
func (rt *runtime) s2REClear(tk Task) {
	if st, ok := appendSeams[rt]; ok {
		delete(st.reCounts, s2KeyOf(tk))
	}
}

// rescheduleResourceExhaustedMs is the S2 escalation backoff for the k-th
// consecutive ResourceExhausted shed of a task (k >= 1): the max of the
// generic reschedule policy on the task's attempt (rescheduleBackoffMs)
// and the resource-exhausted policy — initial 3s × 1.5^(k-1), cap 5min
// (taskResourceExhaustedRescheduleInitialInterval/Coefficient/MaxInterval,
// common/util.go:82-84) — mirroring backoffDuration's max() selection
// (executable.go:800) with ComputeNextDelay's initial×coeff^(attempt-1)
// (common/backoff/retrypolicy.go:160). Jitter is omitted (model
// convention — rescheduleBackoffMs omits it too; the server's 20% jitter
// is wall-clock randomness, excluded by the determinism contract).
func rescheduleResourceExhaustedMs(attempt int32, k int) int64 {
	const initial = 3000 // ms — taskResourceExhaustedRescheduleInitialInterval
	const coeff = 1.5    // taskResourceExhaustedRescheduleBackoffCoefficient
	const capMs = 300000 // 5min — taskResourceExhaustedRescheduleMaxInterval
	b := rescheduleBackoffMs(attempt)
	if k <= 1 {
		return max(b, initial)
	}
	v := float64(initial) * math.Pow(coeff, float64(k-1))
	if math.IsNaN(v) || math.IsInf(v, 1) || v >= float64(capMs) {
		// The >= capMs pre-check (as in rescheduleBackoffMs) also guards the
		// float→int64 conversion against out-of-range overflow at huge k.
		return max(b, capMs)
	}
	ms := int64(math.Round(v))
	if ms > capMs {
		ms = capMs
	}
	if ms < initial {
		ms = initial
	}
	return max(b, ms)
}
