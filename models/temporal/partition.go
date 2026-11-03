package temporal

import (
	"github.com/lawRathod/queuesim/godes/core"
)

// childAgeWin is one 100ms window of the per-tick gate measurement on the
// CHILD activity partition: the max observed child head-of-backlog age, and
// how many of the window's pump ticks saw the backlog-age gate tripped.
// This is the mechanism-fidelity EVIDENCE that MEASURES whether the
// isBacklogNegligible gate (Mechanism A) trips. The gate is OBSERVATIONAL
// in this model (not a routing input; see matchingService) and does NOT
// latch at the calibrated aps in this cell (head-age < 5s, matching the
// real bubble's tiny backlog) — the window-0 drop is carried by Mechanism B
// + the child's structural drain. Read-only sim-time observation, zero RNG,
// deterministic, and NOT part of the digest (byte-neutral).
type childAgeWin struct {
	EndMs       int64
	Ticks       uint64 // pump ticks in this window
	GateTicks   uint64 // of Ticks, how many had the gate tripped
	MaxHeadAge  int64  // max observed child backlog head age (ms) in the window
	GateTripped bool   // any tick in the window had the gate tripped
}

// observeChildGate samples the child partition's backlog-age gate once per
// pump tick: it updates the run counters and folds the head-of-backlog age
// into the current 100ms window (starting a new window on 100ms boundaries,
// aligned to the run's tick cadence). Called from rt.tick after the routing
// decisions; a pure read of rt.childQ, no RNG, no sim mutation.
func (rt *runtime) observeChildGate(now core.SimTime) {
	nowMs := now.Int64()
	rt.childTicks++
	if rt.childGateMs(nowMs) {
		rt.childGateTicks++
	}
	if rt.childAgeCur.EndMs == 0 || nowMs >= rt.childAgeCur.EndMs {
		if rt.childAgeCur.EndMs != 0 {
			rt.childAgeSeries = append(rt.childAgeSeries, rt.childAgeCur)
		}
		rt.childAgeCur = childAgeWin{EndMs: nowMs/100*100 + 100}
	}
	rt.childAgeCur.Ticks++
	if rt.childGateMs(nowMs) {
		rt.childAgeCur.GateTripped = true
		rt.childAgeCur.GateTicks++
	}
	if age := rt.childHeadAgeMs(nowMs); age > rt.childAgeCur.MaxHeadAge {
		rt.childAgeCur.MaxHeadAge = age
		if age > rt.childAgeMax {
			rt.childAgeMax = age
		}
	}
}

// childGateMs reports whether the child partition's backlog-age gate
// (isBacklogNegligible analog, Mechanism A) is tripped at nowMs: the child
// backlog head has been resident >= BacklogGateMs.
func (rt *runtime) childGateMs(nowMs int64) bool {
	return rt.childQ != nil && rt.childQ.GateTripped(core.SimTime(nowMs))
}

// childHeadAgeMs returns the child backlog head-of-backlog age at nowMs (0
// when the child backlog is empty).
func (rt *runtime) childHeadAgeMs(nowMs int64) int64 {
	if rt.childQ == nil {
		return 0
	}
	return rt.childQ.HeadAge(core.SimTime(nowMs))
}

// drainChildToRoot is the child partition's OWN local drain: it moves
// absorbed tasks from the child backlog toward the ROOT at the child's
// structural drain rate (ChildDrainFraction × DispatchRate). This is the
// drain-bound residual path that keeps work flowing when forwarding is dead.
// When the child is empty (healthy windows, forwarding reliable) it is a
// no-op and draws zero tokens — byte-neutral.
//
// There are TWO drain semantics, selected by ChildReSpoolMs:
//
//   - ChildReSpoolMs = 0 (the default): the immediate-FIFO re-offer — each
//     drained task re-offers to the root on the SPOOL path (never the fast
//     sync-match — a drained child task is drain-bound) immediately at its
//     drain rate. This is the memory-resident FIFO drain whose throughput ×
//     head-age coupling the re-spool round-trip exists to break (it cannot
//     age the head past the gate while also keeping aps high). byte-neutral.
//
//   - ChildReSpoolMs > 0: the CREATE-TIME-PRESERVING PERSISTENCE RE-SPOOL
//     round-trip. A drained task is WRITTEN to persistence and READ BACK
//     (a ChildReSpoolMs hop, offerAfterPersistenceRead); its create-time
//     (BornMs) is preserved, so its age keeps counting while it is in the
//     round-trip and not servable. If, at read-back, the root has no parked
//     poller to serve it at once (the storm-congestion state), the task
//     RE-SPOOLS (another round-trip, age still preserved) rather than
//     queueing. Under sustained load this lets an old backlog head exceed
//     BacklogGateMs even while fresh tasks that catch a free poller are
//     served — the real server's getBacklogAge = min create-time across ALL
//     backlogged tasks (matcher.go:540-542).
func (rt *runtime) drainChildToRoot(now core.SimTime) {
	if rt.childQ == nil || rt.childDrainBucket == nil {
		return
	}
	if rt.cfg.ChildReSpoolMs <= 0 {
		rt.drainChildFIFO(now)
		return
	}
	rt.drainChildReSpool(now)
}

// drainChildFIFO is the byte-neutral default child drain (ChildReSpoolMs=0):
// each drained head re-offers to the root immediately on the spool path,
// exactly the pre-re-spool behavior. A capacity reject re-enters via the
// history transfer queue, exactly like a fresh add.
func (rt *runtime) drainChildFIFO(now core.SimTime) {
	for rt.childDrainBucket.Consume(now.Int64()) {
		tk, ok := rt.childQ.PopHead()
		if !ok {
			return
		}
		rt.offerChildToRoot(tk, now)
	}
}

// drainChildReSpool is the persistence re-spool drain (ChildReSpoolMs>0):
// each drained head enters the write→read round-trip instead of being
// offered immediately. The round-trip preserves create-time, so the task
// ages during persistence — and, because the gate measures the child head
// from create-time (now − BornMs), the age the round-trip accumulates is
// still what the gate sees when the task next reaches the head. Note the
// faithfulness nuance: while a task is IN a round-trip it is not in the
// child backlog (so the measured head excludes it) — a conservative
// UNDERSTATEMENT of the real server's getBacklogAge (which counts ALL
// backlogged tasks including those in the durable write/read pipeline,
// matcher.go:540-542). The bias is conservative (the model under-counts
// age), so a latch the model DOES reach is real; it does not inflate the
// bounded-negative finding.
func (rt *runtime) drainChildReSpool(now core.SimTime) {
	for rt.childDrainBucket.Consume(now.Int64()) {
		tk, ok := rt.childQ.PopHead()
		if !ok {
			return
		}
		rt.offerAfterPersistenceRead(tk, now)
	}
}

// offerAfterPersistenceRead performs one write→read persistence round-trip
// for a drained child task: after ChildReSpoolMs (the durable write+read
// latency), the task is offered to the root. If the root has NO parked
// poller to serve it at that instant (the storm-congestion state — all
// activity pollers busy), the task RE-SPOOLS (another round-trip, its
// create-time preserved) rather than queueing; only when a root poller is
// parked is the task actually offered. This is the re-spool semantic that
// lets an old head age (now − BornMs) past the gate while the drain serves
// fresh tasks: throughput decouples from head-age.
//
// Determinism + termination: each re-spool schedules a FUTURE event
// (now + ChildReSpoolMs) and returns — no synchronous recursion. Because
// this path only runs with ChildReSpoolMs > 0, every re-spool advances sim
// time by ≥ ChildReSpoolMs, so the run horizon (SimEndMs) bounds the chain
// to ≤ SimEndMs/ChildReSpoolMs re-spools per task even under full poller
// starvation (no livelock). In practice pollers re-park after each
// execution, so read-back finds a parked poller far sooner once the load
// subsides. All events fire in seeded single-goroutine sim order.
func (rt *runtime) offerAfterPersistenceRead(tk Task, writtenAt core.SimTime) {
	rt.sim.ScheduleAt(writtenAt.Add(rt.cfg.ChildReSpoolMs), func(*core.Simulation) {
		now := rt.sim.Now()
		if rt.actQ.Parked() == 0 {
			// No root poller free at read-back → re-spool: another
			// write→read round-trip, create-time preserved (BornMs intact),
			// so the task's age keeps growing. Record the re-spool for the
			// gate-latch evidence.
			rt.childReSpoolCount++
			rt.rec("matching", "matching", "act_respool", now.Int64(), tk)
			rt.offerAfterPersistenceRead(tk, now)
			return
		}
		rt.offerChildToRoot(tk, now)
	})
}

// offerChildToRoot offers a drained child task to the ROOT partition on the
// spool path (never the fast sync-match — a drained child task is
// drain-bound). A capacity reject re-enters via the history transfer queue,
// exactly like a fresh add.
func (rt *runtime) offerChildToRoot(tk Task, now core.SimTime) {
	if !rt.actQ.SpoolOnly(tk) {
		rt.rec("matching", "matching", "act_root_reject", now.Int64(), tk)
		rt.shardFor(tk.ID).EnqueueTransfer(tk)
		return
	}
	rt.rec("matching", "matching", "act_forward_spool", now.Int64(), tk)
}

// ChildGateEvidence is the run-level summary of the child partition's
// backlog-age gate measure, consumed by the syncmatch_sweep scenario to
// RECORD whether the gate trips per cell (observational; in this calibrated
// cell it does not latch — see the scenario doc.go). All fields are
// deterministic observations of the simulation (zero RNG, zero mutation) —
// reading them never perturbs the trace or digest.
type ChildGateEvidence struct {
	// Ticks is the number of pump ticks the gate was sampled.
	Ticks uint64
	// GateTicks is how many of those ticks had the gate tripped.
	GateTicks uint64
	// MaxHeadAgeMs is the max child head-of-backlog age observed (ms).
	MaxHeadAgeMs int64
	// ReSpools is the number of persistence re-spool round-trips the child
	// drain performed (ChildReSpoolMs > 0; 0 when disabled). The create-time-
	// preserving re-spool (write→read) is what lets the backlog head age past
	// the gate under sustained load — counting it is the mechanism-fidelity
	// evidence that the round-trip actually fired.
	ReSpools uint64
	// Windows is the per-100ms series of gate-tripped/head-age.
	Windows []childAgeWin
}

// ChildGate returns the run's measured child backlog-age gate evidence.
func (rt *runtime) ChildGate() ChildGateEvidence {
	w := make([]childAgeWin, 0, len(rt.childAgeSeries)+1)
	w = append(w, rt.childAgeSeries...)
	if rt.childAgeCur.EndMs != 0 {
		w = append(w, rt.childAgeCur)
	}
	return ChildGateEvidence{
		Ticks:        rt.childTicks,
		GateTicks:    rt.childGateTicks,
		MaxHeadAgeMs: rt.childAgeMax,
		ReSpools:     rt.childReSpoolCount,
		Windows:      w,
	}
}

// childDrainRate computes the child partition's local drain rate (tasks/s)
// from its structural fraction of DispatchRate. Non-positive DispatchRate
// (unlimited) → unlimited child drain (never a bottleneck).
func childDrainRate(dispatch, frac float64) float64 {
	if dispatch <= 0 {
		return 0 // unlimited
	}
	if frac <= 0 || frac > 1 {
		return dispatch
	}
	return dispatch * frac
}
