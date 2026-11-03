package temporal

import (
	"github.com/lawRathod/queuesim/godes/core"
	"github.com/lawRathod/queuesim/model"
	"github.com/lawRathod/queuesim/models/temporal/components/history"
)

// HistoryService returns the history Service: the range-owned shards that
// persist every mutation, run the transfer/timer processors and the
// rescheduler, own the sticky queues, and execute the workflow/activity
// lifecycle (runWF/runAct/complete). The real mechanism (the shards and the
// sticky queue) is constructed directly on the shared runtime's simulation
// below and driven by the runtime tick — history declares NO ports and wires
// NO builder queues: a port shell would render in the DOT schematic without
// a single task traversing it (the honest-schematics principle: if the code
// doesn't traverse it, the diagram doesn't draw it).
func historyService(rt *runtime) model.Service {
	return model.Service{
		Name:  "history",
		Kind:  "temporal.history",
		Label: "shards, sticky, transfer/timer, retry",
		Build: func(b *model.TopologyBuilder) error {
			// S4 persistence rate limiter (ADR-106 WP4): the real history
			// service builds ONE per-service persistence limiter, constructed
			// only when history.persistenceMaxQPS > 0
			// (temporal/common/persistence/client/fx.go:117-138; wired from
			// service/history/fx.go:293). NewPersistenceBucket is the model's
			// rate-limiter CONSTRUCTION call site: it derives the burst
			// (maxQPS × PersistenceQPSBurstRatio) in CODE — the ADR-106:3
			// CODE-not-comment pin (ratio default 1.0,
			// common/dynamicconfig/constants.go:251). The bucket is SHARED by
			// every shard: the server's ceiling is service-wide, not
			// per-shard.
			var pLim *history.PersistenceBucket
			if rt.cfg.PersistenceMaxQPS > 0 {
				pLim = history.NewPersistenceBucket(rt.cfg.PersistenceMaxQPS, rt.cfg.PersistenceQPSBurstRatio)
			}
			// History shards (range-owned; task → shard = ID % Shards).
			for i := 0; i < rt.cfg.Shards; i++ {
				sh := history.NewHistoryShard(rt.sim, i,
					rt.cfg.PersistMs,
					int64(rt.cfg.TransferBatch), int64(rt.cfg.TransferWorkers),
					int64(rt.cfg.TimerBatch), int64(rt.cfg.TimerWorkers),
					rt.cfg.TimerMaxShiftMs, rt.cfg.ReschedulerBackoffMs,
				)
				if pLim != nil {
					sh.SetPersistenceBucket(pLim)
				}
				rt.shards = append(rt.shards, sh)
			}
			rt.sticky = history.NewStickyQueue(rt.sim, rt.cfg.StickyCacheMs, rt.cfg.StickyFallbackMs)
			return nil
		},
	}
}

// startWorkflow enters a new workflow. FAITHFUL to real Temporal: the
// start is the StartWorkflow RPC entry — history PERSISTS the workflow
// record and schedules a transfer task; the transfer-queue executor then
// calls AddWorkflowTask into matching-wf.
func (rt *runtime) startWorkflow(tk Task, atMs core.SimTime) {
	// Capture: load → history (the StartWorkflow RPC entry) — T1.
	rt.rec("load", "history", "workflow_start", atMs.Int64(), tk)
	sh := rt.shardFor(tk.ID)
	if rt.wfTimerMs >= 0 {
		sh.AddTimer(Task{ID: tk.ID, Attempt: tk.Attempt, Class: TaskWfStart, BornMs: atMs.Int64()}, atMs.Int64()+rt.wfTimerMs)
		return
	}
	land := sh.PersistScaled(atMs, rt.persistMult)
	rt.sim.ScheduleAt(land, func(*core.Simulation) {
		sh.EnqueueTransfer(Task{ID: tk.ID, Attempt: tk.Attempt, Class: TaskWfStart, BornMs: rt.sim.Now().Int64()})
	})
}

// onTake handles a task handed to a worker (sync match or dispatch):
// classify and execute. For an activity task it records the
// schedule-to-start latency sample (take time − BornMs — the same clock
// the model's S2S expiry uses) into the automatic S2S histogram collector
// when monitoring is enabled; pure reads, zero RNG, sim-neutral.
func (rt *runtime) onTake(tk Task) {
	now := rt.sim.Now()
	if tk.Class == TaskActivity && rt.s2s != nil {
		rt.s2s.samples = append(rt.s2s.samples, now.Int64()-tk.BornMs)
	}
	switch tk.Class {
	case TaskWfStart, TaskWfComplete:
		// Capture: matching → exec — a workflow task was dispatched to a
		// poller (sync-match or backlog dispatch; the shard executes the
		// workflow code) — T7.
		rt.rec("matching", "exec", "wf_dispatch", now.Int64(), tk)
		rt.runWF(tk, now)
	case TaskActivity:
		// Capture: matching → exec — an activity task was dispatched to a
		// poller (sync-match or backlog dispatch) — T7.
		rt.rec("matching", "exec", "act_dispatch", now.Int64(), tk)
		rt.runAct(tk, now)
	default:
		panic("temporal: unexpected task class in onTake")
	}
}

// runWF executes a workflow task via the owning history shard: the shard
// persists the wf-task mutation, then the workflow code runs. For a
// TaskWfStart this schedules the activity (AddActivityTask); for a
// TaskWfComplete it finishes the workflow (Completed++).
//
// NOTE: workflow tasks carry no history-side schedule-to-close check — the
// wf-task deadline stays with the matching-wf queue guard
// (WorkflowTaskTimeoutMs); history only executes and routes them.
func (rt *runtime) runWF(tk Task, now core.SimTime) {
	sh := rt.shardFor(tk.ID)
	land := sh.PersistScaled(now, rt.persistMult)
	rt.sim.ScheduleAt(land, func(*core.Simulation) {
		switch tk.Class {
		case TaskWfStart:
			born := rt.sim.Now().Int64()
			// Capture: exec → history — the workflow code ran (the
			// TaskWfStart execution finished) and now schedules its first
			// activity.
			rt.rec("exec", "history", "wf_task_done", born, tk)
			rt.addAct(Task{ID: tk.ID, Attempt: 1, Class: TaskActivity, BornMs: born}, rt.sim.Now().Add(rt.cfg.NetworkMs))
		case TaskWfComplete:
			nowMs := rt.sim.Now().Int64()
			rt.stats.Completed++
			rt.monitor.ObserveComplete(nowMs, 0, true)
			// Capture: exec → terminal — the workflow code ran its final
			// task and the workflow completed.
			rt.rec("exec", "terminal", "wf_complete", nowMs, tk)
		}
		rt.parkOne(rt.wfPool, rt.wfQ, rt.sim.Now().Int64())
	})
}

// runAct executes an activity via the owning shard: the shard persists the
// activity start, the worker pool schedules the execution (service time),
// and the completion routes success/failure.
func (rt *runtime) runAct(tk Task, now core.SimTime) {
	rt.stats.ActivityAttempts++
	sh := rt.shardFor(tk.ID)
	land := sh.PersistScaled(now, rt.persistMult)
	rt.sim.ScheduleAt(land, func(*core.Simulation) {
		now := rt.sim.Now()
		at := rt.actPool.Start(tk, now)
		// Capture: exec → exec — the worker pool executes the activity code
		// (service time); complete() fires at `at`.
		rt.rec("exec", "exec", "act_exec", now.Int64(), tk)
		rt.sim.ScheduleAt(at, func(*core.Simulation) { rt.complete(tk) })
	})
}

// complete routes one activity execution outcome at its service-completion
// time. The failure taxonomy is split (the server's retry policy,
// service/history/workflow/retry.go:115-133): an activity-EXECUTION failure
// (the fault-window fail, or a heartbeat timeout) is retryable — it may
// re-enter via the retry timer while the attempt cap allows and the retry
// would still fire inside the schedule-to-close deadline. A
// SCHEDULE-TO-CLOSE expiry is terminal — the S2C deadline bounds the whole
// attempt chain, so the S2C path records Failed++ with no CanRetry consult
// and no Backoff draw. The S2C clock anchors at the FIRST schedule (the
// BornMs lineage, preserved across retries below), not at the latest
// execution.
func (rt *runtime) complete(tk Task) {
	now := rt.sim.Now()
	nowMs := now.Int64()
	rt.stats.AttemptsExecuted++
	s2c := rt.cfg.ScheduleToCloseMs
	if rt.s2cMs > 0 {
		s2c = rt.s2cMs
	}
	if s2c > 0 && nowMs > core.NewSimTime(tk.BornMs).Add(s2c).Int64() {
		rt.stats.Failed++
		// Capture: exec → terminal (activity past schedule-to-close) —
		// T6 fail outcome.
		rt.rec("exec", "terminal", "act_fail", nowMs, tk)
		return
	}
	failed := rt.ActivityFailsFor(tk.ID, nowMs)
	if !failed && rt.cfg.HeartbeatTimeoutMs > 0 && rt.cfg.ActivityMs > rt.cfg.HeartbeatTimeoutMs {
		// Heartbeat timeout is a service-time bound only (no cadence
		// modeling): an execution longer than the heartbeat timeout
		// fails as a retryable activity failure.
		failed = true
	}
	if failed {
		if rt.retry.CanRetry(tk.Attempt) {
			backoff := rt.retry.BackoffMs(tk.Attempt)
			// Deadline gate: a retry that would fire past the S2C
			// deadline (when set) never re-enters — terminal instead.
			var expirationMs int64 // 0 = no deadline (gate open)
			if s2c > 0 {
				expirationMs = core.NewSimTime(tk.BornMs).Add(s2c).Int64()
			}
			if model.ExceedsDeadline(nowMs, backoff, expirationMs) {
				rt.stats.Failed++
				// Capture: exec → terminal (retry past schedule-to-close) —
				// T6 fail outcome.
				rt.rec("exec", "terminal", "act_fail", nowMs, tk)
				return
			}
			rt.stats.Retries++
			rt.monitor.ObserveRetry(nowMs)
			tk.Attempt++
			sh := rt.shardFor(tk.ID)
			// Each retry creates a NEW ActivityTask with a fresh schedule-to-close
			// window (per-attempt S2C — service/history/workflow/retry.go; the
			// timer task is borne at execution time). Do NOT preserve BornMs here:
			// a preserved create-time would age the matching backlog head past the
			// isBacklogNegligible gate (5s) and latch the child. nowMs is the
			// per-attempt birth.
			sh.AddTimer(Task{ID: tk.ID, Attempt: tk.Attempt, Class: TaskActivity, BornMs: nowMs, Depth: tk.Depth, ParentID: tk.ParentID}, now.Add(backoff).Int64())
			// Capture: exec → history (activity failed and is retryable —
			// the retry timer re-enters matching-act) — T6.
			rt.rec("exec", "history", "act_retry", nowMs, tk)
			return
		}
		rt.stats.Failed++
		// Capture: exec → terminal (activity failed past its retry cap) —
		// T6 fail outcome.
		rt.rec("exec", "terminal", "act_fail", nowMs, tk)
		return
	}
	if rt.cfg.FanoutK > 0 {
		if tk.Depth == 0 {
			rt.fanout[tk.ID] = rt.cfg.FanoutK
			for i := 0; i < rt.cfg.FanoutK; i++ {
				rt.addActSpoolOnly(Task{ID: rt.nextID, Attempt: 1, Class: TaskActivity, BornMs: nowMs, Depth: 1, ParentID: tk.ID}, now.Add(rt.cfg.NetworkMs))
				rt.nextID++
			}
			return
		}
		pid := tk.ParentID
		remaining := rt.fanout[pid] - 1
		if remaining > 0 {
			rt.fanout[pid] = remaining
			return
		}
		delete(rt.fanout, pid)
		rt.stats.Completed++
		rt.monitor.ObserveComplete(nowMs, 0, true)
		return
	}
	sh := rt.shardFor(tk.ID)
	land := sh.PersistScaled(now, rt.persistMult)
	rt.sim.ScheduleAt(land, func(*core.Simulation) {
		nowMs := rt.sim.Now().Int64()
		// Capture: exec → history (activity succeeded — a TaskWfComplete is
		// scheduled back into matching-wf or a sticky queue) — T6 success.
		rt.rec("exec", "history", "act_ok", nowMs, tk)
		if rt.cfg.StickyEnabled {
			if ok, fb := rt.sticky.Stick(rt.workerFor(tk.ID), Task{ID: tk.ID, Attempt: tk.Attempt, Class: TaskWfComplete, BornMs: nowMs}, nowMs); !ok {
				rt.sim.ScheduleIn(fb, func(*core.Simulation) {
					rt.addWf(Task{ID: tk.ID, Attempt: tk.Attempt, Class: TaskWfComplete, BornMs: rt.sim.Now().Int64()}, rt.sim.Now().Add(rt.cfg.NetworkMs))
				})
				return
			}
			return
		}
		rt.addWf(Task{ID: tk.ID, Attempt: tk.Attempt, Class: TaskWfComplete, BornMs: nowMs}, rt.sim.Now().Add(rt.cfg.NetworkMs))
	})
}

// workerFor maps a workflow to its sticky worker (deterministic hash).
func (rt *runtime) workerFor(id uint64) uint64 { return id % uint64(rt.cfg.PollersWF) }

// shardFor returns the shard owning workflow id (range-owned hash).
func (rt *runtime) shardFor(id uint64) *history.HistoryShard {
	return rt.shards[id%uint64(len(rt.shards))]
}
